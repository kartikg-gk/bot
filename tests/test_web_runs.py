import asyncio
import json

from delta_gateway.store import Store
from delta_gateway.web import WEB_CHAT, WEB_IDS
from delta_gateway.web_runs import RunJournal
from tests.test_gateway import config, settle
from tests.test_web import web as web


def frames(response):
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/event-stream")
    return [
        (int(record["id"]), record["event"], json.loads(record["data"]))
        for chunk in response.text.split("\n\n")
        if chunk.startswith("id: ")
        for record in [dict(line.split(": ", 1) for line in chunk.splitlines())]
    ]


async def submit(client, text, **options):
    result = await client.post("/api/runs", json={"text": text, **options})
    assert result.status_code == 202, result.text
    assert result.headers["location"].endswith(result.json()["id"])
    return result.json()["id"]


async def wait_for_approval(client, run):
    async with asyncio.timeout(8):
        while True:
            response = await client.get("/api/approvals", params={"run": run})
            cards = response.json()
            if cards:
                return cards[0]
            await asyncio.sleep(0.01)


async def test_json_result_and_typed_replay(web):
    service, client, _ = web
    assert (await client.post("/api/runs", json={"text": "/stop"})).status_code == 400
    run = await submit(client, "usage")
    await settle(service)
    result = (await client.get(f"/api/runs/{run}")).json()
    assert result["status"] == "completed" and "turn 1: done" in result["answer"]
    assert result["finished"] is not None
    assert result["usage"]["cache_read"] == 3 and result["usage"]["cost_usd"] == 0.004
    assert (await client.get("/api/runs?recent=1")).json()[0] == result
    event_url = f"/api/runs/{run}/events"
    replay = frames(await client.get(event_url))
    assert [kind for _, kind, _ in replay] == [
        "run_queued",
        "run_started",
        "assistant_text",
        "run_usage",
        "run_finished",
    ]
    assert all(event["run"] == run and event["type"] == kind for _, kind, event in replay)
    assert replay[-1][2]["answer"] == result["answer"]
    resumed = frames(await client.get(event_url, headers={"Last-Event-ID": str(replay[1][0])}))
    assert resumed == replay[2:]
    assert (await client.get(event_url, params={"after": "invalid"})).status_code == 400


async def test_direct_stream_can_target_unselected_bot(web):
    service, client, _ = web
    bot = service.store.create_bot("Writer", "Writes notes")
    response = await client.post(
        "/api/runs",
        headers={"Accept": "text/event-stream"},
        json={"text": "hello", "bot": bot.id},
    )
    records = frames(response)
    assert records[-1][2]["bot"] == bot.id
    assert records[-1][2]["run"] == response.headers["x-run-id"]
    assert not (await client.get("/api/messages")).json()
    assert (await client.get(f"/api/bots/{bot.id}/messages")).json()[-1]["role"] == "assistant"
    assert service.store.conversation(WEB_CHAT, service.config.users[0]).bot != bot.id


async def test_stop_preserves_other_bots_and_queued_messages_and_reload_recovers_review(web):
    service, client, _ = web
    worker = service.store.create_bot("Reader", "Reads docs")
    slow = await submit(client, "slow", bot=worker.id)
    first = await submit(client, "write")
    review = await wait_for_approval(client, first)
    discard = await submit(client, "remove only this queued run")
    keep = await submit(client, "keep this queued run")
    assert (await client.get(f"/api/runs/{discard}")).json()["status"] == "queued"
    assert (await client.post(f"/api/bots/unknown/runs/{slow}/stop")).status_code == 404
    stopped = await client.post(f"/api/bots/{worker.id}/runs/{slow}/stop")
    assert stopped.status_code == 200 and stopped.json()["status"] == "cancelled"
    assert (await client.post(f"/api/runs/{discard}/stop")).json()["status"] == "cancelled"
    assert (await client.get("/api/approvals")).json() == [review]
    assert review["tool"] == "Write" and json.loads(review["preview"])["file_path"]
    active = {run["id"] for run in (await client.get("/api/runs")).json()}
    assert active == {first, keep}
    decision = {"data": f"a:{review['id']}", "allow": True}
    assert (await client.post("/api/approvals", json=decision)).status_code == 200
    assert (await client.post("/api/approvals", json=decision)).status_code == 404
    await settle(service)
    assert not (await client.get("/api/approvals")).json()
    assert (await client.get(f"/api/runs/{keep}")).json()["status"] == "completed"
    assert (service.config.project / "approved.txt").exists()
    records = frames(await client.get(f"/api/runs/{first}/events"))
    assert {"approval_requested", "approval_resolved", "tool_started", "tool_finished"} <= {
        kind for _, kind, _ in records
    }
    assert frames(await client.get(f"/api/runs/{slow}/events"))[-1][2]["status"] == "cancelled"


def test_restart_retains_replay_and_does_not_requeue_inflight_run(tmp_path):
    c = config(tmp_path)
    store = Store(c.database_path, c.instance)
    journal = RunJournal(store)
    route = journal.route(WEB_CHAT, 11, None)
    run, inbox = journal.queue(route, "inflight", WEB_IDS)
    pending, _ = journal.queue(route, "queued", WEB_IDS)
    store.mark(inbox, "running")
    journal.emit(run, "run_started", bot=route.bot)
    store.close()
    store = Store(c.database_path, c.instance)
    try:
        journal = RunJournal(store)
        assert journal.get(run, 99) is None
        assert journal.get(run, 11)["status"] == "uncertain"
        assert [row["id"] for row in journal.active(11)] == [pending]
        assert [kind for _, kind, _ in journal.events(run, 0)] == [
            "run_queued",
            "run_started",
            "run_finished",
        ]
    finally:
        store.close()


async def test_stream_reports_gap_if_replay_was_trimmed(web, monkeypatch):
    service, client, _ = web
    monkeypatch.setattr("delta_gateway.web_runs.REPLAY_LIMIT", 3)
    run = await submit(client, "hello")
    await settle(service)
    replay = await client.get(f"/api/runs/{run}/events")
    assert "event: replay_gap" in replay.text
    records = frames(replay)
    assert [seq for seq, _, _ in records] == [3, 4, 5]
    assert records[-1][1] == "run_finished"
    assert (await client.post(f"/api/runs/{run}/stop")).status_code == 409


async def test_stop_retires_approval_before_the_tool_can_run(web):
    service, client, _ = web
    run = await submit(client, "write")
    review = await wait_for_approval(client, run)
    response = await client.post(f"/api/runs/{run}/stop")
    assert response.status_code == 200 and response.json()["status"] == "cancelled"
    assert (await client.get("/api/approvals")).json() == []
    decision = {"data": f"a:{review['id']}", "allow": True}
    assert (await client.post("/api/approvals", json=decision)).status_code == 404
    assert not (service.config.project / "approved.txt").exists()
    records = frames(await client.get(f"/api/runs/{run}/events"))
    assert any(kind == "approval_resolved" and not data["allowed"] for _, kind, data in records)
    assert records[-1][1] == "run_finished"
