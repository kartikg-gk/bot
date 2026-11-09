import asyncio
import time

from delta_gateway import wakeups
from delta_gateway.schedule import parse_when
from delta_gateway.store import Store
from tests.test_gateway import config
from tests.test_web import web as web


async def test_event_endpoint_records_delivery_and_honors_master_switch(web):
    service, client, _ = web
    bot = service.store.bots()[0]
    create = await client.post(
        "/api/jobs", json={"bot": bot.id, "when": "on build.done", "task": "hello"}
    )
    assert create.status_code == 201, create.text
    job = create.json()["id"]
    event = {"event": "build.done", "delivery": "build:42"}
    switch = "/api/jobs/automation"
    assert (await client.put(switch, json={"enabled": False})).status_code == 200
    assert (await client.post("/api/jobs/events", json=event)).status_code == 409
    assert not (await client.get("/api/jobs")).json()["automation"]
    assert (await client.put(switch, json={"enabled": True})).status_code == 200
    result = await client.post("/api/jobs/events", json=event)
    assert result.status_code == 202 and result.json() == {"accepted": 1, "duplicate": False}
    assert (await client.post("/api/jobs/events", json=event)).json() == {
        "accepted": 0,
        "duplicate": True,
    }
    async with asyncio.timeout(5):
        while service.job_tasks:
            await asyncio.sleep(0.01)
    details = (await client.get(f"/api/jobs/{job}")).json()
    assert details["status"] == "active" and details["last_status"] == "completed"
    assert len(details["wakeups"]) == 1
    assert details["wakeups"][0]["delivery"] == "build:42"
    assert details["wakeups"][0]["status"] == "completed"


def test_event_claims_are_actor_scoped_and_interrupted_work_is_not_replayed(tmp_path):
    c = config(tmp_path)
    store = Store(c.database_path, c.instance)
    now = time.time()
    bot = store.bots()[0]
    spec, due = parse_when("on build.done")
    job = store.add_job(11, 11, 11, 0, bot.id, "hello", spec, due)
    store.add_job(22, 22, 22, 0, bot.id, "hello", spec, due)
    timed = store.add_job(11, 11, 11, 0, bot.id, "timer", "once", now)
    assert wakeups.receive(store.db, c.instance, 11, "build.done", "one")["accepted"] == 1
    assert wakeups.receive(store.db, c.instance, 11, "build.done", "two")["accepted"] == 1
    wakeups.set_enabled(store.db, c.instance, False)
    assert not store.claim_due(now)
    store.close()
    store = Store(c.database_path, c.instance)
    assert not wakeups.enabled(store.db, c.instance)
    wakeups.set_enabled(store.db, c.instance, True)
    assert {row.id for row in store.claim_due(now)} == {job, timed}
    store.close()
    store = Store(c.database_path, c.instance)
    try:
        assert [row.id for row in store.claim_due(now + 1)] == [job]
        assert [
            (row["delivery"], row["status"]) for row in wakeups.history(store.db, c.instance, job)
        ] == [("two", "running"), ("one", "interrupted")]
    finally:
        store.close()
