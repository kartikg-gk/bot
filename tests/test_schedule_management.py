import asyncio
import time

import pytest

from delta_gateway.store import MAX_ACTIVE_JOBS, Store
from tests.test_web import web as web


def test_scheduler_renewal_and_takeover_fence_old_completions(tmp_path):
    now = time.time()
    original = Store(tmp_path, "a" * 32)
    other = None
    try:
        bot = original.bots()[0].id
        repeat = original.add_job(11, 11, 11, 0, bot, "repeat", "every:60", 0)
        once = original.add_job(11, 11, 11, 0, bot, "once", "once", 0)
        assert original.scheduler_heartbeat("first", now, 10)
        assert {j.id for j in original.claim_due(now, owner="first")} == {repeat, once}
        other = Store(tmp_path, "a" * 32)
        assert other.job(once).running
        assert not other.scheduler_heartbeat("second", now + 5, 10)
        assert original.scheduler_heartbeat("first", now + 6, 10)
        assert not other.scheduler_heartbeat("second", now + 12, 10)
        assert other.scheduler_heartbeat("second", now + 17, 100)
        assert other.job(once).last_status == "interrupted"
        assert other.claim_due(now + 17, owner="second") == []
        assert original.claim_due(now + 61, owner="first") == []
        assert [j.id for j in other.claim_due(now + 61, owner="second")] == [repeat]
        original.finish_job(repeat, "completed", "stale", claimed_at=now)
        assert other.job(repeat).running and other.job(repeat).last_answer is None
        other.finish_job(repeat, "completed", "current", claimed_at=now + 61)
        assert other.job(repeat).last_answer == "current"
    finally:
        if other:
            other.close()
        original.close()


def test_pause_preserves_due_time_and_quota_is_per_person(tmp_path):
    store = Store(tmp_path, "a" * 32)
    try:
        bot = store.bots()[0].id
        first = store.add_job(11, 11, 11, 0, bot, "pause", "every:60", 1000, "skip")
        store.set_job_paused(first, 11, True)
        assert store.claim_due(1500) == []
        assert store.job(first).next_run == 1000
        store.set_job_paused(first, 11, False)
        assert store.claim_due(1500) == []
        assert store.job(first).last_status == "missed"
        store.set_job_paused(first, 11, True)
        for _ in range(MAX_ACTIVE_JOBS - 1):
            store.add_job(11, 11, 11, 0, bot, "other", "every:60", 2000)
        with pytest.raises(ValueError, match="Each person"):
            store.add_job(11, 11, 11, 0, bot, "overflow", "once", 3000)
        store.add_job(22, 22, 22, 0, bot, "separate quota", "once", 3000)
        assert len(store.jobs(actor=22)) == 1
    finally:
        store.close()


async def test_specialist_can_schedule_itself_but_cannot_manage_another_persons_job(web):
    service, _, _ = web
    worker = service.store.create_bot("Reader", "Read documents")
    other = service.store.create_bot("Writer", "Write documents")
    route = service.store.conversation(11, 11)
    args = dict(when="every 1h", task="Read documents")
    await service.team.run("schedule_job", args, route, actor=11, bot=worker)
    job = service.store.jobs(actor=11)[0]
    assert job.bot == worker.id
    with pytest.raises(ValueError, match="only schedule itself"):
        await service.team.run("schedule_job", {**args, "bot_name": other.name}, route, bot=worker)
    with pytest.raises(ValueError, match="No cancellable job"):
        await service.team.run(
            "cancel_scheduled_job", {"job_id": job.id}, route, actor=22, bot=worker
        )
    assert service.store.job(job.id).status == "active"


async def test_web_job_management_and_result_persistence(web):
    service, client, _ = web
    chief = service.store.bots()[0]
    hidden = service.store.add_job(22, 22, 22, 0, chief.id, "other actor", "once", 9e9)
    assert (await client.get(f"/api/jobs/{hidden}")).status_code == 404
    assert (await client.post(f"/api/jobs/{hidden}/cancel")).status_code == 404
    created = await client.post(
        "/api/jobs",
        json={
            "bot": chief.id,
            "task": "original",
            "when": "every 1h",
            "missed": "run",
        },
    )
    assert created.status_code == 201
    path = "/api/jobs/" + str(created.json()["id"])
    assert (await client.patch(path, json={"task": "updated"})).json()["task"] == "updated"
    assert (await client.post(path + "/pause")).json()["status"] == "paused"
    with service.store.db:
        service.store.db.execute("UPDATE jobs SET next_run=0 WHERE id=?", (created.json()["id"],))
    service.run_due()
    assert not service.job_tasks
    assert (await client.post(path + "/resume")).json()["status"] == "active"
    service.run_due()
    await asyncio.wait_for(asyncio.gather(*service.job_tasks.values()), 10)
    job = (await client.get(path)).json()
    assert job["last_status"] == "completed" and "done" in job["last_answer"]
    listing = (await client.get("/api/jobs")).json()
    assert [row["id"] for row in listing["jobs"]] == [job["id"]]
    assert listing["scheduler"]["expires"] > time.time()
    assert (await client.post(path + "/cancel")).json()["status"] == "cancelled"
