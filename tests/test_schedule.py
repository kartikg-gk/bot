import asyncio
import time

import pytest

from delta_gateway.schedule import next_run, parse_when
from delta_gateway.service import Service
from delta_gateway.store import Store
from tests.test_gateway import Bot, config, settle, update


def test_parse_when():
    assert parse_when("every 30m", 1000) == ("every:1800", 2800)
    spec, first = parse_when("daily 08:00", 1_800_000_000)
    assert spec == "daily:08:00" and first > 1_800_000_000
    assert parse_when("at 2099-01-01T00:00+00:00", 1000)[0] == "once"
    assert next_run("once", 1000) is None
    for bad in ("every 0m", "soon", "daily 25:00", "at 2000-01-01T00:00+00:00", "at nope"):
        with pytest.raises(ValueError):
            parse_when(bad)


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path / "state", "a" * 32)
    yield result
    result.close()


def add(store, spec="every:60", at=0, missed="run"):
    return store.add_job(11, 11, 11, 0, store.bots()[0].id, "task", spec, at, missed)


def test_claim_advances_before_running_and_prevents_overlap(store):
    job = add(store)
    claimed = store.claim_due(1000)
    assert [c.id for c in claimed] == [job]
    assert claimed[0].next_run == 1060
    assert store.claim_due(2000) == []
    store.finish_job(job, "completed")
    assert [c.id for c in store.claim_due(2000)] == [job]


def test_once_job_finishes_and_restart_never_replays(tmp_path, store):
    job = add(store, "once")
    assert store.claim_due(1000)
    store.close()
    reopened = Store(tmp_path / "state", "a" * 32)
    try:
        assert reopened.claim_due(5000) == []
        row = reopened.job(job)
        assert row.status == "done" and row.last_status == "interrupted"
    finally:
        reopened.close()


def test_missed_policy_skip_moves_on_without_running(store):
    job = add(store, missed="skip")
    assert store.claim_due(1000) == []
    assert store.job(job).next_run == 1060
    assert store.job(job).last_status == "missed"


def test_active_job_limit(store):
    for _ in range(50):
        add(store)
    with pytest.raises(ValueError):
        add(store)


async def wait_jobs(service):
    await asyncio.wait_for(asyncio.gather(*service.job_tasks.values()), 60)


async def command(service, c, identifier, text):
    service.store.admit([update(identifier, text)], c)
    await service.dispatch()
    await settle(service)


async def test_due_job_runs_as_its_bot_and_reports_to_chat(tmp_path):
    c = config(tmp_path, policy="auto")
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    try:
        worker = service.store.create_bot("Researcher", "Finds things")
        await command(service, c, 1, "/schedule every 1h | Researcher | check things")
        rows = service.store.jobs()
        assert rows[0].bot == worker.id and rows[0].next_run > time.time()
        with service.store.db:
            service.store.db.execute("UPDATE jobs SET next_run=0")
        service.run_due()
        await wait_jobs(service)
        row = service.store.job(rows[0].id)
        assert row.last_status == "completed" and row.next_run > time.time()
        assert any("Researcher" in text and "done" in text for _, text, _ in telegram.sent)
        route = service.store.conversation(11, 11)
        assert route.session is None
    finally:
        await service.close()


async def test_commands_list_and_cancel(tmp_path):
    c = config(tmp_path)
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    try:
        await command(service, c, 1, "/schedule daily 08:00 | check things")
        job = service.store.jobs()[0]
        await command(service, c, 2, "/jobs")
        assert any(f"#{job.id}" in text for _, text, _ in telegram.sent)
        await command(service, c, 3, f"/unschedule {job.id}")
        assert service.store.job(job.id).status == "cancelled"
        await command(service, c, 4, "/schedule whenever | x")
        assert any("every 30m" in text for _, text, _ in telegram.sent)
    finally:
        await service.close()


async def test_unattended_job_refuses_a_write_without_opening_approval(tmp_path):
    c = config(tmp_path, policy="ask")
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        worker = service.store.create_bot("Researcher", "Finds things")
        service.store.add_job(11, 11, 11, 0, worker.id, "write", "once", 0, "run")
        service.run_due()
        async with asyncio.timeout(10):
            await wait_jobs(service)
        assert not service.cards
        assert not (c.state / "bots" / worker.id / "workspace" / "approved.txt").exists()
    finally:
        await service.close()


async def test_chief_tools_schedule_for_the_current_chat(tmp_path):
    c = config(tmp_path)
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        route = service.store.conversation(11, 11)
        await service.team.run("schedule_job", dict(when="every 1h", task="check"), route, actor=11)
        job = service.store.jobs()[0]
        assert (job.chat, job.actor, job.task) == (11, 11, "check")
        assert "check" in await service.team.run("list_scheduled_jobs", {}, route)
        await service.team.run("cancel_scheduled_job", dict(job_id=job.id), route)
        assert service.store.job(job.id).status == "cancelled"
    finally:
        await service.close()
