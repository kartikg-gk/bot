"""The store behaves the same on a local SQLite file and on D1."""

import time
from types import SimpleNamespace

import pytest

from delta_gateway.migrations import ensure_schema
from delta_gateway.store import Store

INSTANCE = "a" * 32


async def opened(database, **options):
    return await Store.open(database, INSTANCE, **options)


def intake(config_backlog=None):
    return SimpleNamespace(backlog=config_backlog, route=lambda chat, user, thread: (chat, user))


def update(number, text="hello", user=11):
    return {
        "update_id": number,
        "message": {"from": {"id": user}, "chat": {"id": user, "type": "private"}, "text": text},
    }


async def test_open_creates_schema_and_one_chief(database):
    store = await opened(database)
    await opened(database)
    bots = await store.bots()
    assert [bot.name for bot in bots] == ["Chief of Staff"] and bots[0].chief


async def test_bots_and_routes(database):
    store = await opened(database)
    reader = await store.create_bot("Reader", "Reads")
    assert await store.find_bot("reader") == reader
    with pytest.raises(ValueError, match="already exists"):
        await store.create_bot("READER", "Other")
    route = await store.conversation(11, 11)
    assert route.bot == (await store.bots())[0].id
    switched = await store.switch_bot(route, reader)
    assert switched.bot == reader.id and await store.get_route(route.id) == switched
    assert [r.id for r in await store.list_routes(11)] == [route.id]


async def test_intake_marks_and_history(database, monkeypatch):
    import delta_gateway.store as module

    monkeypatch.setattr(
        module,
        "admit_message",
        lambda message, config, bot_id, username: (
            message["chat"]["id"],
            message["from"]["id"],
            0,
            message["text"],
        ),
    )
    store = await opened(database)
    await store.admit([update(5, "first"), update(6, "second")], intake())
    await store.admit([update(5, "first")], intake())
    assert [item.text for item in await store.items()] == ["first", "second"]
    assert await store.offset() == 7
    await store.mark(5, "completed")
    assert not await store.is_pending(5) and await store.is_pending(6)
    assert [m["text"] for m in await store.history(11, 11)] == ["first", "second"]


async def test_web_ids_are_unique_and_above_the_base(database):
    store = await opened(database)
    base = 2**59
    first = await store.admit_web(9, 11, "a", base=base)
    second = await store.admit_web(9, 11, "b", base=base)
    assert second == first + 1 > base


async def test_reports_are_claimed_once(database):
    store = await opened(database)
    task = await store.add_task(11, 11, 11, 0, (await store.bots())[0].id, "goal", "ask", False)
    await database.run("UPDATE tasks SET status='interrupted' WHERE id=?", (task,))
    assert [t.id for t in await store.take_unreported()] == [task]
    assert await store.take_unreported() == []
    await store.add_report(await store.task(task), "report")
    assert (await store.items())[-1].id > 2**60


async def test_job_quota_claims_and_lease(database):
    store = await opened(database)
    bot = (await store.bots())[0].id
    now = time.time()
    job = await store.add_job(11, 11, 11, 0, bot, "task", "every:60", now - 1)
    assert await store.scheduler_heartbeat("first", now, 30)
    assert not await store.scheduler_heartbeat("second", now + 1, 30)
    claimed = await store.claim_due(now, owner="first")
    assert [j.id for j in claimed] == [job]
    assert await store.claim_due(now, owner="first") == []
    await store.finish_job(job, "completed", "done", claimed_at=claimed[0].last_run)
    finished = await store.job(job)
    assert not finished.running and finished.last_answer == "done"
    assert await store.scheduler_heartbeat("second", now + 60, 30)


async def test_job_limit_is_enforced(database, monkeypatch):
    import delta_gateway.store as module

    monkeypatch.setattr(module, "MAX_ACTIVE_JOBS", 2)
    store = await opened(database)
    bot = (await store.bots())[0].id
    for _ in range(2):
        await store.add_job(11, 11, 11, 0, bot, "task", "every:60", time.time() + 60)
    with pytest.raises(ValueError, match="at most 2"):
        await store.add_job(11, 11, 11, 0, bot, "task", "every:60", time.time() + 60)


async def test_one_delegation_per_bot(database):
    store = await opened(database)
    route = await store.conversation(11, 11)
    reader = await store.create_bot("Reader", "Reads")
    await store.begin_delegation(route.id, reader, "one")
    with pytest.raises(ValueError, match="busy"):
        await store.begin_delegation(route.id, reader, "two")


async def test_recovery_runs_only_when_asked(database):
    store = await opened(database)
    task = await store.add_task(11, 11, 11, 0, (await store.bots())[0].id, "goal", "ask", False)
    await opened(database, recover=False)
    assert (await store.task(task)).status == "running"
    await opened(database)
    assert (await store.task(task)).status == "interrupted"


async def test_schema_from_another_process_is_reused(database):
    await ensure_schema(database)
    store = await opened(database)
    assert await store.running_tasks() == 0
