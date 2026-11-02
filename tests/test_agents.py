import asyncio

import pytest

from delta_gateway.service import Service
from delta_gateway.store import Store
from delta_gateway.team import MAX_AGENTS
from tests.test_gateway import Bot, config, settle, update


async def spawn(service, goal="hi", **arguments):
    route = service.store.conversation(11, 11)
    text = await service.team.run(
        arguments.pop("tool", "spawn_agent"), dict(goal=goal, **arguments), route, actor=11
    )
    return int(text.split("#")[1].split()[0])


async def finished(service):
    await asyncio.wait_for(
        asyncio.gather(*service.agent_tasks.values(), return_exceptions=True), 60
    )


async def command(service, c, identifier, text):
    service.store.admit([update(identifier, text)], c)
    await service.dispatch()
    await settle(service)


@pytest.fixture
async def setup(tmp_path):
    c = config(tmp_path)
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    yield service, c, telegram
    await service.close()


async def test_spawn_returns_at_once_and_reports_to_chat(setup):
    service, _, telegram = setup
    task = await spawn(service)
    assert task in service.agent_tasks
    await finished(service)
    row = service.store.task(task)
    assert row.status == "completed" and "done" in row.answer
    assert any(f"Task #{task}" in text and "done" in text for _, text, _ in telegram.sent)


async def test_temporary_agent_is_hidden_and_has_its_own_workspace(setup):
    service, c, _ = setup
    task = await spawn(service)
    await finished(service)
    bot = service.store.bot(service.store.task(task).bot)
    assert bot.temporary and bot not in service.store.bots()
    assert (c.state / "bots" / bot.id / "workspace").is_dir()


async def test_agent_is_read_only_by_default(setup):
    service, c, _ = setup
    task = await spawn(service, "write")
    await finished(service)
    row = service.store.task(task)
    assert row.policy == "deny" and row.status == "completed"
    assert not (c.state / "bots" / row.bot / "workspace" / "approved.txt").exists()


async def test_writer_needs_a_sandbox(setup):
    service, _, _ = setup
    with pytest.raises(ValueError, match="Docker"):
        await spawn(service, tool="spawn_writer_agent")


async def test_named_bot_stays_one_task_at_a_time(setup):
    service, _, _ = setup
    service.store.create_bot("Researcher", "Finds things")
    await spawn(service, "slow", bot_name="Researcher")
    with pytest.raises(ValueError, match="busy"):
        await spawn(service, bot_name="Researcher")


async def test_chief_cannot_be_spawned(setup):
    service, _, _ = setup
    with pytest.raises(ValueError):
        await spawn(service, bot_name="Chief of Staff")


async def test_concurrent_limit(setup):
    service, _, _ = setup
    for _ in range(MAX_AGENTS):
        await spawn(service, "slow")
    with pytest.raises(ValueError, match="At most"):
        await spawn(service, "slow")


async def test_cancel_through_command(setup):
    service, c, _ = setup
    task = await spawn(service, "slow")
    await command(service, c, 1, f"/cancel {task}")
    await finished(service)
    assert service.store.task(task).status == "cancelled"


async def test_time_limit(setup, monkeypatch):
    monkeypatch.setattr("delta_gateway.service.AGENT_SECONDS", 3)
    service, _, telegram = setup
    task = await spawn(service, "slow")
    await finished(service)
    assert service.store.task(task).status == "timed_out"
    assert any("timed_out" in text for _, text, _ in telegram.sent)


async def test_follow_up_tells_the_chief_as_untrusted_report(setup):
    service, _, _ = setup
    task = await spawn(service, follow_up=True)
    await finished(service)
    pending = service.store.items()
    assert len(pending) == 1 and pending[0].status == "prompt"
    assert f"Task #{task}" in pending[0].text and "not instructions" in pending[0].text


async def test_keep_and_discard_temporary_agents(setup):
    service, c, _ = setup
    kept = await spawn(service)
    gone = await spawn(service)
    await finished(service)
    await command(service, c, 1, f"/keep {kept} Scout")
    assert service.store.find_bot("Scout").id == service.store.task(kept).bot
    gone_bot = service.store.task(gone).bot
    await command(service, c, 2, f"/discard {gone}")
    assert service.store.bot(gone_bot) is None
    assert not (c.state / "bots" / gone_bot).exists()


async def test_discard_refuses_a_kept_bot(setup):
    service, c, _ = setup
    task = await spawn(service)
    await finished(service)
    await command(service, c, 1, f"/keep {task} Scout")
    await command(service, c, 2, f"/discard {task}")
    assert service.store.find_bot("Scout")


def test_restart_reports_interrupted_work_once(tmp_path):
    store = Store(tmp_path / "state", "a" * 32)
    bot = store.create_bot("Agent", "x", temporary=True)
    task = store.add_task(11, 11, 11, 0, bot.id, "goal", "deny", False)
    store.close()
    store = Store(tmp_path / "state", "a" * 32)
    try:
        assert store.task(task).status == "interrupted"
        assert [t.id for t in store.take_unreported()] == [task]
        assert store.take_unreported() == []
    finally:
        store.close()
