import asyncio
import json

import pytest

from delta_gateway.bots import prepare
from delta_gateway.service import Service
from delta_gateway.store import Store
from delta_gateway.team import Team
from tests.test_gateway import Bot, config, settle, update


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path / "state", "a" * 32)
    yield result
    result.close()


def test_only_the_chief_gets_team_tools(tmp_path, store):
    c = config(tmp_path)
    chief = prepare(c, store.bots()[0])
    worker = prepare(c, store.create_bot("Researcher", "Finds things"))
    assert "--host-tools-file" in chief.engine_args
    assert "--host-tools-file" not in worker.engine_args


def test_one_task_per_bot_and_restart_interrupts(tmp_path, store):
    bot = store.create_bot("Researcher", "Finds things")
    route = store.conversation(1, 1)
    store.begin_delegation(route.id, bot, "one")
    with pytest.raises(ValueError, match="busy"):
        store.begin_delegation(route.id, bot, "two")
    store.close()
    reopened = Store(tmp_path / "state", "a" * 32)
    try:
        assert reopened.db.execute("SELECT status FROM delegations").fetchall() == [
            ("interrupted",)
        ]
        reopened.begin_delegation(route.id, bot, "three")
    finally:
        reopened.close()


async def test_team_refuses_the_chief_and_unknown_bots(store):
    team = Team(store, None)
    route = store.conversation(1, 1)
    with pytest.raises(ValueError):
        await team.run("delegate_task", dict(bot_name="Chief of Staff", task="x"), route)
    with pytest.raises(ValueError):
        await team.run("delegate_task", dict(bot_name="Nobody", task="x"), route)


async def run(service, c, identifier, text):
    service.store.admit([update(identifier, text)], c)
    await service.dispatch()
    await settle(service)


async def test_chief_delegates_to_a_bot_in_its_own_workspace(tmp_path):
    c = config(tmp_path, policy="auto")
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    try:
        worker = service.store.create_bot("Researcher", "Finds things")
        await run(service, c, 1, "delegate:Researcher:hello")
        row = service.store.db.execute("SELECT bot,task,status,answer FROM delegations").fetchone()
        assert row[:3] == (worker.id, "hello", "completed") and "done" in row[3]
        assert any("delegated" in text for _, text, _ in telegram.sent)
    finally:
        await service.close()


async def test_two_bots_run_at_the_same_time(tmp_path):
    c = config(tmp_path, policy="auto")
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.create_bot("One", "a")
        service.store.create_bot("Two", "b")
        team = service.team
        route = service.store.conversation(11, 11)
        answers = await asyncio.wait_for(
            asyncio.gather(
                team.run("delegate_task", dict(bot_name="One", task="hi"), route),
                team.run("delegate_task", dict(bot_name="Two", task="hi"), route),
            ),
            60,
        )
        assert {json.loads(a)["bot"] for a in answers} == {"One", "Two"}
    finally:
        await service.close()


async def test_stop_ends_a_delegation_without_replay(tmp_path):
    c = config(tmp_path, policy="auto")
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.create_bot("Researcher", "Finds things")
        service.store.admit([update(1, "delegate:Researcher:slow")], c)
        await service.dispatch()
        query = "SELECT count(*) FROM delegations WHERE status='running'"
        async with asyncio.timeout(20):
            while not service.store.db.execute(query).fetchone()[0]:
                await asyncio.sleep(0.02)
        service.store.admit([update(2, "/stop")], c)
        await service.dispatch()
        await settle(service)
        status = service.store.db.execute("SELECT status FROM delegations").fetchone()[0]
        assert status == "interrupted"
    finally:
        await service.close()


async def test_delegated_bot_asks_the_user_for_approval(tmp_path):
    c = config(tmp_path, policy="ask")
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    try:
        worker = service.store.create_bot("Researcher", "Finds things")
        service.store.admit([update(1, "delegate:Researcher:write")], c)
        await service.dispatch()
        async with asyncio.timeout(30):
            while not service.cards:
                await asyncio.sleep(0.02)
        handle, card = next(iter(service.cards.items()))
        assert card["event"]["tool"] == "Write"
        target = c.state / "bots" / worker.id / "workspace" / "approved.txt"
        assert not target.exists()
        await card["client"].request(
            "respond_to_approval", approvalId=card["event"]["approval_id"], allow=True
        )
        await settle(service)
        assert target.exists()
    finally:
        await service.close()
