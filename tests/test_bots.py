import sqlite3

import pytest

from delta_gateway.bots import prepare
from delta_gateway.service import Service
from delta_gateway.store import Store
from tests.test_gateway import Bot, config, settle, update


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path / "state", "a" * 32)
    yield result
    result.close()


def test_new_instance_has_one_chief(store):
    bots = store.bots()
    assert [bot.name for bot in bots] == ["Chief of Staff"] and bots[0].chief
    assert store.conversation(1, 1).bot == bots[0].id


def test_bot_names_are_unique_regardless_of_case(store):
    store.create_bot("Researcher", "Finds things")
    with pytest.raises(ValueError):
        store.create_bot("researcher", "Other")


def test_switching_keeps_each_bots_session(store):
    other = store.create_bot("Researcher", "Finds things")
    chief = store.bots()[0]
    route = store.conversation(1, 1)
    store.persist_route(route, dict(sessionId="s1", sessionFile="s1.jsonl", thinkingLevel="off"))
    route = store.switch_bot(route, other)
    assert route.bot == other.id and route.session is None and route.session_file is None
    store.persist_route(route, dict(sessionId="s2", sessionFile="s2.jsonl", thinkingLevel="off"))
    route = store.switch_bot(route, chief)
    assert route.session == "s1" and route.session_file == "s1.jsonl"
    route = store.switch_bot(route, other)
    assert route.session == "s2"


def test_version_four_database_upgrades(tmp_path):
    path = tmp_path / "state" / "gateway.sqlite3"
    Store(tmp_path / "state", "a" * 32).close()
    db = sqlite3.connect(path)
    db.executescript(
        "DROP TABLE memory_facts;"
        "DROP TABLE tasks; DROP TABLE bot_plugins; DROP TABLE jobs; DROP TABLE delegations;"
        "DROP TABLE route_bots; DROP TABLE bots;"
        "ALTER TABLE routes DROP COLUMN bot;"
        "INSERT INTO routes(instance,chat,user,thread,session,created,updated) "
        f"VALUES('{'a' * 32}',1,1,0,'old',0,0); PRAGMA user_version=4;"
    )
    db.close()
    store = Store(tmp_path / "state", "a" * 32)
    try:
        route = store.conversation(1, 1)
        assert route.session == "old" and route.bot == store.bots()[0].id
    finally:
        store.close()


def test_prepare_separates_bot_files(tmp_path, store):
    c = config(tmp_path)
    worker = store.create_bot("Researcher", "Finds things")
    prepared = prepare(c, worker)
    assert prepared.project == c.state / "bots" / worker.id / "workspace"
    assert prepared.project.is_dir()
    system = (c.state / "bots" / worker.id / "system.md").read_text(encoding="utf-8")
    assert "You are Researcher" in system and "KNOWLEDGE.md" in system
    assert prepared.engine_args[-2] == "--append-system-prompt-file"
    assert prepare(c, store.bots()[0]).project == c.project


async def run(service, c, identifier, text):
    service.store.admit([update(identifier, text)], c)
    await service.dispatch()
    await settle(service)


async def test_bot_commands_through_service(tmp_path):
    c = config(tmp_path)
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    try:
        await run(service, c, 1, "first")
        await run(service, c, 2, "/bot new Researcher | Finds things")
        await run(service, c, 3, "/bot researcher")
        route = service.store.conversation(11, 11)
        assert service.store.bot(route.bot).name == "Researcher" and route.session is None
        await run(service, c, 4, "second")
        assert service.store.conversation(11, 11).session
        await run(service, c, 5, "/bot")
        assert any("* Researcher" in text for _, text, _ in telegram.sent)
    finally:
        await service.close()
