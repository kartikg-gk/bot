"""Hosted runs start from the earlier messages the browser sends."""

import pytest

from delta_gateway.hosted_runs import entries, parse_conversation, save_context, take_context
from delta_gateway.session_manager import SessionManager
from delta_gateway.store import Store
from tests.test_gateway import config
from tests.test_web import web  # noqa: F401

INSTANCE = "a" * 32
EARLIER = [
    {"role": "user", "content": "My name is Ada."},
    {"role": "assistant", "content": "Hello Ada."},
    {"role": "user", "content": "I like maps."},
    {"role": "assistant", "content": "Noted."},
]


def test_conversation_is_checked():
    assert parse_conversation(None) == []
    assert parse_conversation(EARLIER) == EARLIER
    message = "^Send at most 20 earlier messages of up to 4000 characters each$"
    with pytest.raises(ValueError, match=message):
        parse_conversation([{"role": "user", "content": "x"}] * 21)
    with pytest.raises(ValueError, match=message):
        parse_conversation([{"role": "user", "content": "x" * 4001}])
    for bad in (
        [{"role": "system", "content": "x"}],
        [{"role": "user", "content": 3}],
        [{"role": "user", "content": "x", "extra": 1}],
        "text",
    ):
        with pytest.raises(ValueError, match=message):
            parse_conversation(bad)


async def test_context_is_taken_once(database):
    store = await Store.open(database, INSTANCE)
    await save_context(store, 7, EARLIER)
    assert await take_context(store, 7) == EARLIER
    assert await take_context(store, 7) == []


async def test_seeded_run_sees_earlier_messages(tmp_path, database):
    store = await Store.open(database, INSTANCE)
    manager = SessionManager(config(tmp_path), store, factory="tests.fake_session:create")
    route = await store.conversation(9, 11)
    try:
        final, outcome = await manager.prompt(route, "hello", seed=entries(EARLIER))
        assert outcome == "completed" and final.startswith("turn 3")
        await manager.fresh(route)
        final, _ = await manager.prompt(await store.get_route(route.id), "hello", seed=[])
        assert final.startswith("turn 1")
    finally:
        await manager.close()


async def test_run_endpoints_refuse_oversized_conversations(web):  # noqa: F811
    _, client = web[0], web[1]
    too_many = [{"role": "user", "content": "x"}] * 21
    for path in ("/api/runs", "/api/messages"):
        response = await client.post(path, json={"text": "hi", "conversation": too_many})
        assert response.status_code == 400
