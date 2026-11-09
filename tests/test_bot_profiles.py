import asyncio

import pytest

from delta_gateway.bot_profiles import profile, reserve, save_profile, saved_session
from delta_gateway.store import Store
from delta_gateway.team import Team
from tests.test_web import web as web


async def wait_working(sessions, bot):
    async with asyncio.timeout(5):
        while bot.id not in sessions.active_bots:
            await asyncio.sleep(0.01)


async def test_foreground_and_delegation_share_bot_reservation(web):
    service, _, _ = web
    manager = service.sessions
    bot = service.store.create_bot("Reader", "Read documents")
    route = service.store.conversation(11, 11)
    route = service.store.switch_bot(route, bot)
    foreground = asyncio.create_task(manager.prompt(route, "slow"))
    await wait_working(manager, bot)
    delegation = asyncio.create_task(manager.delegate(bot, "Follow up", policy="deny"))
    try:
        await asyncio.sleep(0.1)
        assert not delegation.done()
        assert (
            service.store.db.execute(
                "SELECT count(*) FROM bot_chat WHERE bot=? AND role='user'", (bot.id,)
            ).fetchone()[0]
            == 1
        )
        foreground.cancel()
        await asyncio.gather(foreground, return_exceptions=True)
        assert (await asyncio.wait_for(delegation, 5))[1] == "completed"
        assert bot.id not in manager.active_bots
        with reserve(manager, bot.id):
            pass
    finally:
        foreground.cancel()
        delegation.cancel()
        await asyncio.gather(foreground, delegation, return_exceptions=True)


async def test_bot_reservation_excludes_another_manager(web):
    service, _, _ = web
    from delta_gateway.session_manager import SessionManager

    other = SessionManager(service.config, service.store)
    bot = service.store.bots()[0]
    with reserve(service.sessions, bot.id):
        with pytest.raises(RuntimeError, match="already owned"):
            with reserve(other, bot.id):
                pytest.fail("overlapping workspace ownership")
    with reserve(other, bot.id):
        pass


async def test_self_update_reloads_cached_engine_and_only_changes_self(web):
    service, _, _ = web
    manager = service.sessions
    bot = service.store.create_bot("Reader", "Old purpose")
    chief = service.store.bots()[0]
    route = service.store.switch_bot(service.store.conversation(11, 11), bot)
    await manager.prompt(route, "hello")
    before = next(iter(manager.clients.values()))
    await Team(service.store, manager).run(
        "update_own_bot", {"instructions": "Cite primary sources."}, route, bot=bot
    )
    assert service.store.bot(chief.id).instructions == chief.instructions
    assert service.store.bot(bot.id).instructions == "Cite primary sources."
    await manager.prompt(route, "again")
    after = next(iter(manager.clients.values()))
    assert after is not before
    assert after.bot_signature[1] == "Cite primary sources."


async def test_failed_computer_removal_preserves_bot_and_history(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    computer = service.sessions.computer_for(bot)

    async def broken():
        raise RuntimeError("Computer is unavailable")

    computer.remove = broken
    response = await client.delete("/api/bots/" + bot.id)
    assert response.status_code == 409
    assert service.store.bot(bot.id) == bot


async def test_profiles_survive_reopen_and_defaults_differ(web):
    service, _, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    chief = service.store.bots()[0]
    assert profile(service.store, chief)["color"] != profile(service.store, bot)["color"]
    with service.store.db:
        save_profile(service.store, bot, {"style": "thoughtful", "shape": "diamond"})
    reopened = Store(service.config.database_path, service.config.instance)
    try:
        assert profile(reopened, bot)["style"] == "thoughtful"
        assert profile(reopened, bot)["shape"] == "diamond"
    finally:
        reopened.close()


async def test_greeting_does_not_replace_delegated_session(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    await service.sessions.delegate(bot, "First task", policy="deny")
    original = saved_session(service.store, bot.id)
    assert (await client.post("/api/bots/" + bot.id + "/greeting")).status_code == 200
    assert saved_session(service.store, bot.id) == original
    rows = (await client.get("/api/bots/" + bot.id + "/messages")).json()
    assert [r["text"] for r in rows if r["role"] == "user"] == ["First task"]


async def test_rename_collision_rolls_back_profile_and_busy_delete_is_refused(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    service.store.create_bot("Writer", "Write documents")
    response = await client.patch("/api/bots/" + bot.id, json={"name": "Writer", "style": "upbeat"})
    assert response.status_code == 409
    assert service.store.bot(bot.id).name == "Reader"
    assert profile(service.store, bot)["style"] == "direct"
    with reserve(service.sessions, bot.id):
        assert (await client.delete("/api/bots/" + bot.id)).status_code == 409
