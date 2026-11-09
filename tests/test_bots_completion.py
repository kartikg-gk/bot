from delta_gateway.bot_profiles import profile, save_profile
from delta_gateway.team import Team
from delta_gateway.web import WEB_CHAT, WEB_IDS
from tests.test_web import web as web


async def test_delete_does_not_redirect_queued_message_to_chief(web):
    service, client, _ = web
    bot = service.store.create_bot("Queued", "Keep this work separate")
    route = service.store.conversation(WEB_CHAT, service.web.actor)
    await service.sessions.switch_bot(route, bot)
    service.store.admit_web(WEB_CHAT, service.web.actor, "Work for this bot", base=WEB_IDS)
    response = await client.delete("/api/bots/" + bot.id)
    assert response.status_code == 409
    assert service.store.bot(bot.id) == bot
    assert service.store.get_route(route.id).bot == bot.id


async def test_own_instruction_update_invalidates_saved_greeting(web):
    service, _, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    with service.store.db:
        save_profile(service.store, bot, {})
        service.store.db.execute(
            "UPDATE bot_profiles SET greeting='Old greeting' WHERE bot=?", (bot.id,)
        )
    await Team(service.store, service.sessions).run(
        "update_own_bot",
        {"instructions": "Summarize before answering."},
        service.store.conversation(11, 11),
        bot=bot,
    )
    assert profile(service.store, bot)["greeting"] is None


async def test_cancelled_computer_process_is_killed_and_reaped():
    import asyncio
    import sys

    from delta_gateway.computer import finish

    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    task = asyncio.create_task(finish(child, 60))
    try:
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
        assert child.returncode is not None
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
