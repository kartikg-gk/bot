import asyncio

import pytest

from delta_gateway.service import Service
from delta_gateway.telegram import TelegramError
from tests.test_gateway import Bot, config, settle, update


async def test_preview_failure_does_not_drop_prompt(tmp_path):
    class BrokenPreview(Bot):
        async def call(self, method, **payload):
            if method == "sendMessageDraft":
                raise TelegramError("draft unavailable")
            return await super().call(method, **payload)

        async def send(self, chat, text, **kwargs):
            if text == "Working…":
                raise TelegramError("preview unavailable")
            return await super().send(chat, text, **kwargs)

    bot = BrokenPreview()
    service = Service(config(tmp_path), bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1)], service.config)
        await settle(service)
        assert any(text == "turn 1: done" for _, text, _ in bot.sent)
        assert not service.store.issues()
    finally:
        await service.close()


async def test_session_commands_through_service(tmp_path):
    class Documents(Bot):
        async def document(self, *args, **kwargs):
            self.document_payload = (args, kwargs)

    bot = Documents()
    c = config(tmp_path)
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        for number, text in enumerate(
            ["hello", "/name custom", "/clone", "/export", "/sessions", "/status"], 1
        ):
            service.store.admit([update(number, text)], c)
            await settle(service)
        assert not service.store.issues()
        assert len(service.store.sessions(11, 11)) == 2
        assert bot.document_payload[0][1].endswith(".html")
        service.store.admit(
            [update(7, "/switch " + str(service.store.conversation(11, 11, 0).id))], c
        )
        await settle(service)
        assert service.store.mapping(11, 11)[0]
    finally:
        await service.close()


async def test_worker_is_closed_if_mapping_persistence_fails(tmp_path, monkeypatch):
    import delta_gateway.session_manager as module

    children = []
    original = module.EmbeddedClient

    def capture(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(module, "EmbeddedClient", capture)
    service = Service(config(tmp_path), Bot(), factory="tests.fake_session:create")
    try:

        def fail(*args):
            raise RuntimeError("database unavailable")

        monkeypatch.setattr(service.store, "remember", fail)
        with pytest.raises(RuntimeError):
            await service.sessions.client_for(service.store.conversation(11, 11, 0))
        assert len(children) == 1
        assert children[0].failure is not None and children[0].server is None
        assert not service.clients
    finally:
        await service.close()


async def test_stop_does_not_reschedule_cancelled_commands(tmp_path):
    service = Service(config(tmp_path), Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "/stop"), update(2, "/delta hello")], service.config)
        await settle(service)
        assert not service.clients
        assert status(service, 2) == "cancelled"
    finally:
        await service.close()


async def test_queued_slash_text_remains_a_prompt_after_restart(tmp_path):
    c = config(tmp_path, workers=1)
    service = Service(c, Bot(), factory="tests.fake_session:create")
    # Hold the only work slot so the prompt is still waiting when the gateway stops.
    await service.slots.acquire()
    try:
        service.store.admit([update(1, "/queue /custom-prompt")], c)
        await service.dispatch()
        await asyncio.sleep(0)
        assert service.store.pending()[0][4] == "prompt"
    finally:
        await service.close()
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        await settle(service)
        assert service.store.mapping(11, 11)[0]
        assert await service.last((11, 11, 0)) == "turn 1: done"
    finally:
        await service.close()


def status(service, update_id):
    query = "SELECT status FROM inbox WHERE id=?"
    return service.store.db.execute(query, (update_id,)).fetchone()[0]


@pytest.mark.parametrize("command", ["queue", "steer"])
async def test_queue_and_steer_reach_an_active_run(tmp_path, command):
    c = config(tmp_path)
    bot = Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "slow")], c)
        await service.dispatch()
        async with asyncio.timeout(10):
            while status(service, 1) != "running":
                await asyncio.sleep(0.01)
        service.store.admit([update(2, f"/{command} more")], c)
        await service.dispatch()
        assert status(service, 2) == "completed"
        assert any("active run" in text for _, text, _ in bot.sent)
        client = next(iter(service.sessions.clients.values()))
        assert (await client.request("get_state"))["pendingMessageCount"] == 1
    finally:
        await service.close()


async def test_steer_when_idle_runs_as_a_prompt(tmp_path):
    c = config(tmp_path)
    bot = Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "/steer hello")], c)
        await service.dispatch()
        await settle(service)
        assert status(service, 1) == "completed"
        assert not any("active run" in text for _, text, _ in bot.sent)
    finally:
        await service.close()
