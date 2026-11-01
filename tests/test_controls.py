import asyncio

import pytest

from delta_gateway.client import WorkerClient
from delta_gateway.service import Service
from delta_gateway.telegram import TelegramError
from tests.test_gateway import Bot, config, settle, update
from tests.test_subprocess import command, finish


async def control(client, operation, **arguments):
    accepted = await client.request(operation, **arguments)
    events = await finish(client)
    terminal = events[-1]
    assert terminal["run_id"] == accepted["run_id"]
    assert terminal["outcome"] == "completed", terminal
    return terminal["result"]


async def test_session_name_model_export_clone_resume(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        await client.request("submit", text="hello <script>alert(1)</script>")
        await finish(client)
        await control(client, "name", title="A <test>")
        assert (await client.request("status"))["title"] == "A <test>"
        await control(client, "model", selection="fake-v2")
        assert (await client.request("status"))["model"] == "fake-v2"
        document = (await control(client, "export"))["document"]
        assert "&lt;script&gt;" in document["content"]
        assert "<script>" not in document["content"]
        clone = await control(client, "clone")
        original = client.state["session_id"]
    assert clone["session_id"] != original
    async with WorkerClient(command(tmp_path, clone["session_id"])) as client:
        assert (await client.request("status"))["title"] == "Clone of A <test>"
        await client.request("submit", text="second")
        assert "turn 2: done" == "".join(e.get("text", "") for e in await finish(client))


@pytest.mark.parametrize("operation", ["queue", "steer"])
async def test_queue_and_steer_share_terminal_boundary(tmp_path, operation):
    async with WorkerClient(command(tmp_path) + ["--policy", "ask"]) as client:
        accepted = await client.request("submit", text="write")
        event = await client.next_event()
        while event["event"] != "approval.requested":
            event = await client.next_event()
        queued = await client.request(operation, text="follow up")
        assert queued["run_id"] == accepted["run_id"]
        await client.request("approval.respond", approval_id=event["approval_id"], allow=False)
        events = await finish(client)
        text = "".join(e.get("text", "") for e in events)
        assert "turn 2: done" in text
        assert (await client.request("status"))["pending"] == 0


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


async def test_compaction_control_and_resumption(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        for _ in range(7):
            await client.request("submit", text="hello")
            await finish(client)
        result = await control(client, "compact", instructions="Keep the project constraints")
        assert result["summary"]
        session = client.state["session_id"]
    async with WorkerClient(command(tmp_path, session)) as client:
        await client.request("submit", text="continue")
        assert (await finish(client))[-1]["outcome"] == "completed"


async def test_worker_is_closed_if_mapping_persistence_fails(tmp_path, monkeypatch):
    import delta_gateway.session_manager as module

    children = []
    original = module.RpcClient

    def capture(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(module, "RpcClient", capture)
    service = Service(config(tmp_path), Bot(), factory="tests.fake_session:create")
    try:

        def fail(*args):
            raise RuntimeError("database unavailable")

        monkeypatch.setattr(service.store, "remember", fail)
        with pytest.raises(RuntimeError):
            await service.sessions.client_for(service.store.conversation(11, 11, 0))
        assert len(children) == 1
        assert children[0].process.returncode is not None
        assert not service.clients
    finally:
        await service.close()


async def test_stop_does_not_reschedule_cancelled_commands(tmp_path):
    service = Service(config(tmp_path), Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "/stop"), update(2, "/delta hello")], service.config)
        await settle(service)
        assert not service.clients
        assert (
            service.store.db.execute("SELECT status FROM inbox WHERE id=2").fetchone()[0]
            == "cancelled"
        )
    finally:
        await service.close()


async def test_cancelled_operation_before_entry_shutdown(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        await client.request("submit", text="slow")
        await client.next_event()
        await client.cancel()
        await finish(client)
        assert (await client.request("status"))["run_id"] is None


async def test_shutdown_after_task_cancelled_before_first_instruction(tmp_path):
    import io
    from types import SimpleNamespace

    from delta_gateway.protocol import decode
    from delta_gateway.worker import Worker
    from tests.fake_session import create

    session = await create(SimpleNamespace(session_dir=tmp_path, session=None))
    output = io.BytesIO()
    worker = Worker(session, output)
    try:

        def request(operation):
            return dict(
                version=1,
                kind="request",
                request_id=operation,
                operation=operation,
                arguments={"text": "slow"} if operation == "submit" else {},
            )

        await worker.dispatch(request("submit"))
        await worker.dispatch(request("cancel"))
        assert not await worker.dispatch(request("shutdown"))
        records = [decode(line + b"\n") for line in output.getvalue().splitlines()]
        assert records[-1]["request_id"] == "shutdown" and records[-1]["ok"]
    finally:
        await session.shutdown()


async def test_thinking_unsupported_is_clear(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        await client.request("thinking", level="high")
        terminal = (await finish(client))[-1]
        assert terminal["outcome"] == "failed"
        assert "thinking controls" in terminal["result"]["message"]
        await control(client, "thinking", level="off")


async def test_queued_slash_text_remains_a_prompt_after_restart(tmp_path):
    c = config(tmp_path)
    service = Service(c, Bot(), factory="tests.fake_session:create")
    await service.slots.acquire()
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


def test_export_size_counts_encoded_wire():
    from types import SimpleNamespace

    from delta_harness.contracts.transcript import HumanEntry

    from delta_gateway.session_controls import ControlError, html_export

    session = SimpleNamespace(
        title="test", session_id="test", transcript=[HumanEntry(content="😀" * 1100000)]
    )
    with pytest.raises(ControlError):
        html_export(session)


def test_provider_credentials_do_not_cross_endpoints(monkeypatch):
    import os

    from delta_gateway.session_controls import prepare_provider

    original = {
        "OPENAI_API_KEY": "fake-openai-key",
        "OPENROUTER_API_KEY": "fake-router-key",
        "OPENAI_BASE_URL": "https://openai.invalid/v1",
    }
    monkeypatch.setenv("OPENAI_API_KEY", original["OPENAI_API_KEY"])
    monkeypatch.setenv("OPENAI_BASE_URL", original["OPENAI_BASE_URL"])
    prepare_provider("openrouter", original)
    assert os.environ["OPENAI_API_KEY"] == "fake-router-key"
    assert os.environ["OPENAI_BASE_URL"] == "https://openrouter.ai/api/v1"
    prepare_provider("openai", original)
    assert os.environ["OPENAI_API_KEY"] == "fake-openai-key"
    assert os.environ["OPENAI_BASE_URL"] == "https://openai.invalid/v1"


async def test_build_session_selects_provider_credentials_before_engine(tmp_path, monkeypatch):
    import importlib
    import os
    from types import SimpleNamespace

    from delta_gateway.worker import build_session

    monkeypatch.setenv("OPENAI_API_KEY", "fake-openai-key")
    monkeypatch.setenv("OPENROUTER_API_KEY", "fake-router-key")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://openai.invalid/v1")
    seen = {}

    async def build(options):
        seen.update(key=os.environ["OPENAI_API_KEY"], url=os.environ["OPENAI_BASE_URL"])
        return SimpleNamespace(harness=object())

    monkeypatch.setattr("delta_app.runtime.build_session", build)
    monkeypatch.setattr(
        importlib.import_module("delta_app.cli.main"), "_install_safety", lambda *args: None
    )
    await build_session(SimpleNamespace(provider="openrouter", session=None, trust_project=False))
    assert seen == {"key": "fake-router-key", "url": "https://openrouter.ai/api/v1"}


@pytest.mark.parametrize("command", ["queue", "steer"])
async def test_queue_and_steer_reach_an_active_run(tmp_path, command):
    c = config(tmp_path)
    bot = Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "slow")], c)
        await service.dispatch()
        async with asyncio.timeout(10):
            while service.store.db.execute("SELECT status FROM inbox WHERE id=1").fetchone()[0] != "running":
                await asyncio.sleep(0.01)
        service.store.admit([update(2, f"/{command} more")], c)
        await service.dispatch()
        assert service.store.db.execute("SELECT status FROM inbox WHERE id=2").fetchone()[0] == "completed"
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
        assert service.store.db.execute("SELECT status FROM inbox WHERE id=1").fetchone()[0] == "completed"
        assert not any("active run" in text for _, text, _ in bot.sent)
    finally:
        await service.close()
