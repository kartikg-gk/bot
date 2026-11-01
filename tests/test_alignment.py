import asyncio
import hashlib
import json
import sqlite3

import httpx
import pytest
import yaml

from delta_gateway.cli import parser
from delta_gateway.config import Config, load, save
from delta_gateway.database_import import migrate_configs
from delta_gateway.instances import register, registry, resolve
from delta_gateway.service import Service
from delta_gateway.store import Store
from delta_gateway.telegram import TelegramError
from delta_gateway.telegram_sdk import TelegramSDK
from tests.test_gateway import Bot, config, settle, update


def group_update(number, text, *, actor=11, thread=7):
    return {
        "update_id": number,
        "message": {
            "message_id": number,
            "date": 1,
            "from": {"id": actor, "is_bot": False, "first_name": "User"},
            "chat": {"id": -100, "type": "supergroup", "title": "Test"},
            "message_thread_id": thread,
            "text": text,
        },
    }


async def test_shared_groups_topics_and_audit(tmp_path):
    c = config(tmp_path, allow_groups=True, per_user_groups=False, group_mentions_only=False)
    c.users = [11, 22]
    service, bot = None, Bot()
    try:
        service = Service(c, bot, factory="tests.fake_session:create")
        service.store.admit([group_update(1, "hello"), group_update(2, "again", actor=22)], c)
        await settle(service)
        route = service.store.conversation(-100, 0, 7)
        assert len(service.sessions.clients) == 1
        assert await service.last(route.key) == "turn 2: done"
        service.store.admit([group_update(3, "separate", thread=8)], c)
        await settle(service)
        assert service.store.conversation(-100, 0, 8).session != route.session
        assert all(
            kwargs.get("message_thread_id") in {7, 8}
            for chat, text, kwargs in bot.sent
            if chat == -100
        )
        assert (
            service.store.db.execute(
                "SELECT count(*) FROM messages WHERE direction='inbound'"
            ).fetchone()[0]
            == 3
        )
        assert (
            service.store.db.execute(
                "SELECT count(*) FROM messages WHERE direction='outbound'"
            ).fetchone()[0]
            >= 3
        )
    finally:
        if service:
            await service.close()


async def test_aliased_sessions_serialize_and_switch_is_authorized(tmp_path):
    c = config(tmp_path, allow_groups=True, group_mentions_only=False)
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([update(1)], c)
        await settle(service)
        source = service.store.conversation(11, 11)
        service.store.admit([group_update(2, "/switch " + str(source.id))], c)
        await settle(service)
        alias = service.store.conversation(-100, 11, 7)
        assert alias.session_file == source.session_file
        service.store.admit([update(3, "pause")], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not any(
                client.state.get("operation") == "prompt"
                for client in service.sessions.clients.values()
            ):
                client = next(iter(service.sessions.clients.values()))
                client.state = await client.request("get_state")
                await asyncio.sleep(0.01)
        service.store.admit([group_update(4, "followup")], c)
        await service.dispatch()
        await asyncio.sleep(0.05)
        assert len(service.sessions.clients) == 1
        (tmp_path / "release").touch()
        await settle(service)
        assert await service.last(alias.key) == "turn 3: done"
        assert not service.store.issues()
        other = service.store.conversation(33, 33)
        service.store.persist_route(
            other, dict(sessionId="other", sessionFile=str(tmp_path / "other.jsonl"))
        )
        service.store.admit([update(5, "/switch " + str(other.id))], c)
        await settle(service)
        assert service.store.conversation(11, 11).session == source.session
        assert service.store.issues()[-1][2] == "failed"
    finally:
        await service.close()


async def test_conversation_commands_preserve_arrival_order(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1), update(2, "/new"), update(3, "next")], c)
        await settle(service)
        replies = [text for _, text, _ in bot.sent if text.startswith(("turn", "Started new"))]
        assert replies[0] == "turn 1: done"
        assert replies[1].startswith("Started new")
        assert replies[2] == "turn 1: done"
    finally:
        await service.close()


async def test_native_passthrough_and_last_answer_selection(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "/delta /name user title")], c)
        await settle(service)
        assert service.store.conversation(11, 11).title == "user title"
        service.store.admit([update(2, "write")], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not service.cards:
                await asyncio.sleep(0.01)
        handle, card = next(iter(service.cards.items()))
        await service.callback(
            dict(
                data="a:" + handle,
                **{"from": {"id": 11}},
                message={"message_id": card["message"], "chat": {"id": 11, "type": "private"}},
            )
        )
        await settle(service)
        assert any(text == "turn 1: done" for _, text, _ in bot.sent)
        assert (tmp_path / "approved.txt").is_file()
        assert any(method == "deleteMessage" for method, _ in bot.edits)
        service.store.admit([update(3, "/delta /login")], c)
        await settle(service)
        assert any("failed" in text.lower() for _, text, _ in bot.sent)
    finally:
        await service.close()


def test_yaml_defaults_secrets_and_legacy_migration(tmp_path, monkeypatch):
    c = Config(
        users=[11],
        project=tmp_path,
        state=tmp_path / "runtime",
        name="test",
        bot_token="12345:private-fake-value",
    )
    path = tmp_path / "config.yaml"
    save(path, c)
    restored = load(path)
    assert restored.per_user_groups is False and restored.group_mentions_only is False
    assert restored.workers is None and restored.retention_days is None
    assert restored.policy == "engine"
    assert "private-fake-value" not in repr(restored)
    assert restored.token() == "12345:private-fake-value"
    monkeypatch.setenv(restored.token_env, "environment-wins")
    assert restored.token() == "environment-wins"
    document = yaml.safe_load(path.read_text())
    document["delta"]["misspelledSetting"] = True
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(ValueError):
        load(path)
    old = tmp_path / "legacy.json"
    old.write_text(
        json.dumps(dict(users=[11], project=str(tmp_path), state=str(tmp_path / "legacy")))
    )
    first, second = load(old), load(old)
    assert first.instance == second.instance
    assert first.per_user_groups and first.group_mentions_only and first.policy == "ask"
    assert first.database_path == tmp_path / "legacy" / "gateway.sqlite3"


def test_path_selection_and_model_default_clear(tmp_path, monkeypatch):
    from delta_gateway.operators import configure

    monkeypatch.chdir(tmp_path)
    configure(parser().parse_args(["init", "--name", "one", "--user", "11", "--model", "openai/a"]))
    local = tmp_path / ".delta-gateway" / "config.yaml"
    assert resolve(parser().parse_args(["-i", str(tmp_path), "status"])) == local
    configure(parser().parse_args(["configure", "telegram", "--model", "default"]))
    assert load(local).model is None and load(local).provider is None


def test_retention_disabled_by_default(tmp_path):
    c = config(tmp_path)
    store = Store(c.database_path, c.instance)
    try:
        store.admit([update(1)], c)
        store.mark(1, "completed")
        store.db.execute("UPDATE inbox SET created=0")
        store.db.commit()
        store.retain()
        assert store.db.execute("SELECT text FROM inbox").fetchone()[0] == "hello"
    finally:
        store.close()


async def populated(tmp_path, name):
    root = tmp_path / name
    root.mkdir()
    c = config(root, name=name)
    path = root / "config.json"
    save(path, c)
    register(path, c)
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([update(1)], c)
        await settle(service)
        service.store.admit([update(2, "pending")], c)
        route = service.store.conversation(11, 11)
        return c, path, route.session_file
    finally:
        await service.close()


async def test_database_consolidation_backups_id_mapping_and_resume(tmp_path):
    a, a_path, a_session = await populated(tmp_path, "alpha")
    b, b_path, b_session = await populated(tmp_path, "beta")
    destination = tmp_path / "shared" / "gateway.sqlite3"
    originals = {
        c.database_path: hashlib.sha256(c.database_path.read_bytes()).hexdigest() for c in (a, b)
    }
    migrate_configs([a_path, b_path], destination=destination, dry_run=True)
    assert not destination.exists()
    migrate_configs([a_path, b_path], destination=destination)
    for c, path, session in ((a, a_path, a_session), (b, b_path, b_session)):
        assert (
            hashlib.sha256(c.database_path.read_bytes()).hexdigest() == originals[c.database_path]
        )
        assert list(c.database_path.parent.glob("gateway.sqlite3.backup-*"))
        assert path.is_file() and path.with_suffix(".yaml").is_file()
        migrated = load(path.with_suffix(".yaml"))
        assert migrated.database_path == destination
        assert registry()[c.name]["config"] == str(path.with_suffix(".yaml"))
        store = Store(destination, c.instance)
        try:
            route = store.conversation(11, 11)
            assert route.session_file == session
            assert len(store.pending()) == 1
            assert store.offset == 3
        finally:
            store.close()
    with sqlite3.connect(destination) as db:
        ids = [row[0] for row in db.execute("SELECT id FROM routes")]
        count = db.execute("SELECT count(*) FROM messages").fetchone()[0]
        assert len(ids) == len(set(ids)) == 2
    migrate_configs([a_path, b_path], destination=destination)
    with sqlite3.connect(destination) as db:
        assert db.execute("SELECT count(*) FROM messages").fetchone()[0] == count
        assert db.execute("SELECT count(*) FROM imports").fetchone()[0] == 2
    migrated = load(a_path.with_suffix(".yaml"))
    service = Service(migrated, Bot(), factory="tests.fake_session:create")
    try:
        assert await service.last((11, 11, 0)) == "turn 1: done"
    finally:
        await service.close()


async def test_import_resume_after_config_write_failure(tmp_path, monkeypatch):
    import delta_gateway.database_import as module

    c, path, _ = await populated(tmp_path, "alpha")
    destination = tmp_path / "target.sqlite3"
    original = module.save
    monkeypatch.setattr(
        module, "save", lambda *a, **k: (_ for _ in ()).throw(OSError("write failed"))
    )
    with pytest.raises(OSError):
        migrate_configs([path], destination=destination)
    with sqlite3.connect(destination) as db:
        count = db.execute("SELECT count(*) FROM messages").fetchone()[0]
    monkeypatch.setattr(module, "save", original)
    migrate_configs([path], destination=destination)
    with sqlite3.connect(destination) as db:
        assert db.execute("SELECT count(*) FROM messages").fetchone()[0] == count
    assert load(path.with_suffix(".yaml")).database_path == destination


async def test_sdk_dispatch_and_preview_lifecycle(tmp_path):
    calls = []

    def handler(request):
        name = request.url.path.rsplit("/", 1)[-1]
        data = json.loads(request.content) if request.content else {}
        calls.append((name, data))
        if name == "getMe":
            result = {"id": 99, "is_bot": True, "first_name": "Test", "username": "testbot"}
        elif name == "sendMessage":
            result = {
                "message_id": 10,
                "date": 1,
                "chat": {"id": 11, "type": "private"},
                "text": data["text"],
            }
        else:
            result = True
        return httpx.Response(200, json={"ok": True, "result": result})

    sdk = TelegramSDK("12345:offline-test-token", transport=httpx.MockTransport(handler))
    received = []

    async def message(raw):
        received.append(raw)

    try:
        await sdk.start(message, message)
        raw = group_update(1, "hello")
        await sdk.dispatch(raw)
        assert received[0]["message"]["text"] == "hello"
        from delta_gateway.previews import Preview

        preview = Preview(sdk, (11, 11, 0))
        await preview.update("hello", force=True)
        await preview.finish()
        methods = [method for method, _ in calls]
        assert (
            methods.index("sendMessage")
            < methods.index("sendMessageDraft")
            < methods.index("deleteMessage")
        )
        assert "offline-test-token" not in repr(received)
    finally:
        await sdk.close()


async def test_sdk_does_not_retry_ambiguous_send_or_leak_token():
    count = 0

    def handler(request):
        nonlocal count
        if request.url.path.endswith("getMe"):
            return httpx.Response(
                200, json={"ok": True, "result": {"id": 99, "is_bot": True, "first_name": "Test"}}
            )
        count += 1
        raise httpx.ReadTimeout("token in URL", request=request)

    sdk = TelegramSDK("12345:private-offline-token", transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(TelegramError) as error:
            await sdk.send(11, "hello")
        assert "private-offline-token" not in str(error.value)
        assert count == 1
    finally:
        await sdk.close()


async def test_recovery_discards_stale_answers_and_waits_until_settled(tmp_path, monkeypatch):
    from delta_gateway.session_manager import SessionManager

    c = config(tmp_path)
    store = Store(c.database_path, c.instance)
    manager = SessionManager(c, store)
    route = store.conversation(11, 11)
    events = asyncio.Queue()

    class Client:
        def clear_events(self):
            pass

        async def request(self, *args, **kwargs):
            return {}

        async def next_event(self):
            return await events.get()

    client = Client()

    async def get_client(*args, **kwargs):
        return client

    async def persist(*args):
        return {}

    monkeypatch.setattr(manager, "client_for", get_client)
    monkeypatch.setattr(manager, "persist", persist)
    task = asyncio.create_task(manager.prompt(route, "hello"))
    try:
        for answer in ("stale attempt", "final answer"):
            events.put_nowait({"type": "agent_start"})
            events.put_nowait(
                {
                    "type": "message_end",
                    "message": {"role": "assistant", "content": [{"type": "text", "text": answer}]},
                }
            )
            events.put_nowait({"type": "agent_end"})
        await asyncio.sleep(0.01)
        assert not task.done()
        events.put_nowait({"type": "agent_settled", "outcome": "completed"})
        assert await task == ("final answer", "completed")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        store.close()


async def test_sdk_uploads_exported_html_as_document(tmp_path):
    received = []

    def handler(request):
        method = request.url.path.rsplit("/", 1)[-1]
        if method == "getMe":
            result = {"id": 99, "is_bot": True, "first_name": "Test"}
        else:
            received.append((request.headers.get("content-type"), request.content))
            result = {"message_id": 1, "date": 1, "chat": {"id": 11, "type": "private"}}
        return httpx.Response(200, json={"ok": True, "result": result})

    path = tmp_path / "session.html"
    path.write_text("<!doctype html><p>offline export</p>", encoding="utf-8")
    sdk = TelegramSDK("12345:offline-upload", transport=httpx.MockTransport(handler))
    try:
        await sdk.document_path(11, path)
        assert received[0][0].startswith("multipart/form-data")
        assert b"session.html" in received[0][1] and b"offline export" in received[0][1]
    finally:
        await sdk.close()
