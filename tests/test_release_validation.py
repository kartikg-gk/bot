import asyncio
import json
import logging
import sqlite3
import sys
from pathlib import Path

import httpx
import pytest

from delta_gateway.cli import parser
from delta_gateway.config import load, save
from delta_gateway.database_import import migrate_configs
from delta_gateway.instances import OperatorError, config_lease
from delta_gateway.operators import configure, operator_command
from delta_gateway.service import Service
from delta_gateway.telegram_sdk import TelegramSDK
from delta_gateway.version_check import status
from tests.test_alignment import group_update, populated
from tests.test_gateway import Bot, config, settle, update


async def test_three_independent_conversations_can_run_by_default(tmp_path):
    c = config(tmp_path, policy="engine")
    c.users = [11, 22, 33]
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([update(n, "pause", user=user) for n, user in enumerate(c.users, 1)], c)
        await service.dispatch()
        async with asyncio.timeout(15):
            while True:
                if len(service.sessions.clients) == 3:
                    states = await asyncio.gather(
                        *(
                            client.request("get_state")
                            for client in service.sessions.clients.values()
                        )
                    )
                    if all(state.get("isStreaming") for state in states):
                        break
                await asyncio.sleep(0.02)
        (tmp_path / "release").touch()
        await settle(service)
        assert not service.store.issues()
    finally:
        await service.close()


async def test_group_approval_requires_requesting_actor_even_when_both_are_allowed(tmp_path):
    c = config(tmp_path, allow_groups=True, group_mentions_only=False, per_user_groups=False)
    c.users = [11, 22]
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([group_update(1, "write", actor=11)], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not service.cards:
                await asyncio.sleep(0.01)
        handle, card = next(iter(service.cards.items()))
        message = {
            "message_id": card["message"],
            "message_thread_id": 7,
            "chat": {"id": -100, "type": "supergroup"},
        }
        await service.callback({"data": "a:" + handle, "from": {"id": 22}, "message": message})
        assert not (tmp_path / "approved.txt").exists() and handle in service.cards
        await service.callback({"data": "a:" + handle, "from": {"id": 11}, "message": message})
        await settle(service)
        assert (tmp_path / "approved.txt").is_file()
    finally:
        await service.close()


def test_flag_configuration_does_not_prompt_in_a_terminal(tmp_path, monkeypatch):
    import delta_gateway.setup as setup

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(
        setup, "interactive", lambda *a: (_ for _ in ()).throw(AssertionError("unexpected prompt"))
    )
    configure(
        parser().parse_args(["init", "--user", "11", "--name", "flags", "--project", str(tmp_path)])
    )
    assert load(tmp_path / ".delta-gateway" / "config.yaml").name == "flags"


def test_searchable_model_picker_and_manual_fallback(tmp_path, monkeypatch):
    import delta_gateway.setup as setup

    c = config(tmp_path, provider="openai", model="old")
    monkeypatch.setattr(setup, "model_choices", lambda _: ["openrouter/org/model"])
    selected = []

    def picker(*args, **kwargs):
        selected.append(kwargs)
        return "openrouter/org/model"

    monkeypatch.setattr(setup, "prompt", picker)
    setup.choose_model(c)
    assert c.provider == "openrouter" and c.model == "org/model"
    assert selected[0]["completer"] is not None and selected[0]["default"] == "openai/old"
    monkeypatch.setattr(setup, "model_choices", lambda _: [])
    monkeypatch.setattr(setup, "ask", lambda *a: "huggingface/org/model")
    setup.choose_model(c)
    assert c.provider == "huggingface" and c.model == "org/model"
    monkeypatch.setattr(setup, "ask", lambda *a: "default")
    setup.choose_model(c)
    assert c.provider is None and c.model is None


async def test_release_status_is_cached_and_unconfigured_is_honest():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"version": "99.0"})

    url = "https://release.invalid/test-channel"
    assert "update channel not configured" in await status()
    first = await status(url, transport=httpx.MockTransport(handler))
    second = await status(url, transport=httpx.MockTransport(handler))
    assert "update available: 99.0" in first and first == second and len(calls) == 1


def test_migration_refuses_running_source_before_modifying_anything(tmp_path):
    c = config(tmp_path)
    path = tmp_path / "config.json"
    save(path, c)
    destination = tmp_path / "destination.sqlite3"
    with config_lease(c):
        with pytest.raises(OperatorError, match="Stop every affected"):
            migrate_configs([path], destination=destination)
    assert not destination.exists()


async def test_ambiguous_legacy_shared_source_is_refused(tmp_path):
    c, _, _ = await populated(tmp_path, "source")
    paths = [tmp_path / "a.json", tmp_path / "b.json"]
    for path in paths:
        path.write_text(
            json.dumps(
                dict(
                    users=[11],
                    project=str(c.project),
                    state=str(c.state),
                    database=str(c.database_path),
                )
            )
        )
    destination = tmp_path / "destination.sqlite3"
    with pytest.raises(OperatorError, match="ambiguous"):
        migrate_configs(paths, destination=destination)
    assert not destination.exists()


async def test_remove_archives_instance_and_preserves_history(tmp_path):
    c, path, session = await populated(tmp_path, "archive")
    await operator_command(parser().parse_args(["remove", "--config", str(path), "--yes"]))
    assert not path.exists() and c.database_path.is_file() and Path(session).is_file()
    with sqlite3.connect(c.database_path) as db:
        assert db.execute(
            "SELECT archived FROM instance_archive WHERE instance=?", (c.instance,)
        ).fetchone()[0]
        assert db.execute("SELECT count(*) FROM inbox").fetchone()[0] == 2


async def test_full_sdk_polling_dispatch_menu_and_lifecycle(tmp_path, caplog):
    calls, delivered, batches = [], [], 0

    async def handler(request):
        nonlocal batches
        await asyncio.sleep(0.01)
        method = request.url.path.rsplit("/", 1)[-1]
        data = json.loads(request.content) if request.content else {}
        calls.append(method)
        if method == "getMe":
            result = {"id": 99, "is_bot": True, "first_name": "Test", "username": "testbot"}
        elif method == "getWebhookInfo":
            result = {"url": "", "has_custom_certificate": False, "pending_update_count": 0}
        elif method == "getUpdates":
            batches += 1
            result = (
                [
                    {
                        "update_id": 1,
                        "message": {
                            "message_id": 1,
                            "date": 1,
                            "from": {"id": 11, "is_bot": False, "first_name": "User"},
                            "chat": {"id": 11, "type": "private"},
                            "text": "hello",
                        },
                    }
                ]
                if batches == 1
                else []
            )
        elif method == "sendMessage":
            delivered.append(data["text"])
            result = {
                "message_id": len(delivered),
                "date": 1,
                "chat": {"id": data["chat_id"], "type": "private"},
                "text": data["text"],
            }
        else:
            result = True
        return httpx.Response(200, json={"ok": True, "result": result})

    caplog.set_level(logging.INFO, logger="httpx")
    sdk = TelegramSDK("12345:private-polling-token", transport=httpx.MockTransport(handler))
    service = Service(config(tmp_path), sdk, factory="tests.fake_session:create")
    task = asyncio.create_task(service.run())
    try:
        async with asyncio.timeout(10):
            while "turn 1: done" not in delivered:
                if task.done():
                    await task
                await asyncio.sleep(0.02)
        assert "setMyCommands" in calls and "sendChatAction" in calls
        assert "Delta gateway connected." in delivered
        assert "private-polling-token" not in caplog.text
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()
    assert "Delta gateway disconnected." in delivered
