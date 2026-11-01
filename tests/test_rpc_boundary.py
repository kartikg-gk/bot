import asyncio
import sys

import pytest

from delta_gateway.errors import WorkerLost
from delta_gateway.rpc_client import RpcClient
from tests.test_gateway import config


def client_for(tmp_path, **settings):
    return RpcClient(
        config(tmp_path, policy="ask", **settings), factory="tests.fake_session:create", grace=2
    )


async def until(client, kind):
    async with asyncio.timeout(10):
        while True:
            event = await client.next_event()
            if event.get("type") == kind:
                return event


@pytest.mark.parametrize("allow", [True, False])
async def test_tool_waits_for_approval(tmp_path, allow):
    target = tmp_path / "approved.txt"
    async with client_for(tmp_path) as client:
        await client.request("prompt", message="write")
        event = await until(client, "approval_requested")
        assert not target.exists()
        await client.request("respond_to_approval", approvalId=event["approvalId"], allow=allow)
        await until(client, "agent_settled")
    assert target.exists() is allow


async def test_unanswered_approval_expires(tmp_path):
    target = tmp_path / "approved.txt"
    async with client_for(tmp_path, approval_seconds=1) as client:
        await client.request("prompt", message="write")
        resolved = await until(client, "approval_resolved")
        await until(client, "agent_settled")
    assert resolved["allow"] is False and not target.exists()


async def test_engine_exit_during_start_is_reported(tmp_path):
    client = client_for(tmp_path)
    client.command = [sys.executable, "-c", "raise SystemExit(1)"]
    with pytest.raises(WorkerLost):
        await client.start()


async def test_engine_does_not_inherit_bot_token(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "12345:private-fake-value")
    client = client_for(tmp_path)
    captured = {}
    original = asyncio.create_subprocess_exec

    async def spy(*args, **kwargs):
        captured.update(kwargs["env"])
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy)
    async with client:
        assert "TELEGRAM_BOT_TOKEN" not in captured
