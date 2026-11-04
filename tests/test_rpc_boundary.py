import asyncio

import pytest

from delta_gateway.computer import LocalComputer
from delta_gateway.embedded import EmbeddedClient
from delta_gateway.errors import WorkerLost
from delta_gateway.tools import computer_tools
from tests.fake_session import create
from tests.test_gateway import config


def client_for(tmp_path, **settings):
    c = config(tmp_path, policy="ask", **settings)
    return EmbeddedClient(c, computer=LocalComputer(tmp_path), provider=create, grace=2)


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


async def test_provider_failure_during_start_is_reported(tmp_path):
    def broken(config):
        raise OSError("no provider")

    client = EmbeddedClient(config(tmp_path), computer=LocalComputer(tmp_path), provider=broken)
    with pytest.raises(OSError):
        await client.start()
    with pytest.raises(WorkerLost):
        await client.request("get_state")


async def test_commands_never_see_gateway_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "12345:private-fake-value")
    computer = LocalComputer(tmp_path, {"TELEGRAM_BOT_TOKEN"})
    code, output = await computer.run(
        "python -c \"import os;print(os.environ.get('TELEGRAM_BOT_TOKEN','absent'))\"", 30
    )
    assert code == 0 and output.decode().strip() == "absent"


async def test_tools_read_write_edit_and_run_on_the_computer(tmp_path):
    read, write, edit, bash = computer_tools(LocalComputer(tmp_path))
    path = str(tmp_path / "notes" / "a.txt")
    assert (
        "Created"
        in (await write.run("1", dict(file_path=path, content="one\ntwo"))).content[0].text
    )
    assert (
        "Applied 1"
        in (await edit.run("2", dict(file_path=path, old_string="two", new_string="three")))
        .content[0]
        .text
    )
    assert "three" in (await read.run("3", dict(file_path=path))).content[0].text
    assert "absolute" in (await read.run("4", dict(file_path="relative.txt"))).content[0].text
    listing = (await bash.run("5", dict(command='python -c "print(6*7)"'))).content[0].text
    assert "42" in listing
    timed = await bash.run("6", dict(command='python -c "import time;time.sleep(5)"', timeout=200))
    assert "timed out" in timed.content[0].text
