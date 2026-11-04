import re
import shlex
from types import SimpleNamespace

import pytest

from delta_gateway.daytona import DaytonaComputer
from delta_gateway.tools import computer_tools
from tests.test_gateway import config


class NotFound(Exception):
    pass


class Files:
    def __init__(self, store):
        self.store = store

    async def upload_file(self, data, path):
        self.store[path] = data

    async def download_file(self, path):
        return self.store[path]


class Process:
    def __init__(self, sandbox):
        self.sandbox = sandbox

    async def exec(self, command, cwd=None, timeout=None):
        self.sandbox.commands.append(command)
        files = self.sandbox.files
        test = re.match(r"\[ -e (\S+) \] \|\| exit 3", command)
        if test:
            path = shlex.split(test.group(1))[0]
            return SimpleNamespace(exit_code=0 if path in files else 3, result="")
        exists = re.match(r"\[ -e (\S+) \] && echo existed", command)
        if exists:
            path = shlex.split(exists.group(1))[0]
            return SimpleNamespace(exit_code=0, result="existed" if path in files else "")
        return SimpleNamespace(exit_code=0, result="ran: " + command)


class Sandbox:
    def __init__(self, params):
        self.params, self.state, self.files, self.commands = params, "started", {}, []
        self.fs, self.process = Files(self.files), Process(self)

    async def start(self):
        self.state = "started"

    async def stop(self):
        self.state = "stopped"


class Client:
    sandboxes = {}

    def __init__(self, settings):
        self.settings = settings

    async def get(self, name):
        if name not in self.sandboxes:
            raise NotFound(name)
        return self.sandboxes[name]

    async def create(self, params):
        self.sandboxes[params.name] = Sandbox(params)
        return self.sandboxes[params.name]

    async def close(self):
        pass


def sdk():
    Client.sandboxes = {}
    return SimpleNamespace(
        AsyncDaytona=Client,
        DaytonaConfig=lambda **kw: kw,
        DaytonaNotFoundError=NotFound,
        CreateSandboxFromSnapshotParams=lambda **kw: SimpleNamespace(**kw),
    )


@pytest.fixture
def memory(tmp_path):
    folder = tmp_path / "memory"
    folder.mkdir()
    (folder / "KNOWLEDGE.md").write_text("known", encoding="utf-8")
    return folder


async def test_sandbox_is_created_locked_down_then_reused(tmp_path, memory, monkeypatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    fake = sdk()
    computer = DaytonaComputer("delta-bot-x", config(tmp_path), memory, sdk=fake)
    await computer.start()
    sandbox = Client.sandboxes["delta-bot-x"]
    assert sandbox.params.network_block_all is True and sandbox.params.auto_stop_interval == 15
    assert sandbox.files["/home/daytona/memory/KNOWLEDGE.md"] == b"known"
    sandbox.files["/home/daytona/memory/KNOWLEDGE.md"] = b"learned"
    await computer.stop()
    assert sandbox.state == "stopped"
    assert (memory / "KNOWLEDGE.md").read_text() == "learned"
    again = DaytonaComputer("delta-bot-x", config(tmp_path), memory, sdk=fake)
    await again.start()
    assert Client.sandboxes["delta-bot-x"] is sandbox and sandbox.state == "started"


async def test_tools_use_the_sandbox(tmp_path, memory, monkeypatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    computer = DaytonaComputer("delta-bot-y", config(tmp_path), memory, sdk=sdk())
    await computer.start()
    read, write, _, bash = computer_tools(computer)
    target = "/home/daytona/workspace/a.txt"
    assert "Created" in (await write.run("1", dict(file_path=target, content="hi"))).content[0].text
    assert "hi" in (await read.run("2", dict(file_path=target))).content[0].text
    missing = await read.run("3", dict(file_path="/home/daytona/workspace/none.txt"))
    assert "not found" in missing.content[0].text
    assert "ran: ls" in (await bash.run("4", dict(command="ls"))).content[0].text


async def test_missing_key_is_reported(tmp_path, monkeypatch):
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    computer = DaytonaComputer("delta-bot-z", config(tmp_path), sdk=sdk())
    with pytest.raises(RuntimeError, match="DAYTONA_API_KEY"):
        await computer.start()
