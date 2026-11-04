"""Delta's agent loop running inside the gateway process.

The gateway builds Delta's session itself, with tools that act on the bot's computer, and
talks to Delta's own RPC server through memory instead of a child process. Commands and
events keep the shape the rest of the gateway already uses.
"""

import asyncio
import contextlib
import json
from pathlib import Path
from uuid import uuid4

from delta_gateway.errors import RequestRejected, WorkerLost
from delta_gateway.tools import computer_tools


class Output:
    """The file Delta's RPC server writes records to."""

    def __init__(self, client):
        self.client, self.buffer = client, ""

    def write(self, text):
        self.buffer += text
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            if line:
                self.client.receive(json.loads(line))

    def flush(self):
        pass


def default_provider(config):
    """The real provider, using credentials saved on the gateway's machine."""
    from delta_app.cli.main import _resolve_model, _resolve_provider_name
    from delta_app.config.config import resolve_provider
    from delta_app.config.loader import apply_config, load_model

    name = _resolve_provider_name(config.provider)
    apply_config(name)
    model = config.model or load_model(name) or _resolve_model(None, name)
    return resolve_provider(name), name, model


def system_prompt(config, tools, computer, addition):
    from delta_app.instructions import PromptSection, _environment_suffix, prompt_sections

    # Project instructions are read from the host folder only when the project is trusted.
    sections = [
        section
        for section in prompt_sections(tools=tools, cwd=str(config.project))
        if config.trust_project or section.title != "Project instructions"
    ]
    sections[-1] = PromptSection(
        "Environment", "working directory", _environment_suffix(computer.root, include_date=True)
    )
    return "\n\n".join([*(section.text for section in sections), addition])


class EmbeddedClient:
    def __init__(
        self,
        config,
        session=None,
        *,
        computer,
        addition="",
        host_definitions=(),
        provider=None,
        timeout=30,
        grace=10,
    ):
        self.config, self.session_file, self.computer = config, session, computer
        self.addition, self.definitions = addition, list(host_definitions)
        self.make_provider, self.timeout, self.grace = provider, timeout, grace
        self.server = self.lease = None
        self.pending = {}
        self.events = asyncio.Queue()
        self.failure = None
        self.state = {}

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.close()

    def receive(self, record):
        if record.get("type") == "response":
            future = self.pending.get(record.get("id"))
            if future and not future.done():
                future.set_result(record)
            return
        if record.get("type") == "agent_settled" and record.get("state"):
            self.state = record["state"]
        self.events.put_nowait(record)

    async def start(self):
        from delta_app.conversation import CodingSession
        from delta_app.host_tools import HostTools
        from delta_app.rpc import RpcServer
        from delta_app.session_lock import SessionLease

        try:
            await self.computer.start()
            provider, name, model = (self.make_provider or default_provider)(self.config)
            host = HostTools(self.definitions) if self.definitions else None
            tools = [*computer_tools(self.computer), *(host.specs() if host else [])]
            arguments = dict(
                provider=provider,
                provider_name=name,
                model=model,
                system=system_prompt(self.config, tools, self.computer, self.addition),
                tools=tools,
                tools_loader=lambda: tools,
            )
            if self.session_file:
                path = Path(self.session_file)
                self.lease = SessionLease(path.parent / "locks", path.stem)
                session = await CodingSession.resume(
                    path.stem, sessions_dir=path.parent, **arguments
                )
            else:
                session = await CodingSession.create(
                    sessions_dir=Path(self.config.sessions_path),
                    cwd=self.computer.root,
                    **arguments,
                )
                if session.session_file:
                    self.lease = SessionLease(
                        session.session_file.parent / "locks", session.session_id
                    )
            self.server = RpcServer(
                session,
                Output(self),
                lease=self.lease,
                tool_policy=self.config.policy,
                approval_seconds=self.config.approval_seconds,
                host_tools=host,
            )
            if self.config.thinking:
                await self.server.control(
                    dict(type="set_thinking_level", level=self.config.thinking)
                )
            await session.save()
            self.state = await self.request("get_state")
            return self
        except BaseException:
            await self.close()
            raise

    async def request(self, command, *, timeout="default", **arguments):
        if self.failure:
            raise self.failure
        identifier = uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[identifier] = future
        try:
            async with asyncio.timeout(self.timeout if timeout == "default" else timeout):
                if not await self.server.dispatch(dict(type=command, id=identifier, **arguments)):
                    self.fail()
                record = await future
        except TimeoutError:
            raise WorkerLost("Delta did not answer in time; work was not replayed") from None
        finally:
            self.pending.pop(identifier, None)
        if not record.get("success"):
            raise RequestRejected(
                {"code": "rejected", "message": record.get("error", "Delta rejected the request")}
            )
        return record.get("data")

    def fail(self):
        if not self.failure:
            self.failure = WorkerLost("Delta session closed; work was not replayed")
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(self.failure)
            self.events.put_nowait(None)

    def clear_events(self):
        while not self.events.empty():
            self.events.get_nowait()

    async def next_event(self):
        if self.failure and self.events.empty():
            raise self.failure
        event = await self.events.get()
        if event is None:
            raise self.failure
        return event

    async def abort(self):
        await self.request("abort")
        async with asyncio.timeout(self.grace):
            while True:
                self.state = await self.request("get_state")
                if self.state.get("operation") is None:
                    return
                await asyncio.sleep(0.01)

    async def close(self):
        server, self.server = self.server, None
        if server:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(server.close(), self.grace)
        elif self.lease:
            self.lease.close()
        self.fail()
