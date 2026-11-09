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

from delta_gateway.budget import ModelBudget, limited_server
from delta_gateway.computer import ManagedComputer
from delta_gateway.errors import RequestRejected, WorkerLost
from delta_gateway.tools import computer_tools
from delta_gateway.tracing import observe


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


def system_prompt(config, tools, computer, addition, skills=()):
    from delta_app.instructions import PromptSection, _environment_suffix, prompt_sections

    if config.web_auth == "google":
        from delta_app.instructions import _BASE_IDENTITY, _skill_section, _tool_guidelines

        return "\n\n".join(
            filter(
                None,
                (
                    _BASE_IDENTITY,
                    _tool_guidelines(tools),
                    _skill_section(skills),
                    _environment_suffix(computer.root, include_date=True),
                    addition,
                ),
            )
        )

    # Project instructions are read from the host folder only when the project is trusted.
    sections = [
        section
        for section in prompt_sections(tools=tools, cwd=str(config.project), skills=skills)
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
        plugin_definitions=None,
        provider=None,
        budget_store=None,
        skill_directory=None,
        review_durable=True,
        timeout=30,
        grace=10,
    ):
        self.config, self.session_file = config, session
        self.owns_computer = not isinstance(computer, ManagedComputer)
        self.computer = (
            computer if isinstance(computer, ManagedComputer) else ManagedComputer(computer)
        )
        self.addition, self.definitions = addition, list(host_definitions)
        self.plugin_definitions = plugin_definitions
        self.make_provider, self.timeout, self.grace = provider, timeout, grace
        self.review_durable = review_durable
        self.durable_gate = None
        self.skill_directory = skill_directory
        self.budget = ModelBudget(config, budget_store) if budget_store is not None else None
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
        observe(self, record)
        if record.get("type") == "response":
            future = self.pending.get(record.get("id"))
            if future and not future.done():
                future.set_result(record)
            return
        if record.get("type") == "agent_settled" and record.get("state"):
            self.state = record["state"]
        self.events.put_nowait(record)

    def plugin_loader(self, host, tools, initial):
        from delta_app.tools._shared import ToolSpec, fault, text_outcome

        loaded = set()

        async def run(_call_id, arguments, signal=None, on_update=None):
            name = arguments.get("name")
            current = self.plugin_definitions()
            if not isinstance(name, str) or name not in current:
                return fault("App is not connected and enabled for this bot")
            if name not in loaded:
                definitions = current[name]
                host.definitions.extend(definitions)
                tools.extend(host.specs()[-len(definitions) :])
                read_only = {
                    entry["name"] for entry in definitions if entry.get("approval") is False
                }
                host.no_approval.update(read_only)
                if self.server.approvals:
                    self.server.approvals.exempt.update(read_only)
                self.server.session._all_tools = list(tools)
                self.server.session._apply_plan_mode()
                loaded.add(name)
            names = ", ".join(entry["name"] for entry in current[name])
            return text_outcome(f"Loaded {name}. Available tools are {names}.")

        return ToolSpec(
            name="load_plugin",
            label="Load connected app",
            description="Load tool schemas for a connected app. Available apps: "
            + ", ".join(initial),
            parameters={
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
                "additionalProperties": False,
            },
            run=run,
        )

    async def start(self):
        from delta_app.conversation import CodingSession
        from delta_app.host_tools import HostTools
        from delta_app.rpc import RpcServer
        from delta_app.session_lock import SessionLease

        try:
            provider, name, model = (self.make_provider or default_provider)(self.config)
            if self.budget is not None:
                provider = self.budget.wrap(provider, name)
            available = self.plugin_definitions() if self.plugin_definitions else {}
            host = HostTools(list(self.definitions)) if self.definitions or available else None
            tools = [*computer_tools(self.computer), *(host.specs() if host else [])]
            if available:
                host.no_approval.add("load_plugin")
                tools.append(self.plugin_loader(host, tools, available))
            from delta_gateway.bot_skills import load_bot_skills, skill_reader

            skills = load_bot_skills(self.config, self.skill_directory)
            reader = skill_reader(skills)
            if reader is not None:
                tools.append(reader)
            arguments = dict(
                provider=provider,
                provider_name=name,
                model=model,
                system=system_prompt(self.config, tools, self.computer, self.addition, skills),
                skills_loader=lambda: skills,
                tools=tools,
                tools_loader=lambda: tools,
            )
            if self.session_file:
                path = Path(self.session_file)
                self.lease = SessionLease(path.parent / "locks", path.stem)
                session = await CodingSession.resume(
                    path.stem,
                    sessions_dir=path.parent,
                    keep_model=self.config.web_auth == "google",
                    **arguments,
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
            server_type = limited_server(RpcServer, self.budget) if self.budget else RpcServer
            self.server = server_type(
                session,
                Output(self),
                lease=self.lease,
                tool_policy=(
                    "deny"
                    if self.config.policy == "ask" and not self.review_durable
                    else self.config.policy
                ),
                approval_seconds=self.config.approval_seconds,
                host_tools=host,
            )
            from delta_gateway.durable import install_gate

            self.durable_gate = install_gate(
                self.server, self.computer, self.config, self.review_durable, self.skill_directory
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
        if command == "set_model" and self.config.web_auth == "google":
            raise RequestRejected(
                {"code": "account_model", "message": "Change the model in Account settings"}
            )
        if self.failure:
            raise self.failure
        if command == "respond_to_approval" and self.durable_gate is not None:
            if arguments.get("approvalId") in self.durable_gate.pending:
                self.durable_gate.respond(arguments.get("approvalId"), arguments.get("allow"))
                return None
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
        if self.owns_computer:
            with contextlib.suppress(Exception):
                await self.computer.stop()
        self.fail()
