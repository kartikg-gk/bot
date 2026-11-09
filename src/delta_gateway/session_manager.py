"Serial session ownership, native RPC clients, and durable conversation mappings."

import asyncio
import importlib
import time
from contextvars import ContextVar
from pathlib import Path

from delta_gateway.bot_profiles import (
    STYLES,
    bot_operation,
    profile,
    record_chat,
    remember_session,
    saved_session,
)
from delta_gateway.bots import MEMORY_MOUNT, bot_root, prepare, workspace
from delta_gateway.computer import DockerComputer, LocalComputer, ManagedComputer
from delta_gateway.embedded import EmbeddedClient
from delta_gateway.errors import RequestRejected, WorkerLost
from delta_gateway.plugins import PLUGINS

DURABLE_REVIEW = ContextVar("durable_review", default=True)


def secret_variables(config):
    from delta_app.catalog import available_providers

    from delta_gateway.instances import token_variables

    keys = {provider.key_env for provider in available_providers() if provider.key_env}
    return token_variables(config) | keys


def assistant_text(message):
    content = message.get("content", [])
    if isinstance(content, str):
        return content
    return "".join(block.get("text", "") for block in content if block.get("type") == "text")


class SessionManager:
    def __init__(self, config, store, *, factory=None):
        self.config, self.store, self.factory = config, store, factory
        self.plugin_available = lambda name: (
            config.web_auth != "google" and PLUGINS[name].configured()
        )
        self.clients = {}
        self.locks = {}
        self.touched = {}
        self.busy = set()
        self.creation = asyncio.Lock()
        self.computers = {}
        self.bot_locks = {}
        self.active_bots = set()
        if isinstance(factory, str):
            module, _, name = factory.partition(":")
            self.factory = getattr(importlib.import_module(module), name)

    def identity(self, route):
        return (
            str(Path(route.session_file).resolve()) if route.session_file else f"route:{route.id}"
        )

    def lock_for(self, route):
        return self.locks.setdefault(self.identity(route), asyncio.Lock())

    def computer_for(self, bot):
        """One computer per bot, kept between runs so its files and installs stay."""
        if bot.id in self.computers:
            return self.computers[bot.id]
        name = f"delta-bot-{self.config.instance[:8]}-{bot.id[:12]}"
        memory = bot_root(self.config, bot) / "memory"
        if self.config.executor == "docker":
            computer = DockerComputer(
                name,
                self.config.docker_image,
                workspace(self.config, bot),
                memory=self.config.docker_memory,
                mounts=[(memory, MEMORY_MOUNT + ":ro")],
            )
            computer.memory_path = MEMORY_MOUNT
        elif self.config.executor == "daytona":
            from delta_gateway.daytona import DaytonaComputer

            computer = DaytonaComputer(name, self.config, memory)
        else:
            computer = LocalComputer(workspace(self.config, bot), secret_variables(self.config))
            computer.memory_path = str(memory)
        computer = ManagedComputer(computer)
        self.computers[bot.id] = computer
        return computer

    def enabled_plugins(self, bot):
        return tuple(
            name
            for name in self.store.plugins_for(bot.id)
            if name in PLUGINS and self.plugin_available(name)
        )

    def signature(self, bot):
        settings = profile(self.store, bot)
        from delta_gateway.bot_skills import load_bot_skills
        from delta_gateway.skill_store import NOTES

        root = bot_root(self.config, bot)
        notes = tuple(
            (root / "memory" / filename).read_bytes()
            if (root / "memory" / filename).is_file()
            else b""
            for filename in NOTES
        )
        skills = tuple(
            (s.name, s.body, s.description, s.disable_model_invocation, str(s.source))
            for s in load_bot_skills(self.config, root / "skills")
        )
        revision = getattr(self.factory, "revision", lambda: None)()
        return (
            bot.role,
            bot.instructions,
            settings["style"],
            settings["voice"],
            notes,
            skills,
            revision,
            self.enabled_plugins(bot),
        )

    def client(self, bot, session_file=None, *, policy=None, overrides=None):
        computer = self.computer_for(bot)
        prepared = prepare(self.config, bot, memory_path=computer.memory_path)

        def plugin_definitions():
            return {name: PLUGINS[name].tools for name in self.enabled_plugins(bot)}

        settings = profile(self.store, bot)
        prepared.addition += "\n\nSpeaking style\n" + STYLES[settings["style"]]
        if settings["style"] == "custom":
            prepared.addition += "\n" + settings["voice"]
        config = prepared.config
        for name, value in (overrides or {}).items():
            if value:
                setattr(config, name, value)
        if policy:
            config.policy = policy
        client = EmbeddedClient(
            config,
            session_file,
            computer=computer,
            addition=prepared.addition,
            host_definitions=prepared.definitions,
            plugin_definitions=plugin_definitions,
            provider=self.factory,
            budget_store=self.store,
            skill_directory=bot_root(self.config, bot) / "skills",
            review_durable=DURABLE_REVIEW.get(),
            grace=self.config.shutdown_timeout,
        )
        client.bot_signature = self.signature(bot)
        return client

    async def client_for(self, route, *, fresh_if_missing=False):
        async with self.creation:
            current = self.store.get_route(route.id)
            original_identity = self.identity(current)
            if current.session_file and not Path(current.session_file).is_file():
                if not fresh_if_missing:
                    raise WorkerLost("Saved session file is missing; use /new")
                current.session_file = None
            identity = self.identity(current)
            bot = self.store.bot(current.bot)
            client = self.clients.get(identity)
            if client and not client.failure and client.bot_signature == self.signature(bot):
                self.store.persist_route(current, client.state)
                return client
            if client:
                await client.close()
                self.clients.pop(identity, None)
            if self.config.workers and len(self.clients) >= self.config.workers:
                for old in list(self.clients):
                    if old not in self.busy:
                        await self.clients.pop(old).close()
                        break
            client = self.client(
                bot,
                current.session_file,
                overrides=dict(
                    provider=current.provider, model=current.model, thinking=current.thinking
                ),
            )
            try:
                await client.start()
                self.store.persist_route(current, client.state)
            except BaseException:
                await client.close()
                raise
            self.clients[self.identity(current)] = client
            self.locks.setdefault(
                self.identity(current), self.locks.setdefault(original_identity, asyncio.Lock())
            )
            self.touched[self.identity(current)] = time.monotonic()
            return client

    @bot_operation
    async def delegate(
        self, bot, task, event=None, policy=None, *, review_durable=None, persist=True
    ):
        """Resume a bot's saved work and return its answer."""
        bot = self.store.bot(bot.id) or bot
        review_token = DURABLE_REVIEW.set(
            DURABLE_REVIEW.get() if review_durable is None else review_durable
        )
        client = None
        try:
            for identity, cached in list(self.clients.items()):
                if cached.computer is self.computer_for(bot):
                    await cached.close()
                    self.clients.pop(identity, None)
            client = self.client(
                bot, saved_session(self.store, bot.id) if persist else None, policy=policy
            )
            await client.start()
            if persist:
                record_chat(self.store, bot.id, "user", task)
            await client.request("prompt", message=task)
            final = ""
            while True:
                item = await client.next_event()
                kind = item.get("type")
                if kind == "message_end" and item.get("message", {}).get("role") == "assistant":
                    final = assistant_text(item["message"]) or final
                if kind == "agent_settled":
                    answer = item.get("error") or final
                    if persist:
                        remember_session(self.store, bot.id, await client.request("get_state"))
                        record_chat(self.store, bot.id, "assistant", answer)
                    return answer, item.get("outcome", "completed")
                if event:
                    await event(client, item)
        finally:
            try:
                if client is not None:
                    await client.close()
            finally:
                DURABLE_REVIEW.reset(review_token)

    async def persist(self, route, client):
        state = await client.request("get_state")
        self.store.persist_route(route, state)
        client.state = state
        remember_session(self.store, route.bot, state)
        return state

    @bot_operation
    async def prompt(
        self,
        route,
        text,
        *,
        event=None,
        accepted=None,
        before_send=None,
        behavior=None,
        abort_on_error=False,
    ):
        # Resolve aliases before locking, then recheck after a session-changing operation.
        while True:
            route = self.store.get_route(route.id)
            identity = self.identity(route)
            lock = self.lock_for(route)
            await lock.acquire()
            latest = self.store.get_route(route.id)
            if self.identity(latest) == identity:
                route = latest
                break
            lock.release()
        sent = False
        try:
            self.busy.add(identity)
            client = await self.client_for(route)
            self.busy.add(self.identity(self.store.get_route(route.id)))
            client.clear_events()
            arguments = dict(message=text)
            if behavior:
                arguments["streamingBehavior"] = behavior
            if before_send:
                before_send()
            record_chat(self.store, route.bot, "user", text)
            sent = True
            await client.request("prompt", **arguments)
            if accepted:
                accepted()
            final, preview, outcome = "", "", "completed"
            while True:
                item = await client.next_event()
                kind = item.get("type")
                if kind == "agent_start":
                    final, preview = "", ""
                elif kind == "message_start" and item.get("message", {}).get("role") == "assistant":
                    preview = ""
                elif kind == "message_update":
                    delta = item.get("assistantMessageEvent") or {}
                    if delta.get("type") == "text_delta":
                        preview = (preview + delta.get("delta", ""))[:4096]
                elif kind == "message_end" and item.get("message", {}).get("role") == "assistant":
                    final = assistant_text(item["message"]) or final
                if event:
                    await event(item, preview)
                if kind == "agent_settled":
                    outcome = item.get("outcome", "completed")
                    final = item.get("error") or final
                    break
            await self.persist(route, client)
            record_chat(self.store, route.bot, "assistant", final)
            return final, outcome
        except (asyncio.CancelledError, Exception):
            if sent and abort_on_error:
                await self.abort(route)
            raise
        finally:
            self.busy.discard(identity)
            current = self.store.get_route(route.id)
            self.busy.discard(self.identity(current))
            self.touched[self.identity(current)] = time.monotonic()
            lock.release()

    async def forward(self, route, command, text):
        """Deliver to a running prompt; False means nothing was delivered."""
        identity = self.identity(self.store.get_route(route.id))
        client = self.clients.get(identity)
        if identity not in self.busy or not client or client.failure:
            return False
        try:
            await client.request(command, message=text, requireActive=True)
        except RequestRejected:
            return False
        return True

    @bot_operation
    async def control(self, route, command, *, before_send=None, **arguments):
        route = self.store.get_route(route.id)
        async with self.lock_for(route):
            identity = self.identity(route)
            self.busy.add(identity)
            try:
                client = await self.client_for(route, fresh_if_missing=command == "new_session")
                old_identity = self.identity(self.store.get_route(route.id))
                if before_send:
                    before_send()
                data = await client.request(command, timeout=None, **arguments)
                state = await self.persist(route, client)
                new_identity = self.identity(self.store.get_route(route.id))
                self.clients.pop(old_identity, None)
                self.clients[new_identity] = client
                if command in {"new_session", "clone"}:
                    # Existing aliases still refer to their old file and resume it independently.
                    self.locks.setdefault(new_identity, asyncio.Lock())
                return data, state
            finally:
                self.busy.discard(identity)
                current = self.store.get_route(route.id)
                self.touched[self.identity(current)] = time.monotonic()

    async def state(self, route):
        client = self.clients.get(self.identity(route))
        if client and not client.failure:
            return await client.request("get_state"), await client.request("get_session_stats")
        return dict(
            sessionId=route.session,
            sessionFile=route.session_file,
            sessionName=route.title,
            model=dict(provider=route.provider, id=route.model),
            thinkingLevel=route.thinking or "off",
            isStreaming=False,
        ), {}

    async def abort(self, route):
        client = self.clients.get(self.identity(self.store.get_route(route.id)))
        if client and not client.failure:
            try:
                await client.abort()
            except (RuntimeError, TimeoutError):
                await client.close()
                self.clients.pop(self.identity(route), None)

    async def switch(self, route, source):
        # Commands only switch an idle route; aliases to an active source serialize later.
        async with self.lock_for(route):
            if not source.session_file or not Path(source.session_file).is_file():
                raise ValueError("That conversation has no saved session")
            old = self.identity(route)
            route = self.store.point(route, source)
            if old != self.identity(route) and old in self.clients:
                if not any(
                    self.identity(self.store.get_route(row[0])) == old
                    for row in self.store.db.execute(
                        "SELECT id FROM routes WHERE instance=?", (self.store.instance,)
                    )
                ):
                    await self.clients.pop(old).close()
            return route

    async def switch_bot(self, route, bot, *, resume_bot=False):
        async with self.lock_for(route):
            route = self.store.switch_bot(route, bot)
            session = saved_session(self.store, bot.id) if resume_bot else None
            if session:
                with self.store.db:
                    self.store.db.execute(
                        "UPDATE routes SET session_file=?,session=NULL,title=NULL,"
                        "provider=NULL,model=NULL,thinking=NULL WHERE instance=? AND id=?",
                        (session, self.store.instance, route.id),
                    )
                route = self.store.get_route(route.id)
            return route

    async def remove_computer(self, bot):
        from delta_gateway.bot_profiles import reserve

        with reserve(self, bot.id):
            await self.restart_idle(bot.id)
            computer = self.computer_for(bot)
            await computer.remove()
            self.computers.pop(bot.id, None)

    async def restart_idle(self, bot_id=None):
        """Close idle engines so changed tool grants apply on their next start."""
        for identity, client in list(self.clients.items()):
            if bot_id is not None and client.computer is not self.computer_for(
                self.store.bot(bot_id)
            ):
                continue
            if identity not in self.busy:
                await client.close()
                self.clients.pop(identity, None)

    async def cleanup(self):
        now = time.monotonic()
        for identity, client in list(self.clients.items()):
            if (
                identity not in self.busy
                and now - self.touched.get(identity, now) > self.config.idle_seconds
            ):
                await client.close()
                self.clients.pop(identity, None)

    async def close(self):
        await asyncio.gather(
            *(client.close() for client in self.clients.values()), return_exceptions=True
        )
        self.clients.clear()
        await asyncio.gather(
            *(computer.stop() for computer in self.computers.values()), return_exceptions=True
        )
