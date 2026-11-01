"Serial session ownership, native RPC clients, and durable conversation mappings."

import asyncio
import time
from pathlib import Path

from delta_gateway.bots import prepare
from delta_gateway.errors import RequestRejected, WorkerLost
from delta_gateway.rpc_client import RpcClient


class SessionManager:
    def __init__(self, config, store, *, factory=None):
        self.config, self.store, self.factory = config, store, factory
        self.clients = {}
        self.locks = {}
        self.touched = {}
        self.busy = set()
        self.creation = asyncio.Lock()

    def identity(self, route):
        return (
            str(Path(route.session_file).resolve()) if route.session_file else f"route:{route.id}"
        )

    def lock_for(self, route):
        return self.locks.setdefault(self.identity(route), asyncio.Lock())

    async def client_for(self, route, *, fresh_if_missing=False):
        async with self.creation:
            current = self.store.get_route(route.id)
            identity = self.identity(current)
            client = self.clients.get(identity)
            if client and not client.failure:
                return client
            if client:
                await client.close()
                self.clients.pop(identity, None)
            if current.session_file and not Path(current.session_file).is_file():
                if not fresh_if_missing:
                    raise WorkerLost("Saved session file is missing; use /new")
                current.session_file = None
            if self.config.workers and len(self.clients) >= self.config.workers:
                for old in list(self.clients):
                    if old not in self.busy:
                        await self.clients.pop(old).close()
                        break
            config = prepare(self.config, self.store.bot(current.bot))
            if current.provider:
                config.provider = current.provider
            if current.model:
                config.model = current.model
            if current.thinking:
                config.thinking = current.thinking
            client = RpcClient(
                config,
                current.session_file,
                factory=self.factory,
                grace=self.config.shutdown_timeout,
            )
            try:
                await client.start()
                self.store.persist_route(current, client.state)
            except BaseException:
                await client.close()
                raise
            self.clients[self.identity(current)] = client
            self.locks.setdefault(
                self.identity(current), self.locks.setdefault(identity, asyncio.Lock())
            )
            self.touched[self.identity(current)] = time.monotonic()
            return client

    async def persist(self, route, client):
        state = await client.request("get_state")
        self.store.persist_route(route, state)
        client.state = state
        return state

    async def prompt(
        self, route, text, *, event=None, accepted=None, before_send=None, behavior=None
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
                    content = item["message"].get("content", [])
                    text = (
                        content
                        if isinstance(content, str)
                        else "".join(
                            block.get("text", "")
                            for block in content
                            if block.get("type") == "text"
                        )
                    )
                    final = text or final
                if event:
                    await event(item, preview)
                if kind == "agent_settled":
                    outcome = item.get("outcome", "completed")
                    final = final or item.get("error", "")
                    break
            await self.persist(route, client)
            return final, outcome
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

    async def switch_bot(self, route, bot):
        async with self.lock_for(route):
            return self.store.switch_bot(route, bot)

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
