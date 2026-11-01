"Telegram command handlers using durable intake and a native session manager."

import asyncio
import contextlib
import os
import time
from pathlib import Path
from uuid import uuid4

from delta_gateway.client import RequestRejected, WorkerLost
from delta_gateway.commands import split
from delta_gateway.previews import Preview, typing
from delta_gateway.routing import payload
from delta_gateway.session_manager import SessionManager
from delta_gateway.store import Store
from delta_gateway.telegram import TelegramError, chunks
from delta_gateway.version_check import status as version_status

HELP = (
    "/status /new /name <name> /sessions /switch <id> /clone\n"
    "/compact [instructions] /export /last /stop\n"
    "/models /model <provider/model-id> /thinking <level>\n"
    "/queue <text> /steer <text> /delta <text or command>"
)


class Service:
    def __init__(self, config, telegram, *, factory=None):
        self.config, self.telegram = config, telegram
        self.store = Store(
            config.database_path, config.instance, retention_days=config.retention_days
        )
        self.store.activate()
        self.sessions = SessionManager(config, self.store, factory=factory)
        self.jobs, self.cards, self.previews = {}, {}, {}
        self.slots = asyncio.Semaphore(config.workers or 2**30)
        self.dispatch_lock = asyncio.Lock()
        self.stopping = False
        self.on_ready = None

    @property
    def clients(self):
        return {
            route.key: self.sessions.clients[self.sessions.identity(route)]
            for row in self.store.db.execute(
                "SELECT id FROM routes WHERE instance=?", (self.config.instance,)
            )
            if (route := self.store.get_route(row[0]))
            and self.sessions.identity(route) in self.sessions.clients
        }

    async def notify(self, key, text, **kwargs):
        return await self.telegram.send(key[0], text, **payload(key), **kwargs)

    async def reply(self, route, text):
        self.store.log_message(route, text)
        for piece in chunks(text, 3900):
            await self.notify(route.key, piece)

    async def card(self, key, client, event, *, actor=None):
        identifier = event.get("approvalId", event.get("approval_id"))
        normalized = dict(
            approval_id=identifier,
            run_id=event.get("run_id", "native"),
            tool=event["tool"],
            preview=event["preview"],
            digest=event["digest"],
            expires_at=event.get("expiresAt", event.get("expires_at")),
        )
        self.store.audit(normalized, key[0], actor or key[1], key[2])
        handle = uuid4().hex
        card = dict(key=key, actor=actor or key[1], client=client, event=normalized, message=None)
        self.cards[handle] = card
        try:
            text = (
                f"Approve once? Tool: {normalized['tool']}\n"
                f"Project: {self.config.project}\n{normalized['preview']}"
            )
            if len(text.encode("utf-16-le")) // 2 > 3900:
                raise ValueError("Approval preview is too large")
            message = await self.notify(
                key,
                text,
                reply_markup={
                    "inline_keyboard": [
                        [
                            {"text": "Approve once", "callback_data": "a:" + handle},
                            {"text": "Deny", "callback_data": "d:" + handle},
                        ]
                    ]
                },
            )
            card["message"] = message["message_id"]
        except (TelegramError, ValueError):
            with contextlib.suppress(RuntimeError):
                await client.request("respond_to_approval", approvalId=identifier, allow=False)
            self.store.decide(identifier, False, "delivery_failed")
            self.cards.pop(handle, None)

    async def resolve_card(self, handle, allow, source):
        card = self.cards.pop(handle, None)
        if not card:
            return
        self.store.decide(card["event"]["approval_id"], allow, source)
        if card["message"] is not None and not self.stopping:
            with contextlib.suppress(TelegramError):
                await self.telegram.call(
                    "editMessageText",
                    chat_id=card["key"][0],
                    message_id=card["message"],
                    text="Approved once" if allow else "Denied / expired",
                    reply_markup={"inline_keyboard": []},
                )

    async def callback(self, callback):
        action, _, handle = callback.get("data", "").partition(":")
        card = self.cards.get(handle)
        if not card or action not in {"a", "d"}:
            return
        message, actor, key = callback.get("message", {}), card["actor"], card["key"]
        if (
            callback.get("from", {}).get("id") != actor
            or actor not in self.config.users
            or message.get("chat", {}).get("id") != key[0]
            or (message.get("message_thread_id") or 0) != key[2]
            or message.get("message_id") != card["message"]
        ):
            return
        kind = message.get("chat", {}).get("type")
        if kind != "private" and not (
            kind in {"group", "supergroup"}
            and self.config.allow_groups
            and (not self.config.groups or key[0] in self.config.groups)
        ):
            return
        allow = action == "a" and time.time() < card["event"]["expires_at"]
        if callback.get("id"):
            with contextlib.suppress(TelegramError):
                await self.telegram.call(
                    "answerCallbackQuery",
                    callback_query_id=callback["id"],
                    text="Decision received",
                )
        try:
            await card["client"].request(
                "respond_to_approval", approvalId=card["event"]["approval_id"], allow=allow
            )
        except RuntimeError:
            allow = False
        await self.resolve_card(handle, allow, "user")

    async def prompt(self, item, text=None, behavior=None):
        route = self.store.conversation(*item.key)
        started = False
        preview = Preview(self.telegram, item.key, self.config.draft_previews)
        self.previews[item.key] = preview
        activity = asyncio.create_task(typing(self.telegram, item.key))

        def before_send():
            nonlocal started
            started = True
            self.store.mark(item.id, "dispatching")

        async def event(record, partial):
            kind = record.get("type")
            if kind in {"agent_start", "message_start", "message_update"}:
                await preview.update(partial)
            elif kind in {"tool_execution_start", "tool_execution_end"}:
                name = "".join(
                    c for c in record.get("toolName", "")[:60] if c.isalnum() or c in "_-"
                )
                await preview.update("Tool: " + name if kind.endswith("start") else partial)
            elif kind == "approval_requested":
                client = self.sessions.clients[
                    self.sessions.identity(self.store.get_route(route.id))
                ]
                await self.card(item.key, client, record, actor=item.actor)
            elif kind == "approval_resolved":
                for handle, card in list(self.cards.items()):
                    if card["event"]["approval_id"] == record["approvalId"]:
                        await self.resolve_card(handle, record["allow"], "worker")

        try:
            async with self.slots:
                if self.stopping:
                    return
                await preview.update("Working…", force=True)
                final, outcome = await self.sessions.prompt(
                    route,
                    item.text if text is None else text,
                    event=event,
                    before_send=before_send,
                    accepted=lambda: self.store.mark(item.id, "running"),
                    behavior=behavior,
                )
                self.store.mark(item.id, "delivering")
                final = final or f"Run {outcome}; no assistant text returned."
                if outcome != "completed" and final:
                    final += f"\n[Run {outcome}]"
                try:
                    await self.reply(route, final)
                    self.store.mark(item.id, outcome)
                except TelegramError:
                    self.store.mark(item.id, "delivery_failed")
        except asyncio.CancelledError:
            if started:
                self.store.mark(item.id, "uncertain")
            raise
        except (RuntimeError, OSError, ValueError):
            self.store.mark(item.id, "uncertain" if started else "failed")
            with contextlib.suppress(RuntimeError):
                await self.sessions.abort(route)
            with contextlib.suppress(TelegramError):
                await self.notify(
                    item.key,
                    "Task interrupted or could not start. It was not replayed. Check /status.",
                )
        finally:
            activity.cancel()
            await asyncio.gather(activity, return_exceptions=True)
            for handle, card in list(self.cards.items()):
                if card["key"] == item.key:
                    await self.resolve_card(handle, False, "run_closed")
            await preview.finish()
            self.previews.pop(item.key, None)

    async def last(self, key):
        route = self.store.conversation(*key)
        result, _ = await self.sessions.control(route, "get_last_assistant_text")
        return result.get("text") or "No assistant message yet."

    async def stop(self, item, route):
        self.store.clear_route_queue(item.key)
        task = self.jobs.get(item.key)
        if task and not task.done():
            task.cancel()
        await self.sessions.abort(route)
        if task:
            await asyncio.gather(task, return_exceptions=True)
            self.jobs.pop(item.key, None)
        for handle, card in list(self.cards.items()):
            if card["key"] == item.key:
                await self.resolve_card(handle, False, "stopped")
        return "Abort requested; queued prompts cleared."

    async def command(self, item):
        route = self.store.conversation(*item.key)
        self.store.mark(item.id, "dispatching")
        try:
            command, argument = split(item.text)
            if item.status == "rejected":
                reply = "Prompt queue is full; try later."
            elif command in {"/start", "/help"}:
                reply = HELP
            elif command == "/stop":
                reply = await self.stop(item, route)
            elif command == "/sessions":
                lines = ["Known sessions:"]
                for row in self.store.list_routes(item.actor, groups=self.config.allow_groups):
                    marker = "*" if row.id == route.id else " "
                    lines.append(
                        f"{marker} {row.id}: {row.title or '(unnamed)'} {row.session or ''}\n"
                        f"   {row.session_file or '(no file yet)'}"
                    )
                lines.append("Use /switch <id> to select a known session.")
                reply = "\n".join(lines)
            elif command == "/status":
                client = await self.sessions.client_for(route)
                state, stats = (
                    await client.request("get_state"),
                    await client.request("get_session_stats"),
                )
                model = state.get("model") or {}
                reply = (
                    f"Session: {state.get('sessionName') or '(unnamed)'}\n"
                    f"ID: {state.get('sessionId')}\nFile: {state.get('sessionFile')}\n"
                    f"Model: {model.get('provider')}/{model.get('id')}\n"
                    f"Thinking: {state.get('thinkingLevel')}\n"
                    f"Delta generating now: {state.get('isStreaming')}\n"
                    f"Operation: {state.get('operation') or 'idle'}\nTelegram draft preview: "
                    + (
                        self.previews[item.key].mode
                        if item.key in self.previews
                        else "available (private chat)"
                        if item.chat > 0 and self.config.draft_previews
                        else "unavailable"
                    )
                    + f"\nMessages: {stats.get('totalMessages')}\nCost: {stats.get('cost')}\n"
                    f"Context: {stats.get('contextUsage', {}).get('percent')}%\n"
                    + await version_status(self.config.release_url)
                )
                if any(
                    (row[1], self.config.route(row[1], row[3], row[4])[1], row[4]) == item.key
                    for row in self.store.issues()
                ):
                    reply += "\nInterrupted work exists; it was not replayed."
            else:
                reply = "Unknown command. Use /help."
            await self.reply(route, reply)
            self.store.mark(item.id, "completed")
        except (ValueError, RuntimeError, OSError) as exc:
            self.store.mark(
                item.id, "delivery_failed" if isinstance(exc, TelegramError) else "failed"
            )
            with contextlib.suppress(TelegramError):
                await self.notify(
                    item.key,
                    str(exc)
                    if isinstance(exc, (RequestRejected, WorkerLost))
                    else "Invalid command or local configuration. Use /help.",
                )

    async def control(self, item):
        route = self.store.conversation(*item.key)
        started = False

        def before_send():
            nonlocal started
            started = True
            self.store.mark(item.id, "dispatching")

        try:
            async with self.slots:
                command, argument = split(item.text)
                if command == "/switch":
                    target = self.store.get_route(int(argument))
                    if not target or target.user not in {0, item.actor}:
                        raise ValueError("Unknown conversation or another user's session")
                    if target.chat < 0 and (
                        not self.config.allow_groups
                        or (self.config.groups and target.chat not in self.config.groups)
                    ):
                        raise ValueError("Conversation is outside the allowed groups")
                    before_send()
                    await self.sessions.switch(route, target)
                    reply = f"Switched this chat to session from conversation {target.id}."
                elif command == "/last":
                    before_send()
                    reply = await self.last(item.key)
                else:
                    operations = {
                        "/new": ("new_session", {}),
                        "/clone": ("clone", {}),
                        "/name": ("set_session_name", {"name": argument}),
                        "/compact": ("compact", {"customInstructions": argument}),
                        "/export": ("export_html", {}),
                        "/models": ("get_available_models", {}),
                        "/thinking": ("set_thinking_level", {"level": argument}),
                    }
                    if command == "/model":
                        provider, separator, model = argument.partition("/")
                        if not separator or not provider or not model:
                            raise ValueError("Use /model <provider/model-id>")
                        operation, arguments = "set_model", dict(provider=provider, modelId=model)
                    else:
                        operation, arguments = operations[command]
                    if command == "/thinking" and argument not in {
                        "off",
                        "minimal",
                        "low",
                        "medium",
                        "high",
                        "xhigh",
                    }:
                        raise ValueError("Use /thinking <off|minimal|low|medium|high|xhigh>")
                    if command == "/name" and not argument:
                        raise ValueError("Use /name <name>")
                    if command in {"/new", "/clone", "/export"} and argument:
                        raise ValueError("This command takes no arguments")
                    data, state = await self.sessions.control(
                        route, operation, before_send=before_send, **arguments
                    )
                    if command == "/export":
                        path = Path(data["path"])
                        if hasattr(self.telegram, "document_path"):
                            await self.telegram.document_path(item.chat, path, **payload(item.key))
                        else:
                            await self.telegram.document(
                                item.chat,
                                path.name,
                                path.read_text(encoding="utf-8"),
                                "text/html",
                                **payload(item.key),
                            )
                        reply = f"Exported session: {path}"
                    elif command in {"/new", "/clone"}:
                        reply = (
                            "Started new" if command == "/new" else "Cloned"
                        ) + f" session.\nID: {state['sessionId']}\nFile: {state['sessionFile']}"
                    elif command == "/models":
                        reply = "Available models:\n" + "\n".join(
                            f"{m['provider']}/{m['id']}" for m in data["models"][:50]
                        )
                    elif command == "/compact":
                        reply = (
                            f"Compacted. Tokens before: {data.get('tokensBefore', 'unknown')}\n\n"
                            f"{data.get('summary', '')[:2500]}"
                        )
                    elif command == "/name":
                        reply = "Named session: " + argument
                    elif command == "/model":
                        reply = "Model set to " + argument
                    else:
                        reply = "Thinking level set to " + argument
                self.store.mark(item.id, "delivering")
                await self.reply(route, reply)
                self.store.mark(item.id, "completed")
        except asyncio.CancelledError:
            if started:
                self.store.mark(item.id, "uncertain")
            raise
        except (ValueError, RuntimeError, OSError, KeyError) as exc:
            self.store.mark(
                item.id, "delivery_failed" if isinstance(exc, TelegramError) else "failed"
            )
            with contextlib.suppress(TelegramError):
                await self.notify(
                    item.key,
                    str(exc)
                    if isinstance(exc, (ValueError, RequestRejected, WorkerLost))
                    else "Command failed; inspect local configuration.",
                )

    async def dispatch(self):
        async with self.dispatch_lock:
            for key, job in list(self.jobs.items()):
                if job.done():
                    await asyncio.gather(job, return_exceptions=True)
                    self.jobs.pop(key, None)
            for item in self.store.items():
                if not self.store.is_pending(item.id):
                    continue
                command, argument = split(item.text)
                is_command = item.status != "prompt" and item.text.startswith("/")
                if item.status == "rejected" or (
                    is_command and command in {"/start", "/help", "/stop", "/status", "/sessions"}
                ):
                    await self.command(item)
                elif is_command and command in {"/queue", "/steer", "/delta"}:
                    if not argument:
                        self.store.mark(item.id, "failed")
                        with contextlib.suppress(TelegramError):
                            await self.notify(item.key, f"Use {command} <text>")
                    elif item.key not in self.jobs:
                        self.store.replace_prompt(item.id, argument)
                        item.text, item.status = argument, "prompt"
                        self.jobs[item.key] = asyncio.create_task(
                            self.prompt(
                                item,
                                behavior="steer"
                                if command == "/steer"
                                else "followUp"
                                if command == "/queue"
                                else None,
                            )
                        )
                elif is_command:
                    if command not in {
                        "/new",
                        "/clone",
                        "/name",
                        "/compact",
                        "/export",
                        "/last",
                        "/model",
                        "/models",
                        "/thinking",
                        "/switch",
                    }:
                        await self.command(item)
                    elif item.key not in self.jobs:
                        self.store.mark(item.id, "scheduled")
                        self.jobs[item.key] = asyncio.create_task(self.control(item))
                elif item.key not in self.jobs:
                    if item.status != "prompt":
                        self.store.mark(item.id, "scheduled")
                    self.jobs[item.key] = asyncio.create_task(self.prompt(item))
            for item in self.store.items():
                if (
                    item.status == "prompt" or not item.text.startswith("/")
                ) and item.key not in self.jobs:
                    if item.status != "prompt":
                        self.store.mark(item.id, "scheduled")
                    self.jobs[item.key] = asyncio.create_task(self.prompt(item))

    async def cleanup_idle(self):
        await self.sessions.cleanup()

    async def run(self):
        if hasattr(self.telegram, "start"):
            await self.telegram.start(lambda _: self.dispatch(), self.callback)
        bot = await self.telegram.call("getMe")
        self.store.bind_bot(bot["id"])
        if (await self.telegram.call("getWebhookInfo")).get("url"):
            raise TelegramError("Webhook configured; remove it explicitly before long polling")
        with contextlib.suppress(TelegramError):
            await self.telegram.call(
                "setMyCommands",
                commands=[
                    {"command": name, "description": description}
                    for name, description in (
                        ("help", "Show help"),
                        ("status", "Current Delta session"),
                        ("new", "Start a fresh session"),
                        ("name", "Name this session"),
                        ("compact", "Compact context"),
                        ("stop", "Abort current work"),
                        ("last", "Last assistant message"),
                        ("sessions", "Known sessions"),
                        ("model", "Switch model"),
                        ("thinking", "Thinking level"),
                    )
                ],
            )
        if self.on_ready:
            self.on_ready()
        for user in self.config.users:
            with contextlib.suppress(TelegramError):
                await self.telegram.send(user, "Delta gateway connected.")
        await self.dispatch()
        while not self.stopping:
            updates = await self.telegram.call(
                "getUpdates",
                offset=self.store.offset,
                timeout=1,
                limit=100,
                allowed_updates=["message", "callback_query"],
            )
            self.store.admit(updates, self.config, bot["id"], bot.get("username", ""))
            for update in updates:
                if hasattr(self.telegram, "dispatch"):
                    await self.telegram.dispatch(update)
                elif callback := update.get("callback_query"):
                    await self.callback(callback)
            await self.dispatch()
            await self.cleanup_idle()
            self.store.retain()

    async def close(self):
        self.stopping = True
        for job in self.jobs.values():
            job.cancel()
        await asyncio.gather(*self.jobs.values(), return_exceptions=True)
        await self.sessions.close()
        for user in self.config.users:
            with contextlib.suppress(TelegramError):
                await asyncio.wait_for(self.telegram.send(user, "Delta gateway disconnected."), 2)
        await self.telegram.close()
        self.store.close()


async def run_service(config, telegram, *, config_path=None, launch=None):
    import hashlib
    import signal
    import sqlite3
    from contextlib import ExitStack

    from delta_gateway.background import ProcessRecord
    from delta_gateway.instances import OperatorError, home, lease, matching_state
    from delta_gateway.migrations import SCHEMA
    from delta_gateway.ownership import SessionLock

    stack = ExitStack()
    service = record = runner = monitor = None
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    original_signal = None
    outcome = "stopped"
    try:
        instance_lock = lease(config.instance)
        stack.callback(instance_lock.close)
        previous = SessionLock(config.state / "locks", config.instance)
        stack.callback(previous.close)
        token_id = hashlib.sha256((config.token() or "").encode()).hexdigest()
        token_lock = SessionLock(home() / "locks", "token-" + token_id)
        stack.callback(token_lock.close)
        identity = hashlib.sha256(
            os.path.normcase(str(config.database_path.resolve())).encode()
        ).hexdigest()
        schema_lock = SessionLock(home() / "locks", "schema-" + identity)
        try:
            version = 0
            if config.database_path.exists():
                with sqlite3.connect(config.database_path.as_uri() + "?mode=ro", uri=True) as db:
                    version = db.execute("PRAGMA user_version").fetchone()[0]
            with ExitStack() as peers:
                if version < SCHEMA:
                    for instance in matching_state(config):
                        if instance != config.instance:
                            try:
                                owner = lease(instance)
                                peers.callback(owner.close)
                            except RuntimeError:
                                raise OperatorError(
                                    "Stop every instance sharing this database before upgrading"
                                ) from None
                service = Service(config, telegram)
        finally:
            schema_lock.close()
        record = ProcessRecord(
            config, Path(config_path or config.project / ".delta-gateway" / "config.yaml"), launch
        )
        service.on_ready = record.ready
        if os.name != "nt":
            original_signal = signal.getsignal(signal.SIGTERM)
            loop.add_signal_handler(signal.SIGTERM, stopped.set)

        async def watch_stop():
            while not stopped.is_set():
                if record.stop_requested():
                    return
                await asyncio.sleep(0.2)

        runner, monitor = asyncio.create_task(service.run()), asyncio.create_task(watch_stop())
        done, _ = await asyncio.wait({runner, monitor}, return_when=asyncio.FIRST_COMPLETED)
        if monitor in done:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        else:
            await runner
    except Exception:
        outcome = "failed"
        raise
    finally:
        for task in (runner, monitor):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(t for t in (runner, monitor) if t), return_exceptions=True)
        try:
            if service:
                await service.close()
            else:
                await telegram.close()
        finally:
            if original_signal is not None:
                loop.remove_signal_handler(signal.SIGTERM)
                signal.signal(signal.SIGTERM, original_signal)
            try:
                if record:
                    record.finish(outcome)
            finally:
                stack.close()
