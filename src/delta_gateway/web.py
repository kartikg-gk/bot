"""Web channel: an HTTP API and a small chat page over the same inbox as Telegram.

The page posts messages into the durable inbox and follows a stream of what the gateway
sends back: replies, live drafts, approval requests and task reports. It listens on this
machine only by default and every request needs the configured bearer token.
"""

import asyncio
import contextlib
import hmac
import itertools
import json
import os
import re
from contextvars import ContextVar
from pathlib import Path

from delta_gateway.telegram import TelegramError

# One private chat for the web page, far outside Telegram's IDs.
WEB_CHAT = 2**52
# Inbox IDs for web messages; agent reports use IDs above 2**60.
WEB_IDS = 2**59
TOKEN_VARIABLE = "DELTA_GATEWAY_WEB_TOKEN"
PROXY_SECRET_VARIABLE = "DELTA_GATEWAY_PROXY_SECRET"
PROXY_HEADER = "X-Delta-Gateway-Proxy"
PAGE = Path(__file__).with_name("web_page.html")


class WebChannel:
    """Sends to the web page for the web chat and to Telegram for everything else."""

    def __init__(self, telegram):
        self.telegram = telegram
        self.listeners = set()
        self.numbers = itertools.count(1)

    def __getattr__(self, name):
        return getattr(self.telegram, name)

    def require_telegram(self):
        if self.telegram is None:
            raise TelegramError("Telegram is disabled for this gateway")
        return self.telegram

    async def close(self):
        if self.telegram is not None:
            await self.telegram.close()

    def publish(self, record):
        for queue in list(self.listeners):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(record)

    def tool_event(self, chat, record):
        if chat != WEB_CHAT:
            return
        kind = record.get("type")
        if kind not in {"tool_execution_start", "tool_execution_end"}:
            return
        name = record.get("toolName", "")
        args = record.get("args") or {}
        path = args.get("file_path", "") if name in {"Read", "Write", "Edit"} else ""
        status = (
            "running"
            if kind.endswith("start")
            else ("failed" if record.get("isError") else "finished")
        )
        self.publish(
            dict(
                type="tool",
                id=record.get("toolCallId"),
                name=name[:60],
                status=status,
                path=path[:400] if isinstance(path, str) else "",
            )
        )

    async def send(self, chat, text, **kwargs):
        if chat != WEB_CHAT:
            return await self.require_telegram().send(chat, text, **kwargs)
        number = next(self.numbers)
        if kwargs.get("message_thread_id"):
            return {"message_id": number}
        buttons = [
            dict(text=button["text"], data=button["callback_data"])
            for row in kwargs.get("reply_markup", {}).get("inline_keyboard", [])
            for button in row
        ]
        self.publish(dict(type="message", id=number, text=text, buttons=buttons))
        return {"message_id": number}

    async def call(self, method, **payload):
        if payload.get("chat_id") != WEB_CHAT:
            return await self.require_telegram().call(method, **payload)
        if payload.get("message_thread_id"):
            return True
        if method == "sendMessageDraft":
            self.publish(dict(type="draft", text=payload.get("text", "")))
        elif method == "editMessageText":
            self.publish(
                dict(type="edit", id=payload.get("message_id"), text=payload.get("text", ""))
            )
        elif method == "deleteMessage":
            self.publish(dict(type="delete", id=payload.get("message_id")))
        return True

    async def document(self, chat, filename, content, content_type, **kwargs):
        if chat != WEB_CHAT:
            return await self.require_telegram().document(
                chat, filename, content, content_type, **kwargs
            )
        self.publish(dict(type="file", name=filename, content=content))

    async def document_path(self, chat, path, **kwargs):
        if chat != WEB_CHAT:
            return await self.require_telegram().document_path(chat, path, **kwargs)
        await self.document(chat, Path(path).name, Path(path).read_text(encoding="utf-8"), "")


class ResourceScope:
    def __init__(self, server):
        self.server = server

    @property
    def service(self):
        return self.server.service

    @property
    def config(self):
        return self.service.config

    @property
    def store(self):
        return self.service.store

    @property
    def sessions(self):
        return self.service.sessions


class WebServer:
    def __init__(
        self,
        service,
        channel,
        *,
        host="127.0.0.1",
        port=8765,
        token=None,
        google_transport=None,
        provider_transport=None,
        app_transport=None,
    ):
        self.parent, self.parent_channel = service, channel
        self.scope = ContextVar("gateway_account_service", default=None)
        self.host, self.port = host, port
        self.token = token or os.environ.get(TOKEN_VARIABLE)
        self.proxy_secret = os.environ.get(PROXY_SECRET_VARIABLE)
        if self.proxy_secret is not None and not re.fullmatch(
            r"[A-Za-z0-9_-]{43,128}", self.proxy_secret
        ):
            raise ValueError(f"{PROXY_SECRET_VARIABLE} must be a 43–128 character base64url token")
        self.runner = self.google = self.accounts = self.runtimes = self.models = self.apps = None
        if service and service.config.web_auth == "google":
            self.configure_google(google_transport, provider_transport, app_transport)
        elif not self.token or len(self.token) < 16:
            raise TelegramError(f"Set {TOKEN_VARIABLE} to at least 16 characters for the web page")

    def configure_google(self, google_transport, provider_transport, app_transport):
        try:
            from delta_gateway.account_models import AccountModels
            from delta_gateway.account_runtimes import AccountRuntimes
            from delta_gateway.account_store import AccountStore
            from delta_gateway.app_connections import AppConnections
            from delta_gateway.google_oidc import GoogleLogin, canonical_origin
        except ImportError:
            raise ValueError("Install delta-gateway[web,accounts] to use Google sign-in") from None
        config = self.parent.config
        config.validate_paths()
        origin = canonical_origin(config.web_origin)
        if origin.startswith("http:") and self.host not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("HTTP Google sign-in requires a loopback bind address")
        self.accounts = AccountStore(
            config.state / "web-accounts.sqlite3",
            config.instance,
            os.environ.get("DELTA_GATEWAY_MASTER_KEY", ""),
        )
        try:
            self.google = GoogleLogin(
                self.accounts,
                origin,
                os.environ.get("DELTA_GATEWAY_GOOGLE_CLIENT_ID"),
                os.environ.get("DELTA_GATEWAY_GOOGLE_CLIENT_SECRET"),
                config.web_allowed_emails,
                config.web_allowed_domains,
                transport=google_transport,
            )
        except BaseException:
            self.accounts.close()
            raise
        self.apps = AppConnections(self, transport=app_transport)
        self.runtimes = AccountRuntimes(self.parent, self.accounts, self.google, self.apps)
        self.models = AccountModels(self, transport=provider_transport)

    @property
    def service(self):
        return self.scope.get() or self.parent

    @property
    def channel(self):
        service = self.scope.get()
        return service.telegram if service else self.parent_channel

    @property
    def actor(self):
        # The web page acts as the instance's first allowed person.
        return self.service.config.users[0]

    def allowed(self, request):
        header = request.headers.get("Authorization", "")
        return hmac.compare_digest(header.encode(), f"Bearer {self.token}".encode())

    async def start(self):
        from aiohttp import web

        @web.middleware
        async def guard(request, handler):
            if self.proxy_secret is not None:
                supplied = request.headers.getall(PROXY_HEADER, [])
                if len(supplied) != 1 or not hmac.compare_digest(
                    supplied[0].encode("utf-8", "surrogateescape"), self.proxy_secret.encode()
                ):
                    return web.json_response(dict(error="Untrusted gateway ingress"), status=403)
            if self.google:
                from delta_gateway.google_oidc import ACCOUNT_REQUEST, SESSION_COOKIE

                if not self.google.request_allowed(
                    request, trusted_proxy=self.proxy_secret is not None
                ):
                    return web.json_response(dict(error="Untrusted host or origin"), status=403)
                token = request.cookies.get(SESSION_COOKIE)
                account = self.google.identity(token)
                if account is not None:
                    request[ACCOUNT_REQUEST] = account
                if request.path.startswith("/api/"):
                    if account is None:
                        return web.json_response(dict(error="Sign in with Google"), status=401)
                    scope = self.scope.set(self.runtimes.get(account))
                    try:
                        response = await handler(request)
                        response.headers["Cache-Control"] = "no-store"
                        return response
                    finally:
                        self.scope.reset(scope)
            elif request.path.startswith("/api/") and not self.allowed(request):
                return web.json_response(dict(error="Missing or wrong token"), status=401)
            return await handler(request)

        app = web.Application(middlewares=[guard], client_max_size=2 * 1024 * 1024)
        from delta_gateway.bot_api import BotAPI
        from delta_gateway.jobs_api import JobsAPI
        from delta_gateway.resources_api import ResourcesAPI
        from delta_gateway.run_api import RunAPI

        RunAPI(self).install(app)
        BotAPI(self).install(app)
        JobsAPI(self).install(app)
        ResourcesAPI(self).install(app)
        app.router.add_get("/", self.page)
        app.router.add_get("/api/messages", self.messages)
        app.router.add_post("/api/messages", self.post_message)
        app.router.add_post("/api/approvals", self.approval)
        app.router.add_get("/api/tasks", self.tasks)
        app.router.add_get("/api/stream", self.stream)
        app.router.add_get("/auth/session", self.auth_session)
        if self.google:
            self.apps.install(app)
            app.router.add_get("/auth/login", self.google.start)
            app.router.add_get("/auth/callback", self.google.callback)
            app.router.add_post("/auth/logout", self.google.logout)
            app.router.add_get("/api/account/model", self.models.read)
            app.router.add_put("/api/account/model", self.models.save)
            app.router.add_delete("/api/account/keys/{provider}", self.models.remove)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        if self.runtimes:
            self.runtimes.start()

    async def close(self):
        if self.runner:
            services = [self.parent] if self.parent else []
            if self.runtimes:
                services.extend(self.runtimes.services.values())
            for service in services:
                if service.runs:
                    service.runs.close()
            channels = [self.parent_channel, *(service.telegram for service in services[1:])]
            for channel in channels:
                for queue in list(channel.listeners):
                    if queue.full():
                        queue.get_nowait()
                    queue.put_nowait(None)
            await self.runner.cleanup()
            self.runner = None
        if self.runtimes:
            try:
                await self.runtimes.close()
            finally:
                await self.google.http.aclose()
                await self.models.http.aclose()
                await self.apps.http.aclose()
                self.accounts.close()
                self.runtimes = None

    def account(self, request):
        if not self.google:
            return None
        from delta_gateway.google_oidc import ACCOUNT_REQUEST

        return request.get(ACCOUNT_REQUEST)

    def account_from_cookie(self, request):
        from delta_gateway.google_oidc import SESSION_COOKIE

        return self.google.identity(request.cookies.get(SESSION_COOKIE))

    async def auth_session(self, request):
        from aiohttp import web

        account = self.account(request)
        return web.json_response(
            {
                "mode": "google" if self.google else "bearer",
                "email": account["email"] if account else None,
            },
            headers={"Cache-Control": "no-store"},
        )

    async def page(self, request):
        from aiohttp import web

        return web.Response(text=PAGE.read_text(encoding="utf-8"), content_type="text/html")

    async def messages(self, request):
        from aiohttp import web

        return web.json_response(self.service.store.history(WEB_CHAT, self.actor))

    def needs_model(self, request):
        return (
            self.google
            and hasattr(self.service.sessions.factory, "revision")
            and not self.account(request)["provider"]
        )

    async def post_message(self, request):
        from aiohttp import web

        try:
            body = await request.json()
        except ValueError:
            return web.json_response(dict(error="Send JSON"), status=400)
        text = body.get("text") if isinstance(body, dict) else None
        if not isinstance(text, str) or not text.strip() or len(text) > 16000:
            return web.json_response(dict(error="Send text up to 16000 characters"), status=400)
        if self.needs_model(request):
            return web.json_response(
                dict(error="Choose a model and API key in Account settings"), status=409
            )
        text = text.strip()
        if not text.startswith("/") or text.startswith("/skill:"):
            route = self.service.store.conversation(WEB_CHAT, self.actor)
            run, number = self.service.runs.queue(route, text, WEB_IDS)
        else:
            run = None
            number = self.service.store.admit_web(WEB_CHAT, self.actor, text, base=WEB_IDS)
        await self.service.dispatch()
        return web.json_response(dict(id=number, run=run), status=202)

    async def approval(self, request):
        from aiohttp import web

        try:
            body = await request.json()
            data, allow = body["data"], body["allow"]
        except (ValueError, KeyError, TypeError):
            return web.json_response(dict(error="Send data and allow"), status=400)
        if not isinstance(data, str) or not isinstance(allow, bool):
            return web.json_response(
                dict(error="Send a callback and a Boolean decision"), status=400
            )
        handle = data.partition(":")[2]
        decided = await self.service.decide_web(handle, allow, WEB_CHAT, self.actor)
        return web.json_response(dict(decided=decided), status=200 if decided else 404)

    async def tasks(self, request):
        from aiohttp import web

        rows = [
            dict(
                id=t.id,
                status=t.status,
                goal=t.goal[:200],
                bot=getattr(self.service.store.bot(t.bot), "name", "-"),
            )
            for t in self.service.store.tasks()
        ]
        return web.json_response(rows)

    async def stream(self, request):
        from aiohttp import web

        response = web.StreamResponse(
            headers={"Content-Type": "text/event-stream", "Cache-Control": "no-store"}
        )
        await response.prepare(request)
        queue = asyncio.Queue(maxsize=200)
        self.channel.listeners.add(queue)
        try:
            await response.write(b": connected\n\n")
            while True:
                try:
                    record = await asyncio.wait_for(queue.get(), 15)
                    if record is None or (self.google and not self.account_from_cookie(request)):
                        break
                    await response.write(f"data: {json.dumps(record)}\n\n".encode())
                except TimeoutError:
                    if self.google and not self.account_from_cookie(request):
                        break
                    await response.write(b": keep-alive\n\n")
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            self.channel.listeners.discard(queue)
        return response
