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
from pathlib import Path

from delta_gateway.telegram import TelegramError

# One private chat for the web page, far outside Telegram's IDs.
WEB_CHAT = 2**52
# Inbox IDs for web messages; agent reports use IDs above 2**60.
WEB_IDS = 2**59
TOKEN_VARIABLE = "DELTA_GATEWAY_WEB_TOKEN"
PAGE = Path(__file__).with_name("web_page.html")


class WebChannel:
    """Sends to the web page for the web chat and to Telegram for everything else."""

    def __init__(self, telegram):
        self.telegram = telegram
        self.listeners = set()
        self.numbers = itertools.count(1)

    def __getattr__(self, name):
        return getattr(self.telegram, name)

    def publish(self, record):
        for queue in list(self.listeners):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(record)

    async def send(self, chat, text, **kwargs):
        if chat != WEB_CHAT:
            return await self.telegram.send(chat, text, **kwargs)
        number = next(self.numbers)
        buttons = [
            dict(text=button["text"], data=button["callback_data"])
            for row in kwargs.get("reply_markup", {}).get("inline_keyboard", [])
            for button in row
        ]
        self.publish(dict(type="message", id=number, text=text, buttons=buttons))
        return {"message_id": number}

    async def call(self, method, **payload):
        if payload.get("chat_id") != WEB_CHAT:
            return await self.telegram.call(method, **payload)
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
            return await self.telegram.document(chat, filename, content, content_type, **kwargs)
        self.publish(dict(type="file", name=filename, content=content))

    async def document_path(self, chat, path, **kwargs):
        if chat != WEB_CHAT:
            return await self.telegram.document_path(chat, path, **kwargs)
        await self.document(chat, Path(path).name, Path(path).read_text(encoding="utf-8"), "")


class WebServer:
    def __init__(self, service, channel, *, host="127.0.0.1", port=8765, token=None):
        self.service, self.channel = service, channel
        self.host, self.port = host, port
        self.token = token or os.environ.get(TOKEN_VARIABLE)
        self.runner = None
        if not self.token or len(self.token) < 16:
            raise TelegramError(f"Set {TOKEN_VARIABLE} to at least 16 characters for the web page")

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
            if request.path.startswith("/api/") and not self.allowed(request):
                return web.json_response(dict(error="Missing or wrong token"), status=401)
            return await handler(request)

        app = web.Application(middlewares=[guard], client_max_size=64 * 1024)
        app.router.add_get("/", self.page)
        app.router.add_get("/api/messages", self.messages)
        app.router.add_post("/api/messages", self.post_message)
        app.router.add_post("/api/approvals", self.approval)
        app.router.add_get("/api/tasks", self.tasks)
        app.router.add_get("/api/stream", self.stream)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]

    async def close(self):
        if self.runner:
            await self.runner.cleanup()

    async def page(self, request):
        from aiohttp import web

        return web.Response(text=PAGE.read_text(encoding="utf-8"), content_type="text/html")

    async def messages(self, request):
        from aiohttp import web

        return web.json_response(self.service.store.history(WEB_CHAT, self.actor))

    async def post_message(self, request):
        from aiohttp import web

        try:
            body = await request.json()
        except ValueError:
            return web.json_response(dict(error="Send JSON"), status=400)
        text = body.get("text") if isinstance(body, dict) else None
        if not isinstance(text, str) or not text.strip() or len(text) > 16000:
            return web.json_response(dict(error="Send text up to 16000 characters"), status=400)
        number = self.service.store.admit_web(WEB_CHAT, self.actor, text.strip(), base=WEB_IDS)
        await self.service.dispatch()
        return web.json_response(dict(id=number), status=202)

    async def approval(self, request):
        from aiohttp import web

        try:
            body = await request.json()
            data, allow = body["data"], body["allow"]
        except (ValueError, KeyError, TypeError):
            return web.json_response(dict(error="Send data and allow"), status=400)
        handle = str(data).partition(":")[2]
        decided = await self.service.decide_web(handle, bool(allow), WEB_CHAT, self.actor)
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

        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        queue = asyncio.Queue(maxsize=200)
        self.channel.listeners.add(queue)
        try:
            await response.write(b": connected\n\n")
            while True:
                try:
                    record = await asyncio.wait_for(queue.get(), 15)
                    await response.write(f"data: {json.dumps(record)}\n\n".encode())
                except TimeoutError:
                    await response.write(b": keep-alive\n\n")
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            self.channel.listeners.discard(queue)
        return response
