"Telegram SDK lifecycle, message dispatch, and bounded public API errors."

import asyncio
import logging
import re
from datetime import timedelta
from io import BytesIO

import httpx
from telegram import BotCommand, LinkPreviewOptions, Update
from telegram.error import BadRequest, Conflict, NetworkError, RetryAfter
from telegram.error import TelegramError as SDKError
from telegram.ext import Application, CallbackQueryHandler, MessageHandler, filters
from telegram.request import BaseRequest, HTTPXRequest

from delta_gateway.telegram import TelegramError


class MockRequest(BaseRequest):
    """Adapt an injected HTTP transport for offline Bot API contract tests."""

    def __init__(self, transport):
        self.http = httpx.AsyncClient(transport=transport)

    @property
    def read_timeout(self):
        return 40

    async def initialize(self):
        pass

    async def shutdown(self):
        await self.http.aclose()

    async def do_request(self, url, method, request_data=None, **kwargs):
        try:
            if request_data and request_data.contains_files:
                response = await self.http.request(
                    method,
                    url,
                    data=request_data.json_parameters,
                    files=request_data.multipart_data,
                )
            else:
                response = await self.http.request(
                    method,
                    url,
                    content=request_data.json_payload if request_data else None,
                    headers={"content-type": "application/json"},
                )
            return response.status_code, response.content
        except httpx.HTTPError:
            raise NetworkError("Connection failed") from None


def plain(value):
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value


class TelegramSDK:
    def __init__(self, token, *, transport=None):
        if not re.fullmatch(r"\d+:\S+", token):
            raise TelegramError("Invalid bot token format")
        # Validation remains in the SDK. Test tokens need its numeric-id syntax.
        self.requests = (
            [MockRequest(transport), MockRequest(transport)]
            if transport
            else [HTTPXRequest(read_timeout=40), HTTPXRequest(read_timeout=40)]
        )
        builder = (
            Application.builder()
            .token(token)
            .request(self.requests[0])
            .get_updates_request(self.requests[1])
        )

        class RedactToken(logging.Filter):
            def filter(self, record):
                record.msg, record.args = record.getMessage().replace(token, "[redacted]"), ()
                return True

        self.log_filter = RedactToken()
        logging.getLogger("httpx").addFilter(self.log_filter)
        self.app = builder.updater(None).build()
        self.initialized = self.started = False
        self._initializing = asyncio.Lock()

    async def initialize(self):
        async with self._initializing:
            if not self.initialized:
                try:
                    await self.app.initialize()
                    self.initialized = True
                except SDKError:
                    raise TelegramError(
                        "Telegram initialization failed; check bot credentials/connectivity"
                    ) from None

    async def start(self, on_message, on_callback):
        async def message(update, context):
            await on_message(update.to_dict())

        async def callback(update, context):
            await on_callback(update.callback_query.to_dict())

        self.app.add_handler(CallbackQueryHandler(callback))
        self.app.add_handler(MessageHandler(filters.TEXT, message))
        await self.initialize()
        await self.app.start()
        self.started = True

    async def dispatch(self, raw):
        await self.app.process_update(Update.de_json(raw, self.app.bot))

    async def call(self, method, *, _files=None, **payload):
        if not self.initialized:
            await self.initialize()
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", method).lower()
        if method == "setMyCommands":
            payload["commands"] = [
                BotCommand(row["command"], row["description"]) if isinstance(row, dict) else row
                for row in payload["commands"]
            ]
        if _files:
            filename, content, _ = _files["document"]
            stream = BytesIO(content)
            stream.name = filename
            payload["document"] = stream
        repeatable = method in {
            "getMe",
            "getWebhookInfo",
            "getUpdates",
            "editMessageText",
            "sendChatAction",
            "deleteMessage",
            "setMyCommands",
        }
        for attempt in range(4):
            try:
                return plain(await getattr(self.app.bot, name)(**payload))
            except BadRequest as exc:
                if method == "editMessageText" and "not modified" in str(exc).lower():
                    return None
                raise TelegramError("Telegram request was rejected") from None
            except Conflict:
                raise TelegramError("Polling conflict: another process is using this bot") from None
            except RetryAfter as exc:
                if attempt == 3:
                    raise TelegramError("Telegram rate limit exceeded") from None
                delay = (
                    exc.retry_after.total_seconds()
                    if isinstance(exc.retry_after, timedelta)
                    else exc.retry_after
                )
                await asyncio.sleep(min(60, max(1, delay)))
            except NetworkError:
                if repeatable and attempt < 3:
                    await asyncio.sleep(2**attempt)
                    continue
                raise TelegramError(
                    "Telegram connection failed; delivery may be uncertain"
                ) from None
            except (SDKError, AttributeError, TypeError, ValueError):
                raise TelegramError("Telegram operation is unavailable or failed") from None
        raise TelegramError("Telegram retries exhausted")

    async def send(self, chat, text, **kwargs):
        return await self.call(
            "sendMessage",
            chat_id=chat,
            text=text,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            **kwargs,
        )

    async def document(self, chat, filename, content, content_type, **kwargs):
        return await self.call(
            "sendDocument",
            chat_id=chat,
            _files={"document": (filename, content.encode("utf-8"), content_type)},
            **kwargs,
        )

    async def document_path(self, chat, path, **kwargs):
        with path.open("rb") as source:
            return await self.call("sendDocument", chat_id=chat, document=source, **kwargs)

    async def close(self):
        try:
            if self.started:
                await self.app.stop()
            if self.initialized:
                await self.app.shutdown()
        finally:
            for request in self.requests:
                await request.shutdown()
            logging.getLogger("httpx").removeFilter(self.log_filter)
