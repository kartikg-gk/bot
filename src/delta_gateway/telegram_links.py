"""Private Telegram chat linking for signed-in gateway accounts."""

import asyncio
import re
import secrets
import time

from delta_gateway.telegram import TelegramError
from delta_gateway.telegram_sdk import TelegramSDK

TOKEN_VARIABLE = "DELTA_GATEWAY_TELEGRAM_APP_TOKEN"
LINK_SECONDS = 600


class TelegramLinks:
    def __init__(self, token, transport=None):
        self.bot = (
            TelegramSDK(token, transport=transport)
            if re.fullmatch(r"\d+:[A-Za-z0-9_-]{30,}", token)
            else None
        )
        self.bot_id = int(token.partition(":")[0]) if self.bot else None
        self.pending = {}
        self.offset = 0
        self.lock = asyncio.Lock()

    def forget(self, account):
        self.pending.pop(account, None)

    async def begin(self, account):
        profile = await self.bot.call("getMe")
        username = profile.get("username", "")
        if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
            raise TelegramError("Telegram did not return a usable bot name")
        code = secrets.token_urlsafe(24)
        now = time.monotonic()
        self.pending = {key: value for key, value in self.pending.items() if value[1] > now}
        self.pending[account] = (code, now + LINK_SECONDS)
        return "https://t.me/" + username + "?start=" + code

    async def confirm(self, account, save):
        async with self.lock:
            link = self.pending.get(account)
            if link is None or link[1] <= time.monotonic():
                self.forget(account)
                return
            updates = await self.bot.call(
                "getUpdates", offset=self.offset, timeout=0, limit=100, allowed_updates=["message"]
            )
            pending = {code: key for key, (code, _) in self.pending.items()}
            for update in updates:
                update_id = update.get("update_id")
                if type(update_id) is not int or update_id < self.offset:
                    continue
                message = update.get("message") or {}
                chat, sender = message.get("chat") or {}, message.get("from") or {}
                text = message.get("text")
                if (
                    chat.get("type") == "private"
                    and type(chat.get("id")) is int
                    and chat["id"] > 0
                    and sender.get("id") == chat["id"]
                    and not sender.get("is_bot")
                    and isinstance(text, str)
                    and text.startswith("/start ")
                ):
                    code = text.removeprefix("/start ")
                    owner = pending.get(code)
                    current = self.pending.get(owner)
                    if current is not None and current[0] == code and current[1] > time.monotonic():
                        await save(owner, chat["id"])
                        self.forget(owner)
                self.offset = update_id + 1

    async def send(self, chat, text):
        try:
            delivery = await self.bot.send(chat, text)
        except TelegramError:
            raise ValueError(
                "Telegram delivery could not be confirmed. Check the linked chat before retrying."
            ) from None
        if not isinstance(delivery, dict) or type(delivery.get("message_id")) is not int:
            raise ValueError("Telegram delivery could not be confirmed. Check the linked chat.")
        return {"message_id": delivery["message_id"]}

    async def close(self):
        if self.bot is not None:
            await self.bot.close()
