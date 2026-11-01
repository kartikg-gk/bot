"""Temporary working messages and private-chat draft progress."""

import contextlib
import time
from uuid import uuid4

from delta_gateway.routing import payload
from delta_gateway.telegram import TelegramError, chunks


class Preview:
    def __init__(self, telegram, key, enabled=True):
        self.telegram, self.key = telegram, key
        self.draft = enabled and key[0] > 0
        self.draft_id = uuid4().int % (2**31 - 1) + 1
        self.message = None
        self.last = 0.0

    @property
    def mode(self):
        return "ephemeral draft" if self.draft else "working message"

    async def update(self, text, *, force=False):
        if self.message is None:
            self.message = False
            with contextlib.suppress(TelegramError):
                message = await self.telegram.send(self.key[0], "Working…", **payload(self.key))
                self.message = message["message_id"]
        if not force and time.monotonic() - self.last < 1:
            return
        self.last = time.monotonic()
        if self.draft:
            try:
                await self.telegram.call(
                    "sendMessageDraft",
                    chat_id=self.key[0],
                    draft_id=self.draft_id,
                    text=next(chunks(text or "Working…", 4096)),
                    **payload(self.key),
                )
            except TelegramError:
                self.draft = False

    async def finish(self, text=""):
        if self.message is not None and self.message is not False:
            with contextlib.suppress(TelegramError):
                await self.telegram.call(
                    "deleteMessage", chat_id=self.key[0], message_id=self.message
                )


async def typing(telegram, key):
    import asyncio

    while True:
        with contextlib.suppress(TelegramError):
            await telegram.call("sendChatAction", chat_id=key[0], action="typing", **payload(key))
        await asyncio.sleep(4)
