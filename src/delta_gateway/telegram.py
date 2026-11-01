"Narrow Bot API adapter. HTTP exception strings never cross this boundary."

import asyncio

import httpx


class TelegramError(RuntimeError):
    pass


class Telegram:
    def __init__(self, token, *, transport=None):
        self.http = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{token}/", timeout=40, transport=transport
        )

    async def call(self, method, *, _files=None, **payload):
        # Retrying an ambiguous send could produce duplicate final replies or approval cards.
        repeatable = method in {
            "getMe",
            "getWebhookInfo",
            "getUpdates",
            "editMessageText",
            "sendChatAction",
        }
        for attempt in range(4):
            try:
                if _files:
                    response = await self.http.post(
                        method, data={k: str(v) for k, v in payload.items()}, files=_files
                    )
                else:
                    response = await self.http.post(method, json=payload)
                data = response.json()
                if data.get("ok"):
                    return data["result"]
                code = data.get("error_code", response.status_code)
                if (
                    code == 400
                    and method == "editMessageText"
                    and "not modified" in data.get("description", "")
                ):
                    return None
                if code == 409:
                    raise TelegramError("Polling conflict: another process is using this bot")
                if code == 429 and attempt < 3:
                    await asyncio.sleep(
                        min(60, max(1, data.get("parameters", {}).get("retry_after", 1)))
                    )
                    continue
                if code >= 500 and repeatable and attempt < 3:
                    await asyncio.sleep(2**attempt)
                    continue
                raise TelegramError(f"Telegram operation failed ({code})")
            except (httpx.HTTPError, ValueError):
                if repeatable and attempt < 3:
                    await asyncio.sleep(2**attempt)
                    continue
                raise TelegramError(
                    "Telegram connection failed; delivery may be uncertain"
                ) from None
        raise TelegramError("Telegram retries exhausted")

    async def send(self, chat, text, **kwargs):
        return await self.call("sendMessage", chat_id=chat, text=text, **kwargs)

    async def document(self, chat, filename, content, content_type, **kwargs):
        return await self.call(
            "sendDocument",
            chat_id=chat,
            _files={"document": (filename, content.encode("utf-8"), content_type)},
            **kwargs,
        )

    async def close(self):
        await self.http.aclose()


def chunks(text, limit=3500):
    current, size = [], 0
    for character in text:
        units = len(character.encode("utf-16-le", errors="replace")) // 2
        if size + units > limit:
            yield "".join(current)
            current, size = [], 0
        current.append(character)
        size += units
    if current:
        yield "".join(current)
