"""Model proxy: sandboxes hold a short-lived token; real credentials stay here."""

import asyncio
import contextlib
import hmac
import os
from dataclasses import dataclass
from secrets import token_urlsafe

import httpx

MAX_HEADER = 64 * 1024
MAX_BODY = 16 * 1024 * 1024
ALLOWED_SUFFIXES = ("/chat/completions", "/responses", "/messages")
FORWARDED_HEADERS = {"content-type", "accept", "anthropic-version", "anthropic-beta"}


class CredentialError(ValueError):
    """An operator-facing error which contains no credential values."""


@dataclass
class Upstream:
    base_url: str
    key: str
    anthropic: bool = False


def resolve_upstream(provider):
    """Find one provider's API key and endpoint in this process's Delta settings."""
    from delta_app.catalog import provider_info
    from delta_app.config.store import load_config

    try:
        info = provider_info(provider)
    except KeyError:
        raise CredentialError("Unknown provider for sandboxed bots") from None
    config = load_config()
    key = (info.key_env and os.environ.get(info.key_env)) or config.api_keys.get(provider)
    if not key:
        raise CredentialError(
            "Sandboxed bots need an API key for this provider; subscription logins are not "
            "supported through the proxy yet"
        )
    base_url = (
        os.environ.get(provider.upper() + "_BASE_URL")
        or config.base_urls.get(provider)
        or info.base_url
    )
    return Upstream(base_url, key, provider == "anthropic")


@dataclass
class Grant:
    provider: str
    upstream: Upstream
    remaining: int


class ModelProxy:
    def __init__(self, *, host="127.0.0.1", port=0, resolve=resolve_upstream, transport=None):
        self.host, self.port, self.resolve = host, port, resolve
        self.grants = {}
        self.server = None
        self.http = httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(600, connect=30))

    async def start(self):
        if not self.server:
            self.server = await asyncio.start_server(
                self.handle, self.host, self.port, limit=MAX_HEADER
            )
            self.port = self.server.sockets[0].getsockname()[1]
        return self

    def issue(self, provider, max_requests):
        token = token_urlsafe(32)
        self.grants[token] = Grant(provider, self.resolve(provider), max_requests)
        return token

    def revoke(self, token):
        self.grants.pop(token, None)

    def grant_for(self, headers):
        bearer = headers.get("authorization", "").removeprefix("Bearer ").strip()
        supplied = bearer or headers.get("x-api-key", "")
        for token, grant in self.grants.items():
            if hmac.compare_digest(token.encode(), supplied.encode()):
                return grant
        return None

    async def handle(self, reader, writer):
        try:
            async with asyncio.timeout(30):
                head = await reader.readuntil(b"\r\n\r\n")
                lines = head.decode("latin-1").split("\r\n")
                method, path, _ = lines[0].split(" ", 2)
                headers = {}
                for line in lines[1:]:
                    name, separator, value = line.partition(":")
                    if separator:
                        headers[name.strip().lower()] = value.strip()
                length = int(headers.get("content-length", "0"))
                if not 0 <= length <= MAX_BODY:
                    return await self.refuse(writer, 413, "Request too large")
                body = await reader.readexactly(length) if length else b""
            grant = self.grant_for(headers)
            if not grant:
                return await self.refuse(writer, 401, "Invalid or expired token")
            target = path.partition("?")[0]
            if method != "POST" or not target.endswith(ALLOWED_SUFFIXES) or ".." in target:
                return await self.refuse(writer, 403, "Only model requests are allowed")
            if grant.remaining <= 0:
                return await self.refuse(writer, 429, "Model request limit reached for this run")
            grant.remaining -= 1
            await self.forward(writer, grant.upstream, path, headers, body)
        except (ValueError, TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            with contextlib.suppress(OSError):
                await self.refuse(writer, 400, "Bad request")
        except (OSError, httpx.HTTPError):
            with contextlib.suppress(OSError):
                await self.refuse(writer, 502, "Upstream request failed")
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    async def forward(self, writer, upstream, path, headers, body):
        outgoing = {k: v for k, v in headers.items() if k in FORWARDED_HEADERS}
        if upstream.anthropic:
            outgoing["x-api-key"] = upstream.key
        else:
            outgoing["authorization"] = "Bearer " + upstream.key
        url = upstream.base_url.rstrip("/") + path
        async with self.http.stream("POST", url, headers=outgoing, content=body) as response:
            kind = response.headers.get("content-type", "application/octet-stream")
            writer.write(
                f"HTTP/1.1 {response.status_code} Proxied\r\nContent-Type: {kind}\r\n"
                "Connection: close\r\n\r\n".encode("latin-1")
            )
            await writer.drain()
            async for chunk in response.aiter_bytes():
                writer.write(chunk)
                await writer.drain()

    async def refuse(self, writer, status, message):
        data = message.encode()
        writer.write(
            f"HTTP/1.1 {status} Refused\r\nContent-Type: text/plain\r\n"
            f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode()
            + data
        )
        await writer.drain()

    async def close(self):
        self.grants.clear()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        await self.http.aclose()
