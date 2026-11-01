"""Cached release status for an explicitly configured release channel."""

import asyncio
import importlib.metadata
import time

import httpx
from packaging.version import InvalidVersion, Version

_cache = {}
_locks = {}


def current_version():
    try:
        return importlib.metadata.version("delta-gateway")
    except importlib.metadata.PackageNotFoundError:
        return "0.1.0"


async def status(url=None, *, transport=None):
    current = current_version()
    if not url:
        return f"Gateway: {current} · update channel not configured"
    if url in _cache and time.monotonic() - _cache[url][0] < 3600:
        return _cache[url][1]
    async with _locks.setdefault(url, asyncio.Lock()):
        if url in _cache and time.monotonic() - _cache[url][0] < 3600:
            return _cache[url][1]
        try:
            async with httpx.AsyncClient(timeout=3, transport=transport) as client:
                reply = await client.get(url)
                reply.raise_for_status()
                if len(reply.content) > 65536:
                    raise ValueError("Oversized release metadata")
                latest = str(reply.json()["version"])
                text = f"Gateway: {current} · " + (
                    f"update available: {latest}"
                    if Version(latest) > Version(current)
                    else "up to date"
                )
        except (httpx.HTTPError, ValueError, KeyError, InvalidVersion):
            text = f"Gateway: {current} · update status unavailable"
        _cache[url] = time.monotonic(), text
        return text
