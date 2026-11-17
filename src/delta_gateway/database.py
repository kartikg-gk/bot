"""One database interface with two backends: a local SQLite file, or Cloudflare D1.

D1 is used when ``CLOUDFLARE_D1_DATABASE_ID`` is set, which then needs the API token and
account ID as well. Both backends run the same SQL, since D1 runs SQLite. D1's HTTP API
binds text only, so numbers are sent as text and column affinity converts them back. It
cannot bind NULL either, so a None parameter is written into the statement as NULL,
which behaves exactly like binding it. Both backends do this, so they stay identical.
"""

import asyncio
import os
import sqlite3
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

import httpx

D1_VARIABLES = ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_D1_DATABASE_ID")
D1_TIMEOUT = 30
D1_ENDPOINT = "https://api.cloudflare.com/client/v4/accounts/{account}/d1/database/{database}/query"


class DatabaseConfigError(ValueError):
    """The D1 settings are incomplete."""


class ConstraintError(RuntimeError):
    """A write broke a UNIQUE, NOT NULL, CHECK or foreign key rule. Same type on both backends."""


class Database(Protocol):
    async def query(self, sql: str, params: Sequence = ()) -> list[dict]: ...

    async def run(self, sql: str, params: Sequence = ()) -> int:
        """Run a write and return how many rows it changed, so callers can detect lost races."""

    async def check(self) -> None: ...

    async def close(self) -> None: ...


def inline_nulls(sql, params):
    """Write None as NULL and bytes as X'..' into the statement, skipping quoted text.

    D1's HTTP API binds text only, so these two kinds of value are written as literals.
    Both backends do it, so local and hosted runs execute the same SQL.
    """
    params = list(params)
    if all(value is not None and not isinstance(value, bytes) for value in params):
        return sql, params
    out, kept, index, quote = [], [], 0, None
    for char in sql:
        if quote:
            quote = None if char == quote else quote
        elif char in "'\"":
            quote = char
        elif char == "?":
            value = params[index]
            index += 1
            if value is None:
                out.append("NULL")
                continue
            if isinstance(value, bytes):
                out.append(f"X'{value.hex()}'")
                continue
            kept.append(value)
        out.append(char)
    return "".join(out), kept


class SqliteDatabase:
    """A SQLite file, used from a worker thread so queries never block the event loop."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False, timeout=30
        )
        self.connection.row_factory = sqlite3.Row
        # A thread lock, not an asyncio one, so the file works from any event loop.
        self.lock = threading.Lock()

    def _execute(self, sql, params):
        with self.lock:
            before = self.connection.total_changes
            try:
                rows = self.connection.execute(sql, params).fetchall()
            except sqlite3.IntegrityError as error:
                raise ConstraintError(str(error)) from None
            return [dict(row) for row in rows], self.connection.total_changes - before

    async def _call(self, sql, params):
        return await asyncio.to_thread(self._execute, *inline_nulls(sql, params))

    async def query(self, sql, params=()):
        return (await self._call(sql, params))[0]

    async def run(self, sql, params=()):
        return (await self._call(sql, params))[1]

    async def check(self):
        await self.query("SELECT 1 AS ok")

    async def sync(self, work):
        """Run ``work(connection)`` in the worker thread, for steps that need sqlite3 itself."""

        def locked():
            with self.lock:
                return work(self.connection)

        return await asyncio.to_thread(locked)

    def close_now(self):
        with self.lock:
            self.connection.close()

    async def close(self):
        await asyncio.to_thread(self.close_now)


def as_text(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


class D1Database:
    """Cloudflare D1 through its HTTP query endpoint."""

    def __init__(self, token, account, database, *, http=None):
        self.url = D1_ENDPOINT.format(
            account=quote(account, safe=""), database=quote(database, safe="")
        )
        self.headers = {"Authorization": f"Bearer {token}"}
        self.http = http or httpx.AsyncClient()

    async def _execute(self, sql, params):
        sql, params = inline_nulls(sql, params)
        body = {"sql": sql, "params": [as_text(value) for value in params]}
        try:
            response = await self.http.post(
                self.url, headers=self.headers, json=body, timeout=D1_TIMEOUT
            )
        except httpx.TimeoutException:
            raise RuntimeError(
                f"D1 query failed: the request timed out after {D1_TIMEOUT} s"
            ) from None
        except httpx.HTTPError as error:
            raise RuntimeError(f"D1 query failed: {type(error).__name__}") from None
        status = f"D1 query failed (HTTP {response.status_code})"
        try:
            payload = response.json()
        except ValueError:
            raise RuntimeError(f"{status}: response was not JSON.") from None
        results = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("success"), bool):
            raise RuntimeError(f"{status}: response was not understood.")
        failed = not response.is_success or not payload["success"]
        if not failed and (
            not isinstance(results, list)
            or not all(isinstance(item, dict) and item.get("success") for item in results)
        ):
            failed = True
        if failed:
            details = "; ".join(
                str(error.get("message", ""))
                for error in payload.get("errors") or []
                if isinstance(error, dict)
            )
            message = f"{status}: {details}" if details else f"{status}."
            if "constraint failed" in details.lower():
                raise ConstraintError(message)
            raise RuntimeError(message)
        return results

    async def query(self, sql, params=()):
        results = await self._execute(sql, params)
        # D1 returns a BLOB as a list of byte values.
        return [
            {key: bytes(value) if isinstance(value, list) else value for key, value in row.items()}
            for item in results
            for row in item.get("results") or []
        ]

    async def run(self, sql, params=()):
        results = await self._execute(sql, params)
        return sum(int((item.get("meta") or {}).get("changes") or 0) for item in results)

    async def check(self):
        await self.query("SELECT 1 AS ok")

    async def close(self):
        await self.http.aclose()


def create_database(config, env=None):
    """D1 when its database ID is set, otherwise the instance's SQLite file."""
    env = os.environ if env is None else env
    if not (env.get("CLOUDFLARE_D1_DATABASE_ID") or "").strip():
        return SqliteDatabase(config.database_path)
    missing = [name for name in D1_VARIABLES if not (env.get(name) or "").strip()]
    if missing:
        raise DatabaseConfigError(f"Missing D1 configuration: {', '.join(missing)}")
    token, account, database = (env[name].strip() for name in D1_VARIABLES)
    return D1Database(token, account, database)
