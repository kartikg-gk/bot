"""Named leases in the shared database, for processes that cannot share a file lock.

A lease lasts ``seconds`` unless renewed. Used as an async context manager it renews
itself every third of that time and is released on exit. A holder that stops renewing
loses the lease once it expires, so a crashed process never blocks others for long.
"""

import asyncio
import contextlib
import time
from uuid import uuid4


class LeaseLost(RuntimeError):
    """Another holder took the lease after it expired."""


class Lease:
    def __init__(self, db, name, seconds=60, *, clock=time.time):
        self.db, self.name, self.seconds, self.clock = db, name, seconds, clock
        self.owner = uuid4().hex
        self.renewing = None
        self.lost = False

    async def acquire(self):
        expires = self.clock() + self.seconds
        if await self.db.run(
            "INSERT OR IGNORE INTO leases VALUES(?,?,?)", (self.name, self.owner, expires)
        ):
            return
        if not await self.db.run(
            "UPDATE leases SET owner=?,expires=? WHERE name=? AND expires<=?",
            (self.owner, expires, self.name, self.clock()),
        ):
            raise RuntimeError(f"{self.name} is busy")

    async def renew(self):
        if not await self.db.run(
            "UPDATE leases SET expires=? WHERE name=? AND owner=?",
            (self.clock() + self.seconds, self.name, self.owner),
        ):
            self.lost = True
            raise LeaseLost(f"{self.name} was taken over after its lease expired")

    async def release(self):
        if self.renewing is not None:
            self.renewing.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.renewing
            self.renewing = None
        await self.db.run("DELETE FROM leases WHERE name=? AND owner=?", (self.name, self.owner))

    async def keep(self):
        while True:
            await asyncio.sleep(self.seconds / 3)
            try:
                await self.renew()
            except LeaseLost:
                return

    async def __aenter__(self):
        await self.acquire()
        self.renewing = asyncio.create_task(self.keep())
        return self

    async def __aexit__(self, *_):
        await self.release()
