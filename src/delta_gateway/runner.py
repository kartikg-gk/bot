"""The process that owns an instance's scheduled work when the web runs elsewhere.

Hosted web requests are short and many; scheduled jobs and event wake-ups need one
long-lived owner, as in the reference. The runner holds a database lease for the
instance (60 seconds, renewed while it runs), so a second runner is refused and a
crashed one is replaced once its lease expires. Only the lease holder settles work a
previous run left unfinished. It does no Telegram polling.
"""

import asyncio

from delta_gateway.leases import Lease
from delta_gateway.migrations import ensure_schema

RUNNER_SECONDS = 60
POLL_SECONDS = 2


async def run_runner(config, database, *, once=False, factory=None, poll=POLL_SECONDS):
    """Run due jobs until stopped; with ``once``, only the jobs due when it starts."""
    from delta_gateway.service import Service
    from delta_gateway.store import Store

    await ensure_schema(database)
    lease = Lease(database, f"runner-{config.instance}", RUNNER_SECONDS)
    try:
        await lease.__aenter__()
    except RuntimeError:
        raise RuntimeError("A runner is already running for this instance") from None
    service = None
    try:
        store = await Store.open(
            database, config.instance, retention_days=config.retention_days, recover=True
        )
        service = Service(config, None, factory=factory, store=store)
        if once:
            await service.run_due()
            await asyncio.gather(*service.job_tasks.values(), return_exceptions=True)
            return
        while not lease.lost:
            await service.run_due()
            await asyncio.sleep(poll)
        raise RuntimeError("Another runner took over after this one's lease expired")
    finally:
        if service is not None:
            await service.close()
        await lease.__aexit__()


async def command(config, options):
    from delta_gateway.database import create_database

    database = create_database(config)
    try:
        await run_runner(config, database, once=options.once)
    finally:
        await database.close()
