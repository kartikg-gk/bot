"""Named leases in the database stand in for file locks when hosted."""

import asyncio

import pytest

from delta_gateway.leases import Lease, LeaseLost
from delta_gateway.migrations import ensure_schema


async def test_second_holder_refused_until_expiry(database):
    await ensure_schema(database)
    first = Lease(database, "bot-1", seconds=60, clock=lambda: 1000)
    await first.acquire()
    second = Lease(database, "bot-1", seconds=60, clock=lambda: 1030)
    with pytest.raises(RuntimeError, match="^bot-1 is busy$"):
        await second.acquire()
    await first.release()
    await second.acquire()
    await second.release()


async def test_expired_lease_taken_over_and_old_holder_finds_out(database):
    await ensure_schema(database)
    now = [1000]
    first = Lease(database, "bot-1", seconds=60, clock=lambda: now[0])
    await first.acquire()
    now[0] = 1061
    second = Lease(database, "bot-1", seconds=60, clock=lambda: now[0])
    await second.acquire()
    with pytest.raises(LeaseLost):
        await first.renew()
    await first.release()
    assert (await database.query("SELECT owner FROM leases WHERE name='bot-1'"))[0][
        "owner"
    ] == second.owner
    await second.release()


async def test_context_manager_renews_while_held(database):
    await ensure_schema(database)
    async with Lease(database, "bot-1", seconds=0.3) as lease:
        await asyncio.sleep(0.5)
        await lease.renew()
    assert await database.query("SELECT * FROM leases") == []


async def test_hosted_bot_reservation_spans_processes(database, tmp_path):
    from types import SimpleNamespace

    from delta_gateway.bot_profiles import reserve
    from delta_gateway.store import Store

    store = await Store.open(database, "a" * 32)
    one = SimpleNamespace(active_bots=set(), store=store, config=SimpleNamespace(state=tmp_path))
    two = SimpleNamespace(active_bots=set(), store=store, config=SimpleNamespace(state=tmp_path))
    async with reserve(one, "bot-x"):
        with pytest.raises(RuntimeError, match="This bot is working"):
            async with reserve(two, "bot-x"):
                pass
    async with reserve(two, "bot-x"):
        assert "bot-x" in two.active_bots
