"""Run events keep one order and runs finish once, on SQLite and on D1."""

import asyncio

from delta_gateway.store import Store
from delta_gateway.web_runs import RunJournal

INSTANCE = "a" * 32


async def test_concurrent_events_get_distinct_sequence_numbers(database):
    store = await Store.open(database, INSTANCE)
    journal = RunJournal(store)
    route = await journal.route(9, 11, None)
    run, _ = await journal.queue(route, "hello", 2**59)
    await asyncio.gather(*(journal.emit(run, "tick", n=n) for n in range(10)))
    sequence = [seq for seq, _, _ in await journal.events(run, 0)]
    assert sequence == list(range(1, 12))


async def test_cancelled_run_finishes_once(database):
    store = await Store.open(database, INSTANCE)
    journal = RunJournal(store)
    route = await journal.route(9, 11, None)
    run, _ = await journal.queue(route, "hello", 2**59)
    assert await journal.cancel(run, 11)
    await asyncio.gather(journal.finish(run), journal.finish(run))
    kinds = [kind for _, kind, _ in await journal.events(run, 0)]
    assert kinds.count("run_finished") == 1
    assert (await journal.get(run, 11))["status"] == "cancelled"
