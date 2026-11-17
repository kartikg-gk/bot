"""The runner owns scheduled work for an instance under one database lease."""

import asyncio
import time

import pytest

from delta_gateway.leases import Lease
from delta_gateway.migrations import ensure_schema
from delta_gateway.runner import run_runner
from delta_gateway.store import Store
from delta_gateway.web import WEB_CHAT
from tests.test_gateway import config

FACTORY = "tests.fake_session:create"


def runner_config(tmp_path):
    return config(tmp_path, web_only=True, web_port=0)


async def test_second_runner_refused(tmp_path, database):
    c = runner_config(tmp_path)
    await ensure_schema(database)
    holder = Lease(database, f"runner-{c.instance}")
    await holder.acquire()
    with pytest.raises(RuntimeError, match="^A runner is already running for this instance$"):
        await run_runner(c, database, once=True, factory=FACTORY)
    await holder.release()


async def test_runner_takes_over_after_expiry(tmp_path, database):
    c = runner_config(tmp_path)
    await ensure_schema(database)
    stale = Lease(database, f"runner-{c.instance}", clock=lambda: time.time() - 120)
    await stale.acquire()
    await run_runner(c, database, once=True, factory=FACTORY)
    assert await database.query("SELECT * FROM leases") == []


async def test_running_job_interrupted_on_takeover(tmp_path, database):
    c = runner_config(tmp_path)
    store = await Store.open(database, c.instance)
    bot = (await store.bots())[0].id
    job = await store.add_job(WEB_CHAT, 11, 11, 0, bot, "task", "every:3600", time.time() + 3600)
    await database.run("UPDATE jobs SET running=1 WHERE id=?", (job,))
    await run_runner(c, database, once=True, factory=FACTORY)
    finished = await store.job(job)
    assert not finished.running and finished.last_status == "interrupted"


async def test_once_runs_only_jobs_due_at_start(tmp_path, database):
    c = runner_config(tmp_path)
    store = await Store.open(database, c.instance)
    bot = (await store.bots())[0].id
    due = await store.add_job(WEB_CHAT, 11, 11, 0, bot, "hello", "once", time.time() - 1)
    later = await store.add_job(WEB_CHAT, 11, 11, 0, bot, "later", "once", time.time() + 3600)
    await asyncio.wait_for(run_runner(c, database, once=True, factory=FACTORY), 30)
    assert (await store.job(due)).last_status == "completed"
    assert (await store.job(later)).last_status is None


def test_runner_command_parses():
    from delta_gateway.cli import parser

    options = parser().parse_args(["runner", "--once"])
    assert options.command == "runner" and options.once
