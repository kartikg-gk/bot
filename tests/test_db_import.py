"""`db check` and `db import`: copy a local instance into the hosted database."""

import sqlite3

import pytest

from delta_gateway.d1_import import check_database, import_local
from delta_gateway.database import SqliteDatabase
from delta_gateway.instances import OperatorError
from delta_gateway.migrations import ensure_schema
from delta_gateway.store import Store
from delta_gateway.web import WEB_CHAT
from tests.fake_d1 import FakeD1
from tests.test_gateway import config


async def local_instance(tmp_path):
    """A local instance with a job, notes and a shared skill on disk."""
    c = config(tmp_path)
    local = SqliteDatabase(c.database_path)
    try:
        store = await Store.open(local, c.instance)
        chief = (await store.bots())[0].id
        job = await store.add_job(WEB_CHAT, 11, 11, 0, chief, "check mail", "every:3600", 4e9)
    finally:
        await local.close()
    notes = c.state / "bots" / chief / "memory"
    notes.mkdir(parents=True)
    (notes / "PREFERENCES.md").write_text("Short answers.", encoding="utf-8")
    skill = c.state / "skills" / "triage" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("---\nname: triage\n---\nSort issues.", encoding="utf-8")
    return c, chief, job


async def test_import_keeps_ids_and_owners(tmp_path):
    c, chief, job = await local_instance(tmp_path)
    target = FakeD1().database()
    await import_local(c, target)
    rows = await target.query("SELECT id,instance,bot,task FROM jobs")
    assert rows == [{"id": job, "instance": c.instance, "bot": chief, "task": "check mail"}]
    bots = await target.query("SELECT id FROM bots WHERE instance=?", (c.instance,))
    assert [row["id"] for row in bots] == [chief]
    notes = await target.query("SELECT bot,file,content FROM bot_memory")
    assert notes == [{"bot": chief, "file": "PREFERENCES.md", "content": "Short answers."}]
    skills = await target.query("SELECT scope,skill,path FROM skill_files")
    assert skills == [{"scope": "", "skill": "triage", "path": "SKILL.md"}]
    # The hosted store opens on the imported rows without adding a second chief.
    store = await Store.open(target, c.instance, recover=False)
    assert [bot.id for bot in await store.bots()] == [chief]
    await target.close()


async def test_import_twice_copies_nothing_new(tmp_path, capsys):
    c, _, _ = await local_instance(tmp_path)
    target = FakeD1().database()
    first = await import_local(c, target)
    assert first["jobs"] == (1, 0)
    assert first["skill_files"] == (1, 0)
    second = await import_local(c, target)
    assert all(copied == 0 for copied, _ in second.values())
    assert second["jobs"] == (0, 1)
    assert "jobs: 0 copied, 1 already there" in capsys.readouterr().out
    await target.close()


async def test_import_refuses_other_bots_in_target(tmp_path):
    c, _, _ = await local_instance(tmp_path)
    target = FakeD1().database()
    await Store.open(target, c.instance, recover=False)  # creates its own chief
    with pytest.raises(OperatorError, match="already has bots"):
        await import_local(c, target)
    await target.close()


async def test_import_refuses_older_local_schema(tmp_path):
    c, _, _ = await local_instance(tmp_path)
    with sqlite3.connect(c.database_path) as db:
        db.execute("PRAGMA user_version=3")
    with pytest.raises(OperatorError, match="older schema"):
        await import_local(c, FakeD1().database())


async def test_check_reports_backend(database, capsys):
    await ensure_schema(database)
    await check_database(database)
    out = capsys.readouterr().out
    backend = "D1" if type(database).__name__ == "D1Database" else "SQLite"
    assert out.startswith(f"{backend}: reachable")
    assert "schema gateway-" in out


async def test_check_reports_empty_database(database, capsys):
    await check_database(database)
    assert "no schema yet" in capsys.readouterr().out
