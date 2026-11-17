import sqlite3

import pytest

from delta_gateway.database import SqliteDatabase
from delta_gateway.migrations import SCHEMA, VERSIONED_TABLES, ensure_schema
from delta_gateway.rows import ConflictError, update_versioned
from tests.fake_d1 import FakeD1


async def columns(db, table):
    return [row["name"] for row in await db.query(f"SELECT name FROM pragma_table_info('{table}')")]


async def tables(db):
    rows = await db.query(
        "SELECT name FROM sqlite_master WHERE type IN ('table','index','trigger') "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name"
    )
    return [row["name"] for row in rows]


async def test_schema_applies_twice_without_error(database):
    await ensure_schema(database)
    first = await tables(database)
    await ensure_schema(database)
    assert await tables(database) == first
    assert {"jobs", "inbox", "web_runs", "schema_migrations"} <= set(first)


async def test_hosted_schema_matches_local(tmp_path):
    local, hosted = SqliteDatabase(tmp_path / "local.sqlite3"), FakeD1().database()
    await ensure_schema(local)
    await ensure_schema(hosted)
    assert await tables(hosted) == [name for name in await tables(local)]
    for table in ("jobs", "inbox", "web_runs", "approvals"):
        assert await columns(hosted, table) == await columns(local, table)
    await local.close()
    await hosted.close()


async def test_shared_tables_carry_versions(database):
    await ensure_schema(database)
    for table in VERSIONED_TABLES:
        assert "version" in await columns(database, table)


def test_existing_local_database_gains_version_columns(tmp_path):
    from delta_gateway.migrations import migrate

    path = tmp_path / "old.sqlite3"
    db = sqlite3.connect(path)
    migrate(db, 0)
    assert db.execute("PRAGMA user_version").fetchone()[0] == SCHEMA
    for table in VERSIONED_TABLES:
        names = [row[1] for row in db.execute(f"PRAGMA table_info({table})")]
        assert "version" in names
    db.close()


async def counter(db):
    await db.run(
        "CREATE TABLE counters (id TEXT PRIMARY KEY, value INTEGER NOT NULL, "
        "note TEXT, version INTEGER NOT NULL DEFAULT 0)"
    )
    await db.run("INSERT INTO counters (id, value) VALUES ('c', 0)")


async def test_concurrent_writers_both_land(database):
    import asyncio

    await counter(database)

    def increment(row):
        row["value"] += 1

    await asyncio.gather(
        *(update_versioned(database, "counters", {"id": "c"}, increment) for _ in range(2))
    )
    [row] = await database.query("SELECT value, version FROM counters")
    assert row == {"value": 2, "version": 2}


async def test_versioned_update_writes_null(database):
    await counter(database)
    await update_versioned(database, "counters", {"id": "c"}, lambda row: row.update(note="x"))
    updated = await update_versioned(
        database, "counters", {"id": "c"}, lambda row: row.update(note=None)
    )
    assert updated["note"] is None
    assert (await database.query("SELECT note FROM counters"))[0]["note"] is None


async def test_missing_row_is_reported(database):
    await counter(database)
    with pytest.raises(KeyError, match="No counters row"):
        await update_versioned(database, "counters", {"id": "missing"}, lambda row: None)


class LosingDatabase:
    def __init__(self, db):
        self.db = db

    async def query(self, sql, params=()):
        return await self.db.query(sql, params)

    async def run(self, sql, params=()):
        return 0


async def test_conflict_after_ten_attempts(database):
    await counter(database)
    with pytest.raises(ConflictError, match=r"^counters \{'id': 'c'\} kept changing; try again\.$"):
        await update_versioned(LosingDatabase(database), "counters", {"id": "c"}, lambda row: None)
