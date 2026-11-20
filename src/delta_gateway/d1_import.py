"""Check the configured database, and copy a local instance into the hosted one.

``db import`` reads the local SQLite file read-only and writes each row into the target
with ``INSERT OR IGNORE``, so ids and owners stay as they were and a second run copies
nothing new. Bot notes and skill folders go into the tables a hosted gateway reads them
from. Leases are live locks, so they are left behind.

Delta session files are not copied: a hosted gateway keeps no session files and starts
runs from the earlier messages the browser sends. Their paths stay in the copied rows.
"""

import hashlib
import sqlite3
import time

from delta_gateway.database import D1Database, create_database
from delta_gateway.instances import OperatorError
from delta_gateway.migrations import SCHEMA, ensure_schema
from delta_gateway.skill_store import NOTES

SKIPPED = {"sqlite_sequence", "schema_migrations", "leases", "scheduler_leases"}


def backend_name(db):
    return "D1" if isinstance(db, D1Database) else "SQLite"


async def check_database(db):
    """Print the backend, that it answers, and the schema it holds. Changes nothing."""
    await db.check()
    tables = await db.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    )
    marks = []
    if tables:
        marks = sorted(
            row["id"]
            for row in await db.query("SELECT id FROM schema_migrations")
            if row["id"].startswith("gateway-")
        )
    schema = f"schema {marks[-1]}" if marks else "no schema yet"
    print(f"{backend_name(db)}: reachable, {schema} (this gateway expects gateway-{SCHEMA})")


def read_local(config, source):
    """Rows of this instance, by table, in the order the schema creates the tables."""
    if not source.is_file():
        raise OperatorError(f"No local database at {source}")
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version != SCHEMA:
            raise OperatorError(
                f"Local database has an {'older' if version < SCHEMA else 'newer'} schema "
                f"({version}, expected {SCHEMA}); run the gateway on it once with this "
                "version first"
            )
        names = [
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY rowid"
            )
            if row[0] not in SKIPPED
        ]
        data = {}
        for table in names:
            columns = [row[1] for row in db.execute(f"PRAGMA table_info({table})")]
            if table == "instances":
                sql, params = "SELECT * FROM instances WHERE id=?", (config.instance,)
            elif table == "route_bots":
                sql = (
                    "SELECT route_bots.* FROM route_bots JOIN routes "
                    "ON routes.id=route_bots.route WHERE routes.instance=?"
                )
                params = (config.instance,)
            elif "instance" in columns:
                sql, params = f"SELECT * FROM {table} WHERE instance=?", (config.instance,)
            else:
                continue
            data[table] = [dict(row) for row in db.execute(sql, params)]
    return data


def folder_rows(config, bots):
    """Notes and skill files from disk, shaped as ``bot_memory`` and ``skill_files`` rows."""
    now = time.time()
    notes, skills = [], []
    places = [("", config.state / "skills", None)]
    for bot in bots:
        root = config.state / "bots" / bot
        places.append((bot, root / "skills", root / "memory"))
    for scope, skill_folder, note_folder in places:
        if note_folder is not None:
            for name in NOTES:
                path = note_folder / name
                if path.is_file():
                    notes.append(
                        dict(
                            instance=config.instance,
                            bot=scope,
                            file=name,
                            content=path.read_text(encoding="utf-8"),
                            updated=now,
                        )
                    )
        if skill_folder.is_dir():
            for path in sorted(skill_folder.rglob("*")):
                if not path.is_file():
                    continue
                skill, *rest = path.relative_to(skill_folder).parts
                if not rest:
                    continue
                content = path.read_text(encoding="utf-8")
                skills.append(
                    dict(
                        instance=config.instance,
                        scope=scope,
                        skill=skill,
                        path="/".join(rest),
                        content=content,
                        revision=hashlib.sha256(content.encode()).hexdigest(),
                    )
                )
    return notes, skills


async def import_local(config, target, source=None):
    """Copy the local instance into *target*; return ``{table: (copied, already_there)}``."""
    source = source or config.database_path
    data = read_local(config, source)
    local_bots = {row["id"] for row in data.get("bots", [])}
    await ensure_schema(target)
    there = {
        row["id"]
        for row in await target.query("SELECT id FROM bots WHERE instance=?", (config.instance,))
    }
    if there - local_bots:
        raise OperatorError(
            "The hosted database already has bots for this instance that the local one "
            "does not; import into an empty hosted database"
        )
    notes, skills = folder_rows(config, sorted(local_bots))
    data["bot_memory"] = data.get("bot_memory", []) + notes
    data["skill_files"] = data.get("skill_files", []) + skills
    counts = {}
    for table, rows in data.items():
        if not rows:
            continue
        columns = {
            row["name"]
            for row in await target.query(f"SELECT name FROM pragma_table_info('{table}')")
        }
        copied = 0
        for row in rows:
            row = {key: value for key, value in row.items() if key in columns}
            names = ",".join(row)
            marks = ",".join("?" for _ in row)
            copied += await target.run(
                f"INSERT OR IGNORE INTO {table}({names}) VALUES({marks})", list(row.values())
            )
        counts[table] = (copied, len(rows) - copied)
    for table, (copied, skipped) in counts.items():
        print(f"{table}: {copied} copied, {skipped} already there")
    if not counts:
        print("Nothing to copy.")
    return counts


async def command(config, options):
    target = create_database(config)
    try:
        if options.action == "check":
            await check_database(target)
            return
        if not isinstance(target, D1Database):
            raise OperatorError(
                "db import copies into D1; set CLOUDFLARE_D1_DATABASE_ID, "
                "CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN first"
            )
        await import_local(config, target, options.source)
    finally:
        await target.close()
