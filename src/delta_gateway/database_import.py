"Stopped-instance consolidation with backups and resumable per-instance imports."

import hashlib
import json
import os
import sqlite3
import time
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import yaml

from delta_gateway.config import load, profile_home, save
from delta_gateway.instances import OperatorError, atomic_json, config_lease, home, registry
from delta_gateway.migrations import SCHEMA
from delta_gateway.ownership import SessionLock
from delta_gateway.store import Store

TABLES = (
    "instances",
    "conversations",
    "routes",
    "poll_state",
    "inbox",
    "approvals",
    "session_owners",
    "session_status",
    "messages",
    "instance_archive",
)


def snapshot(path, config, config_path):
    if not path.exists():
        return {}, config.instance
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA user_version").fetchone()[0] > SCHEMA:
            raise OperatorError("Source database is newer than this gateway")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        data = {
            table: [dict(row) for row in db.execute(f"SELECT * FROM {table}")]
            for table in TABLES
            if table in tables
        }
    scopes = {row["instance"] for rows in data.values() for row in rows if "instance" in row}
    scopes.update(row["id"] for row in data.get("instances", []))
    source = config.instance
    if scopes and source not in scopes:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        explicit = bool(raw.get("instanceId") or raw.get("instance"))
        if explicit or len(scopes) != 1:
            raise OperatorError("Database ownership is ambiguous or does not match this config")
        source = next(iter(scopes))
    result = {}
    for table, rows in data.items():
        result[table] = [
            row
            for row in rows
            if (
                row.get("instance", source) == source
                and (table != "instances" or row["id"] == source)
            )
        ]
    return result, source


def import_rows(store, config, data, source_key, fingerprint):
    db = store.db
    receipt = db.execute(
        "SELECT fingerprint FROM imports WHERE instance=? AND source=?",
        (config.instance, source_key),
    ).fetchone()
    if receipt:
        if receipt[0] != fingerprint:
            raise OperatorError(
                "An imported source changed; keep affected bots stopped and review it"
            )
        return False
    source_routes = data.get("routes", [])
    if not source_routes:
        source_routes = [
            dict(
                row,
                session_file=str(config.sessions_path / f"{row['session']}.jsonl")
                if row.get("session")
                else None,
            )
            for row in data.get("conversations", [])
        ]
    id_map, key_map = {}, {}
    with db:
        db.execute("BEGIN IMMEDIATE")
        for row in source_routes:
            key = row["chat"], row.get("user", 0) or 0, row.get("thread", 0) or 0
            route = store.conversation(*key)
            if (
                route.session_file
                and row.get("session_file")
                and Path(route.session_file).resolve() != Path(row["session_file"]).resolve()
            ):
                raise OperatorError(
                    "Destination has a conflicting conversation; no rows were imported "
                    "for this instance"
                )
            owner = next(
                (
                    owner
                    for owner in data.get("session_owners", [])
                    if owner["session"] == row.get("session")
                ),
                {},
            )
            db.execute(
                (
                    "UPDATE routes SET session=?,session_file=?,title=?,provider=?,mode"
                    "l=?,thinking=?,created=?,updated=? WHERE instance=? AND id=?"
                ),
                (
                    row.get("session"),
                    row.get("session_file"),
                    row.get("title", owner.get("title")),
                    row.get("provider", owner.get("provider")),
                    row.get("model", owner.get("model")),
                    row.get("thinking", owner.get("thinking")),
                    row.get("created", 0),
                    row.get("updated", time.time()),
                    config.instance,
                    route.id,
                ),
            )
            db.execute(
                (
                    "INSERT INTO conversations VALUES(?,?,?,?,?,?) ON CONFLICT(instance"
                    ",chat,user,thread) DO UPDATE SET session=excluded.session,persiste"
                    "d=excluded.persisted"
                ),
                (
                    config.instance,
                    key[0],
                    key[1],
                    row.get("session"),
                    int(bool(row.get("session_file"))),
                    key[2],
                ),
            )
            if "id" in row:
                id_map[row["id"]] = route.id
            key_map[key] = route.id
        for table in ("poll_state", "inbox", "approvals", "session_owners", "session_status"):
            columns = [row[1] for row in db.execute(f"PRAGMA table_info({table})")]
            for original in data.get(table, []):
                row = {key: value for key, value in original.items() if key in columns}
                row["instance"] = config.instance
                if table in {"inbox", "approvals", "session_owners"}:
                    row.setdefault("thread", 0)
                if table == "inbox":
                    row.setdefault("route_user", row["user"])
                    if row["status"] in {"dispatching", "running", "delivering"}:
                        row["status"] = "uncertain"
                    if row["status"] == "scheduled":
                        row["status"] = "pending"
                    existing = db.execute(
                        "SELECT chat,user,text FROM inbox WHERE instance=? AND id=?",
                        (config.instance, row["id"]),
                    ).fetchone()
                    if existing and (
                        existing[0] != row["chat"]
                        or existing[1] != row["user"]
                        or (
                            existing[2] is not None
                            and row.get("text") is not None
                            and existing[2] != row["text"]
                        )
                    ):
                        raise OperatorError("Conflicting update receipt in destination")
                if table == "approvals" and row.get("decision") is None:
                    row.update(decision="denied", source="migration")
                names = list(row)
                placeholders = ",".join("?" for _ in names)
                db.execute(
                    f"INSERT OR IGNORE INTO {table}({','.join(names)}) VALUES({placeholders})",
                    tuple(row.values()),
                )
                if table == "poll_state":
                    db.execute(
                        "UPDATE poll_state SET offset=max(offset,?) WHERE instance=?",
                        (row["offset"], config.instance),
                    )
        for row in data.get("messages", []):
            target = id_map.get(row.get("route"))
            if target is None:
                raise OperatorError("Audit message refers to an unknown source conversation")
            db.execute(
                (
                    "INSERT OR IGNORE INTO messages(instance,route,direction,text,platf"
                    "orm_id,created) VALUES(?,?,?,?,?,?)"
                ),
                (
                    config.instance,
                    target,
                    row["direction"],
                    row.get("text"),
                    row.get("platform_id"),
                    row.get("created", 0),
                ),
            )
        for row in data.get("instances", []):
            if row.get("bot_id") is not None:
                old = db.execute(
                    "SELECT bot_id FROM instances WHERE id=?", (config.instance,)
                ).fetchone()
                if old and old[0] not in {None, row["bot_id"]}:
                    raise OperatorError("Destination instance is bound to another bot")
                db.execute(
                    "UPDATE instances SET bot_id=? WHERE id=?", (row["bot_id"], config.instance)
                )
        if data.get("instance_archive"):
            db.execute(
                "INSERT OR REPLACE INTO instance_archive VALUES(?,?)",
                (config.instance, data["instance_archive"][0].get("archived")),
            )
        db.execute(
            "INSERT INTO imports VALUES(?,?,?,?)",
            (config.instance, source_key, fingerprint, time.time()),
        )
    return True


def migrate_configs(paths, *, destination=None, dry_run=False):
    paths = [Path(path).expanduser().resolve() for path in paths]
    configs = [(path, load(path, require_project=False)) for path in paths]
    destination = (
        Path(destination or profile_home() / "state" / "gateway.sqlite3").expanduser().resolve()
    )
    if len({config.instance for _, config in configs}) != len(configs):
        raise OperatorError("Duplicate instance IDs in migration batch")
    sources = {config.database_path.resolve() for _, config in configs}
    with ExitStack() as stack:
        # Include registered peers even when only one config was selected.
        peers = {config.instance: config for _, config in configs}
        for entry in registry().values():
            other = load(entry["config"], require_project=False)
            if other.database_path.resolve() in sources | {destination}:
                peers[other.instance] = other
        for identifier in sorted(peers):
            try:
                stack.enter_context(config_lease(peers[identifier]))
            except RuntimeError:
                raise OperatorError("Stop every affected gateway before migration") from None
        schema_id = hashlib.sha256(os.path.normcase(str(destination)).encode()).hexdigest()
        lock = SessionLock(home() / "locks", "schema-" + schema_id)
        stack.callback(lock.close)
        work = []
        for path, config in configs:
            source = config.database_path.resolve()
            data, scope = snapshot(source, config, path)
            # Multiple unidentified configs may not claim the same legacy rows.
            for earlier in work:
                if (
                    earlier[2] == source
                    and earlier[4] == scope
                    and earlier[1].instance != config.instance
                ):
                    raise OperatorError(
                        "Multiple configs claim the same source rows; ownership is ambiguous"
                    )
            target_config = path.with_suffix(".yaml") if path.suffix == ".json" else path
            if (
                target_config != path
                and target_config.exists()
                and load(target_config, require_project=False).instance != config.instance
            ):
                raise OperatorError("Destination YAML config belongs to another instance")
            counts = {table: len(rows) for table, rows in data.items()}
            print(f"{path}: {source} -> {destination}; counts={counts}; config={target_config}")
            work.append((path, config, source, data, scope, target_config))
        if dry_run:
            return
        for _path, config, source, data, scope, target_config in work:
            if source != destination and source.exists():
                backup_path = source.with_name(source.name + ".backup-" + uuid4().hex)
                with (
                    sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as original,
                    sqlite3.connect(backup_path) as backup,
                ):
                    original.backup(backup)
                backup_path.chmod(0o600)
            store = Store(destination, config.instance)
            try:
                if source != destination:
                    key = source.as_uri() + "#" + str(scope)
                    fingerprint = hashlib.sha256(
                        json.dumps(data, sort_keys=True, ensure_ascii=True).encode()
                    ).hexdigest()
                    import_rows(store, config, data, key, fingerprint)
            finally:
                store.close()
            config.database = destination
            # Preserve the old session location even when source state had defaults.
            config.session_dir = config.sessions_path
            save(target_config, config, require_project=False)
            registry_lock = SessionLock(home() / "locks", "registry")
            try:
                entries = registry()
                for entry in entries.values():
                    if entry["instance"] == config.instance:
                        entry["config"] = str(target_config)
                if config.name:
                    name = config.name.casefold()
                    if name in entries and entries[name]["instance"] != config.instance:
                        raise OperatorError(
                            "Instance name collision while updating migrated config"
                        )
                    entries[name] = dict(
                        name=config.name, instance=config.instance, config=str(target_config)
                    )
                atomic_json(home() / "instances.json", dict(version=1, instances=entries))
            finally:
                registry_lock.close()
