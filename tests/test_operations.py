import asyncio
import json
import sqlite3

import pytest

from delta_gateway.background import ProcessRecord, metadata, status
from delta_gateway.cli import parser
from delta_gateway.config import load, save
from delta_gateway.instances import OperatorError, forget, lease, register, registry, resolve
from delta_gateway.operators import configure
from delta_gateway.routing import admit_message
from delta_gateway.service import run_service
from delta_gateway.store import Store
from tests.test_gateway import Bot, config


def group(text="@testbot hello", *, user=11, thread=7):
    return {
        "from": {"id": user},
        "chat": {"id": -100, "type": "supergroup"},
        "message_thread_id": thread,
        "text": text,
    }


def test_group_topic_allowlists_and_ownership(tmp_path):
    c = config(tmp_path, allow_groups=True, groups=[-100])
    assert admit_message(group(), c, 99, "testbot") == (-100, 11, 7, "hello")
    assert admit_message(group("/status@testbot"), c, 99, "testbot")
    assert not admit_message(group("/status@otherbot"), c, 99, "testbot")
    assert not admit_message(group("hello"), c, 99, "testbot")
    assert not admit_message(group(user=22), c, 99, "testbot")
    message = group("hello")
    message["reply_to_message"] = {"from": {"id": 99}}
    assert admit_message(message, c, 99, "testbot")
    store = Store(c.state, c.instance)
    try:
        store.map(-100, 11, 7, "session-a", True)
        store.map(-100, 11, 8, "session-b", True)
        assert not store.owns(-100, 11, 8, "session-a")
        with pytest.raises(ValueError):
            store.map(-100, 11, 8, "session-a")
        assert store.mapping(-100, 11, 7)[0] == "session-a"
        store.admit(
            [{"update_id": 1, "message": group()}, {"update_id": 2, "message": group(thread=8)}],
            c,
            99,
            "testbot",
        )
        store.clear_queue(-100, 11, 7)
        assert [row[5] for row in store.pending()] == [8]
    finally:
        store.close()


@pytest.mark.parametrize("version", [1, 2])
def test_upgrade_preserves_routes_receipts_and_backup(tmp_path, version):
    c = config(tmp_path)
    c.state.mkdir()
    with sqlite3.connect(c.state / "gateway.sqlite3") as db:
        db.executescript("""
        CREATE TABLE instances(id TEXT PRIMARY KEY, bot_id INTEGER);
        CREATE TABLE poll_state(instance TEXT PRIMARY KEY, offset INTEGER NOT NULL);
        CREATE TABLE conversations(instance TEXT,chat INTEGER,user INTEGER,session TEXT,
          persisted INTEGER DEFAULT 0,PRIMARY KEY(instance,chat,user));
        CREATE TABLE inbox(instance TEXT,id INTEGER,chat INTEGER,user INTEGER,text TEXT,
          status TEXT,run TEXT,created REAL,PRIMARY KEY(instance,id));
        CREATE TABLE approvals(instance TEXT,id TEXT,chat INTEGER,user INTEGER,run TEXT,
          tool TEXT,digest TEXT,created REAL,expires REAL,decision TEXT,source TEXT,
          PRIMARY KEY(instance,id));
        """)
        db.execute(f"PRAGMA user_version={version}")
        db.execute("INSERT INTO conversations VALUES(?,11,11,'old',1)", (c.instance,))
        db.execute("INSERT INTO inbox VALUES(?,1,11,11,'hello','scheduled',NULL,0)", (c.instance,))
        db.execute("INSERT INTO poll_state VALUES(?,42)", (c.instance,))
    store = Store(c.state, c.instance)
    try:
        assert store.mapping(11, 11, 0) == ("old", 1)
        assert store.owns(11, 11, 0, "old")
        assert store.offset == 42
        assert store.pending()[0][4:] == ("pending", 0)
        backup = next(c.state.glob("gateway.before-v7-*.sqlite3"))
        with sqlite3.connect(backup) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == version
    finally:
        store.close()


def test_config_registry_selectors_rename_and_forget(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / ".delta-gateway" / "config.yaml"
    configure(parser().parse_args(["init", "--name", "test", "--user", "11"]))
    c = load(path)
    assert resolve(parser().parse_args(["-i", "test", "status"])) == path
    assert resolve(parser().parse_args(["status", "-i", "test"])) == path
    configure(parser().parse_args(["configure", "-i", "test", "--name", "renamed"]))
    assert list(registry()) == ["renamed"]
    lock = lease(c.instance)
    try:
        with pytest.raises(OperatorError):
            forget("renamed")
    finally:
        lock.close()
    forget("renamed")
    assert not registry()
    assert path.exists()


def test_explicit_config_wins_over_instance_selector(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    a, b = tmp_path / "a.json", tmp_path / "b.json"
    for path, name in [(a, "alpha"), (b, "beta")]:
        c = config(tmp_path, name=name)
        save(path, c)
        register(path, c)
    configure(parser().parse_args(["configure", "-c", str(a), "-i", "beta", "--model", "new"]))
    assert load(a).model == "new"
    assert load(b).model is None


def test_config_can_repair_missing_project(tmp_path):
    path = tmp_path / "config.json"
    c = config(tmp_path, name="test")
    save(path, c)
    data = json.loads(path.read_text())
    data["project"] = str(tmp_path / "removed-project")
    path.write_text(json.dumps(data))
    configure(parser().parse_args(["configure", "-c", str(path), "--project", str(tmp_path)]))
    assert load(path).project == tmp_path


def test_failed_repair_restores_original_config(tmp_path):
    other = config(tmp_path, name="taken")
    save(tmp_path / "other.json", other)
    register(tmp_path / "other.json", other)
    path = tmp_path / "config.json"
    c = config(tmp_path, name="original")
    save(path, c)
    data = json.loads(path.read_text())
    data["project"] = str(tmp_path / "missing")
    path.write_text(json.dumps(data))
    with pytest.raises(OperatorError):
        configure(
            parser().parse_args(
                ["configure", "-c", str(path), "--project", str(tmp_path), "--name", "taken"]
            )
        )
    assert json.loads(path.read_text()) == data


def test_stale_stop_nonce_and_lifecycle_metadata(tmp_path):
    from delta_gateway.background import directory
    from delta_gateway.instances import atomic_json

    c = config(tmp_path)
    record = ProcessRecord(c, tmp_path / "config.json")
    atomic_json(directory(c) / "stop.json", {"nonce": "stale"})
    assert not record.stop_requested()
    atomic_json(directory(c) / "stop.json", {"nonce": record.data["nonce"]})
    assert record.stop_requested()
    assert status(c)["state"] == "stopped"
    with lease(c.instance).file:
        assert status(c)["running"]
    assert not status(c)["running"]


async def test_cooperative_shutdown_during_long_poll(tmp_path):
    from delta_gateway.background import directory
    from delta_gateway.instances import atomic_json

    class Polling(Bot):
        async def call(self, method, **payload):
            if method == "getMe":
                return {"id": 99, "username": "testbot"}
            if method == "getWebhookInfo":
                return {}
            if method == "getUpdates":
                await asyncio.sleep(60)
            return {}

    c = config(tmp_path)
    task = asyncio.create_task(run_service(c, Polling()))
    try:
        async with asyncio.timeout(3):
            while metadata(c).get("state") != "ready":
                await asyncio.sleep(0.02)
        atomic_json(directory(c) / "stop.json", {"nonce": metadata(c)["nonce"]})
        await asyncio.wait_for(task, 3)
        assert metadata(c)["state"] == "stopped"
        assert not status(c)["running"]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_real_background_start_status_logs_stop_without_network(tmp_path, monkeypatch, capsys):
    import subprocess
    import sys

    from delta_gateway import background

    c = config(tmp_path, name="offline")
    path = tmp_path / "config.json"
    save(path, c)
    monkeypatch.setenv(c.token_env, "fake-offline-token")
    script = """
import asyncio, sys
from delta_gateway.config import load
from delta_gateway.service import run_service
class Bot:
    async def call(self, method, **payload):
        if method == "getMe": return {"id": 99, "username": "offline"}
        if method == "getWebhookInfo": return {}
        if method == "getUpdates": await asyncio.sleep(60)
        return {}
    async def send(self, *args, **kwargs): return {"message_id": 1}
    async def close(self): pass
asyncio.run(run_service(load(sys.argv[1]), Bot(), config_path=sys.argv[1], launch=sys.argv[2]))
"""
    real_popen = subprocess.Popen
    children = []

    def spawn(args, **kwargs):
        child = real_popen(
            [
                sys.executable,
                "-c",
                script,
                args[args.index("--config") + 1],
                args[args.index("--launch-id") + 1],
            ],
            **kwargs,
        )
        children.append(child)
        return child

    monkeypatch.setattr(background.subprocess, "Popen", spawn)
    try:
        started = background.start(c, path)
        assert started["state"] == "ready"
        assert background.status(c)["running"]
        background.logs(c)
        assert "Gateway ready" in capsys.readouterr().out
        assert background.stop(c)["state"] == "stopped"
        assert children[0].wait(timeout=3) == 0
        # An old stop request must not stop a new launch.
        assert background.start(c, path)["state"] == "ready"
        assert background.stop(c)["state"] == "stopped"
        assert children[1].wait(timeout=3) == 0
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=3)
