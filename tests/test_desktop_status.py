import json
import time

from delta_gateway import background
from delta_gateway.cli import parser
from delta_gateway.config import load
from delta_gateway.operators import operator_command
from delta_gateway.store import Store
from delta_gateway.wakeups import set_enabled


async def test_desktop_cli_status_resolves_real_loopback_port_and_scheduler(
    tmp_path, capsys, monkeypatch
):
    path = tmp_path / "gateway.yaml"
    parse = parser().parse_args
    await operator_command(
        parse(
            [
                "init",
                "--config",
                str(path),
                "--project",
                str(tmp_path),
                "--name",
                "Desktop",
                "--web-only",
                "--web-port",
                "0",
                "--executor",
                "docker",
                "--docker-image",
                "test-image",
            ]
        )
    )
    capsys.readouterr()
    cfg = load(path)
    assert cfg.executor == "docker" and cfg.docker_image == "test-image"
    await operator_command(parse(["instances", "list", "--json"]))
    entries = json.loads(capsys.readouterr().out)
    assert entries == [{"instance": cfg.instance, "name": "Desktop", "config": str(path)}]
    store = Store(cfg.database_path, cfg.instance)
    store.scheduler_heartbeat("fixture", time.time(), 30)
    monkeypatch.setattr(
        background,
        "status",
        lambda _: {
            "instance": cfg.instance,
            "name": cfg.name,
            "running": True,
            "state": "ready",
            "web_port": 15111,
            "pid": 123,
        },
    )
    try:
        args = parse(["status", "--config", str(path), "--json"])
        await operator_command(args)
        result = json.loads(capsys.readouterr().out)
        assert result["dashboard"] == "http://127.0.0.1:15111/"
        assert result["scheduler"]["state"] == "online"
        assert "token" not in result
        set_enabled(store.db, cfg.instance, False)
        await operator_command(args)
        assert json.loads(capsys.readouterr().out)["scheduler"]["state"] == "paused"
    finally:
        store.close()
