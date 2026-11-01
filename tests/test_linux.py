import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from delta_gateway.background import metadata, status
from delta_gateway.config import save
from tests.test_gateway import config

pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX lifecycle checks")


def test_sigterm_during_long_poll_stops_cleanly(tmp_path):
    c = config(tmp_path)
    path = tmp_path / "config.yaml"
    save(path, c)
    script = """
import asyncio,sys
from delta_gateway.config import load
from delta_gateway.service import run_service
class Bot:
    async def call(self, method, **kwargs):
        if method=="getMe": return {"id":99,"username":"fake"}
        if method=="getUpdates": await asyncio.sleep(60)
        return {}
    async def send(self,*args,**kwargs): return {"message_id":1}
    async def close(self): pass
asyncio.run(run_service(load(sys.argv[1]),Bot(),config_path=sys.argv[1]))
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        while metadata(c).get("state") != "ready":
            assert child.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        child.send_signal(signal.SIGTERM)
        assert child.wait(timeout=5) == 0
        assert metadata(c)["state"] == "stopped"
        assert not status(c)["running"]
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)


def test_systemd_template_syntax(tmp_path):
    if not shutil.which("systemd-analyze"):
        pytest.skip("systemd-analyze is unavailable")
    source = Path(__file__).parents[1] / "systemd" / "delta-gateway.service"
    text = source.read_text().replace("User=delta-agent", "")
    text = text.replace("WorkingDirectory=/srv/delta-agent/project", "WorkingDirectory=/tmp")
    text = text.replace(
        "EnvironmentFile=/etc/delta-gateway/research.env", "EnvironmentFile=-/dev/null"
    )
    text = text.replace(
        "ExecStart=/opt/delta-gateway/bin/python -m delta_gateway run "
        "--config /etc/delta-gateway/research.yaml",
        "ExecStart=/usr/bin/true",
    )
    unit = tmp_path / "delta-gateway.service"
    unit.write_text(text)
    result = subprocess.run(
        ["systemd-analyze", "verify", str(unit)], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr
