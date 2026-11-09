"Detached execution with cooperative stopping; never signal an unverified PID."

import json
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from uuid import uuid4

from delta_gateway.instances import OperatorError, atomic_json, config_lease, home, running
from delta_gateway.ownership import SessionLock


def directory(config):
    return home() / "runtime" / config.instance


def metadata(config):
    path = directory(config) / "process.json"
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("instance") != config.instance:
        raise OperatorError("Process ownership metadata does not match this instance")
    return data


class ProcessRecord:
    def __init__(self, config, path, launch=None):
        self.config = config
        self.data = dict(
            instance=config.instance,
            config=str(Path(path).resolve()),
            pid=os.getpid(),
            nonce=uuid4().hex,
            launch=launch,
            state="starting",
            started=time.time(),
        )
        atomic_json(directory(config) / "process.json", self.data)

    def ready(self, web_port=None):
        self.data.update(state="ready", web_port=web_port)
        atomic_json(directory(self.config) / "process.json", self.data)
        print("Gateway ready: " + (self.config.name or self.config.instance), flush=True)

    def stop_requested(self):
        path = directory(self.config) / "stop.json"
        if not path.exists():
            return False
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("nonce") == self.data["nonce"]
        except (ValueError, OSError):
            return False

    def finish(self, state):
        self.data.update(state=state, stopped=time.time())
        atomic_json(directory(self.config) / "process.json", self.data)
        print("Gateway " + state, flush=True)


def status(config):
    try:
        lock = config_lease(config)
    except RuntimeError:
        active = True
    else:
        lock.close()
        active = False
    record = metadata(config)
    state = record.get("state", "starting") if active else "stopped"
    if active and state not in {"starting", "ready"}:
        state = "starting"
    return dict(
        name=config.name,
        instance=config.instance,
        running=active,
        state=state,
        pid=record.get("pid") if active else None,
        web_port=record.get("web_port") if active and state == "ready" else None,
    )


def start(config, path):
    lock = SessionLock(home() / "locks", "launch-" + config.instance)
    try:
        if status(config)["running"]:
            raise OperatorError("Instance is already running")
        if not config.web_only and not config.token():
            raise OperatorError("Set the configured token environment variable before starting")
        root = directory(config)
        root.mkdir(parents=True, exist_ok=True)
        root.chmod(0o700)
        logfile = root / "gateway.log"
        if logfile.exists() and logfile.stat().st_size > 5 * 1024 * 1024:
            os.replace(logfile, root / "gateway.previous.log")
        launch = uuid4().hex
        args = [
            sys.executable,
            "-m",
            "delta_gateway",
            "run",
            "--config",
            str(path),
            "--launch-id",
            launch,
        ]
        options = (
            {"creationflags": subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP}
            if os.name == "nt"
            else {"start_new_session": True}
        )
        with logfile.open("ab", buffering=0) as output:
            logfile.chmod(0o600)
            child = subprocess.Popen(
                args,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=output,
                cwd=str(config.project),
                close_fds=True,
                **options,
            )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            record = metadata(config)
            if (
                record.get("launch") == launch
                and record.get("state") == "ready"
                and running(config.instance)
            ):
                return status(config)
            if child.poll() is not None:
                raise OperatorError("Gateway failed to start; inspect logs for this instance")
            time.sleep(0.1)
        # Startup may still be in progress. Do not kill an unverified/reused process ID.
        raise OperatorError("Startup is still pending; use status or logs before retrying")
    finally:
        lock.close()


def stop(config):
    if not status(config)["running"]:
        return status(config)
    record = metadata(config)
    if not record.get("nonce"):
        raise OperatorError("Process is still starting; retry stop shortly")
    atomic_json(directory(config) / "stop.json", {"nonce": record["nonce"]})
    deadline = time.monotonic() + config.shutdown_timeout
    while time.monotonic() < deadline:
        if not status(config)["running"]:
            return status(config)
        current = metadata(config)
        if current.get("nonce") and current["nonce"] != record["nonce"]:
            record = current
            atomic_json(directory(config) / "stop.json", {"nonce": record["nonce"]})
        time.sleep(0.1)
    raise OperatorError(
        "Shutdown deadline exceeded; configuration was preserved. Inspect the host supervisor."
    )


def logs(config, lines=100, follow=False):
    if not 1 <= lines <= 10000:
        raise OperatorError("Log line count must be between 1 and 10000")
    path = directory(config) / "gateway.log"
    if not path.exists():
        print("No background log yet; foreground output belongs to its terminal.")
        return
    with path.open(encoding="utf-8", errors="replace") as source:
        print("".join(deque(source, maxlen=lines)), end="", flush=True)
        while follow:
            line = source.readline()
            if line:
                print(line, end="", flush=True)
            elif not running(config.instance):
                break
            else:
                time.sleep(0.2)
