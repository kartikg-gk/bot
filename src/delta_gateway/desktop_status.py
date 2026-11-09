"""Machine-readable local operator status for the desktop app."""

import sqlite3
import time
from contextlib import closing
from ipaddress import ip_address

from delta_gateway import background


def dashboard_url(config, state):
    if state["state"] != "ready" or config.web_port is None:
        return None
    if config.web_auth == "google":
        return config.web_origin
    host = config.web_host
    try:
        local = ip_address(host).is_loopback
    except ValueError:
        local = host == "localhost"
    port = state.get("web_port") or config.web_port
    if not local or not port:
        return None
    address = f"[{host}]" if ":" in host else host
    return f"http://{address}:{port}/"


def scheduler_state(config, state):
    if not state["running"]:
        return {"state": "stopped"}
    if config.web_auth == "google":
        return {"state": "per-account"}
    try:
        uri = config.database_path.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=0.2)) as db:
            lease = db.execute(
                "SELECT heartbeat,expires FROM scheduler_leases WHERE instance=?",
                (config.instance,),
            ).fetchone()
            setting = db.execute(
                "SELECT enabled FROM job_automation WHERE instance=?", (config.instance,)
            ).fetchone()
    except sqlite3.Error:
        return {"state": "unknown"}
    details = {"heartbeat": lease[0], "expires": lease[1]} if lease else {}
    if setting is not None and not setting[0]:
        return {"state": "paused", **details}
    return {"state": "online" if lease and lease[1] > time.time() else "waiting", **details}


def inspect_instance(config, path):
    state = background.status(config)
    return {
        "instance": state["instance"],
        "name": state["name"],
        "running": state["running"],
        "state": state["state"],
        "pid": state["pid"],
        "config": str(path),
        "web_auth": config.web_auth if config.web_port is not None else None,
        "dashboard": dashboard_url(config, state),
        "scheduler": scheduler_state(config, state),
    }
