"""Named config discovery and instance ownership on the current host."""

import json
import os
import time
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

from delta_gateway.config import load
from delta_gateway.ownership import SessionLock
from delta_gateway.plugins import CREDENTIAL_VARIABLES


class OperatorError(RuntimeError):
    """An operator-facing error which contains no credential values."""


def home():
    return (
        Path(os.environ.get("DELTA_GATEWAY_HOME", str(Path.home() / ".delta-gateway")))
        .expanduser()
        .resolve()
    )


def atomic_json(path, document):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        temporary.write_text(json.dumps(document, indent=2), encoding="utf-8")
        temporary.chmod(0o600)
        for attempt in range(4):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 3:
                    raise
                time.sleep(0.05)
    finally:
        temporary.unlink(missing_ok=True)


def lease(instance):
    return SessionLock(home() / "locks", instance)


def running(instance):
    try:
        lock = lease(instance)
    except RuntimeError:
        return True
    lock.close()
    return False


def config_lease(config):
    stack = ExitStack()
    try:
        primary = lease(config.instance)
        stack.callback(primary.close)
        if (config.state / "locks" / f"{config.instance}.lock").exists():
            legacy = SessionLock(config.state / "locks", config.instance)
            stack.callback(legacy.close)
        return stack
    except BaseException:
        stack.close()
        raise


def token_variables(config):
    names = {"TELEGRAM_BOT_TOKEN", config.token_env, *CREDENTIAL_VARIABLES}
    for entry in registry().values():
        try:
            names.add(load(entry["config"], require_project=False).token_env)
        except (OSError, ValueError):
            pass
    return names


def registry():
    path = home() / "instances.json"
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("version") != 1 or not isinstance(value.get("instances"), dict):
        raise OperatorError("Unsupported instance registry")
    return value["instances"]


def register(path, config):
    if not config.name:
        return
    path = Path(path).resolve()
    lock = SessionLock(home() / "locks", "registry")
    try:
        entries = registry()
        name = config.name.casefold()
        for old_name, entry in list(entries.items()):
            previous = Path(entry["config"]).resolve()
            if old_name == name and entry["instance"] != config.instance:
                raise OperatorError("Instance name already exists; choose another name")
            if previous == path and entry["instance"] != config.instance:
                raise OperatorError("Config path is already registered to another instance")
            if entry["instance"] == config.instance:
                if previous != path and previous.exists():
                    raise OperatorError(
                        "Duplicate instance ID: use init for another bot, not a config copy"
                    )
                entries.pop(old_name)
        entries[name] = {"name": config.name, "instance": config.instance, "config": str(path)}
        atomic_json(home() / "instances.json", {"version": 1, "instances": entries})
    finally:
        lock.close()


def forget(name):
    lock = SessionLock(home() / "locks", "registry")
    try:
        entries = registry()
        entry = entries.get(name.casefold())
        if not entry:
            raise OperatorError("Unknown instance name")
        try:
            config = load(entry["config"], require_project=False)
            owner = config_lease(config)
        except (OSError, ValueError):
            try:
                owner = lease(entry["instance"])
            except RuntimeError:
                raise OperatorError("Stop the instance before forgetting it") from None
        except RuntimeError:
            raise OperatorError("Stop the instance before forgetting it") from None
        try:
            entries.pop(name.casefold())
            if "config" in locals() and config.database_path.is_file():
                from delta_gateway.store import Store

                store = Store(config.database_path, config.instance)
                try:
                    store.archive()
                finally:
                    store.close()
            atomic_json(home() / "instances.json", {"version": 1, "instances": entries})
        finally:
            owner.close()
    finally:
        lock.close()


def resolve(options):
    explicit = getattr(options, "config", None)
    if (
        not explicit
        and getattr(options, "instance", None)
        and getattr(options, "target", None)
        and options.instance.casefold() != options.target.casefold()
    ):
        raise OperatorError("Instance selectors refer to different bots")
    selected = getattr(options, "instance", None) or getattr(options, "target", None)
    if explicit:
        path = Path(explicit).expanduser().resolve()
        return path
    if selected:
        directory = Path(selected).expanduser()
        if directory.is_dir() or any(separator in selected for separator in ("/", "\\")):
            root = directory.resolve()
            for name in ("config.yaml", "config.yml", "config.json"):
                for candidate in (root / ".delta-gateway" / name, root / name):
                    if candidate.is_file():
                        return candidate
            raise OperatorError("No gateway config found in selected directory")
        entry = registry().get(selected.casefold())
        if not entry:
            raise OperatorError("Unknown instance name; use instances to list names")
        return Path(entry["config"]).resolve()
    for parent in (Path.cwd(), *Path.cwd().parents):
        for name in ("config.yaml", "config.yml", "config.json"):
            path = parent / ".delta-gateway" / name
            if path.is_file():
                return path.resolve()
    for name in ("config.yaml", "config.yml", "config.json"):
        legacy = home() / name
        if legacy.is_file():
            return legacy
    raise OperatorError("No config found; use init, --config PATH, or --instance NAME")


def matching_state(config):
    result = {config.instance}
    for entry in registry().values():
        path = Path(entry["config"])
        if path.is_file():
            other = load(path, require_project=False)
            if other.database_path.resolve() == config.database_path.resolve():
                result.add(other.instance)
    return sorted(result)
