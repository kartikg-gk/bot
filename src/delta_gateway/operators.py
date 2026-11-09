"Configuration, lifecycle, migration, and diagnostics for named gateways."

import hashlib
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from uuid import uuid4

from delta_gateway import background
from delta_gateway.config import Config, load, save
from delta_gateway.database_import import migrate_configs
from delta_gateway.embedded import default_provider
from delta_gateway.instances import (
    OperatorError,
    atomic_json,
    config_lease,
    forget,
    home,
    register,
    registry,
    resolve,
)
from delta_gateway.ownership import SessionLock
from delta_gateway.service import run_service
from delta_gateway.store import Store
from delta_gateway.telegram_sdk import TelegramSDK as Telegram


def default_config():
    return Path.cwd() / ".delta-gateway" / "config.yaml"


def configure(options):
    if options.config or options.instance:
        path = resolve(options)
    else:
        try:
            path = resolve(options)
        except OperatorError:
            path = default_config()
    identity = hashlib.sha256(os.path.normcase(str(path.resolve())).encode()).hexdigest()
    lock = SessionLock(home() / "locks", "config-" + identity)
    try:
        configure_path(options, path)
    finally:
        lock.close()


def configure_path(options, path):
    from pydantic import SecretStr

    previous = load(path, require_project=False) if path.exists() else None
    if previous and options.command == "init":
        raise OperatorError("Config already exists; use configure telegram to edit it")
    updates = (
        "user",
        "project",
        "name",
        "model",
        "provider",
        "thinking",
        "bot_token",
        "token_env",
        "state_dir",
        "database",
        "agent_dir",
        "session_dir",
        "policy",
        "group",
        "allow_groups",
        "per_user_groups",
        "group_mentions_only",
        "draft_previews",
        "workers",
        "backlog",
        "idle_seconds",
        "retention_days",
        "release_url",
        "web_only",
        "web_port",
        "executor",
        "docker_image",
        "daytona_snapshot",
    )
    explicit_updates = any(getattr(options, name, None) is not None for name in updates)
    asking = getattr(options, "interactive", False) or (
        sys.stdin.isatty()
        and sys.stdout.isatty()
        and (
            (previous is None and not options.user and not options.web_only) or not explicit_updates
        )
    )
    if previous is None:
        users = options.user or ([1] if options.web_only else None)
        if not users and asking:
            users = [
                int(value.strip())
                for value in input("Allowed Telegram user IDs (comma separated): ").split(",")
            ]
        if not users:
            raise OperatorError("Supply at least one --user with your numeric Telegram ID")
        identifier = uuid4().hex
        config = Config(
            instance=identifier,
            users=users,
            project=(options.project or Path.cwd()).expanduser().resolve(),
            state=(options.state_dir or home() / "state" / identifier).expanduser().resolve(),
        )
    else:
        config = previous.model_copy(deep=True)
    fields = {
        "project": "project",
        "users": "user",
        "state": "state_dir",
        "name": "name",
        "token_env": "token_env",
        "provider": "provider",
        "model": "model",
        "thinking": "thinking",
        "policy": "policy",
        "groups": "group",
        "allow_groups": "allow_groups",
        "group_mentions_only": "group_mentions_only",
        "draft_previews": "draft_previews",
        "database": "database",
        "agent_dir": "agent_dir",
        "session_dir": "session_dir",
        "per_user_groups": "per_user_groups",
        "workers": "workers",
        "backlog": "backlog",
        "idle_seconds": "idle_seconds",
        "retention_days": "retention_days",
        "release_url": "release_url",
        "web_only": "web_only",
        "web_port": "web_port",
        "executor": "executor",
        "docker_image": "docker_image",
        "daytona_snapshot": "daytona_snapshot",
    }
    for field, option in fields.items():
        value = getattr(options, option, None)
        if value is not None:
            if field in {"workers", "backlog"} and value == 0:
                value = None
            setattr(
                config, field, value.expanduser().resolve() if isinstance(value, Path) else value
            )
    if config.web_only and config.web_port is None:
        config.web_port = 8765
    if getattr(options, "bot_token", None):
        config.bot_token = SecretStr(options.bot_token)
    if config.model and config.model.lower() == "default":
        config.provider = config.model = None
    elif config.model and not config.provider and "/" in config.model:
        config.provider, config.model = config.model.split("/", 1)
    if not config.name:
        config.name = (
            re.sub(r"[^A-Za-z0-9-]+", "-", config.project.name).strip("-")[:64] or "gateway"
        )
    try:
        owner = config_lease(config)
    except RuntimeError:
        raise OperatorError("Stop this instance before editing configuration") from None
    try:
        if asking:
            from delta_gateway.setup import interactive

            config = interactive(config, options)
        config = Config.model_validate(config.model_dump())
        config.validate_paths()
        if (
            previous
            and (previous.database_path != config.database_path or previous.state != config.state)
            and previous.database_path.exists()
        ):
            raise OperatorError(
                "State contains history; use migrate-db to change its database or p"
                "reserve the runtime/session paths"
            )
        save(path, config)
        try:
            register(path, config)
        except BaseException:
            if previous:
                save(path, previous, require_project=False)
            else:
                path.unlink(missing_ok=True)
            raise
    finally:
        owner.close()
    print(f"Configured {config.name}: {path}")
    if not config.web_only and not config.token():
        print(f"Set {config.token_env} in the process environment before starting.")


async def doctor(config, options):
    config.validate_paths()
    print("Configuration and project directory: OK")
    config.state.mkdir(parents=True, exist_ok=True)
    probe = config.state / (".write-probe-" + uuid4().hex)
    try:
        with probe.open("x"):
            pass
    finally:
        probe.unlink(missing_ok=True)
    print("State directory writable: OK")
    print("Process: " + background.status(config)["state"])
    if options.worker:
        default_provider(config)
        print("Delta and the model provider load: OK")
    if options.online and not config.web_only:
        token = config.token()
        if not token:
            raise OperatorError("Set the configured token before checking online")
        telegram = Telegram(token)
        try:
            await telegram.call("getMe")
            print("Bot connectivity: OK")
        finally:
            await telegram.close()


def archive(config):
    if config.database_path.exists():
        store = Store(config.database_path, config.instance)
        try:
            store.archive()
        finally:
            store.close()


async def operator_command(options):
    if options.command in {"configure", "init"}:
        configure(options)
        return
    if options.command == "config-path":
        try:
            path = resolve(options)
        except OperatorError:
            if options.config or options.instance:
                raise
            path = default_config()
        print(path)
        return
    if options.command == "instances":
        if options.action == "forget":
            if not options.target:
                raise OperatorError("Use instances forget NAME")
            forget(options.target)
            print("Registry entry archived; config and history preserved.")
        elif options.json:
            print(json.dumps(sorted(registry().values(), key=lambda row: row["name"].casefold())))
        else:
            for entry in registry().values():
                try:
                    state = background.status(load(entry["config"], require_project=False))["state"]
                except (ValueError, OSError):
                    state = "config missing or invalid"
                print(f"{entry['name']} | {state} | {entry['config']}")
        return
    if options.command in {"migrate", "migrate-db"}:
        if options.all and (options.config or options.instance):
            raise OperatorError("Choose --all or an instance selector")
        if options.all or (
            options.command == "migrate-db" and not options.config and not options.instance
        ):
            paths = sorted({entry["config"] for entry in registry().values()})
        else:
            paths = [resolve(options)]
        if not paths:
            print("No registered configs; select --config PATH to migrate an unregistered one.")
            return
        migrate_configs(paths, destination=options.database, dry_run=options.dry_run)
        return
    path = resolve(options)
    config = load(
        path, require_project=options.command not in {"stop", "status", "logs", "remove", "state"}
    )
    for entry in registry().values():
        previous = Path(entry["config"]).resolve()
        if entry["instance"] == config.instance and previous != path and previous.exists():
            raise OperatorError(
                "Copied instance ID; select the registered config or use init for another bot"
            )
    if getattr(options, "timeout", None) is not None:
        if not 0 < options.timeout <= 300:
            raise OperatorError("Timeout must be between zero and 300 seconds")
        config.shutdown_timeout = options.timeout
    if options.command == "task":
        from delta_gateway.task_cli import command

        await command(config, options)
        return
    if options.command == "skills":
        from delta_gateway.skills_cli import command

        command(config, options)
        return
    if options.command in {"run", "start"}:
        register(path, config)
    if options.command == "run":
        telegram = None
        if not config.web_only:
            token = config.token()
            if not token:
                raise OperatorError("Set the configured bot token before running")
            telegram = Telegram(token)
        await run_service(config, telegram, config_path=path, launch=options.launch_id)
    elif options.command == "start":
        state = background.start(config, path)
        print(
            f"Gateway {state['state']}: {state.get('name') or state['instance']} "
            f"(PID {state.get('pid')})"
        )
        print(f'Stop: delta-gateway stop --config "{path}"')
        print(f'Logs: delta-gateway logs --config "{path}" --follow')
    elif options.command == "status" and options.json:
        from delta_gateway.desktop_status import inspect_instance

        print(json.dumps(inspect_instance(config, path)))
    elif options.command in {"stop", "status"}:
        state = background.stop(config) if options.command == "stop" else background.status(config)
        print(
            f"{state.get('name') or state['instance']}: {state['state']}"
            + (f" (PID {state['pid']})" if state.get("pid") else "")
        )
        print("Config: " + str(path))
    elif options.command == "logs":
        background.logs(config, options.lines, options.follow)
    elif options.command == "doctor":
        await doctor(config, options)
    elif options.command == "state":
        with config_lease(config):
            if not config.database_path.exists():
                print("No state database yet.")
                return
            with sqlite3.connect(config.database_path) as db:
                if options.dismiss is not None:
                    db.execute(
                        (
                            "UPDATE inbox SET status='dismissed' WHERE instance=? AND id=? AND "
                            "status IN ('uncertain','failed','delivery_failed')"
                        ),
                        (config.instance, options.dismiss),
                    )
                columns = {row[1] for row in db.execute("PRAGMA table_info(inbox)")}
                selected = (
                    "id,chat,user,thread,status" if "thread" in columns else "id,chat,user,status"
                )
                print(
                    db.execute(
                        f"SELECT {selected} FROM inbox WHERE instance=? "
                        "AND status IN ('uncertain','failed','delivery_failed')",
                        (config.instance,),
                    ).fetchall()
                )
    elif options.command == "remove":
        if options.dry_run:
            print(f"Would remove config only: {path}; history, logs and database preserved")
            return
        if options.stop:
            background.stop(config)
        try:
            owner = config_lease(config)
        except RuntimeError:
            raise OperatorError(
                "Stop the instance or use remove --stop before removing config"
            ) from None
        try:
            if not options.yes and input(f"Remove config {path}? [y/N] ").strip().lower() != "y":
                print("Cancelled.")
                return
            lock = SessionLock(home() / "locks", "registry")
            try:
                archive(config)
                entries = {
                    key: entry
                    for key, entry in registry().items()
                    if entry["instance"] != config.instance
                }
                atomic_json(home() / "instances.json", dict(version=1, instances=entries))
                path.unlink()
            finally:
                lock.close()
        finally:
            owner.close()
        print("Config removed and instance archived; history, logs and database preserved.")
