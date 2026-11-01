"""Operator command line."""

import argparse
import asyncio
from pathlib import Path


def parser():
    result = argparse.ArgumentParser(prog="delta-gateway")
    from delta_gateway.version_check import current_version

    result.add_argument("--version", action="version", version="delta-gateway " + current_version())
    result.add_argument("-c", "--config", type=Path, default=None)
    result.add_argument("-i", "--instance", default=None)
    commands = result.add_subparsers(dest="command", required=True)
    for name in (
        "configure",
        "init",
        "run",
        "doctor",
        "state",
        "start",
        "stop",
        "status",
        "logs",
        "remove",
        "migrate",
        "migrate-db",
        "config-path",
    ):
        sub = commands.add_parser(name)
        sub.add_argument("-c", "--config", type=Path, default=argparse.SUPPRESS)
        sub.add_argument("-i", "--instance", default=argparse.SUPPRESS)
        if name in {"configure", "init"}:
            if name == "configure":
                sub.add_argument("section", nargs="?", choices=["telegram"])
            sub.add_argument("--interactive", action="store_true")
            sub.add_argument("--project", type=Path)
            sub.add_argument("--user", "--allowed-user-id", type=int, action="append")
            sub.add_argument("--state-dir", type=Path)
            sub.add_argument("--name")
            sub.add_argument("--token-env")
            sub.add_argument("--provider")
            sub.add_argument("--model")
            sub.add_argument("--policy", choices=["engine", "ask", "deny", "auto"])
            sub.add_argument("--bot-token")
            sub.add_argument(
                "--thinking", choices=["off", "minimal", "low", "medium", "high", "xhigh", "max"]
            )
            sub.add_argument("--database", type=Path)
            sub.add_argument("--agent-dir", type=Path)
            sub.add_argument("--session-dir", type=Path)
            sub.add_argument("--engine-command")
            sub.add_argument("--engine-arg", action="append")
            sub.add_argument("--workers", type=int)
            sub.add_argument("--backlog", type=int)
            sub.add_argument("--idle-seconds", type=int)
            sub.add_argument("--retention-days", type=int)
            sub.add_argument("--release-url")
            sub.add_argument(
                "--include-user-in-group-session-key",
                dest="per_user_groups",
                action=argparse.BooleanOptionalAction,
                default=None,
            )
            sub.add_argument("--group", type=int, action="append")
            sub.add_argument("--allow-groups", action=argparse.BooleanOptionalAction, default=None)
            sub.add_argument(
                "--group-mentions-only", action=argparse.BooleanOptionalAction, default=None
            )
            sub.add_argument(
                "--draft-previews", action=argparse.BooleanOptionalAction, default=None
            )
        if name == "run":
            sub.add_argument("--launch-id", help=argparse.SUPPRESS)
        if name == "doctor":
            sub.add_argument("--online", action="store_true")
            sub.add_argument("--worker", action="store_true")
        if name == "state":
            sub.add_argument("--dismiss", type=int)
        if name == "logs":
            sub.add_argument("-f", "--follow", action="store_true")
            sub.add_argument("--lines", type=int, default=100)
        if name in {"start", "stop", "remove"}:
            sub.add_argument("--timeout", type=float)
        if name == "remove":
            sub.add_argument("target", nargs="?")
            sub.add_argument("--dry-run", action="store_true")
            sub.add_argument("--stop", action="store_true")
            sub.add_argument("--yes", action="store_true")
        if name in {"migrate", "migrate-db"}:
            sub.add_argument("--dry-run", action="store_true")
            sub.add_argument("--all", action="store_true")
            sub.add_argument("--database", type=Path)
    instances = commands.add_parser("instances")
    instances.add_argument("action", nargs="?", choices=["list", "forget"], default="list")
    instances.add_argument("target", nargs="?")
    return result


def main():
    options = parser().parse_args()
    from delta_gateway.health import operator_command
    from delta_gateway.instances import OperatorError
    from delta_gateway.telegram import TelegramError

    try:
        asyncio.run(operator_command(options))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except (OperatorError, TelegramError) as exc:
        raise SystemExit(str(exc)) from None
    except Exception:
        raise SystemExit(
            "Gateway operation failed; check configuration, state and bot access."
        ) from None
