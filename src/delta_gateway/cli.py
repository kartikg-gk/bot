"""Worker entry point and a local console for exercising the connection."""

import argparse
import asyncio
import sys
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
    for name in ("worker", "console"):
        sub = commands.add_parser(name)
        sub.add_argument("--cwd", type=Path, default=Path.cwd())
        sub.add_argument(
            "--session-dir", type=Path, default=Path.home() / ".delta-gateway/sessions"
        )
        sub.add_argument("--session", help="Resume an existing Delta session ID")
        sub.add_argument("--provider")
        sub.add_argument("--model")
        sub.add_argument("--policy", choices=["ask", "deny", "auto"], default="deny")
        sub.add_argument("--approval-seconds", type=int, default=300)
        sub.add_argument(
            "--trust-project",
            action="store_true",
            help="Explicitly trust this project's instructions and extensions",
        )
        sub.add_argument(
            "--allow-mutations",
            action="store_true",
            help="Allow mutating tools without individual approval",
        )
        sub.add_argument("--session-factory", help=argparse.SUPPRESS)
        if name == "console":
            sub.add_argument("prompt")
    return result


async def console(options):
    from delta_gateway.client import WorkerClient, worker_command

    args = [
        "--cwd",
        options.cwd,
        "--session-dir",
        options.session_dir,
        "--policy",
        options.policy,
        "--approval-seconds",
        options.approval_seconds,
    ]
    for key in ("session", "provider", "model", "session_factory"):
        value = getattr(options, key)
        if value:
            args.extend(["--" + key.replace("_", "-"), value])
    for key in ("trust_project", "allow_mutations"):
        if getattr(options, key):
            args.append("--" + key.replace("_", "-"))
    async with WorkerClient(worker_command(*args)) as client:
        print("Session: " + client.state["session_id"], file=sys.stderr)
        await client.request("submit", text=options.prompt)
        while True:
            event = await client.next_event()
            if event["event"] == "assistant.text":
                print(event["text"], end="", flush=True)
            elif event["event"] == "tool.started":
                print("\nTool: " + event["tool"], file=sys.stderr)
            elif event["event"] == "approval.requested":
                await client.request(
                    "approval.respond", approval_id=event["approval_id"], allow=False
                )
            elif event["event"] == "run.finished":
                print()
                if event["outcome"] != "completed":
                    raise RuntimeError("Run " + event["outcome"])
                break


def main():
    options = parser().parse_args()
    if options.command not in {"worker", "console"}:
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
        return
    options.cwd = options.cwd.expanduser().resolve()
    options.session_dir = str(options.session_dir.expanduser().resolve())
    if not options.cwd.is_dir():
        raise SystemExit("Working directory does not exist")
    if options.command == "worker":
        from delta_gateway.worker import run_worker

        run_worker(options)
    else:
        try:
            asyncio.run(console(options))
        except KeyboardInterrupt:
            raise SystemExit(130) from None
        except (RuntimeError, TimeoutError) as exc:
            raise SystemExit(str(exc)) from None
