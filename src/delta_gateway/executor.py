"""Run a bot's Delta process in a container with no real credentials."""

from dataclasses import dataclass, field
from uuid import uuid4

from delta_gateway.bots import bot_root
from delta_gateway.proxy import CredentialError

SANDBOX_WORKSPACE = "/workspace"
SANDBOX_SESSIONS = "/sessions"
SANDBOX_BOT = "/bot"
# Reachable from Docker Desktop and, through --add-host, from Linux Docker.
HOST_ALIAS = "host.docker.internal"


@dataclass
class SandboxLaunch:
    command: list[str]
    environment: dict[str, str]
    paths: list[tuple[str, str]] = field(default_factory=list)
    token: str | None = None


def key_variable(provider):
    from delta_app.catalog import provider_info

    return provider_info(provider).key_env


def docker_launch(config, bot, session, token, proxy_port, *, key_env=None, egress=None):
    """Build the container command. Secrets travel by name, never in arguments."""
    if not config.provider:
        raise CredentialError("Set a default provider for sandboxed bots")
    root = bot_root(config, bot)
    sessions = config.sessions_path
    key_env = key_env or key_variable(config.provider)
    base_env = config.provider.upper() + "_BASE_URL"
    if config.docker_network == "locked" and not egress:
        raise CredentialError("The locked sandbox network is not ready")
    base_url = egress.url if egress else f"http://{HOST_ALIAS}:{proxy_port}"
    environment = {key_env: token, base_env: base_url}
    command = [
        "docker",
        "run",
        "--rm",
        "-i",
        "--name",
        f"delta-bot-{bot.id[:12]}-{uuid4().hex[:8]}",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "512",
        "--memory",
        config.docker_memory,
        "--network",
        egress.network if egress else config.docker_network,
        *(() if egress else ("--add-host", f"{HOST_ALIAS}:host-gateway")),
        "-v",
        f"{config.project}:{SANDBOX_WORKSPACE}",
        "-v",
        f"{sessions}:{SANDBOX_SESSIONS}",
        "-v",
        f"{root}:{SANDBOX_BOT}",
    ]
    for name in environment:
        command += ["-e", name]
    command += [
        config.docker_image,
        "python",
        "-m",
        "delta_app.cli.main",
        "--mode",
        "rpc",
        "--cwd",
        SANDBOX_WORKSPACE,
        "--session-dir",
        SANDBOX_SESSIONS,
        "--agent-dir",
        f"{SANDBOX_BOT}/home",
        "--tool-policy",
        config.policy,
        "--approval-seconds",
        str(config.approval_seconds),
        "--approve" if config.trust_project else "--no-approve",
        "--provider",
        config.provider,
    ]
    paths = [
        (SANDBOX_SESSIONS, str(sessions)),
        (SANDBOX_WORKSPACE, str(config.project)),
    ]
    if session:
        name = str(session).replace("\\", "/").rsplit("/", 1)[-1]
        command += ["--session", f"{SANDBOX_SESSIONS}/{name}"]
    for flag, value in (("--model", config.model), ("--thinking", config.thinking)):
        if value:
            command += [flag, value]
    return SandboxLaunch(command + list(config.engine_args), environment, paths, token)
