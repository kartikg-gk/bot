"""Interactive configuration and a searchable engine-backed model picker."""

import getpass
import json
import os
import subprocess

from prompt_toolkit import prompt
from prompt_toolkit.completion import FuzzyCompleter, WordCompleter

from delta_gateway.instances import token_variables
from delta_gateway.rpc_client import engine_command


def ask(label, current=""):
    value = input(f"{label}" + (f" [{current}]" if current else "") + ": ").strip()
    return value or current


def model_choices(config):
    records = [
        dict(type="get_capabilities", id="caps"),
        dict(type="get_available_models", id="models"),
        dict(type="shutdown", id="stop"),
    ]
    try:
        result = subprocess.run(
            engine_command(config),
            input="".join(json.dumps(row) + "\n" for row in records),
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=15,
            cwd=str(config.project),
            env={k: v for k, v in os.environ.items() if k not in token_variables(config)},
        )
        parsed = [json.loads(line) for line in result.stdout.splitlines() if line]
        capabilities = next(
            row["data"] for row in parsed if row.get("id") == "caps" and row.get("success")
        )
        if capabilities.get("engine") != "delta" or capabilities.get("protocolVersion") != 1:
            return []
        response = next(
            row["data"] for row in parsed if row.get("id") == "models" and row.get("success")
        )
        return [row["provider"] + "/" + row["id"] for row in response["models"]]
    except (OSError, ValueError, KeyError, StopIteration, subprocess.TimeoutExpired):
        return []


def choose_model(config):
    models = model_choices(config)
    current = f"{config.provider}/{config.model}" if config.provider and config.model else ""
    if models:
        selection = prompt(
            "Model (type to search; 'default' clears): ",
            default=current,
            completer=FuzzyCompleter(WordCompleter(models, sentence=True)),
            complete_while_typing=True,
        ).strip()
    else:
        selection = ask("Model provider/model-id (manual; 'default' clears)", current)
    if selection.lower() == "default":
        config.provider = config.model = None
    elif selection:
        provider, separator, model = selection.partition("/")
        if not separator or not provider or not model:
            raise ValueError("Use provider/model-id")
        config.provider, config.model = provider, model


def interactive(config, options):
    from pathlib import Path

    from pydantic import SecretStr

    if not getattr(options, "name", None):
        config.name = ask("Gateway name", config.name or "gateway")
    if not getattr(options, "user", None):
        config.users = [
            int(value.strip())
            for value in ask(
                "Allowed Telegram user IDs (comma separated)", ",".join(map(str, config.users))
            ).split(",")
        ]
    if not getattr(options, "bot_token", None):
        token = getpass.getpass(f"Bot token (blank keeps current / {config.token_env}): ").strip()
        if token:
            config.bot_token = SecretStr(token)
    if not getattr(options, "project", None):
        config.project = (
            Path(ask("Delta working directory", str(config.project))).expanduser().resolve()
        )
    if not getattr(options, "agent_dir", None):
        value = ask(
            "Separate Delta agent directory (blank uses default)", str(config.agent_dir or "")
        )
        config.agent_dir = Path(value).expanduser().resolve() if value else None
    if not getattr(options, "model", None):
        choose_model(config)
    if not getattr(options, "thinking", None):
        config.thinking = (
            ask("Default thinking (blank keeps engine default)", config.thinking or "") or None
        )
    return config
