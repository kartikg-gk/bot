"""Interactive configuration and a searchable engine-backed model picker."""

import getpass
import os

from prompt_toolkit import prompt
from prompt_toolkit.completion import FuzzyCompleter, WordCompleter


def ask(label, current=""):
    value = input(f"{label}" + (f" [{current}]" if current else "") + ": ").strip()
    return value or current


def model_choices(config):
    """Models from Delta's catalog for providers that have credentials on this machine."""
    try:
        from delta_app.catalog import available_providers, models_for
        from delta_app.config.store import load_config

        saved = load_config()
        return [
            f"{provider.key}/{model}"
            for provider in available_providers()
            if not provider.needs_key
            or os.environ.get(provider.key_env)
            or saved.api_keys.get(provider.key)
            for model in models_for(provider.key)
        ]
    except (ImportError, OSError, ValueError, AttributeError):
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
    if not config.web_only and not getattr(options, "user", None):
        config.users = [
            int(value.strip())
            for value in ask(
                "Allowed Telegram user IDs (comma separated)", ",".join(map(str, config.users))
            ).split(",")
        ]
    if not config.web_only and not getattr(options, "bot_token", None):
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
