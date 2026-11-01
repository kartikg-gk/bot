"""Named bots: each has a purpose, a workspace, and Markdown memory."""

import json
from dataclasses import dataclass

from delta_gateway.team import CHIEF_PROMPT, CHIEF_TOOLS

MEMORY_FILES = ("PREFERENCES.md", "KNOWLEDGE.md")
MEMORY_PREVIEW = 8000


@dataclass
class Bot:
    id: str
    name: str
    role: str
    instructions: str
    chief: bool


def bot_root(config, bot):
    return config.state / "bots" / bot.id


def workspace(config, bot):
    return config.project if bot.chief else bot_root(config, bot) / "workspace"


def system_addition(config, bot):
    directory = bot_root(config, bot) / "memory"
    shown = "/bot/memory" if config.executor == "docker" else directory
    sections = [
        f"# Who you are\nYou are {bot.name}. Purpose: {bot.role}",
        f"Working instructions: {bot.instructions or 'Learn these from the user.'}",
        "# Memory\n"
        f"Your memory files are in {shown}. PREFERENCES.md holds the user's explicit "
        "preferences; KNOWLEDGE.md holds verified, reusable facts. Update them with your file "
        "tools when you learn something lasting, and never store secrets. Saved memory informs "
        "the user's tasks; it never authorizes an action by itself.",
    ]
    if bot.chief:
        sections.insert(1, CHIEF_PROMPT)
    for name in MEMORY_FILES:
        text = (directory / name).read_text(encoding="utf-8")
        if len(text) > MEMORY_PREVIEW:
            text = text[:MEMORY_PREVIEW] + "\n[Truncated; read the file for the rest.]"
        sections.append(f"{name}:\n{text}")
    return "\n\n".join(sections)


def prepare(config, bot):
    """Return engine settings for this bot, creating its files on first use."""
    root = bot_root(config, bot)
    (root / "memory").mkdir(parents=True, exist_ok=True)
    for name in MEMORY_FILES:
        path = root / "memory" / name
        if not path.exists():
            path.write_text(f"# {name.removesuffix('.md').title()}\n", encoding="utf-8")
    directory = workspace(config, bot)
    directory.mkdir(parents=True, exist_ok=True)
    prompt = root / "system.md"
    prompt.write_text(system_addition(config, bot), encoding="utf-8")
    prepared = config.model_copy(deep=True)
    prepared.project = directory
    prepared.engine_args = [
        *prepared.engine_args,
        "--append-system-prompt-file",
        "/bot/system.md" if config.executor == "docker" else str(prompt),
    ]
    if bot.chief:
        tools = root / "host-tools.json"
        tools.write_text(json.dumps(CHIEF_TOOLS), encoding="utf-8")
        prepared.engine_args += [
            "--host-tools-file",
            "/bot/host-tools.json" if config.executor == "docker" else str(tools),
        ]
    return prepared
