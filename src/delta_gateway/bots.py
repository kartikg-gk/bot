"""Named bots: each has a purpose, a workspace, and Markdown memory."""

from dataclasses import dataclass

from delta_gateway.plugins import PLUGINS
from delta_gateway.team import CHIEF_PROMPT, CHIEF_TOOLS, JOB_TOOLS

SELF_TOOLS = [
    dict(
        name="update_own_bot",
        description="Save your own purpose or instructions after explicit user approval.",
        parameters={
            "type": "object",
            "properties": {
                "description": {"type": "string", "minLength": 1, "maxLength": 10000},
                "instructions": {"type": "string", "maxLength": 10000},
            },
            "minProperties": 1,
            "additionalProperties": False,
        },
    )
]

MEMORY_FILES = ("PREFERENCES.md", "KNOWLEDGE.md")
MEMORY_PREVIEW = 8000
# Where a sandboxed bot sees its memory folder.
MEMORY_MOUNT = "/bot/memory"


@dataclass
class Bot:
    id: str
    name: str
    role: str
    instructions: str
    chief: bool
    temporary: bool = False


def bot_root(config, bot):
    return config.state / "bots" / bot.id


def workspace(config, bot):
    return config.project if bot.chief else bot_root(config, bot) / "workspace"


def system_addition(config, bot, memory_path=None):
    directory = bot_root(config, bot) / "memory"
    shown = memory_path or directory
    sections = [
        f"# Who you are\nYou are {bot.name}. Purpose: {bot.role}",
        f"Working instructions: {bot.instructions or 'Learn these from the user.'}",
        "# Memory\n"
        f"Your memory files are in {shown}. PREFERENCES.md holds the user's explicit "
        "preferences. KNOWLEDGE.md holds verified, reusable facts. Use write_memory to propose "
        "lasting changes for review, and never store secrets. Saved memory informs "
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


@dataclass
class Prepared:
    config: object
    addition: str
    definitions: list

    @property
    def project(self):
        return self.config.project


def prepare(config, bot, plugins=(), memory_path=None):
    """Return the bot's settings, prompt addition and gateway tools, creating its files."""
    root = bot_root(config, bot)
    (root / "memory").mkdir(parents=True, exist_ok=True)
    for name in MEMORY_FILES:
        path = root / "memory" / name
        if not path.exists():
            path.write_text(f"# {name.removesuffix('.md').title()}\n", encoding="utf-8")
    directory = workspace(config, bot)
    directory.mkdir(parents=True, exist_ok=True)
    prepared = config.model_copy(deep=True)
    prepared.project = directory
    from delta_gateway.skill_tools import TOOLS

    definitions = [*SELF_TOOLS, *TOOLS, *JOB_TOOLS, *(CHIEF_TOOLS if bot.chief else [])]
    for plugin in plugins:
        definitions += PLUGINS[plugin].tools
    return Prepared(prepared, system_addition(config, bot, memory_path), definitions)
