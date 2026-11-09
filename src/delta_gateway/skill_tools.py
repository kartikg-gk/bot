"""Host tools for reviewed skill packages and persistent bot notes."""

import json

from delta_gateway.bots import bot_root
from delta_gateway.skill_store import NOTES, SkillStore, safe_file, text_file, write_note

TOOLS = [
    dict(
        name="write_skill",
        description="Save a complete reusable skill package after user review.",
        parameters={
            "type": "object",
            "properties": {
                "name": {"type": "string", "pattern": "^[a-z0-9][a-z0-9-]{0,63}$"},
                "files": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "required": ["name", "files"],
            "additionalProperties": False,
        },
    ),
    dict(
        name="read_memory",
        description="Read one of your saved bot note files.",
        approval=False,
        parameters={
            "type": "object",
            "properties": {"file": {"type": "string", "enum": list(NOTES)}},
            "required": ["file"],
            "additionalProperties": False,
        },
    ),
    dict(
        name="write_memory",
        description="Replace a saved bot note file after explicit user review.",
        parameters={
            "type": "object",
            "properties": {
                "file": {"type": "string", "enum": list(NOTES)},
                "text": {"type": "string", "maxLength": 64000},
            },
            "required": ["file", "text"],
            "additionalProperties": False,
        },
    ),
]
NAMES = {tool["name"] for tool in TOOLS}


async def run(sessions, bot, name, arguments):
    if bot is None or sessions.store.bot(bot.id) is None:
        raise ValueError("A named bot is required")
    root = bot_root(sessions.config, bot)
    if name == "write_skill":
        SkillStore(root / "skills").write(arguments["name"], arguments["files"])
        return "Saved. The skill catalog refreshes on the next run."
    filename = arguments.get("file")
    if filename not in NOTES:
        raise ValueError("Choose a supported bot note file")
    if name == "read_memory":
        path = safe_file(root / "memory", filename)
        return json.dumps({"file": filename, "text": text_file(path) if path.exists() else ""})
    if name != "write_memory":
        raise ValueError("Unknown managed resource tool")
    text = arguments.get("text")
    # Validate before either side changes, and mirror into an already acquired remote computer.
    if not isinstance(text, str) or len(text.encode()) > 64000:
        raise ValueError("Bot notes must be at most 64 KB of text")
    computer = sessions.computers.get(bot.id)
    read_only_mount = sessions.config.executor == "docker"
    if computer is not None and computer.started and not read_only_mount:
        import posixpath

        path = posixpath.join(computer.memory_path, filename)
        await computer.write(path, text.encode())
    write_note(root / "memory", filename, text)
    return "Saved. The updated note informs the next run."
