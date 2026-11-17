"""Notes and skill files for hosted bots, kept in the database.

A hosted function's disk does not last between requests, so the tables are the source of
truth there and each bot's folder is only a working copy: ``pull`` fills it before use
and ``push`` saves it after a change. Skills shared by every bot use the scope ''.
A gateway on a local file keeps using its folders directly; both calls do nothing.
"""

import hashlib
import time

from delta_gateway.bots import bot_root
from delta_gateway.skill_store import NOTES


def hosted(store):
    """Whether this store has no local file, as on D1."""
    return store.db is None


def folders(config, bot):
    if bot is None:
        return None, config.state / "skills", ""
    root = bot_root(config, bot)
    return root / "memory", root / "skills", bot.id


async def pull(store, config, bot):
    """Write the saved notes and skills into the bot's folder (or the shared skill folder)."""
    if not hosted(store):
        return
    notes, skills, scope = folders(config, bot)
    if notes is not None:
        notes.mkdir(parents=True, exist_ok=True)
        for row in await store.shared.query(
            "SELECT file,content FROM bot_memory WHERE instance=? AND bot=?",
            (store.instance, scope),
        ):
            if row["file"] in NOTES:
                (notes / row["file"]).write_text(row["content"], encoding="utf-8")
    rows = await store.shared.query(
        "SELECT skill,path,content FROM skill_files WHERE instance=? AND scope=?",
        (store.instance, scope),
    )
    saved = {(row["skill"], row["path"]): row["content"] for row in rows}
    if skills.is_dir():
        for path in skills.rglob("*"):
            relative = path.relative_to(skills).parts
            if path.is_file() and (relative[0], "/".join(relative[1:])) not in saved:
                path.unlink()
    for (skill, relative), content in saved.items():
        target = skills / skill / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    if skills.is_dir():
        for path in sorted(skills.rglob("*"), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()


async def push(store, config, bot):
    """Save the bot's folder (or the shared skill folder) into the tables."""
    if not hosted(store):
        return
    notes, skills, scope = folders(config, bot)
    now = time.time()
    if notes is not None:
        for name in NOTES:
            path = notes / name
            if path.is_file():
                await store.shared.run(
                    "INSERT INTO bot_memory VALUES(?,?,?,?,?) ON CONFLICT(instance,bot,file) "
                    "DO UPDATE SET content=excluded.content,updated=excluded.updated",
                    (store.instance, scope, name, path.read_text(encoding="utf-8"), now),
                )
    present = set()
    if skills.is_dir():
        for path in skills.rglob("*"):
            if not path.is_file():
                continue
            skill, *rest = path.relative_to(skills).parts
            relative = "/".join(rest)
            content = path.read_text(encoding="utf-8")
            present.add((skill, relative))
            await store.shared.run(
                "INSERT INTO skill_files VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(instance,scope,skill,path) DO UPDATE SET "
                "content=excluded.content,revision=excluded.revision",
                (
                    store.instance,
                    scope,
                    skill,
                    relative,
                    content,
                    hashlib.sha256(content.encode()).hexdigest(),
                ),
            )
    for row in await store.shared.query(
        "SELECT skill,path FROM skill_files WHERE instance=? AND scope=?",
        (store.instance, scope),
    ):
        if (row["skill"], row["path"]) not in present:
            await store.shared.run(
                "DELETE FROM skill_files WHERE instance=? AND scope=? AND skill=? AND path=?",
                (store.instance, scope, row["skill"], row["path"]),
            )
