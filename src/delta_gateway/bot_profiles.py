"""Persistent profiles and shared ownership for bot workspaces."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps

from delta_gateway.ownership import SessionLock

ACTIVE_BOTS = ContextVar("active_bot_workspaces", default=())
STYLES = {
    "direct": "Use clear, concise language and concrete answers.",
    "thoughtful": "Explain decisions patiently and consider relevant alternatives.",
    "upbeat": "Use a warm, energetic tone while keeping claims precise.",
    "custom": "",
}


async def profile(store, bot):
    rows = await store.shared.query(
        "SELECT style,voice,color,shape,greeting FROM bot_profiles WHERE instance=? AND bot=?",
        (store.instance, bot.id),
    )
    if rows:
        return dict(rows[0])
    return dict(style="direct", voice="", color="#" + bot.id[:6], shape="orb", greeting=None)


async def save_profile(store, bot, values):
    current = await profile(store, bot)
    current.update(values)
    validate_profile(current)
    await store.shared.run(
        "INSERT INTO bot_profiles VALUES(?,?,?,?,?,?,NULL) "
        "ON CONFLICT(instance,bot) DO UPDATE SET style=excluded.style,voice=excluded.voice,"
        "color=excluded.color,shape=excluded.shape,greeting=NULL",
        (
            store.instance,
            bot.id,
            current["style"],
            current["voice"],
            current["color"],
            current["shape"],
        ),
    )


def validate_profile(current):
    import re

    if current["style"] not in STYLES:
        raise ValueError("Choose a supported speaking style")
    if not isinstance(current["voice"], str) or len(current["voice"]) > 2000:
        raise ValueError("Voice instructions must be at most 2000 characters")
    if not isinstance(current["color"], str) or not re.fullmatch(
        r"#[0-9a-fA-F]{6}", current["color"]
    ):
        raise ValueError("Choose a six-digit hexadecimal color")
    if current["shape"] not in {"orb", "diamond", "square"}:
        raise ValueError("Choose a supported avatar shape")


@asynccontextmanager
async def holding(manager, bot_id):
    """The cross-process hold on a bot: a database lease when hosted, else a file lock."""
    if manager.store.db is None:
        from delta_gateway.leases import Lease

        lease = Lease(manager.store.shared, f"bot-{bot_id}")
        try:
            await lease.__aenter__()
        except RuntimeError:
            raise RuntimeError("This bot is working. Wait for it to finish.") from None
        try:
            yield
        finally:
            await lease.__aexit__()
        return
    owner = SessionLock(manager.config.state / "bot-locks", bot_id)
    try:
        yield
    finally:
        owner.close()


@asynccontextmanager
async def reserve(manager, bot_id):
    if bot_id in manager.active_bots:
        raise RuntimeError("This bot is working. Wait for it to finish.")
    async with holding(manager, bot_id):
        token = ACTIVE_BOTS.set((*ACTIVE_BOTS.get(), bot_id))
        manager.active_bots.add(bot_id)
        try:
            yield
        finally:
            manager.active_bots.discard(bot_id)
            ACTIVE_BOTS.reset(token)


def bot_operation(function):
    @wraps(function)
    async def run(manager, target, *args, **kwargs):
        delegated = function.__name__ == "delegate"
        while True:
            bot_id = target.id if delegated else (await manager.store.get_route(target.id)).bot
            if bot_id in ACTIVE_BOTS.get():
                raise ValueError("A bot cannot delegate back into its active workspace")
            lock = manager.bot_locks.setdefault(bot_id, asyncio.Lock())
            async with lock:
                if not delegated and (await manager.store.get_route(target.id)).bot != bot_id:
                    continue
                if await manager.store.bot(bot_id) is None:
                    raise ValueError("This bot has been deleted")
                async with reserve(manager, bot_id):
                    try:
                        return await function(manager, target, *args, **kwargs)
                    finally:
                        computer = manager.computers.get(bot_id)
                        if computer is not None:
                            await computer.stop()

    return run


async def remember_session(store, bot_id, state):
    await store.shared.run(
        "INSERT INTO bot_sessions VALUES(?,?,?) ON CONFLICT(instance,bot) "
        "DO UPDATE SET session_file=excluded.session_file",
        (store.instance, bot_id, state.get("sessionFile")),
    )


async def saved_session(store, bot_id):
    rows = await store.shared.query(
        "SELECT session_file FROM bot_sessions WHERE instance=? AND bot=?",
        (store.instance, bot_id),
    )
    return rows[0]["session_file"] if rows else None


async def record_chat(store, bot_id, role, text):
    await store.shared.run(
        "INSERT INTO bot_chat(instance,bot,role,text) VALUES(?,?,?,?)",
        (store.instance, bot_id, role, text),
    )
