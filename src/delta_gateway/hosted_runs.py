"""Hosted runs start from the earlier messages the browser sends.

A hosted function keeps no agent session between requests. The browser sends up to 20
earlier messages of up to 4000 characters each with a run; they are kept next to the
queued message until the run starts, and the run begins a fresh session seeded with them.
"""

import json

from delta_harness.contracts.transcript import HumanEntry, ModelEntry, TextSegment

MAX_MESSAGES = 20
MAX_CHARACTERS = 4000
LIMITS = "Send at most 20 earlier messages of up to 4000 characters each"


def parse_conversation(value):
    """The checked list of earlier messages, or ValueError."""
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > MAX_MESSAGES:
        raise ValueError(LIMITS)
    for message in value:
        if (
            not isinstance(message, dict)
            or set(message) != {"role", "content"}
            or message["role"] not in {"user", "assistant"}
            or not isinstance(message["content"], str)
            or len(message["content"]) > MAX_CHARACTERS
        ):
            raise ValueError(LIMITS)
    return [dict(role=m["role"], content=m["content"]) for m in value]


def entries(conversation):
    """Transcript entries for a session seeded with *conversation*."""
    return [
        HumanEntry(content=m["content"])
        if m["role"] == "user"
        else ModelEntry(content=[TextSegment(text=m["content"])])
        for m in conversation
    ]


async def save_context(store, inbox, conversation):
    await store.shared.run(
        "INSERT OR REPLACE INTO run_context VALUES(?,?,?)",
        (store.instance, inbox, json.dumps(conversation)),
    )


async def take_context(store, inbox):
    """The saved earlier messages for a queued message, removed as they are read."""
    rows = await store.shared.query(
        "DELETE FROM run_context WHERE instance=? AND inbox=? RETURNING conversation",
        (store.instance, inbox),
    )
    return json.loads(rows[0]["conversation"]) if rows else []
