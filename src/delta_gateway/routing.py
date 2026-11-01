"""Authorize Telegram intake and keep topic identity in every reply route."""

import re


def admit_message(message, config, bot_id=None, username=""):
    sender, chat = message.get("from", {}), message.get("chat", {})
    user = sender.get("id")
    if (
        type(user) is not int
        or user not in config.users
        or sender.get("is_bot")
        or message.get("sender_chat")
    ):
        return None
    if type(chat.get("id")) is not int:
        return None
    private = chat.get("type") == "private"
    if not private and not (
        config.allow_groups
        and chat.get("type") in {"group", "supergroup"}
        and (not config.groups or chat["id"] in config.groups)
    ):
        return None
    text = message.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    thread = message.get("message_thread_id") or 0
    if type(thread) is not int or thread < 0:
        return None
    token = text.strip().split(maxsplit=1)[0]
    if token.startswith("/") and "@" in token:
        destination = token.partition("@")[2]
        if not username or destination.casefold() != username.casefold():
            return None
    mention = bool(username and re.search(r"@" + re.escape(username) + r"\b", text, re.I))
    reply = (
        message.get("reply_to_message", {}).get("from", {}).get("id") == bot_id if bot_id else False
    )
    if not private and config.group_mentions_only and not (mention or reply):
        return None
    if username:
        text = re.sub(r"^\s*@" + re.escape(username) + r"\b\s*", "", text, count=1, flags=re.I)
    return (chat["id"], user, thread, text.strip() or "/help")


def payload(key):
    return {"message_thread_id": key[2]} if key[2] else {}
