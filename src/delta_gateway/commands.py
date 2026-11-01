"""Telegram command parsing."""


def split(text):
    tokens = text.strip().split(maxsplit=1)
    if not tokens:
        return "", ""
    verb = tokens[0].split("@")[0]
    if verb == "/think":
        verb = "/thinking"
    return verb, tokens[1].strip() if len(tokens) > 1 else ""
