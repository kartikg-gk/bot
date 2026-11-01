"""Telegram command parsing and bounded catalog pages."""

import shlex

from delta_gateway.session_controls import ControlError

HELP = (
    "/status /new /name [title] /sessions [page] /switch <session-id> /"
    "clone\n/compact [instructions] /export /last /stop\n/models [filte"
    "r] [--page N] /model [provider/model-id] /thinking [level]\n/queue"
    " <text> /steer <text> /delta <prompt, skill, template, or supporte"
    "d command>"
)
LONG = {
    "/name": "name",
    "/compact": "compact",
    "/clone": "clone",
    "/export": "export",
    "/model": "model",
    "/thinking": "thinking",
}


def split(text):
    tokens = text.strip().split(maxsplit=1)
    if not tokens:
        return "", ""
    verb = tokens[0].split("@")[0]
    if verb == "/think":
        verb = "/thinking"
    return verb, tokens[1].strip() if len(tokens) > 1 else ""


def operation(text):
    verb, argument = split(text)
    if verb == "/delta":
        if not argument:
            raise ControlError("Use /delta <prompt or command>")
        if not argument.startswith("/") or argument.startswith("/skill:"):
            return "submit", {"text": argument}
        native, rest = split(argument)
        if native in LONG:
            return operation(argument)
        if native in {"/system", "/skills", "/tools", "/prompts", "/plan", "/stats"}:
            return "command", {"text": native + (" " + rest if rest else "")}
        if native in {"/shell", "/login", "/logout", "/reload", "/local", "/provider", "/delta"}:
            raise ControlError("This action must be configured locally; use the gateway controls")
        # Prompt templates are expanded by Delta when a matching template exists.
        return "submit", {"text": argument}
    if verb not in LONG:
        return None
    if verb in {"/name", "/model", "/thinking"} and not argument:
        return None
    if verb in {"/clone", "/export"} and argument:
        raise ControlError(f"{verb} does not accept a path or arguments")
    keys = {
        "/name": "title",
        "/compact": "instructions",
        "/model": "selection",
        "/thinking": "level",
    }
    return LONG[verb], {keys[verb]: argument} if verb in keys else {}


def model_page(argument):
    tokens = shlex.split(argument)
    page = 1
    if "--page" in tokens:
        index = tokens.index("--page")
        if index != len(tokens) - 2:
            raise ControlError("Use /models [filter] --page N")
        try:
            page = int(tokens[index + 1])
        except ValueError:
            raise ControlError("Page must be a positive number") from None
        tokens = tokens[:index]
    if page < 1 or page > 10000:
        raise ControlError("Page is out of range")
    return " ".join(tokens), page
