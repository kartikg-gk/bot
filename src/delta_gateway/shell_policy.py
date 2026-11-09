"""Gateway rules for recognizing read-only and prohibited shell requests."""

import re
import shlex


def family(names):
    return rf"(?<![\w.-])(?:{names})(?:\.exe)?(?![\w.-])"


DELETION = family(
    "rm|rmdir|shred|dd|mkfs(?:\\.[a-z0-9]+)?|wipefs|fdisk|parted|remove-item|clear-disk|"
    "format-volume"
)
SYSTEM = family("sudo|su|doas|chmod|chown|shutdown|reboot|poweroff|halt")
WINDOWS = family("del|erase|rd|diskpart|format|takeown|icacls|stop-computer|restart-computer")
GIT = family("git")
DOWNLOAD = family("curl|wget|invoke-webrequest|iwr")
SHELL = family("sh|bash|dash|zsh|fish|pwsh|powershell|cmd|iex|invoke-expression")
RULES = (
    (re.compile(DELETION, re.I), "File deletion and disk operations are blocked."),
    (re.compile(SYSTEM, re.I), "Privilege, access and system changes are blocked."),
    (
        re.compile(
            GIT + r"[^\n;&|]*(?:\bclean\b|\breset\b[^\n;&|]*--hard\b|"
            r"\bpush\b[^\n;&|]*(?:--force\b|(?:^|\s)-[a-z]*f[a-z]*\b|\s\+\S+))",
            re.I,
        ),
        "Destructive Git operations are blocked.",
    ),
    (
        re.compile(DOWNLOAD + r"[^\n;]*?(?<!\|)\|(?!\|)[^;]*?" + SHELL, re.I),
        "Piping a download into a command interpreter is blocked.",
    ),
)
WINDOWS_RULE = re.compile(WINDOWS, re.I)
SIMPLE = re.compile(r"[a-zA-Z0-9_./:\\=, \t-]+\Z")
READ_OPTIONS = {
    "cat": r"-[AbEnstTv]+",
    "echo": r"-[neE]+",
    "head": r"-\d+|-[ncvq]|--(?:lines|bytes)=\d+",
    "tail": r"-\d+|-[ncvq]|--(?:lines|bytes)=\d+",
    "ls": r"-[aAbBdFghilnoprRstU1]+|--(?:all|almost-all|directory|human-readable|recursive)",
    "pwd": r"-[LP]",
    "uname": r"-[asnrvmpio]+|--all",
    "wc": r"-[clmwL]+|--(?:bytes|chars|lines|words|max-line-length)",
    "whoami": r"--(?:help|version)",
}
NO_FILE_OPERANDS = {"pwd", "uname", "whoami"}


def read_only(command, windows=False):
    if not SIMPLE.fullmatch(command) or not command.strip():
        return False
    name, *args = command.split()
    if windows:
        # These names are built into cmd.exe. An executable on PATH does not replace them.
        return (name.lower() == "dir" and not args) or name.lower() in {"echo", "type"}
    if "\\" in command or name not in READ_OPTIONS:
        return False
    for arg in args:
        if arg == "--" and name not in NO_FILE_OPERANDS:
            return True
        if arg.startswith("-") or name in NO_FILE_OPERANDS:
            if not re.fullmatch(READ_OPTIONS[name], arg):
                return False
    return True


def blocked_command(command, windows=False):
    # Join shell-quoted fragments and line continuations before checking command families.
    script = re.sub(r"\\\r?\n", "", command).translate(str.maketrans("", "", "\"'"))
    if windows:
        script = script.translate(str.maketrans("", "", "^`"))
        if WINDOWS_RULE.search(script):
            return "Destructive Windows commands are blocked."
    else:
        script = re.sub(r"\\([A-Za-z])", r"\1", script)
    return next((reason for pattern, reason in RULES if pattern.search(script)), None)


def operands(command, windows=False):
    if windows:
        command = command.replace("\\", "/").replace("^", "")
    words = shlex.shlex(command, posix=True, punctuation_chars="|&;<>()")
    words.whitespace_split = True
    words.commenters = ""
    try:
        yield from words
    except ValueError:
        return
