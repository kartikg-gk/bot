"""Approval and shell rules for foreground and unattended gateway runs."""

import os
import re
from pathlib import Path, PurePosixPath

from delta_app.rpc_approval import RpcApprovals

from delta_gateway.external_actions import REVIEW_TOOLS as EXTERNAL_WRITES
from delta_gateway.google_files import REVIEW_TOOLS as GOOGLE_FILES_WRITES
from delta_gateway.google_schedule import REVIEW_TOOLS as GOOGLE_SCHEDULE_WRITES
from delta_gateway.shell_policy import blocked_command, operands, read_only
from delta_gateway.tools import absolute
from delta_gateway.workspace_apps import REVIEW_TOOLS as WORKSPACE_WRITES

DURABLE_TOOLS = {
    "create_bot",
    "update_bot",
    "update_own_bot",
    "schedule_job",
    "cancel_scheduled_job",
    "save_skill",
    "write_skill",
    "write_memory",
}
APP_REVIEW_TOOLS = (
    {
        "gmail_send",
        "gmail_modify_labels",
        "github_create_pull_request",
        "todoist_write_tool",
    }
    | GOOGLE_SCHEDULE_WRITES
    | GOOGLE_FILES_WRITES
    | WORKSPACE_WRITES
    | EXTERNAL_WRITES
)
INSTRUCTION_FILES = {
    "SKILL.md",
    "AGENTS.md",
    "SYSTEM.md",
    "APPEND_SYSTEM.md",
    "PREFERENCES.md",
    "KNOWLEDGE.md",
}
RESERVED_NAMES = {filename.casefold() for filename in INSTRUCTION_FILES}
RESERVED_SHELL_NAME = re.compile(
    r"(?<![\w.-])(?:" + "|".join(re.escape(name) for name in INSTRUCTION_FILES) + r")(?![\w.-])",
    re.IGNORECASE,
)


def reserved_path(raw, computer, roots):
    if not isinstance(raw, str) or not raw.strip():
        return False

    def reserved_name(path):
        filename = PurePosixPath(path.replace("\\", "/")).name
        return filename.split(":", 1)[0].rstrip(" .").casefold() in RESERVED_NAMES

    if reserved_name(raw):
        return True
    try:
        resolved = absolute(computer, raw)
        if getattr(computer, "posix", False):
            path = PurePosixPath(resolved)
        else:
            path = Path(resolved)
        if reserved_name(resolved) or any(path.is_relative_to(root) for root in roots):
            return True
        # An inode shared with another filename may alias a saved instruction.
        return isinstance(path, Path) and path.stat().st_nlink > 1 and path.is_file()
    except (ValueError, OSError, RuntimeError):
        return False


def protected_roots(computer, config, skill_directory):
    memory = getattr(computer, "memory_path", None)
    if getattr(computer, "posix", False):
        return (PurePosixPath(memory),) if memory else ()
    from delta_app.discovery import default_paths, skill_search_paths

    paths = default_paths(home=config.agent_dir, project=config.project)
    candidates = [memory, skill_directory, config.state / "skills", *skill_search_paths(paths)]
    return tuple(Path(root).resolve() for root in candidates if root is not None)


def install_gate(server, computer, config, interactive, skill_directory=None):
    normal = server.session.harness.settings.before_tool_call
    gate = RpcApprovals(server, "ask", config.approval_seconds)
    # Durable actions use one explicit decision even when ordinary tools are automatic.
    gate.original = None
    roots = protected_roots(computer, config, skill_directory)
    windows = os.name == "nt" and not getattr(computer, "posix", False)

    async def check(call):
        if call.name == "LoadSkill":
            return False, None
        review = call.name in DURABLE_TOOLS
        if call.name in {"Write", "Edit"}:
            review = reserved_path(call.arguments.get("file_path"), computer, roots)
        if call.name == "Bash":
            command = call.arguments.get("command")
            if isinstance(command, str):
                if read_only(command, windows):
                    return False, None
                reason = blocked_command(command, windows)
                if reason:
                    return True, reason
                review = RESERVED_SHELL_NAME.search(command) is not None or any(
                    reserved_path(word, computer, roots)
                    for token in operands(command, windows)
                    if not re.fullmatch(r"[|&;<>()]+", token)
                    for word in (token, token.partition("=")[2])
                )
        app_review = call.name in APP_REVIEW_TOOLS
        if review or app_review:
            if not interactive:
                if app_review:
                    return True, "This connected-app action requires interactive approval."
                return True, "This change affects future runs and requires interactive approval."
            if config.policy == "deny":
                return True, "Tool denied by operator policy."
            return await gate.check(call)
        denied, reason = await normal(call) if normal else (False, None)
        if denied and not interactive and config.policy == "ask":
            return True, "This action requires approval and no interactive reviewer is available."
        return denied, reason

    server.session.harness.settings.before_tool_call = check
    return gate
