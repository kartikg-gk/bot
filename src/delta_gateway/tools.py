"""Delta's Read, Write, Edit and Bash tools, pointed at a bot's computer.

Names, parameters and descriptions come from Delta unchanged, so the model and Delta's
approval rules see the same tools; only where the work happens differs.
"""

import asyncio
import posixpath
from dataclasses import replace
from pathlib import PurePosixPath, PureWindowsPath

from delta_app.tools._shared import (
    BINARY_EXTENSIONS,
    MAX_OUTPUT_LINES,
    fault,
    format_with_line_numbers,
    optional_bool,
    optional_int,
    require_str,
    text_outcome,
    truncate_output,
)
from delta_app.tools.bash import DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS, make_bash_tool
from delta_app.tools.files import make_edit_tool, make_read_tool, make_write_tool

from delta_gateway.computer import CONTAINER_ROOT


def absolute(computer, raw):
    raw = raw.strip()
    if not raw:
        raise ValueError("File path must not be empty.")
    flavour = PurePosixPath if computer.root == CONTAINER_ROOT else PureWindowsPath
    if computer.root != CONTAINER_ROOT and PurePosixPath(raw).is_absolute():
        flavour = PurePosixPath
    if not flavour(raw).is_absolute():
        raise ValueError(f"File path must be absolute, got relative path: {raw}")
    return posixpath.normpath(raw) if computer.root == CONTAINER_ROOT else raw


def computer_tools(computer):
    locks = {}

    def lock(path):
        return locks.setdefault(path, asyncio.Lock())

    async def read(call, arguments, signal=None, on_update=None):
        try:
            path = absolute(computer, require_str(arguments, "file_path"))
            if PurePosixPath(path.replace("\\", "/")).suffix.lower() in BINARY_EXTENSIONS:
                return fault(f"Cannot read binary file: {path}")
            text = (await computer.read(path)).decode("utf-8", errors="replace")
        except ValueError as exc:
            return fault(str(exc))
        except FileNotFoundError:
            return fault(f"File not found: {path}")
        except IsADirectoryError:
            return fault(f"Not a regular file: {path}. Use a shell command to list it.")
        except OSError as exc:
            return fault(f"Failed to read file: {exc}")
        if not text:
            return text_outcome(f"(empty file: {path})")
        lines = text.split("\n")
        total, start = len(lines), 1
        offset, limit = optional_int(arguments, "offset"), optional_int(arguments, "limit")
        if offset and offset > 1:
            start = min(offset, total + 1)
            lines = lines[start - 1 :]
        limit = limit or MAX_OUTPUT_LINES
        body = format_with_line_numbers("\n".join(lines[:limit]), start=start)
        if len(lines) > limit:
            body += f"\n\n... ({limit} of {total} lines shown. Use offset/limit to read more.)"
        return text_outcome(body)

    async def write(call, arguments, signal=None, on_update=None):
        try:
            path = absolute(computer, require_str(arguments, "file_path"))
            content = require_str(arguments, "content")
            async with lock(path):
                existed = await computer.write(path, content.encode("utf-8"))
        except ValueError as exc:
            return fault(str(exc))
        except OSError as exc:
            return fault(f"Failed to write file: {exc}")
        count = content.count("\n") + (1 if content else 0)
        return text_outcome(f"{'Updated' if existed else 'Created'} {path} ({count} lines)")

    async def edit(call, arguments, signal=None, on_update=None):
        try:
            path = absolute(computer, require_str(arguments, "file_path"))
            old, new = require_str(arguments, "old_string"), require_str(arguments, "new_string")
        except ValueError as exc:
            return fault(str(exc))
        every = optional_bool(arguments, "replace_all") or False
        if old == new:
            return fault("old_string and new_string are identical; nothing to change.")
        async with lock(path):
            try:
                content = (await computer.read(path)).decode("utf-8", errors="replace")
            except FileNotFoundError:
                return fault(f"File not found: {path}")
            except IsADirectoryError:
                return fault(f"Not a regular file: {path}")
            except OSError as exc:
                return fault(f"Failed to read file: {exc}")
            count = content.count(old)
            if not count:
                return fault(f"old_string was not found in {path}. Read the file and copy exactly.")
            if count > 1 and not every:
                return fault(
                    f"old_string matches {count} locations in {path}. Add context to make it "
                    "unique, or set replace_all to true."
                )
            try:
                await computer.write(path, content.replace(old, new, -1 if every else 1).encode())
            except OSError as exc:
                return fault(f"Failed to write file: {exc}")
        made = count if every else 1
        return text_outcome(f"Applied {made} replacement{'s' if made > 1 else ''} in {path}")

    async def bash(call, arguments, signal=None, on_update=None):
        try:
            command = require_str(arguments, "command")
        except ValueError as exc:
            return fault(str(exc))
        limit = max(
            1, min(optional_int(arguments, "timeout") or DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS)
        )
        try:
            code, output = await computer.run(command, limit / 1000)
        except OSError as exc:
            return fault(f"Failed to start command: {exc}")
        if code is None:
            return fault(f"Command timed out after {limit}ms and was killed.\nCommand: {command}")
        if signal is not None and signal.is_cancelled():
            return fault("Command cancelled.")
        text = truncate_output(output.decode("utf-8", errors="replace"))
        return text_outcome((f"Exit code: {code}\n" if code else "") + text)

    return [
        replace(make_read_tool(), run=read),
        replace(make_write_tool(), run=write),
        replace(make_edit_tool(), run=edit),
        replace(make_bash_tool(), run=bash),
    ]
