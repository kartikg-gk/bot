"""Each bot's computer: where its shell commands run and its files live.

The agent loop runs in the gateway with the model key; only tool work reaches a computer.
"""

import asyncio
import contextlib
import os
import shlex
from pathlib import Path

CONTAINER_ROOT = "/workspace"


class ComputerError(RuntimeError):
    pass


class LocalComputer:
    """Runs directly on this machine, in the bot's workspace. Not a sandbox."""

    def __init__(self, root, hidden=()):
        self.root = str(Path(root).resolve())
        # The gateway holds the bot token, plugin and model keys; commands must not see them.
        self.hidden = set(hidden)

    async def start(self):
        Path(self.root).mkdir(parents=True, exist_ok=True)

    async def run(self, command, timeout):
        process = await asyncio.create_subprocess_shell(
            command,
            cwd=self.root,
            env={k: v for k, v in os.environ.items() if k not in self.hidden},
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        return await finish(process, timeout)

    async def read(self, path):
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(path)
        if not path.is_file():
            raise IsADirectoryError(path)
        return path.read_bytes()

    async def write(self, path, data):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        path.write_bytes(data)
        return existed

    async def stop(self):
        pass


class DockerComputer:
    """A long-lived container per bot with no network. Stopping keeps its files."""

    def __init__(self, name, image, root, *, memory="2g", mounts=()):
        self.name, self.image, self.host_root, self.memory = name, image, str(root), memory
        self.mounts = list(mounts)
        self.root = CONTAINER_ROOT

    async def docker(self, *arguments, stdin=None, timeout=60):
        process = await asyncio.create_subprocess_exec(
            "docker",
            *arguments,
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, error = await asyncio.wait_for(process.communicate(stdin), timeout)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise
        return process.returncode, output, error

    async def start(self):
        Path(self.host_root).mkdir(parents=True, exist_ok=True)
        code, output, _ = await self.docker("inspect", "--format", "{{.State.Running}}", self.name)
        if code == 0 and output.strip() == b"true":
            return
        if code == 0:
            code, _, error = await self.docker("start", self.name)
        else:
            code, _, error = await self.docker(
                "run",
                "-d",
                "--name",
                self.name,
                "--network",
                "none",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "512",
                "--memory",
                self.memory,
                "-v",
                f"{self.host_root}:{CONTAINER_ROOT}",
                *(f for host, inner in self.mounts for f in ("-v", f"{host}:{inner}")),
                "-w",
                CONTAINER_ROOT,
                self.image,
                "sleep",
                "infinity",
                timeout=300,
            )
        if code:
            raise ComputerError("Could not start the bot's container; check Docker and the image")

    async def run(self, command, timeout):
        # The deadline is enforced inside the container too, so a killed exec leaves nothing
        # running behind it.
        inner = f"timeout -s KILL {max(1, int(timeout))} sh -c {shlex.quote(command)}"
        process = await asyncio.create_subprocess_exec(
            "docker",
            "exec",
            "-w",
            CONTAINER_ROOT,
            self.name,
            "sh",
            "-c",
            inner + " 2>&1",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        return await finish(process, timeout + 5)

    async def read(self, path):
        code, output, _ = await self.docker(
            "exec",
            self.name,
            "sh",
            "-c",
            '[ -e "$1" ] || exit 3; [ -f "$1" ] || exit 4; cat "$1"',
            "sh",
            path,
        )
        if code == 3:
            raise FileNotFoundError(path)
        if code == 4:
            raise IsADirectoryError(path)
        if code:
            raise OSError(f"Could not read {path}")
        return output

    async def write(self, path, data):
        code, output, _ = await self.docker(
            "exec",
            "-i",
            self.name,
            "sh",
            "-c",
            '[ -e "$1" ] && echo existed; mkdir -p "$(dirname "$1")" && cat > "$1"',
            "sh",
            path,
            stdin=data,
        )
        if code:
            raise OSError(f"Could not write {path}")
        return output.strip() == b"existed"

    async def stop(self):
        await self.docker("stop", "-t", "5", self.name)

    async def remove(self):
        await self.docker("rm", "-f", self.name)


async def finish(process, timeout):
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        return None, b""
    return process.returncode, output
