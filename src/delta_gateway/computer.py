"""Each bot's computer: where its shell commands run and its files live.

The agent loop runs in the gateway with the model key; only tool work reaches a computer.
"""

import asyncio
import contextlib
import json
import os
import shlex
from pathlib import Path

CONTAINER_ROOT = "/workspace"


class ComputerError(RuntimeError):
    pass


class ManagedComputer:
    """Acquire a persistent computer only when a file or command tool needs it."""

    def __init__(self, computer):
        self.computer = computer
        self.started = False
        self.cleanup_needed = False
        self.lock = asyncio.Lock()

    def __getattr__(self, name):
        return getattr(self.computer, name)

    async def start(self):
        async with self.lock:
            if not self.started:
                if self.cleanup_needed:
                    await self.computer.stop()
                    self.cleanup_needed = False
                self.cleanup_needed = True
                try:
                    await self.computer.start()
                except BaseException:
                    with contextlib.suppress(Exception):
                        await self.computer.stop()
                        self.cleanup_needed = False
                    raise
                self.started = True

    async def read(self, path):
        await self.start()
        return await self.computer.read(path)

    async def write(self, path, data):
        await self.start()
        return await self.computer.write(path, data)

    async def run(self, command, timeout):
        await self.start()
        return await self.computer.run(command, timeout)

    async def stop(self):
        async with self.lock:
            if self.cleanup_needed:
                await self.computer.stop()
                self.started = self.cleanup_needed = False

    async def remove(self):
        async with self.lock:
            if hasattr(self.computer, "remove"):
                await self.computer.remove()
            self.started = self.cleanup_needed = False


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

    posix = True

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
        if code == 0:
            await self.verify_read_only_mounts()
            if output.strip() == b"true":
                return
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

    async def verify_read_only_mounts(self):
        expected = {
            target.removesuffix(":ro") for _, target in self.mounts if target.endswith(":ro")
        }
        if not expected:
            return
        code, output, _ = await self.docker("inspect", "--format", "{{json .Mounts}}", self.name)
        if code:
            raise ComputerError("Could not verify the container's read-only mounts")
        try:
            mounts = json.loads(output)
            read_only = {entry["Destination"] for entry in mounts if entry.get("RW") is False}
        except (ValueError, AttributeError, TypeError, KeyError):
            raise ComputerError("Could not verify the container's read-only mounts") from None
        if not expected <= read_only:
            raise ComputerError(
                f"Container {self.name} needs to be recreated with read-only bot note mounts"
            )

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
        code, _, error = await self.docker("stop", "-t", "5", self.name)
        if code and b"No such container" not in error:
            raise ComputerError("Could not stop the bot's container; check Docker")

    async def remove(self):
        code, _, error = await self.docker("rm", "-f", self.name)
        if code and b"No such container" not in error:
            raise ComputerError("Could not remove the bot's container; check Docker")


async def finish(process, timeout):
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout)
    except (TimeoutError, asyncio.CancelledError) as error:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        if isinstance(error, asyncio.CancelledError):
            raise
        return None, b""
    return process.returncode, output
