"""A bot's computer as a persistent Daytona sandbox with all network blocked.

The sandbox keeps its files between runs and stops itself when idle. It cannot see the
gateway's disk, so the bot's memory files are copied up when it starts and back when it stops.
"""

import asyncio
import os
import shlex
from pathlib import Path

from delta_gateway.computer import ComputerError

IDLE_MINUTES = 15
MEMORY_FOLDER = "memory"


def load_sdk():
    try:
        import daytona
    except ImportError:
        raise ComputerError(
            "The Daytona executor needs the daytona extra: pip install 'delta-gateway[daytona]'"
        ) from None
    return daytona


class DaytonaComputer:
    posix = True

    def __init__(self, name, config, memory=None, *, sdk=None):
        self.name, self.config, self.memory = name, config, Path(memory) if memory else None
        self.sdk = sdk
        self.client = self.sandbox = None
        self.root = "/home/daytona/workspace"
        self.memory_path = "/home/daytona/" + MEMORY_FOLDER
        self.lock = asyncio.Lock()

    async def start(self):
        async with self.lock:
            sdk = self.sdk or load_sdk()
            if not os.environ.get("DAYTONA_API_KEY"):
                raise ComputerError("Set DAYTONA_API_KEY to use the Daytona executor")
            if not self.client:
                self.client = sdk.AsyncDaytona(sdk.DaytonaConfig(target=self.config.daytona_target))
            try:
                self.sandbox = await self.client.get(self.name)
                if str(self.sandbox.state).lower().split(".")[-1] != "started":
                    await self.sandbox.start()
            except sdk.DaytonaNotFoundError:
                self.sandbox = await self.client.create(
                    sdk.CreateSandboxFromSnapshotParams(
                        name=self.name,
                        snapshot=self.config.daytona_snapshot,
                        network_block_all=True,
                        auto_stop_interval=IDLE_MINUTES,
                        labels={"delta-gateway": self.config.instance},
                    )
                )
            await self.exec(f"mkdir -p {self.root} {self.memory_path}", 60)
            if self.memory and self.memory.is_dir():
                for path in self.memory.glob("*.md"):
                    await self.sandbox.fs.upload_file(
                        path.read_bytes(), f"{self.memory_path}/{path.name}"
                    )

    async def exec(self, command, timeout):
        try:
            response = await asyncio.wait_for(
                self.sandbox.process.exec(command, cwd=self.root, timeout=int(timeout)),
                timeout + 10,
            )
        except TimeoutError:
            return None, b""
        return response.exit_code, (response.result or "").encode("utf-8")

    async def run(self, command, timeout):
        return await self.exec(command, max(1, int(timeout)))

    async def read(self, path):
        code, _ = await self.exec(
            f"[ -e {shlex.quote(path)} ] || exit 3; [ -f {shlex.quote(path)} ] || exit 4", 30
        )
        if code == 3:
            raise FileNotFoundError(path)
        if code == 4:
            raise IsADirectoryError(path)
        if code:
            raise OSError(f"Could not read {path}")
        return await self.sandbox.fs.download_file(path) or b""

    async def write(self, path, data):
        quoted = shlex.quote(path)
        code, output = await self.exec(
            f'[ -e {quoted} ] && echo existed; mkdir -p "$(dirname {quoted})"', 30
        )
        if code not in (0, 1):
            raise OSError(f"Could not write {path}")
        await self.sandbox.fs.upload_file(data, path)
        return output.strip() == b"existed"

    async def stop(self):
        if not self.sandbox:
            return
        # Bring memory edits home before the sandbox sleeps.
        if self.memory and self.memory.is_dir():
            for path in self.memory.glob("*.md"):
                try:
                    data = await self.sandbox.fs.download_file(f"{self.memory_path}/{path.name}")
                except Exception:  # noqa: BLE001 - a missing copy keeps the local file
                    continue
                if data:
                    path.write_bytes(data)
        await self.sandbox.stop()
        if self.client:
            await self.client.close()
            self.client = None
