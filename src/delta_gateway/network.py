"""Locked sandbox network: bots reach nothing except a forwarder to the model proxy."""

import asyncio
from dataclasses import dataclass

FORWARD_PORT = 8080
HOST_ALIAS = "host.docker.internal"
# Listens inside Docker and forwards only to the gateway's proxy port.
FORWARDER = """
import asyncio, sys
target = int(sys.argv[1])
async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except OSError:
        pass
    finally:
        writer.close()
async def handle(reader, writer):
    try:
        out_reader, out_writer = await asyncio.open_connection("host.docker.internal", target)
    except OSError:
        writer.close()
        return
    await asyncio.gather(pipe(reader, out_writer), pipe(out_reader, writer))
async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", 8080)
    async with server:
        await server.serve_forever()
asyncio.run(main())
"""


class NetworkError(RuntimeError):
    pass


@dataclass
class Egress:
    network: str
    url: str


async def docker(*arguments, check=True):
    process = await asyncio.create_subprocess_exec(
        "docker",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, _ = await process.communicate()
    if check and process.returncode:
        raise NetworkError("Could not set up the sandbox network; check Docker")
    return process.returncode, output.decode(errors="replace").strip()


class LockedNetwork:
    def __init__(self, instance, image):
        self.image = image
        self.network = f"delta-sandbox-{instance[:12]}"
        self.forwarder = f"delta-egress-{instance[:12]}"
        self.ready = None

    async def ensure(self, proxy_port):
        if self.ready:
            return self.ready
        # A previous run may have stopped uncleanly and used another proxy port.
        await docker("rm", "-f", self.forwarder, check=False)
        code, _ = await docker("network", "inspect", self.network, check=False)
        if code:
            await docker("network", "create", "--internal", self.network)
        await docker(
            "run",
            "-d",
            "--rm",
            "--name",
            self.forwarder,
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--read-only",
            "--add-host",
            f"{HOST_ALIAS}:host-gateway",
            self.image,
            "python",
            "-c",
            FORWARDER,
            str(proxy_port),
        )
        await docker("network", "connect", self.network, self.forwarder)
        self.ready = Egress(self.network, f"http://{self.forwarder}:{FORWARD_PORT}")
        return self.ready

    async def close(self):
        await docker("rm", "-f", self.forwarder, check=False)
        await docker("network", "rm", self.network, check=False)
        self.ready = None
