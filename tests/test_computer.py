import shutil
import subprocess
from uuid import uuid4

import pytest

from delta_gateway.computer import DockerComputer
from delta_gateway.tools import computer_tools

IMAGE = "python:3.12-slim"


def docker_ready():
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", IMAGE], capture_output=True, timeout=10
        )
    except subprocess.TimeoutExpired:
        return False
    return result.returncode == 0


pytestmark = pytest.mark.skipif(not docker_ready(), reason="Docker or the test image is missing")


@pytest.fixture
async def computer(tmp_path):
    memory = tmp_path / "memory"
    memory.mkdir()
    (memory / "KNOWLEDGE.md").write_text("known", encoding="utf-8")
    result = DockerComputer(
        f"delta-test-{uuid4().hex[:8]}",
        IMAGE,
        tmp_path / "workspace",
        mounts=[(memory, "/bot/memory")],
    )
    await result.start()
    yield result
    await result.remove()


async def test_container_has_no_network(computer):
    code, output = await computer.run(
        "python -c \"import socket;socket.create_connection(('1.1.1.1',443),3)\"", 30
    )
    assert code != 0


async def test_tools_work_inside_and_files_survive_a_stop(computer, tmp_path):
    read, write, edit, bash = computer_tools(computer)
    await write.run("1", dict(file_path="/workspace/a.txt", content="one"))
    await edit.run("2", dict(file_path="/workspace/a.txt", old_string="one", new_string="two"))
    assert (tmp_path / "workspace" / "a.txt").read_text() == "two"
    await bash.run("3", dict(command="echo installed > /tmp/marker"))
    await computer.stop()
    await computer.start()
    assert "two" in (await read.run("4", dict(file_path="/workspace/a.txt"))).content[0].text
    kept = (await bash.run("5", dict(command="cat /tmp/marker"))).content[0].text
    assert "installed" in kept
    assert (
        "known" in (await read.run("6", dict(file_path="/bot/memory/KNOWLEDGE.md"))).content[0].text
    )


async def test_timeout_kills_the_command(computer):
    _, _, _, bash = computer_tools(computer)
    result = await bash.run("1", dict(command="sleep 30", timeout=1000))
    assert "Exit code: 137" in result.content[0].text or "timed out" in result.content[0].text
