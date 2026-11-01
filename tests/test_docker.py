"""Real-container checks. Build the image first; see docker/Dockerfile."""

import asyncio
import json
import os
import shutil
import subprocess

import httpx
import pytest

from delta_gateway.bots import Bot as Profile
from delta_gateway.bots import prepare
from delta_gateway.executor import docker_launch
from delta_gateway.proxy import ModelProxy, Upstream
from delta_gateway.rpc_client import RpcClient
from tests.test_gateway import config

IMAGE = "delta-bot:local"
REAL_KEY = "real-provider-key-never-leaves"


def image_ready():
    if not shutil.which("docker"):
        return False
    probe = subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True)
    return probe.returncode == 0


pytestmark = pytest.mark.skipif(not image_ready(), reason="Docker image is not available")


def sse(*events):
    return "".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n"


def model_reply(request, seen):
    seen.append((request.url.path, request.headers.get("authorization")))
    chunk = dict(
        id="c",
        object="chat.completion.chunk",
        model="fake",
        choices=[
            dict(index=0, delta=dict(role="assistant", content="from the fake"), finish_reason=None)
        ],
    )
    end = dict(
        id="c",
        object="chat.completion.chunk",
        model="fake",
        choices=[dict(index=0, delta={}, finish_reason="stop")],
    )
    return httpx.Response(200, text=sse(chunk, end), headers={"content-type": "text/event-stream"})


async def test_prompt_runs_in_a_container_through_the_proxy(tmp_path):
    seen = []
    proxy = ModelProxy(
        resolve=lambda _: Upstream("https://provider.test/v1", REAL_KEY),
        transport=httpx.MockTransport(lambda request: model_reply(request, seen)),
    )
    await proxy.start()
    c = config(
        tmp_path,
        executor="docker",
        docker_image=IMAGE,
        provider="openai",
        model="fake-model",
        policy="auto",
    )
    bot = Profile("b" * 32, "Researcher", "Finds things", "", False)
    prepared = prepare(c, bot)
    prepared.sessions_path.mkdir(parents=True, exist_ok=True)
    token = proxy.issue("openai", 5)
    launch = docker_launch(prepared, bot, None, token, proxy.port, key_env="OPENAI_API_KEY")
    client = RpcClient(
        prepared,
        command=launch.command,
        environment=launch.environment,
        paths=launch.paths,
        timeout=60,
    )
    try:
        async with client:
            await client.request("prompt", message="hello")
            final = None
            async with asyncio.timeout(60):
                while True:
                    event = await client.next_event()
                    if event.get("type") == "agent_settled":
                        final = event
                        break
            assert final["outcome"] == "completed", (final, list(client.diagnostics))
            state = await client.request("get_state")
            assert (await client.request("get_last_assistant_text"))["text"] == "from the fake"
        assert seen and seen[0][1] == "Bearer " + REAL_KEY
        assert state["sessionFile"].startswith(str(prepared.sessions_path))
    finally:
        await proxy.close()


async def test_container_sees_only_the_run_token_and_can_write_its_files(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "12345:private-fake-value")
    monkeypatch.setenv("OPENAI_API_KEY", REAL_KEY)
    c = config(tmp_path, executor="docker", docker_image=IMAGE, provider="openai")
    bot = Profile("b" * 32, "Researcher", "Finds things", "", False)
    prepared = prepare(c, bot)
    prepared.sessions_path.mkdir(parents=True, exist_ok=True)
    launch = docker_launch(prepared, bot, None, "run-token", 1, key_env="OPENAI_API_KEY")
    image = launch.command.index(IMAGE)
    script = (
        "env; touch /workspace/w /sessions/s /bot/memory/m && echo WRITES_OK; "
        "cat /bot/system.md | head -1"
    )
    command = [*launch.command[: image + 1], "sh", "-c", script]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, **launch.environment},
    )
    assert "WRITES_OK" in result.stdout, result.stderr
    assert "OPENAI_API_KEY=run-token" in result.stdout
    assert REAL_KEY not in result.stdout and "private-fake-value" not in result.stdout


async def test_session_resumes_in_a_new_container(tmp_path):
    proxy = ModelProxy(
        resolve=lambda _: Upstream("https://provider.test/v1", REAL_KEY),
        transport=httpx.MockTransport(lambda request: model_reply(request, [])),
    )
    await proxy.start()
    c = config(
        tmp_path, executor="docker", docker_image=IMAGE, provider="openai", model="m", policy="auto"
    )
    bot = Profile("b" * 32, "Researcher", "Finds things", "", False)
    prepared = prepare(c, bot)
    prepared.sessions_path.mkdir(parents=True, exist_ok=True)

    def client(session):
        launch = docker_launch(
            prepared, bot, session, proxy.issue("openai", 5), proxy.port, key_env="OPENAI_API_KEY"
        )
        return RpcClient(
            prepared,
            command=launch.command,
            environment=launch.environment,
            paths=launch.paths,
            timeout=60,
        )

    try:
        async with client(None) as first:
            await first.request("prompt", message="hello")
            async with asyncio.timeout(60):
                while (await first.next_event()).get("type") != "agent_settled":
                    pass
            state = await first.request("get_state")
        async with client(state["sessionFile"]) as second:
            resumed = await second.request("get_state")
        assert resumed["sessionId"] == state["sessionId"]
    finally:
        await proxy.close()
