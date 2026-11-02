"""Real-container check of the path production uses to start a sandboxed agent."""

import subprocess

import httpx
import pytest

from delta_gateway.proxy import ModelProxy, Upstream
from delta_gateway.session_manager import SessionManager
from delta_gateway.store import Store
from tests.test_docker import IMAGE, image_ready, model_reply
from tests.test_gateway import config

pytestmark = pytest.mark.skipif(not image_ready(), reason="Docker image is not available")


async def test_manager_runs_a_writer_agent_on_the_locked_network(tmp_path, monkeypatch):
    def proxy(**settings):
        return ModelProxy(
            resolve=lambda _: Upstream("https://provider.test/v1", "real-key"),
            transport=httpx.MockTransport(lambda request: model_reply(request, [])),
            **settings,
        )

    monkeypatch.setattr("delta_gateway.session_manager.ModelProxy", proxy)
    c = config(
        tmp_path, executor="docker", docker_image=IMAGE, provider="openai", model="m", policy="ask"
    )
    c.sessions_path.mkdir(parents=True, exist_ok=True)
    store = Store(c.state, c.instance)
    sessions = SessionManager(c, store)
    try:
        bot = store.create_bot("Agent", "One goal", temporary=True)
        answer, status = await sessions.delegate(bot, "hello", policy="auto")
        assert (answer, status) == ("from the fake", "completed")
        network = f"delta-sandbox-{c.instance[:12]}"
        inspect = subprocess.run(
            ["docker", "network", "inspect", network, "--format", "{{.Internal}}"],
            capture_output=True,
            text=True,
        )
        assert inspect.stdout.strip() == "true"
    finally:
        await sessions.close()
        store.close()
    gone = subprocess.run(["docker", "network", "inspect", network], capture_output=True)
    assert gone.returncode != 0
