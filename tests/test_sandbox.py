import asyncio
import json

import httpx
import pytest

from delta_gateway.bots import Bot as Profile
from delta_gateway.bots import prepare
from delta_gateway.config import load, save
from delta_gateway.executor import docker_launch
from delta_gateway.proxy import ModelProxy, Upstream
from delta_gateway.rpc_client import RpcClient
from tests.test_gateway import config

REAL_KEY = "real-provider-key-never-leaves"


def upstream(request):
    return httpx.Response(
        200,
        content=json.dumps(
            dict(
                url=str(request.url),
                auth=request.headers.get("authorization"),
                body=request.content.decode(),
            )
        ),
        headers={"content-type": "application/json"},
    )


@pytest.fixture
async def proxy():
    result = ModelProxy(
        resolve=lambda _: Upstream("https://provider.test/v1", REAL_KEY),
        transport=httpx.MockTransport(upstream),
    )
    await result.start()
    yield result
    await result.close()


async def post(proxy, path, token):
    async with httpx.AsyncClient() as client:
        return await client.post(
            f"http://127.0.0.1:{proxy.port}{path}",
            headers={"authorization": f"Bearer {token}"},
            content=b'{"model":"m"}',
        )


async def test_proxy_swaps_token_for_real_key(proxy):
    reply = await post(proxy, "/chat/completions", proxy.issue("openai", 5))
    data = reply.json()
    assert reply.status_code == 200 and data["auth"] == "Bearer " + REAL_KEY
    assert data["url"] == "https://provider.test/v1/chat/completions"
    assert data["body"] == '{"model":"m"}'


async def test_proxy_refuses_unknown_and_revoked_tokens(proxy):
    assert (await post(proxy, "/chat/completions", "wrong")).status_code == 401
    token = proxy.issue("openai", 5)
    proxy.revoke(token)
    reply = await post(proxy, "/chat/completions", token)
    assert reply.status_code == 401 and REAL_KEY not in reply.text


@pytest.mark.parametrize("path", ["/models", "/files"])
async def test_proxy_only_forwards_model_requests(proxy, path):
    assert (await post(proxy, path, proxy.issue("openai", 5))).status_code in {400, 403}


async def test_proxy_refuses_path_traversal(proxy):
    token = proxy.issue("openai", 5)
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
    writer.write(
        f"POST /../x/chat/completions HTTP/1.1\r\nAuthorization: Bearer {token}\r\n\r\n".encode()
    )
    await writer.drain()
    assert b"403" in await reader.readline()
    writer.close()


async def test_proxy_enforces_request_limit(proxy):
    token = proxy.issue("openai", 1)
    assert (await post(proxy, "/responses", token)).status_code == 200
    assert (await post(proxy, "/responses", token)).status_code == 429


def sandbox_config(tmp_path, **settings):
    return config(
        tmp_path,
        executor="docker",
        docker_image="delta-bot:test",
        docker_network="bridge",
        provider="openai",
        **settings,
    )


def test_container_command_is_locked_down_and_holds_no_secret(tmp_path):
    c = sandbox_config(tmp_path, bot_token="12345:private-fake-value")
    launch = docker_launch(
        c, Profile("b" * 32, "R", "r", "", False), None, "run-token", 4000, key_env="OPENAI_API_KEY"
    )
    joined = " ".join(launch.command)
    assert "run-token" not in joined and "private-fake-value" not in joined
    assert launch.environment == {
        "OPENAI_API_KEY": "run-token",
        "OPENAI_BASE_URL": "http://host.docker.internal:4000",
    }
    for flag in ("--cap-drop", "no-new-privileges", "--pids-limit", "--memory", "-e"):
        assert flag in launch.command
    assert launch.command.count("-v") == 3


def test_resume_uses_the_sandbox_session_path(tmp_path):
    launch = docker_launch(
        sandbox_config(tmp_path),
        Profile("b" * 32, "R", "r", "", False),
        tmp_path / "state" / "sessions" / "abc.jsonl",
        "t",
        1,
        key_env="OPENAI_API_KEY",
    )
    assert launch.command[launch.command.index("--session") + 1] == "/sessions/abc.jsonl"


def test_prepare_uses_sandbox_paths(tmp_path):
    c = sandbox_config(tmp_path)
    bot = Profile("b" * 32, "R", "r", "", False)
    prepared = prepare(c, bot)
    assert prepared.engine_args[-1] == "/bot/system.md"
    assert "/bot/memory" in (c.state / "bots" / bot.id / "system.md").read_text(encoding="utf-8")


def test_client_translates_sandbox_paths(tmp_path):
    client = RpcClient(config(tmp_path), paths=[("/sessions", "E:/host/sessions")])
    record = dict(data=dict(sessionFile="/sessions/a.jsonl", other="/sessions/a.jsonl"))
    client.translate(record)
    assert record["data"] == dict(sessionFile="E:/host/sessions/a.jsonl", other="/sessions/a.jsonl")


def test_sandbox_settings_round_trip(tmp_path):
    c = sandbox_config(tmp_path)
    path = tmp_path / "config.yaml"
    save(path, c)
    restored = load(path)
    assert restored.executor == "docker" and restored.docker_image == "delta-bot:test"


def test_docker_needs_an_image(tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, executor="docker").validate_paths()


def test_locked_network_is_the_default_and_fails_closed(tmp_path):
    c = config(tmp_path, executor="docker", docker_image="i", provider="openai")
    assert c.docker_network == "locked"
    with pytest.raises(ValueError):
        docker_launch(c, Profile("b" * 32, "R", "r", "", False), None, "t", 1, key_env="K")


def test_real_credential_lookup_and_key_variable(monkeypatch):
    from delta_gateway.executor import key_variable
    from delta_gateway.proxy import resolve_upstream

    monkeypatch.setenv("OPENAI_API_KEY", REAL_KEY)
    assert key_variable("openai") == "OPENAI_API_KEY"
    found = resolve_upstream("openai")
    assert found.key == REAL_KEY and found.base_url.startswith("http")
