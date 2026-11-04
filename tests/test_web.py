import asyncio
import json

import httpx
import pytest

from delta_gateway.service import Service
from delta_gateway.web import WEB_CHAT, WebChannel
from tests.test_gateway import Bot, config, settle

TOKEN = "web-token-for-tests-only"


@pytest.fixture
async def web(tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_GATEWAY_WEB_TOKEN", TOKEN)
    c = config(tmp_path, web_port=0)
    telegram = Bot()
    service = Service(c, telegram, factory="tests.fake_session:create")
    await service.start_web()
    client = httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{service.web.port}",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    yield service, client, telegram
    await client.aclose()
    await service.close()


async def events(client, count):
    found = []
    async with client.stream("GET", "/api/stream", timeout=20) as response:
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                found.append(json.loads(line[6:]))
                if len(found) >= count:
                    return found
    return found


async def test_token_is_required(web):
    service, client, _ = web
    response = await client.get("/api/messages", headers={"Authorization": "Bearer wrong"})
    assert response.status_code == 401
    assert (await client.get("/")).status_code == 200


async def test_message_runs_and_reply_streams_back(web):
    service, client, telegram = web
    listener = asyncio.create_task(events(client, 3))
    await asyncio.sleep(0.2)
    response = await client.post("/api/messages", json={"text": "hello"})
    assert response.status_code == 202
    await settle(service)
    found = await asyncio.wait_for(listener, 20)
    assert any(r["type"] == "message" and "turn 1" in r["text"] for r in found)
    assert telegram.sent == []
    history = (await client.get("/api/messages")).json()
    assert history[0] == {**history[0], "direction": "inbound", "text": "hello"}
    assert "turn 1" in history[-1]["text"]


async def test_approval_from_the_page(web, tmp_path):
    service, client, _ = web
    listener = asyncio.create_task(events(client, 10))
    await asyncio.sleep(0.2)
    await client.post("/api/messages", json={"text": "write"})
    card = None
    async with asyncio.timeout(20):
        while card is None:
            await asyncio.sleep(0.05)
            card = next(iter(service.cards.values()), None)
    handle = next(iter(service.cards))
    wrong = await client.post("/api/approvals", json={"data": "a:missing", "allow": True})
    assert wrong.status_code == 404
    response = await client.post("/api/approvals", json={"data": f"a:{handle}", "allow": True})
    assert response.status_code == 200
    await settle(service)
    listener.cancel()
    assert (tmp_path / "approved.txt").exists()


async def test_bad_input_is_refused(web):
    _, client, _ = web
    assert (await client.post("/api/messages", json={"text": ""})).status_code == 400
    assert (await client.post("/api/messages", content=b"nope")).status_code == 400


async def test_other_chats_still_go_to_telegram():
    telegram = Bot()
    channel = WebChannel(telegram)
    await channel.send(11, "hi")
    assert telegram.sent == [(11, "hi", {})]
    queue = asyncio.Queue()
    channel.listeners.add(queue)
    await channel.send(WEB_CHAT, "web")
    assert (await queue.get())["text"] == "web"


def test_short_token_is_refused(tmp_path, monkeypatch):
    from delta_gateway.web import WebServer

    monkeypatch.setenv("DELTA_GATEWAY_WEB_TOKEN", "short")
    with pytest.raises(RuntimeError):
        WebServer(None, None)
