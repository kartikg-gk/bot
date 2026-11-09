import asyncio

import httpx

from delta_gateway.cli import parser
from delta_gateway.config import load
from delta_gateway.operators import operator_command
from delta_gateway.service import Service
from delta_gateway.web import WEB_CHAT
from tests.test_gateway import update
from tests.test_web import TOKEN


async def test_web_only_init_runtime_jobs_and_open_stream_shutdown(tmp_path, monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DELTA_GATEWAY_WEB_TOKEN", TOKEN)
    path = tmp_path / "gateway.yaml"
    await operator_command(
        parser().parse_args(
            [
                "init",
                "--config",
                str(path),
                "--project",
                str(tmp_path),
                "--web-only",
                "--web-port",
                "0",
            ]
        )
    )
    c = load(path)
    assert c.web_only and c.web_port == 0 and c.users == [1] and c.token() is None
    service = Service(c, None, factory="tests.fake_session:create")
    service.store.admit([update(1, "Pending Telegram message", user=1)], c)
    bot = service.store.bots()[0].id
    telegram_job = service.store.add_job(1, 1, 1, 0, bot, "Telegram job", "once", 0)
    web_job = service.store.add_job(WEB_CHAT, 1, 1, 0, bot, "Web job", "once", 0)
    ready = asyncio.Event()
    service.on_ready = ready.set
    runner = asyncio.create_task(service.run())
    closed = False
    try:
        await asyncio.wait_for(ready.wait(), 5)
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{service.web.port}",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            response = await client.post("/api/messages", json={"text": "Web chat"})
            assert response.status_code == 202
            await asyncio.wait_for(asyncio.gather(*service.jobs.values()), 5)
            async with asyncio.timeout(5):
                while service.store.job(web_job).last_status != "completed":
                    await asyncio.sleep(0.02)
            assert service.store.job(telegram_job).status == "active"
            assert service.store.is_pending(1)
            history = (await client.get("/api/messages")).json()
            assert any("turn" in row["text"] for row in history if row["direction"] == "outbound")
            async with client.stream("GET", "/api/stream") as stream:
                assert stream.status_code == 200
                lines = stream.aiter_lines()
                assert await anext(lines) == ": connected"
                runner.cancel()
                await asyncio.gather(runner, return_exceptions=True)
                await asyncio.wait_for(service.close(), 5)
                closed = True
                assert [line async for line in lines] == [""]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if not closed:
            await service.close()
