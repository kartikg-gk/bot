import asyncio
import time
from types import SimpleNamespace

import httpx
import pytest

from delta_gateway.config import Config, load, save
from delta_gateway.service import Service
from delta_gateway.store import Store
from delta_gateway.telegram import Telegram, TelegramError, chunks


def config(tmp_path, **kw):
    kw.setdefault("database", tmp_path / "state" / "gateway.sqlite3")
    kw.setdefault("policy", "ask")
    kw.setdefault("per_user_groups", True)
    kw.setdefault("group_mentions_only", True)
    return Config(project=tmp_path, state=tmp_path / "state", users=[11], **kw)


def update(number, text="hello", user=11, kind="private"):
    return {
        "update_id": number,
        "message": {"from": {"id": user}, "chat": {"id": user, "type": kind}, "text": text},
    }


class Bot:
    def __init__(self):
        self.sent = []
        self.edits = []
        self.fail = False

    async def send(self, chat, text, **kwargs):
        if self.fail:
            raise TelegramError("offline")
        self.sent.append((chat, text, kwargs))
        return {"message_id": len(self.sent)}

    async def call(self, method, **payload):
        self.edits.append((method, payload))
        return {}

    async def close(self):
        pass


async def settle(service):
    async with asyncio.timeout(10):
        while service.jobs or service.store.pending():
            await service.dispatch()
            await asyncio.sleep(0.02)


def test_config_store_duplicates_recovery_retention(tmp_path):
    c = config(tmp_path)
    save(tmp_path / "config.json", c)
    assert load(tmp_path / "config.json") == c
    store = Store(c.state, c.instance)
    store.admit([update(1), update(1), update(2, user=33), update(3, kind="group")], c)
    assert len(store.pending()) == 1
    assert store.offset == 4
    store.mark(1, "running", "r1")
    store.close()
    store = Store(c.state, c.instance)
    assert store.issues() == [(1, 11, "uncertain", 11, 0)]
    assert not store.pending()
    store.mark(1, "completed")
    with store.db:
        store.db.execute("UPDATE inbox SET created=0")
    store.retain(days=7)
    assert store.db.execute("SELECT text FROM inbox").fetchone()[0] is None
    store.admit([update(1)], c)
    assert not store.pending()
    store.close()


async def test_service_stream_resume_last_and_new(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1), update(1), update(2, user=22)], c)
        await settle(service)
        assert any("turn 1: done" == text for _, text, _ in bot.sent)
        assert len(service.clients) == 1
        assert await service.last((11, 11, 0)) == "turn 1: done"
        session = service.store.mapping(11, 11, 0)[0]
    finally:
        await service.close()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(3)], c)
        await settle(service)
        assert any("turn 2: done" == text for _, text, _ in bot.sent)
        service.store.admit([update(4, "/new")], c)
        await settle(service)
        assert service.store.mapping(11, 11, 0)[0] != session
        assert (c.state / "sessions" / f"{session}.jsonl").exists()
    finally:
        await service.close()


async def test_service_stop_is_responsive_and_clears_queue(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "slow"), update(2, "queued")], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not bot.edits:
                await asyncio.sleep(0.02)
        service.store.admit([update(3, "/stop")], c)
        await asyncio.wait_for(service.dispatch(), 3)
        await settle(service)
        assert not service.store.pending()
        assert any(text.startswith("Abort requested") for _, text, _ in bot.sent)
    finally:
        await service.close()


async def test_callback_authorization_and_audit(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot)
    calls = []

    async def request(*args, **kwargs):
        calls.append(kwargs)

    client = SimpleNamespace(request=request)
    event = dict(
        approval_id="a1",
        run_id="r1",
        tool="Write",
        preview="change file",
        digest="digest",
        expires_at=time.time() + 20,
    )
    try:
        await service.card((11, 11, 0), client, event)
        handle = next(iter(service.cards))
        callback = {
            "data": "a:" + handle,
            "from": {"id": 99},
            "message": {"message_id": 1, "chat": {"id": 11, "type": "private"}},
        }
        await service.callback(callback)
        assert not calls
        callback["from"]["id"] = 11
        await service.callback(callback)
        await service.callback(callback)
        assert len(calls) == 1
        assert calls[0]["allow"] is True
        assert service.store.db.execute("SELECT decision FROM approvals").fetchone()[0] == "allowed"
    finally:
        await service.close()


async def test_telegram_no_secret_errors_or_ambiguous_send_retry():
    requests = []

    def handler(request):
        requests.append(request)
        raise httpx.ReadTimeout("secret token in URL", request=request)

    telegram = Telegram("secret", transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(TelegramError) as error:
            await telegram.send(11, "hello")
        assert "secret" not in str(error.value)
        assert len(requests) == 1
    finally:
        await telegram.close()


def test_unicode_chunk_limits():
    text = "😀" * 5000
    pieces = list(chunks(text))
    assert "".join(pieces) == text
    assert all(len(p.encode("utf-16-le")) // 2 <= 3500 for p in pieces)


def test_backlog_and_newer_schema(tmp_path):
    c = config(tmp_path, backlog=1)
    store = Store(c.state, c.instance)
    store.admit([update(1), update(2), update(3, "/stop")], c)
    assert [r[4] for r in store.pending()] == ["pending", "rejected", "pending"]
    store.db.execute("PRAGMA user_version=99")
    store.close()
    with pytest.raises(ValueError):
        Store(c.state, c.instance)


async def test_service_approval_and_failed_final_delivery(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "write")], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not service.cards:
                await asyncio.sleep(0.02)
        handle = next(iter(service.cards))
        card = service.cards[handle]
        assert not (tmp_path / "approved.txt").exists()
        await service.callback(
            {
                "data": "a:" + handle,
                "from": {"id": 11},
                "message": {"message_id": card["message"], "chat": {"id": 11, "type": "private"}},
            }
        )
        bot.fail = True
        await settle(service)
        assert (tmp_path / "approved.txt").read_text() == "ok"
        assert service.store.issues() == [(1, 11, "delivery_failed", 11, 0)]
        assert not service.cards
    finally:
        await service.close()


async def test_stop_pending_approval_and_stale_button(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "write")], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not service.cards:
                await asyncio.sleep(0.02)
        handle = next(iter(service.cards))
        service.store.admit([update(2, "/stop")], c)
        await asyncio.wait_for(service.dispatch(), 3)
        await settle(service)
        await service.callback(
            {
                "data": "a:" + handle,
                "from": {"id": 11},
                "message": {"message_id": 2, "chat": {"id": 11, "type": "private"}},
            }
        )
        assert not (tmp_path / "approved.txt").exists()
        assert not service.cards
    finally:
        await service.close()


async def test_failed_approval_delivery_denies(tmp_path):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot)
    calls = []

    async def request(*args, **kwargs):
        calls.append(kwargs)

    bot.fail = True
    try:
        await service.card(
            (11, 11, 0),
            SimpleNamespace(request=request),
            dict(
                approval_id="a",
                run_id="r",
                tool="Unknown",
                preview="action",
                digest="digest",
                expires_at=time.time() + 30,
            ),
        )
        assert calls[0]["allow"] is False
        assert not service.cards
    finally:
        await service.close()


async def test_poll_rate_limit(tmp_path, monkeypatch):
    delays = []

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("delta_gateway.telegram.asyncio.sleep", sleep)
    count = 0

    def handler(request):
        nonlocal count
        count += 1
        return (
            httpx.Response(
                429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 2}}
            )
            if count == 1
            else httpx.Response(200, json={"ok": True, "result": []})
        )

    telegram = Telegram("token", transport=httpx.MockTransport(handler))
    try:
        assert await telegram.call("getUpdates") == []
        assert delays == [2]
    finally:
        await telegram.close()


def test_instance_lock_release(tmp_path):
    from delta_gateway.ownership import SessionLock

    lock = SessionLock(tmp_path, "instance")
    try:
        with pytest.raises(RuntimeError):
            SessionLock(tmp_path, "instance")
    finally:
        lock.close()
    SessionLock(tmp_path, "instance").close()


async def test_idle_cleanup_preserves_jobs_and_approvals(tmp_path):
    service = Service(config(tmp_path, idle_seconds=1), Bot())
    closed = []

    async def close():
        closed.append(True)

    service.sessions.clients["session"] = SimpleNamespace(close=close)
    service.sessions.touched["session"] = time.monotonic() - 100
    service.sessions.busy.add("session")
    service.cards["pending"] = {"key": (11, 11, 0)}
    await service.cleanup_idle()
    assert not closed
    service.cards.clear()
    await service.cleanup_idle()
    assert not closed
    service.sessions.busy.clear()
    await service.cleanup_idle()
    assert closed == [True]
    await service.close()


async def test_worker_cap_and_webhook_refusal(tmp_path):
    c = config(tmp_path, workers=1)
    c.users = [11, 22]
    service = Service(c, Bot(), factory="tests.fake_session:create")
    try:
        service.store.admit([update(1, "slow"), update(2, "hello", user=22)], c)
        await service.dispatch()
        async with asyncio.timeout(5):
            while not service.clients:
                await asyncio.sleep(0.02)
        assert len(service.clients) == 1
        assert (22, 22, 0) not in service.clients
    finally:
        await service.close()

    class WebhookBot(Bot):
        async def call(self, method, **payload):
            return {"id": 123} if method == "getMe" else {"url": "https://example.test/webhook"}

    service = Service(c, WebhookBot())
    try:
        with pytest.raises(TelegramError, match="Webhook"):
            await service.run()
        assert not service.clients
    finally:
        await service.close()
