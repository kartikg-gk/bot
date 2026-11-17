import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from pydantic import SecretStr

from delta_gateway.telegram_links import TOKEN_VARIABLE, TelegramLinks
from tests.test_web_accounts import accounts as accounts
from tests.test_web_accounts import login


async def test_telegram_private_links_isolate_accounts_and_destinations(accounts):
    server, browser, begin, _ = accounts
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    updates, sends = [], []

    def respond(request):
        assert request.url.host == "api.telegram.org"
        method = request.url.path.rsplit("/", 1)[1]
        fields = json.loads(request.content or b"{}")
        if method == "getMe":
            result = {
                "id": 12345,
                "is_bot": True,
                "first_name": "Fixture",
                "username": "FixtureBot",
            }
        elif method == "getUpdates":
            result = [update for update in updates if update["update_id"] >= int(fields["offset"])]
        else:
            assert method == "sendMessage"
            sends.append(fields)
            result = {
                "message_id": 42,
                "date": 12345,
                "chat": {"id": int(fields["chat_id"]), "type": "private"},
                "text": fields["text"],
            }
        return httpx.Response(200, json={"ok": True, "result": result})

    await server.apps.telegram_links.close()
    server.apps.telegram_links = TelegramLinks(
        "12345:" + "TEST" * 10, transport=httpx.MockTransport(respond)
    )
    path = "/api/apps/telegram"
    alice_link = await alice.post(path + "/connect")
    bob_link = await bob.post(path + "/connect")
    assert alice_link.headers["cache-control"] == "no-store"
    alice_code, bob_code = (
        parse_qs(urlsplit(response.json()["url"]).query)["start"][0]
        for response in (alice_link, bob_link)
    )
    assert alice_code != bob_code

    def update(identifier, code, chat_id, kind):
        return {
            "update_id": identifier,
            "message": {
                "message_id": identifier,
                "date": 12345,
                "chat": {"id": chat_id, "type": kind},
                "from": {"id": chat_id, "is_bot": False, "first_name": "Owner"},
                "text": "/start " + code,
            },
        }

    updates.extend([update(1, alice_code, 100, "group"), update(2, bob_code, 202, "private")])
    assert (await alice.post(path + "/confirm")).json() == {"connected": False}
    assert (await bob.post(path + "/confirm")).json() == {"connected": True}
    updates.append(update(3, alice_code, 101, "private"))
    assert (await alice.post(path + "/confirm")).json() == {"connected": True}
    assert not sends
    identities = {row["email"]: row for row in server.accounts.all_accounts()}
    for client, email in ((alice, "alice@example.test"), (bob, "bob@team.test")):
        runtime = server.runtimes.get(identities[email])
        bot = (await runtime.store.bots())[0]
        with pytest.raises(ValueError, match="not enabled"):
            await runtime.team.call_plugin("telegram_send_message", {"text": "Fixture"}, bot)
        assert (
            await client.put(path + "/bots/" + bot.id, json={"enabled": True})
        ).status_code == 200
        sent = await runtime.team.call_plugin(
            "telegram_send_message", {"text": "<b>Fixture</b>"}, bot
        )
        assert json.loads(sent.split("\n", 1)[1]) == {"message_id": 42}
    assert [int(record["chat_id"]) for record in sends] == [101, 202]
    assert all(record["text"].endswith(": <b>Fixture</b>") for record in sends)
    assert all("parse_mode" not in record for record in sends)
    assert all(json.loads(record["link_preview_options"])["is_disabled"] for record in sends)
    assert (await alice.delete(path)).json()["connected"] is False
    assert (await alice.post(path + "/confirm")).json() == {"connected": False}


APP_TOKEN = "12345:" + "APP_TEST" * 5
MAIN_TOKEN_ENV = "FIXTURE_GATEWAY_MAIN_BOT_TOKEN"


@pytest.fixture
def telegram_startup(monkeypatch):
    monkeypatch.setenv(TOKEN_VARIABLE, APP_TOKEN)
    monkeypatch.setenv(MAIN_TOKEN_ENV, APP_TOKEN)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    created = []

    class AppBot:
        def __init__(self, token, **kwargs):
            created.append(token)

        async def call(self, method, **kwargs):
            assert method == "getMe"
            return {"username": "FixtureBot"}

        async def close(self):
            pass

    monkeypatch.setattr("delta_gateway.telegram_links.TelegramSDK", AppBot)
    return created


@pytest.mark.parametrize(
    ("accounts", "conflict"),
    [
        ({"token_env": MAIN_TOKEN_ENV}, True),
        ({"bot_token": SecretStr(APP_TOKEN)}, True),
        ({"bot_token": SecretStr("54321:" + "MAIN_TEST" * 5)}, False),
    ],
    indirect=["accounts"],
)
async def test_telegram_app_token_checked_at_startup(telegram_startup, accounts, conflict, caplog):
    server, browser, begin, _ = accounts
    client = browser()
    await login(client, begin, "alice", "alice@example.test")
    apps = (await client.get("/api/apps")).json()
    telegram = next(app for app in apps if app["id"] == "telegram")
    assert telegram["configured"] is not conflict
    assert not telegram["connected"]
    response = await client.post("/api/apps/telegram/connect")
    if conflict:
        assert telegram_startup == []
        assert server.apps.telegram_links.bot is None
        error = telegram["configurationError"]
        assert "matches the gateway Telegram bot token" in error
        assert any(error == record.getMessage() for record in caplog.get_records("setup"))
        assert all(APP_TOKEN not in record.getMessage() for record in caplog.get_records("setup"))
        assert APP_TOKEN not in response.text
        assert response.status_code == 409 and response.text == error
        confirm = await client.post("/api/apps/telegram/confirm")
        assert confirm.status_code == 409 and confirm.text == error
    else:
        assert telegram_startup == [APP_TOKEN]
        assert "configurationError" not in telegram
        assert response.status_code == 200
        assert response.json()["url"].startswith("https://t.me/FixtureBot?start=")
