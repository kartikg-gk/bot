import asyncio
import base64
import hashlib
import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from delta_gateway.app_connections import APPS
from tests.test_web_accounts import ORIGIN, login
from tests.test_web_accounts import accounts as accounts


@pytest.mark.parametrize("name", ["notion", "linear"])
async def test_workspace_oauth_is_private_rotates_on_401_and_revokes(accounts, monkeypatch, name):
    server, browser, begin, _ = accounts
    app = APPS[name]
    monkeypatch.setenv(app.client_id, "workspace-client")
    monkeypatch.setenv(app.secret, "workspace-secret")
    calls = []
    query = {}
    basic = "Basic " + base64.b64encode(b"workspace-client:workspace-secret").decode()
    route = "/api/apps/" + name
    callback = "/auth/apps/" + name + "/callback"

    async def provider(req):
        if str(req.url) == app.token_url:
            if name == "notion":
                assert req.headers["authorization"] == basic
                assert req.headers["notion-version"] == "2026-03-11"
                fields = json.loads(req.content)
                assert "code_verifier" not in fields
            else:
                fields = {key: val[0] for key, val in parse_qs(req.content.decode()).items()}
                assert fields["client_id"] == "workspace-client"
                assert fields["client_secret"] == "workspace-secret"
            grant = fields["grant_type"]
            calls.append(grant)
            if grant == "authorization_code":
                assert fields["code"] == "app-code" and fields["redirect_uri"] == ORIGIN + callback
                if name == "linear":
                    digest = hashlib.sha256(fields["code_verifier"].encode()).digest()
                    assert (
                        base64.urlsafe_b64encode(digest).decode().rstrip("=")
                        == query["code_challenge"]
                    )
                token = {"access_token": "initial-access", "refresh_token": "initial-refresh"}
            else:
                assert grant == "refresh_token" and fields["refresh_token"] == "initial-refresh"
                await asyncio.sleep(0.02)
                token = {"access_token": "fresh-access", "refresh_token": "fresh-refresh"}
            if name == "linear":
                token.update(expires_in=3600, scope=["read", "issues:create", "comments:create"])
            return httpx.Response(200, json=token)
        if req.url.path.endswith("/revoke"):
            calls.append("revoke")
            if name == "notion":
                assert req.url.host == "api.notion.com" and req.headers["authorization"] == basic
                assert json.loads(req.content) == {"token": "fresh-access"}
            else:
                assert req.url.host == "api.linear.app"
                assert parse_qs(req.content.decode()) == {
                    "token": ["fresh-refresh"],
                    "token_type_hint": ["refresh_token"],
                }
            return httpx.Response(200, json={})
        calls.append("tool")
        if req.headers["authorization"] == "Bearer initial-access":
            return httpx.Response(401, json={"error": "unauthorized"})
        assert req.headers["authorization"] == "Bearer fresh-access"
        return httpx.Response(
            200, json=({"results": []} if name == "notion" else {"data": {"teams": {"nodes": []}}})
        )

    transport = httpx.MockTransport(provider)
    server.apps.transport = transport
    await server.apps.http.aclose()
    server.apps.http = httpx.AsyncClient(transport=transport)
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    response = await alice.post(route + "/connect")
    parsed = urlsplit(response.json()["url"])
    query.update({key: val[0] for key, val in parse_qs(parsed.query).items()})
    assert query["redirect_uri"] == ORIGIN + callback
    if name == "notion":
        assert query["owner"] == "user" and "code_challenge" not in query
    else:
        assert query["actor"] == "user" and query["scope"] == "comments:create,issues:create,read"
        assert query["code_challenge_method"] == "S256"
    params = {"state": query["state"], "code": "app-code"}
    assert (await bob.get(callback, params=params)).status_code == 400
    assert (await alice.get(callback, params=params)).status_code == 302
    assert (await alice.get(callback, params=params)).status_code == 400
    principal = server.google.identity(alice.cookies["delta_session"])
    runtime = server.runtimes.get(principal)
    bot = runtime.store.create_bot("Workspace", "Use reviewed actions")
    grant = route + f"/bots/{bot.id}"
    assert (await bob.put(grant, json={"enabled": True})).status_code == 404
    tool = "notion_search" if name == "notion" else "linear_list_teams"
    with pytest.raises(ValueError, match="not enabled"):
        await runtime.team.call_plugin(tool, {}, bot)
    assert (await alice.put(grant, json={"enabled": True})).status_code == 200
    await asyncio.gather(*(runtime.team.call_plugin(tool, {}, bot) for _ in range(3)))
    assert calls.count("refresh_token") == 1
    sealed = server.accounts.db.execute(
        "SELECT sealed FROM credentials WHERE kind='app'"
    ).fetchone()[0]
    assert b"fresh-access" not in sealed and b"fresh-refresh" not in sealed
    assert (await alice.delete(route)).json() == {"connected": False, "providerRevoked": True}
    assert runtime.store.plugins_for(bot.id) == [] and calls[-1] == "revoke"
