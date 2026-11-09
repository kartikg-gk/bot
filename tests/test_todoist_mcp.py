import asyncio
import base64
import hashlib
import json
from urllib.parse import parse_qs, urlsplit

import httpx
import httpx2
import pytest

import delta_gateway.todoist as adapter
from delta_gateway.app_connections import APPS
from delta_gateway.plugins import ACCOUNT_ADAPTERS
from delta_gateway.todoist import TODOIST_MCP_URL, bridge, todoist_session
from tests.test_web_accounts import ORIGIN, login
from tests.test_web_accounts import accounts as accounts

INPUT = {"type": "object", "properties": {"query": {"type": "string"}}}


async def test_todoist_hosted_mcp_pagination_classification_and_sse(monkeypatch):
    calls = []
    seen = []

    def provider(request):
        assert str(request.url) == TODOIST_MCP_URL
        assert request.headers["authorization"] == "Bearer sample"
        if request.method == "GET":
            return httpx2.Response(405)
        if request.method == "DELETE":
            assert request.headers["mcp-session-id"] == "mock-session"
            return httpx2.Response(204)
        payload = json.loads(request.content)
        method = payload["method"]
        seen.append(method)
        if method == "notifications/initialized":
            return httpx2.Response(202)
        if method == "server/discover":
            return httpx2.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "error": {"code": -32601, "message": "Method not found"},
                },
            )
        assert method == "initialize" or request.headers["mcp-session-id"] == "mock-session"
        params = payload.get("params") or {}
        headers = {}
        if method == "initialize":
            result = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "Mock Todoist", "version": "1"},
            }
            headers["Mcp-Session-Id"] = "mock-session"
        elif method == "tools/list":
            if params.get("cursor") == "second":
                result = {
                    "tools": [
                        {"name": "add-tasks", "inputSchema": INPUT},
                    ]
                }
            else:
                assert not params.get("cursor")
                result = {
                    "tools": [
                        {
                            "name": "find-tasks",
                            "inputSchema": INPUT,
                            "annotations": {"readOnlyHint": True},
                        },
                        {
                            "name": "undocumented",
                            "inputSchema": INPUT,
                            "annotations": {"readOnlyHint": True},
                        },
                    ],
                    "nextCursor": "second",
                }
        else:
            assert method == "tools/call"
            calls.append(params)
            message = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "content": [{"type": "text", "text": "server result"}],
                    "structuredContent": {"ok": True},
                },
            }
            return httpx2.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                text=f"event: message\ndata: {json.dumps(message)}\n\n",
            )
        return httpx2.Response(
            200,
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": result,
            },
        )

    async with todoist_session(
        {"authorization": "Bearer sample"}, transport=httpx2.MockTransport(provider)
    ) as client:
        first = await bridge(client, "todoist_list_tools", {})
        assert [t["mode"] for t in first["tools"]] == ["read", "write"]
        second = await bridge(client, "todoist_list_tools", {"cursor": first["nextCursor"]})
        assert second["tools"][0]["name"] == "add-tasks"
        spec = await bridge(client, "todoist_list_tools", {"name": "add-tasks"})
        assert spec["inputSchema"] == INPUT
        with pytest.raises(ValueError, match="todoist_write_tool"):
            await bridge(client, "todoist_read_tool", {"name": "undocumented"})
        with pytest.raises(ValueError, match="todoist_write_tool"):
            await bridge(client, "todoist_read_tool", {"name": "add-tasks"})
        with pytest.raises(ValueError, match="todoist_read_tool"):
            await bridge(client, "todoist_write_tool", {"name": "find-tasks"})
        assert not calls
        assert await bridge(
            client,
            "todoist_read_tool",
            {
                "name": "find-tasks",
                "arguments": {"query": "today"},
            },
        ) == {"data": {"ok": True}}
        assert await bridge(
            client,
            "todoist_write_tool",
            {
                "name": "add-tasks",
                "arguments": {"query": "reviewed"},
            },
        ) == {"data": {"ok": True}}
    assert [p["name"] for p in calls] == ["find-tasks", "add-tasks"]
    assert calls[1]["arguments"] == {"query": "reviewed"}
    assert "initialize" in seen and "notifications/initialized" in seen

    def mock_session(headers):
        return todoist_session(headers, transport=httpx2.MockTransport(provider))

    monkeypatch.setattr(adapter, "todoist_session", mock_session)
    with pytest.raises(ValueError, match="todoist_write_tool"):
        await adapter.todoist(
            None, {"authorization": "Bearer sample"}, "todoist_read_tool", {"name": "add-tasks"}
        )


async def test_todoist_oauth_binds_resource_rotates_and_revokes(accounts, monkeypatch):
    server, browser, begin, _ = accounts
    app = APPS["todoist"]
    monkeypatch.setenv(app.client_id, "todoist-client")
    monkeypatch.setenv(app.secret, "todoist-secret")
    route = "/api/apps/todoist"
    callback = "/auth/apps/todoist/callback"
    query = {}
    calls = []

    async def provider(request):
        fields = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        if str(request.url) == "https://todoist.com/api/v1/revoke":
            calls.append("revoke")
            assert request.headers["authorization"] == (
                "Basic " + base64.b64encode(b"todoist-client:todoist-secret").decode()
            )
            assert fields == {"token": "new-access", "token_type_hint": "access_token"}
            return httpx.Response(200)
        assert str(request.url) == app.token_url
        assert fields["client_secret"] == "todoist-secret"
        assert fields["resource"] == TODOIST_MCP_URL
        grant = fields["grant_type"]
        calls.append(grant)
        if grant == "authorization_code":
            assert fields["redirect_uri"] == ORIGIN + callback
            assert fields["code"] == "todoist-code"
            expected = base64.urlsafe_b64encode(
                hashlib.sha256(fields["code_verifier"].encode()).digest()
            )
            assert expected.decode().rstrip("=") == query["code_challenge"]
            return httpx.Response(
                200,
                json={
                    "access_token": "first-access",
                    "refresh_token": "first-refresh",
                    "expires_in": 10,
                    "scope": "data:read_write",
                },
            )
        assert grant == "refresh_token" and fields["refresh_token"] == "first-refresh"
        await asyncio.sleep(0.02)
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 3600,
                "scope": "data:read_write",
            },
        )

    async def mock_adapter(_http, headers, name, args):
        assert headers["authorization"] == "Bearer new-access"
        assert name == "todoist_list_tools" and args == {}
        calls.append("tool")
        return {"tools": []}

    monkeypatch.setitem(ACCOUNT_ADAPTERS, "todoist", mock_adapter)
    await server.apps.http.aclose()
    server.apps.http = httpx.AsyncClient(transport=httpx.MockTransport(provider))
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    response = await alice.post(route + "/connect")
    url = urlsplit(response.json()["url"])
    assert f"{url.scheme}://{url.netloc}{url.path}" == app.auth_url
    query.update({k: v[0] for k, v in parse_qs(url.query).items()})
    assert query["scope"] == "data:read_write" and query["resource"] == TODOIST_MCP_URL
    assert query["code_challenge_method"] == "S256"
    params = {"state": query["state"], "code": "todoist-code"}
    assert (await bob.get(callback, params=params)).status_code == 400
    assert (await alice.get(callback, params=params)).status_code == 302
    assert (await alice.get(callback, params=params)).status_code == 400
    account = server.google.identity(alice.cookies["delta_session"])
    runtime = server.runtimes.get(account)
    bot = runtime.store.create_bot("Todoist reader", "Find tasks")
    grant = route + f"/bots/{bot.id}"
    assert (await bob.put(grant, json={"enabled": True})).status_code == 404
    assert (await alice.put(grant, json={"enabled": True})).status_code == 200
    await asyncio.gather(
        *(runtime.team.call_plugin("todoist_list_tools", {}, bot) for _ in range(2))
    )
    assert calls == ["authorization_code", "refresh_token", "tool", "tool"]
    sealed = server.accounts.db.execute(
        "SELECT sealed FROM credentials WHERE kind='app'"
    ).fetchone()[0]
    assert b"new-access" not in sealed and b"new-refresh" not in sealed
    assert (await alice.delete(route)).json() == {"connected": False, "providerRevoked": True}
    assert runtime.store.plugins_for(bot.id) == [] and calls[-1] == "revoke"
