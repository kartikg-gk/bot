import asyncio
import base64
import hashlib
import json
from urllib.parse import parse_qs, urlsplit

import httpx
import httpx2
import pytest

from delta_gateway.plugins import ACCOUNT_ADAPTERS
from delta_gateway.wispr import (
    WISPR_AUTH_URL,
    WISPR_ISSUER,
    WISPR_MCP_URL,
    WISPR_REGISTRATION_URL,
    WISPR_RESOURCE_METADATA,
    WISPR_TOKEN_URL,
    bridge,
    register_client,
    wispr_session,
)
from tests.test_web_accounts import ORIGIN, login
from tests.test_web_accounts import accounts as accounts


async def test_wispr_sdk_discovery_and_read_only_call():
    calls = []

    def provider(req):
        assert str(req.url) == WISPR_MCP_URL
        assert req.headers["authorization"] == "Bearer flow"
        if req.method == "GET":
            return httpx2.Response(405)
        body = json.loads(req.content)
        method = body["method"]
        if method == "notifications/initialized":
            return httpx2.Response(202)
        reply = {"jsonrpc": "2.0", "id": body["id"]}
        if method == "server/discover":
            return httpx2.Response(
                200,
                json={
                    **reply,
                    "error": {"code": -32601, "message": "Method not found"},
                },
            )
        if method == "initialize":
            result = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "Mock Wispr Flow", "version": "1"},
            }
        elif method == "tools/list":
            schema = {"type": "object"}
            result = {
                "tools": [
                    {"name": "get_meeting", "inputSchema": schema},
                    {
                        "name": "search_meetings",
                        "inputSchema": schema,
                        "annotations": {"readOnlyHint": True},
                    },
                    {
                        "name": "delete_meeting",
                        "inputSchema": schema,
                        "annotations": {"readOnlyHint": False},
                    },
                    {
                        "name": "conflicting",
                        "inputSchema": schema,
                        "annotations": {"readOnlyHint": True, "destructiveHint": True},
                    },
                ]
            }
        else:
            assert method == "tools/call"
            calls.append(body["params"])
            result = {"content": [{"type": "text", "text": "Meeting notes"}]}
        return httpx2.Response(200, json={**reply, "result": result})

    async with wispr_session(
        {"authorization": "Bearer flow"}, transport=httpx2.MockTransport(provider)
    ) as client:
        result = await bridge(client, "wispr_list_tools", {})
        assert [item["name"] for item in result["tools"]] == ["get_meeting", "search_meetings"]
        schema = await bridge(client, "wispr_list_tools", {"name": "get_meeting"})
        assert schema["inputSchema"] == {"type": "object"}
        for blocked in ("delete_meeting", "conflicting"):
            with pytest.raises(ValueError, match="not read-only"):
                await bridge(client, "wispr_call_tool", {"name": blocked})
        with pytest.raises(ValueError, match="did not advertise"):
            await bridge(client, "wispr_call_tool", {"name": "other"})
        assert not calls
        result = await bridge(
            client, "wispr_call_tool", {"name": "get_meeting", "arguments": {"id": "m1"}}
        )
        assert result["text"] == "Meeting notes"
    assert len(calls) == 1 and calls[0]["arguments"] == {"id": "m1"}


async def test_wispr_rejects_untrusted_metadata_before_registering():
    calls = []

    def provider(req):
        calls.append(str(req.url))
        return httpx.Response(
            200,
            json={
                "resource": WISPR_MCP_URL,
                "authorization_servers": ["https://untrusted.test"],
                "scopes_supported": ["openid", "offline_access"],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        with pytest.raises(ValueError, match="unexpected resource metadata"):
            await register_client(client, "https://gateway.example.test/auth/apps/wispr/callback")
    assert calls == [WISPR_RESOURCE_METADATA]


async def test_wispr_registration_browser_binding_rotation_and_local_disconnect(
    accounts, monkeypatch
):
    server, browser, begin, _ = accounts
    route = "/api/apps/wispr"
    callback = "/auth/apps/wispr/callback"
    query = {}
    calls = []
    reject_client = False

    async def provider(req):
        target = str(req.url)
        if target == WISPR_RESOURCE_METADATA:
            calls.append("resource")
            return httpx.Response(
                200,
                json={
                    "resource": WISPR_MCP_URL,
                    "authorization_servers": [WISPR_ISSUER],
                    "scopes_supported": ["openid", "offline_access"],
                },
            )
        if target == WISPR_ISSUER + "/.well-known/oauth-authorization-server":
            calls.append("issuer")
            return httpx.Response(
                200,
                json={
                    "issuer": WISPR_ISSUER,
                    "authorization_endpoint": WISPR_AUTH_URL,
                    "token_endpoint": WISPR_TOKEN_URL,
                    "registration_endpoint": WISPR_REGISTRATION_URL,
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                },
            )
        if target == WISPR_REGISTRATION_URL:
            calls.append("register")
            data = json.loads(req.content)
            assert data["redirect_uris"] == [ORIGIN + callback]
            assert data["token_endpoint_auth_method"] == "none"
            assert data["grant_types"] == ["authorization_code", "refresh_token"]
            return httpx.Response(
                201,
                json={
                    "client_id": "flow-client-" + str(calls.count("register")),
                    "redirect_uris": data["redirect_uris"],
                    "token_endpoint_auth_method": "none",
                },
            )
        assert target == WISPR_TOKEN_URL
        fields = {key: val[0] for key, val in parse_qs(req.content.decode()).items()}
        assert "authorization" not in req.headers and "client_secret" not in fields
        assert fields["client_id"] == "flow-client-1" and fields["resource"] == WISPR_MCP_URL
        calls.append(fields["grant_type"])
        if fields["grant_type"] == "authorization_code":
            assert fields["redirect_uri"] == ORIGIN + callback
            digest = hashlib.sha256(fields["code_verifier"].encode()).digest()
            assert base64.urlsafe_b64encode(digest).decode().rstrip("=") == query["code_challenge"]
            if reject_client:
                return httpx.Response(400, json={"error": "invalid_client"})
            token = {
                "access_token": "first-access",
                "refresh_token": "first-refresh",
                "expires_in": 10,
            }
        else:
            assert (
                fields["grant_type"] == "refresh_token"
                and fields["refresh_token"] == "first-refresh"
            )
            await asyncio.sleep(0.02)
            token = {
                "access_token": "fresh-access",
                "refresh_token": "fresh-refresh",
                "expires_in": 3600,
            }
        return httpx.Response(200, json={**token, "scope": "openid"})

    async def mock_bridge(_http, headers, name, args):
        assert headers["authorization"] == "Bearer fresh-access"
        assert name == "wispr_list_tools" and not args
        calls.append("read")
        return {"tools": []}

    monkeypatch.setitem(ACCOUNT_ADAPTERS, "wispr", mock_bridge)
    await server.apps.http.aclose()
    server.apps.http = httpx.AsyncClient(transport=httpx.MockTransport(provider))
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")

    async def start():
        response = await alice.post(route + "/connect")
        assert response.status_code == 200, response.text
        url = urlsplit(response.json()["url"])
        assert f"{url.scheme}://{url.netloc}{url.path}" == WISPR_AUTH_URL
        query.update({k: v[0] for k, v in parse_qs(url.query).items()})
        assert query["resource"] == WISPR_MCP_URL and query["scope"] == "offline_access openid"
        assert query["code_challenge_method"] == "S256"
        return {"state": query["state"], "code": "app-code"}

    await start()
    params = await start()
    assert query["client_id"] == "flow-client-1"
    assert calls == ["resource", "issuer", "register"]
    assert (await bob.get(callback, params=params)).status_code == 400
    assert (await alice.get(callback, params=params)).status_code == 302
    assert (await alice.get(callback, params=params)).status_code == 400
    principal = server.google.identity(alice.cookies["delta_session"])
    runtime = server.runtimes.get(principal)
    bot = runtime.store.create_bot("Meeting reader", "Find meeting notes")
    grant = route + f"/bots/{bot.id}"
    assert (await bob.put(grant, json={"enabled": True})).status_code == 404
    with pytest.raises(ValueError, match="not enabled"):
        await runtime.team.call_plugin("wispr_list_tools", {}, bot)
    assert (await alice.put(grant, json={"enabled": True})).status_code == 200
    await asyncio.gather(*(runtime.team.call_plugin("wispr_list_tools", {}, bot) for _ in range(2)))
    assert calls.count("refresh_token") == 1
    assert calls.count("read") == 2
    sealed = server.accounts.db.execute(
        "SELECT sealed FROM credentials WHERE kind='app'"
    ).fetchone()[0]
    assert b"fresh-access" not in sealed and b"fresh-refresh" not in sealed
    assert (await alice.delete(route)).json() == {"connected": False, "providerRevoked": False}
    assert runtime.store.plugins_for(bot.id) == []
    reject_client = True
    pending = await start()
    assert (await alice.get(callback, params=pending)).status_code == 502
    await start()
    assert calls.count("register") == 2 and query["client_id"] == "flow-client-2"
