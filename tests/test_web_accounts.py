import asyncio
import base64
import hashlib
import json
import time
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import rsa

from delta_gateway.account_models import AccountProvider
from delta_gateway.config import load, save
from delta_gateway.google_oidc import AUTH_URL, KEYS_URL, SESSION_COOKIE, TOKEN_URL
from delta_gateway.service import Service
from delta_gateway.web import WEB_CHAT, WebServer
from tests.test_gateway import config, settle

ORIGIN = "http://localhost:8765"
CLIENT = "gateway-client.apps.googleusercontent.com"


@pytest.fixture
async def accounts(tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_GATEWAY_GOOGLE_CLIENT_ID", CLIENT)
    monkeypatch.setenv("DELTA_GATEWAY_GOOGLE_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("DELTA_GATEWAY_MASTER_KEY", base64.urlsafe_b64encode(b"A" * 32).decode())
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwk["kid"] = "fixture-key"
    grants, checks = {}, []

    def google(request):
        if str(request.url) == KEYS_URL:
            return httpx.Response(
                200, json={"keys": [jwk]}, headers={"Cache-Control": "max-age=60"}
            )
        assert str(request.url) == TOKEN_URL and request.method == "POST"
        fields = {k: values[0] for k, values in parse_qs(request.content.decode()).items()}
        assert fields["redirect_uri"] == ORIGIN + "/auth/callback"
        assert fields["client_secret"] == "test-secret"
        claims, challenge = grants.pop(fields["code"])
        actual = base64.urlsafe_b64encode(hashlib.sha256(fields["code_verifier"].encode()).digest())
        assert actual.decode().rstrip("=") == challenge
        token = jwt.encode(claims, key, algorithm="RS256", headers={"kid": "fixture-key"})
        return httpx.Response(200, json={"id_token": token})

    def provider(request):
        endpoint = str(request.url)
        assert endpoint in {
            "https://api.openai.com/v1/models",
            "https://api.anthropic.com/v1/models",
            "https://openrouter.ai/api/v1/key",
        }
        if request.url.host == "api.anthropic.com":
            assert request.headers["anthropic-version"] == "2023-06-01"
            assert "authorization" not in request.headers
            sent_key = request.headers["x-api-key"]
        else:
            sent_key = request.headers["authorization"].removeprefix("Bearer ")
        checks.append((endpoint, sent_key))
        return httpx.Response(401 if sent_key == "invalid" else 200)

    c = config(
        tmp_path,
        web_port=0,
        web_only=True,
        web_auth="google",
        web_origin=ORIGIN,
        web_allowed_emails=["alice@example.test"],
        web_allowed_domains=["team.test"],
        executor="docker",
        docker_image="python:3.12-slim",
    )
    path = tmp_path / "gateway.yaml"
    save(path, c)
    service = Service(load(path), None, factory="tests.fake_session:create")
    server = service.web = WebServer(
        service,
        service.telegram,
        port=0,
        google_transport=httpx.MockTransport(google),
        provider_transport=httpx.MockTransport(provider),
    )
    await server.start()
    clients = []

    def browser():
        result = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.port}",
            headers={"Host": "localhost:8765", "Origin": ORIGIN},
        )
        clients.append(result)
        return result

    async def begin(client, name, email, **overrides):
        response = await client.get("/auth/login")
        assert response.status_code == 302
        assert "HttpOnly" in response.headers["set-cookie"]
        assert response.headers["location"].startswith(AUTH_URL + "?")
        q = {k: v[0] for k, v in parse_qs(urlsplit(response.headers["location"]).query).items()}
        assert q["code_challenge_method"] == "S256"
        claims = dict(
            iss="https://accounts.google.com",
            aud=CLIENT,
            sub=name,
            email=email,
            email_verified=True,
            nonce=q["nonce"],
            iat=int(time.time()),
            exp=int(time.time()) + 60,
        )
        grants[name] = ({**claims, **overrides}, q["code_challenge"])
        return {"state": q["state"], "code": name}

    try:
        yield server, browser, begin, checks
    finally:
        for client in clients:
            await client.aclose()
        await service.close()


async def login(client, begin, name, email, **claims):
    result = await client.get("/auth/callback", params=await begin(client, name, email, **claims))
    assert result.status_code == 302, result.text
    assert SESSION_COOKIE in client.cookies
    assert any(
        "HttpOnly" in line and "SameSite=Lax" in line
        for line in result.headers.get_list("set-cookie")
    )


async def test_google_login_binds_flow_to_browser_and_revokes_cookie(accounts):
    _, browser, begin, _ = accounts
    alice, stranger = browser(), browser()
    assert (await alice.get("/api/bots")).status_code == 401
    assert (await alice.get("/api/bots", headers={"Host": "untrusted.test"})).status_code == 403
    params = await begin(alice, "alice", "alice@example.test")
    assert (await stranger.get("/auth/callback", params=params)).status_code == 400
    assert (await alice.get("/auth/callback", params=params)).status_code == 302
    assert (await alice.get("/auth/callback", params=params)).status_code == 400
    saved = alice.cookies[SESSION_COOKIE]
    assert (
        await alice.post("/auth/logout", headers={"Origin": "https://other.test"})
    ).status_code == 403
    assert (await alice.get("/api/bots")).status_code == 200
    assert (await alice.post("/auth/logout")).status_code == 200
    assert (
        await stranger.get("/api/bots", headers={"Cookie": f"{SESSION_COOKIE}={saved}"})
    ).status_code == 401


async def test_proxy_ingress_preserves_google_cookie_and_origin_checks(tmp_path, monkeypatch):
    origin = "https://gateway.example.test"
    secret = "x" * 43
    proxy = {"X-Delta-Gateway-Proxy": secret}
    monkeypatch.setenv("DELTA_GATEWAY_PROXY_SECRET", secret)
    monkeypatch.setenv("DELTA_GATEWAY_GOOGLE_CLIENT_ID", CLIENT)
    monkeypatch.setenv("DELTA_GATEWAY_GOOGLE_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("DELTA_GATEWAY_MASTER_KEY", base64.urlsafe_b64encode(b"A" * 32).decode())
    c = config(
        tmp_path,
        web_port=0,
        web_only=True,
        web_auth="google",
        web_origin=origin,
        web_allowed_emails=["alice@example.test"],
        executor="docker",
        docker_image="python:3.12-slim",
    )
    service = Service(c, None, factory="tests.fake_session:create")
    try:
        await service.start_web()
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{service.web.port}") as browser:
            assert (
                await browser.get("/", headers={"Host": "gateway.example.test"})
            ).status_code == 403
            assert (
                await browser.get("/", headers={"X-Delta-Gateway-Proxy": "wrong"})
            ).status_code == 403
            assert (await browser.get("/api/bots", headers=proxy)).status_code == 401
            login = await browser.get("/auth/login", headers=proxy)
            assert login.status_code == 302
            assert "Secure" in login.headers["set-cookie"]
            query = parse_qs(urlsplit(login.headers["location"]).query)
            assert query["redirect_uri"] == [origin + "/auth/callback"]
            assert (await browser.post("/auth/logout", headers=proxy)).status_code == 403
            assert (
                await browser.post(
                    "/auth/logout", headers={**proxy, "Origin": "https://other.test"}
                )
            ).status_code == 403
            assert (
                await browser.post("/auth/logout", headers={**proxy, "Origin": origin})
            ).status_code == 200
    finally:
        await service.close()


@pytest.mark.parametrize(
    ("overrides", "status"),
    [
        ({"aud": "wrong-client"}, 401),
        ({"nonce": "wrong-nonce"}, 401),
        ({"email_verified": False}, 403),
        ({"email": "stranger@team.test"}, 403),
    ],
)
async def test_google_rejects_wrong_token_or_admission(accounts, overrides, status):
    _, browser, begin, _ = accounts
    client = browser()
    # Replace the pending grant with a token containing the claim under test.
    params = await begin(
        client,
        "rejected",
        overrides.get("email", "alice@example.test"),
        **{k: v for k, v in overrides.items() if k != "email"},
    )
    assert (await client.get("/auth/callback", params=params)).status_code == status
    assert (await client.get("/api/bots")).status_code == 401


async def test_accounts_separate_bots_chats_jobs_and_event_streams(accounts):
    server, browser, begin, _ = accounts
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    for client in (alice, bob):
        assert (await client.get("/api/bots")).status_code == 200
    bot = (await alice.post("/api/bots", json={"name": "Private", "role": "Only Alice"})).json()
    assert (await bob.get(f"/api/bots/{bot['id']}/messages")).status_code == 404
    assert (
        await alice.post(
            "/api/jobs", json={"bot": bot["id"], "when": "every 1h", "task": "private"}
        )
    ).status_code == 201
    assert (await bob.get("/api/jobs")).json()["jobs"] == []
    alice_service = server.runtimes.get(server.google.identity(alice.cookies[SESSION_COOKIE]))
    bob_service = server.runtimes.get(server.google.identity(bob.cookies[SESSION_COOKIE]))
    assert alice_service.config.project != bob_service.config.project
    async with alice.stream("GET", "/api/stream") as stream:
        lines = stream.aiter_lines()
        assert await anext(lines) == ": connected"
        assert await anext(lines) == ""
        await bob_service.telegram.send(WEB_CHAT, "bob-only")
        await alice_service.telegram.send(WEB_CHAT, "alice-only")
        row = await asyncio.wait_for(anext(lines), 3)
        assert json.loads(row[6:])["text"] == "alice-only"
    await asyncio.gather(
        alice.post("/api/messages", json={"text": "private-alice"}),
        bob.post("/api/messages", json={"text": "private-bob"}),
    )
    await asyncio.gather(settle(alice_service), settle(bob_service))
    assert "private-bob" not in (await alice.get("/api/messages")).text
    assert "private-alice" not in (await bob.get("/api/messages")).text


async def test_key_validation_is_rate_limited_and_ciphertext_is_account_bound(accounts):
    server, browser, begin, checks = accounts
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    path = "/api/account/model"
    settings = {"provider": "openai", "model": "fixture", "key": "test-alice-secret"}
    result = await alice.put(path, json=settings)
    assert result.status_code == 200 and settings["key"] not in result.text
    assert (await bob.get(path)).json() == {"provider": None, "model": None, "keys": []}
    owner = server.google.identity(alice.cookies[SESSION_COOKIE])["id"]
    other = server.google.identity(bob.cookies[SESSION_COOKIE])["id"]
    sealed = server.accounts.db.execute("SELECT sealed FROM credentials").fetchone()[0]
    assert settings["key"].encode() not in sealed
    factory = AccountProvider(server.accounts, owner)
    provider, name, model = factory(None)
    try:
        assert name == "openai" and model == "fixture"
        assert provider._profile.credential.api_key == settings["key"]
    finally:
        await provider.close()
    with server.accounts.db:
        server.accounts.db.execute(
            "INSERT INTO credentials VALUES(?,?,?,?)", (other, "model", "openai", sealed)
        )
    with pytest.raises(InvalidTag):
        server.accounts.credential(other, "model", "openai")
    for _ in range(4):
        assert (await alice.put(path, json={**settings, "key": "invalid"})).status_code == 400
    assert (await alice.put(path, json=settings)).status_code == 429
    assert len(checks) == 5
    assert (await alice.delete("/api/account/keys/openai")).json()["keys"] == []


async def test_signout_ends_an_existing_event_stream(accounts):
    server, browser, begin, _ = accounts
    client = browser()
    await login(client, begin, "alice", "alice@example.test")
    row = server.google.identity(client.cookies[SESSION_COOKIE])
    channel = server.runtimes.get(row).telegram
    async with client.stream("GET", "/api/stream") as stream:
        lines = stream.aiter_lines()
        assert await anext(lines) == ": connected"
        assert await anext(lines) == ""
        assert (await client.post("/auth/logout")).status_code == 200
        await channel.send(WEB_CHAT, "This was sent after revocation")
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(lines), 3)


def test_google_mode_refuses_local_shell(tmp_path):
    c = config(tmp_path, web_only=True, web_port=8765, web_auth="google", executor="local")
    with pytest.raises(ValueError, match="Docker or Daytona"):
        c.validate_paths()


@pytest.mark.parametrize(
    ("name", "endpoint", "base"),
    [
        ("anthropic", "https://api.anthropic.com/v1/models", "https://api.anthropic.com"),
        ("openrouter", "https://openrouter.ai/api/v1/key", "https://openrouter.ai/api/v1"),
    ],
)
async def test_validation_and_runtime_use_the_selected_provider(accounts, name, endpoint, base):
    server, browser, begin, checks = accounts
    client = browser()
    await login(client, begin, "alice", "alice@example.test")
    saved = await client.put(
        "/api/account/model",
        json={
            "provider": name,
            "model": "test-model",
            "key": "test-provider-key",
        },
    )
    assert saved.status_code == 200
    assert checks == [(endpoint, "test-provider-key")]
    user = server.google.identity(client.cookies[SESSION_COOKIE])
    provider, selected, model = AccountProvider(server.accounts, user["id"])(None)
    try:
        assert (selected, model) == (name, "test-model")
        assert provider._profile.credential.base_url == base
        assert provider._profile.credential.api_key == "test-provider-key"
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "name", ["github", "gmail", "calendar", "tasks", "drive", "docs", "sheets", "contacts"]
)
async def test_app_connection_is_private_refreshes_and_revokes(accounts, monkeypatch, name):
    from delta_gateway.app_connections import APPS, LEGACY_GMAIL_SCOPES

    server, browser, begin, _ = accounts
    app = APPS[name]
    tool, tool_arguments, tool_path = {
        "github": ("github_list_issues", {"repo": "a/b"}, "/repos/a/b/issues"),
        "gmail": ("gmail_search", {"query": "is:unread"}, "/gmail/v1/users/me/messages"),
        "calendar": ("calendar_list_calendars", {}, "/calendar/v3/users/me/calendarList"),
        "tasks": ("tasks_list_lists", {}, "/tasks/v1/users/@me/lists"),
        "drive": ("drive_search", {}, "/drive/v3/files"),
        "docs": ("docs_create", {"title": "Mock"}, "/v1/documents"),
        "sheets": (
            "sheets_read",
            {"spreadsheet_id": "abc", "range": "A1"},
            "/v4/spreadsheets/abc/values/A1",
        ),
        "contacts": ("contacts_search", {"query": "Test"}, "/v1/people:searchContacts"),
    }[name]
    monkeypatch.setenv(app.client_id, "connected-app")
    monkeypatch.setenv(app.secret, "app-secret")
    monkeypatch.setenv("DELTA_GATEWAY_GITHUB_TOKEN", "operator-token")
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    path = f"/api/apps/{name}"
    callback = f"/auth/apps/{name}/callback"
    reply = await alice.post(path + "/connect")
    assert reply.status_code == 200, reply.text
    url = urlsplit(reply.json()["url"])
    query = {key: value[0] for key, value in parse_qs(url.query).items()}
    assert query["client_id"] == "connected-app"
    assert query["redirect_uri"] == ORIGIN + callback
    assert query["code_challenge_method"] == "S256"
    if name in {"calendar", "tasks", "drive", "docs", "sheets", "contacts"}:
        with pytest.raises(ValueError, match="requires its requested permissions"):
            server.apps.normalize(
                name,
                {
                    "access_token": "partial",
                    "refresh_token": "refresh",
                    "expires_in": 3600,
                    "scope": "https://www.googleapis.com/auth/userinfo.email",
                },
            )
    if app.scopes:
        assert query["access_type"] == "offline"
        assert set(query["scope"].split()) == app.scopes
    calls = []
    reject_refresh = False

    async def provider(request):
        fields = parse_qs(request.content.decode())
        if str(request.url) == app.token_url:
            assert fields["client_secret"] == ["app-secret"]
            grant = fields["grant_type"][0]
            calls.append(grant)
            if grant == "authorization_code":
                assert fields["code"] == ["consent-code"]
                assert fields["redirect_uri"] == [ORIGIN + callback]
                verifier = fields["code_verifier"][0].encode()
                challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier).digest())
                assert challenge.decode().rstrip("=") == query["code_challenge"]
                token = {"access_token": "initial-access", "refresh_token": "initial-refresh"}
                if app.scopes:
                    token["scope"] = " ".join(app.scopes)
                return httpx.Response(200, json={**token, "expires_in": 10})
            assert grant == "refresh_token" and fields["refresh_token"] == ["initial-refresh"]
            if reject_refresh:
                return httpx.Response(400, json={"error": "invalid_grant"})
            await asyncio.sleep(0.02)
            token = {"access_token": "refreshed-access", "expires_in": 3600}
            if name == "github":
                token["refresh_token"] = "rotated-refresh"
            return httpx.Response(200, json=token)
        if request.method == "GET" or request.url.host == "docs.googleapis.com":
            calls.append("tool")
            assert request.headers["Authorization"] == "Bearer refreshed-access"
            assert request.url.path == tool_path
            return httpx.Response(200, json=[] if name == "github" else {})
        calls.append("revoke")
        if name == "github":
            assert (
                request.method == "DELETE"
                and request.url.path == "/applications/connected-app/grant"
            )
            assert json.loads(request.content) == {"access_token": "refreshed-access"}
        else:
            assert str(request.url) == "https://oauth2.googleapis.com/revoke"
            assert fields["token"] == ["initial-refresh"]
        return httpx.Response(204)

    transport = httpx.MockTransport(provider)
    # Replace the HTTP client before the first connection and the account runners' transports.
    await server.apps.http.aclose()
    server.apps.http = httpx.AsyncClient(transport=transport)
    for runtime in server.runtimes.services.values():
        runner = runtime.team.plugins
        await runner.http.aclose()
        runner.http = httpx.AsyncClient(transport=transport)

    params = {"state": query["state"], "code": "consent-code"}
    assert (await bob.get(callback, params=params)).status_code == 400
    result = await alice.get(callback, params=params)
    assert result.status_code == 302, result.text
    assert result.headers["location"] == "/?apps=1"
    assert (await alice.get(callback, params=params)).status_code == 400
    alice_account = next(
        item for item in server.accounts.all_accounts() if item["email"].startswith("alice")
    )
    runtime = server.runtimes.get(alice_account)
    bot = runtime.store.create_bot("Apps worker", "Tests app access")
    grant = path + f"/bots/{bot.id}"
    assert (await bob.put(grant, json={"enabled": True})).status_code == 404
    assert (await alice.put(grant, json={"enabled": True})).status_code == 200
    alice_apps = (await alice.get("/api/apps")).json()
    bob_apps = (await bob.get("/api/apps")).json()
    assert next(item for item in alice_apps if item["id"] == name)["connected"]
    assert not next(item for item in bob_apps if item["id"] == name)["connected"]
    with pytest.raises(ValueError, match="not enabled"):
        await runtime.team.call_plugin(tool, tool_arguments, runtime.store.bots()[0])

    account = alice_account["id"]
    tokens = await asyncio.gather(*(server.apps.token(account, name) for _ in range(3)))
    assert tokens == ["refreshed-access"] * 3
    token = server.apps.credential(account, name)
    assert token["refresh"] == ("rotated-refresh" if name == "github" else "initial-refresh")
    if name == "gmail":
        legacy_reply = {
            "access_token": "legacy",
            "refresh_token": "legacy-refresh",
            "expires_in": 3600,
            "scope": " ".join(LEGACY_GMAIL_SCOPES),
        }
        assert server.apps.normalize(name, legacy_reply, token)["access"] == "legacy"
        with pytest.raises(ValueError, match="label permissions"):
            server.apps.normalize(name, legacy_reply)
    assert calls == ["authorization_code", "refresh_token"]
    data = server.accounts.db.execute("SELECT sealed FROM credentials WHERE kind='app'").fetchone()[
        0
    ]
    assert b"refreshed-access" not in data and b"refresh" not in data
    await runtime.team.call_plugin(tool, tool_arguments, bot)

    next_flow = (await alice.post(path + "/connect")).json()["url"]
    next_state = parse_qs(urlsplit(next_flow).query)["state"][0]
    if name == "gmail":
        reject_refresh = True
        server.apps.save(account, name, {**token, "expires": 0})
        with pytest.raises(ValueError, match="Reconnect gmail"):
            await server.apps.token(account, name)
        assert not server.apps.connected(account, name)
        assert runtime.store.plugins_for(bot.id) == []
        assert calls[-1] == "refresh_token"
        server.apps.save(account, name, token)

    if name == "gmail":
        monkeypatch.delenv(app.secret)
        apps = (await alice.get("/api/apps")).json()
        gmail = next(item for item in apps if item["id"] == "gmail")
        assert gmail["saved"] and not gmail["connected"]
        assert (await alice.delete(path)).json() == {"connected": False, "providerRevoked": False}
        monkeypatch.setenv(app.secret, "app-secret")
        server.apps.save(account, name, token)
        next_flow = (await alice.post(path + "/connect")).json()["url"]
        next_state = parse_qs(urlsplit(next_flow).query)["state"][0]
    result = await alice.delete(path)
    assert result.json() == {"connected": False, "providerRevoked": True}
    assert calls[-1] == "revoke"
    assert (await alice.get(callback, params={**params, "state": next_state})).status_code == 400
    assert server.apps.credential(account, name) is None
    assert runtime.store.plugins_for(bot.id) == []


async def test_web_runs_and_streams_are_owned_by_the_signed_in_account(accounts):
    _, browser, begin, _ = accounts
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    result = await alice.post("/api/runs", json={"text": "slow"})
    assert result.status_code == 202
    path = result.headers["location"]
    assert (await bob.get("/api/runs")).json() == []
    assert (await bob.get(path)).status_code == 404
    assert (await bob.get(path + "/events")).status_code == 404
    assert (await bob.post(path + "/stop")).status_code == 404
    assert (await bob.get("/api/approvals", params={"run": result.json()["id"]})).status_code == 404
    assert (await alice.get(path)).json()["id"] == result.json()["id"]
    assert (await alice.post(path + "/stop")).json()["status"] == "cancelled"
