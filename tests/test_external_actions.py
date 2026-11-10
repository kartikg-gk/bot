import json

import httpx
import pytest

from delta_gateway.external_actions import BLAND_CALLS, TINY_API, TINY_FETCH, TINY_SEARCH
from delta_gateway.plugins import UNTRUSTED, PluginRunner
from tests.test_web_accounts import accounts as accounts
from tests.test_web_accounts import login

CALL = "call-test-1"


def data(output):
    return json.loads(output.removeprefix(UNTRUSTED))


async def test_tinyfish_search_fetch_and_browser_run_contract():
    calls = []

    async def token(name):
        assert name == "tinyfish"
        return "test-tinyfish-key"

    def provider(req):
        assert req.headers["x-api-key"] == "test-tinyfish-key"
        assert "authorization" not in req.headers
        calls.append(req)
        url = str(req.url.copy_with(query=None)).rstrip("/")
        if url == TINY_SEARCH:
            assert req.method == "GET" and dict(req.url.params) == {
                "query": "documentation",
                "page": "2",
            }
            return httpx.Response(200, json={"results": [{"title": "Page", "snippet": "Text"}]})
        if url == TINY_FETCH:
            assert req.method == "POST"
            assert json.loads(req.content) == {
                "urls": ["https://example.test/"],
                "format": "markdown",
            }
            return httpx.Response(200, json={"results": [{"text": "x" * 2201, "title": "Page"}]})
        if url.endswith("/run-async"):
            assert req.method == "POST"
            assert json.loads(req.content) == {
                "url": "https://example.test/",
                "goal": "Find the page heading",
                "agent_config": {"max_duration_seconds": 90},
            }
            return httpx.Response(200, json={"run_id": "run_test", "status": "PENDING"})
        if url == TINY_API + "/runs/run_test":
            return httpx.Response(
                200,
                json={
                    "run_id": "run_test",
                    "status": "RUNNING",
                    "streamingUrl": "secret-preview-url",
                },
            )
        assert req.method == "POST" and url == TINY_API + "/runs/run_test/cancel"
        return httpx.Response(200, json={"status": "CANCELLED"})

    runner = PluginRunner(transport=httpx.MockTransport(provider), access_token=token)
    try:
        searched = data(await runner.run("tinyfish_search", {"query": "documentation", "page": 2}))
        assert searched["results"][0]["title"] == "Page"
        fetched = data(await runner.run("tinyfish_fetch", {"url": "https://example.test/"}))
        assert fetched["nextOffset"] == 2200
        for target in ("http://127.0.0.1/admin", "http://127.1/admin", "http://0x7f.0.0.1/admin"):
            with pytest.raises(ValueError, match="public"):
                await runner.run("tinyfish_fetch", {"url": target})
        args = {
            "url": "https://example.test/",
            "goal": "Find the page heading",
            "duration_seconds": 90,
        }
        with pytest.raises(ValueError, match="Duration"):
            await runner.run("tinyfish_agent", {**args, "duration_seconds": 301})
        run = data(await runner.run("tinyfish_agent", args))
        assert run["status"] == "PENDING"
        key = {"run_id": run["run_id"]}
        status = data(await runner.run("tinyfish_get_run", key))
        assert status == {"run_id": "run_test", "status": "RUNNING"}
        assert data(await runner.run("tinyfish_cancel_run", key))["status"] == "CANCELLED"
        assert len(calls) == 5
    finally:
        await runner.close()


async def test_bland_key_grants_call_tool_and_account_result_view(accounts):
    server, browser, begin, _ = accounts
    bland_key = "test-bland-key"
    tiny_key = "test-tinyfish-key"
    calls = []

    def provider(req):
        url = str(req.url.copy_with(query=None))
        calls.append((req.method, url))
        if url == TINY_API + "/runs":
            assert req.headers["x-api-key"] == tiny_key and req.url.params["limit"] == "1"
            return httpx.Response(200, json={"data": []})
        assert req.headers["authorization"] == bland_key
        if url == BLAND_CALLS and req.method == "GET":
            assert req.url.params["limit"] == "1"
            return httpx.Response(200, json={"calls": []})
        if url == BLAND_CALLS and req.method == "POST":
            assert json.loads(req.content) == {
                "phone_number": "+12025550123",
                "task": "At the beginning of the call, identify yourself as an AI assistant. "
                "Ask about hours.",
                "max_duration": 2,
                "record": False,
            }
            return httpx.Response(200, json={"status": "success", "call_id": CALL})
        assert req.method == "GET" and url == BLAND_CALLS + "/" + CALL
        return httpx.Response(
            200,
            json={
                "call_id": CALL,
                "status": "completed",
                "summary": "Open at 9",
                "concatenated_transcript": "x" * 2201,
                "recording_url": "hidden",
            },
        )

    transport = httpx.MockTransport(provider)
    server.apps.transport = transport
    await server.apps.http.aclose()
    server.apps.http = httpx.AsyncClient(transport=transport)
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    bland_route = "/api/apps/bland"
    assert (await alice.put("/api/apps/tinyfish/key", json={"key": tiny_key})).status_code == 200
    assert (await alice.put(bland_route + "/key", json={"key": bland_key})).status_code == 200
    lookup = bland_route + "/calls/" + CALL
    assert (await bob.get(lookup)).status_code == 409
    assert (await alice.get(lookup + "?offset=-1")).status_code == 400
    principal = server.google.identity(alice.cookies["delta_session"])
    runtime = server.runtimes.get(principal)
    bot = runtime.store.create_bot("Phone assistant", "Call a number only with approval")
    with pytest.raises(ValueError, match="not enabled"):
        await runtime.team.call_plugin("bland_get_call", {"call_id": CALL}, bot)
    assert (
        await alice.put(bland_route + f"/bots/{bot.id}", json={"enabled": True})
    ).status_code == 200
    args = {"phone_number": "+12025550123", "task": "Ask about hours.", "duration_minutes": 2}
    with pytest.raises(ValueError, match="E.164"):
        await runtime.team.call_plugin("bland_call", {**args, "phone_number": "911"}, bot)
    result = data(await runtime.team.call_plugin("bland_call", args, bot))
    assert result["call_id"] == CALL
    tool_result = data(await runtime.team.call_plugin("bland_get_call", {"call_id": CALL}, bot))
    assert "recording_url" not in tool_result and tool_result["nextOffset"] == 2200
    response = await alice.get(lookup + "?offset=2200")
    assert response.status_code == 200
    assert response.json()["transcript"] == "x" and response.json()["nextOffset"] is None
    assert response.headers["cache-control"] == "no-store"
    assert (await alice.delete(bland_route)).status_code == 200
    assert (await alice.get(lookup)).status_code == 409
    assert calls.count(("POST", BLAND_CALLS)) == 1
