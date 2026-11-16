import json

import httpx
import pytest

from delta_gateway.plugins import CREDENTIAL_VARIABLES, PluginRunner
from delta_gateway.web_research import EXA_KEY, SCRAPER_KEY, TAVILY_KEY


@pytest.mark.parametrize("failure", ["empty", "unavailable", "bad-json", "no-key", "success"])
async def test_search_falls_back_without_forwarding_primary_key(monkeypatch, failure):
    if failure == "no-key":
        monkeypatch.delenv(TAVILY_KEY, raising=False)
    else:
        monkeypatch.setenv(TAVILY_KEY, "tavily-test-key")
    monkeypatch.setenv(EXA_KEY, "exa-test-key")
    hosts = []

    def respond(request):
        hosts.append(request.url.host)
        payload = json.loads(request.content)
        assert request.url.path == "/search" and payload["query"] == "search term"
        if request.url.host == "api.tavily.com":
            assert request.headers["authorization"] == "Bearer tavily-test-key"
            assert "x-api-key" not in request.headers
            if failure == "unavailable":
                return httpx.Response(429)
            if failure == "bad-json":
                return httpx.Response(200, text="invalid")
            items = (
                [{"url": "https://example.org", "content": "Snippet"}]
                if failure == "success"
                else []
            )
            return httpx.Response(200, json={"results": items})
        assert request.headers["x-api-key"] == "exa-test-key"
        assert "authorization" not in request.headers
        assert payload["contents"] == {"highlights": True}
        return httpx.Response(
            200, json={"results": [{"url": "https://example.org", "highlights": ["Snippet"]}]}
        )

    runner = PluginRunner(transport=httpx.MockTransport(respond))
    try:
        assert runner.configured("web_search")
        result = await runner.run("web_search", {"query": "search term"})
    finally:
        await runner.http.aclose()
    output = json.loads(result.split("\n", 1)[1])
    assert output["provider"] == ("Tavily" if failure == "success" else "Exa")
    assert output["results"][0]["snippet"] == "Snippet"
    assert hosts == {"success": ["api.tavily.com"], "no-key": ["api.exa.ai"]}.get(
        failure, ["api.tavily.com", "api.exa.ai"]
    )
    assert "test-key" not in result


async def test_scrape_validates_url_and_caps_page_response(monkeypatch):
    monkeypatch.setenv(SCRAPER_KEY, "scraper-test-key")
    calls = []

    def respond(request):
        calls.append(request)
        assert request.url.host == "api.scraperapi.com"
        assert request.headers["x-sapi-api_key"] == "scraper-test-key"
        assert dict(request.url.params) == {
            "url": "https://example.org",
            "output_format": "markdown",
            "follow_redirect": "false",
        }
        return httpx.Response(200, text="a" * 2203 + "abc")

    runner = PluginRunner(transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(ValueError, match="public"):
            await runner.run("web_scrape", {"url": "http://127.1"})
        assert not calls
        response = await runner.run("web_scrape", {"url": "https://example.org", "offset": 2200})
        assert json.loads(response.split("\n", 1)[1]) == {
            "url": "https://example.org",
            "text": "aaaabc",
            "nextOffset": None,
        }
    finally:
        await runner.http.aclose()
    assert {TAVILY_KEY, EXA_KEY, SCRAPER_KEY} <= CREDENTIAL_VARIABLES
