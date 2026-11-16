"""Gateway-side search and public page reading."""

import os

import httpx

from delta_gateway.external_actions import public_url
from delta_gateway.google_api import request
from delta_gateway.plugin_schema import OFFSET, TEXT, tool

TAVILY_KEY = "DELTA_GATEWAY_TAVILY_API_KEY"
EXA_KEY = "DELTA_GATEWAY_EXA_API_KEY"
SCRAPER_KEY = "DELTA_GATEWAY_SCRAPERAPI_API_KEY"
SEARCH_KEYS = (TAVILY_KEY, EXA_KEY)

SEARCH_TOOLS = [
    tool("web_search", "Search the public web for links and excerpts.", {"query": TEXT}, ["query"])
]
SCRAPE_TOOLS = [
    tool(
        "web_scrape",
        "Read a public page as Markdown. Each request can use page extraction credits.",
        {"url": TEXT, "offset": OFFSET},
        ["url"],
        write=True,
    )
]


async def search(http, args):
    query = args["query"].strip()
    if not query or len(query) > 500:
        raise ValueError("Enter a search query from 1 to 500 characters")
    tavily, exa = (os.environ.get(name, "").strip() for name in SEARCH_KEYS)
    if not tavily and not exa:
        raise ValueError("Configure a search provider on the gateway")
    had_success = False
    for provider, url, headers, body in (
        (
            "Tavily",
            "https://api.tavily.com/search",
            {"Authorization": "Bearer " + tavily},
            {
                "query": query,
                "search_depth": "basic",
                "max_results": 5,
                "include_published_date": True,
            },
        ),
        (
            "Exa",
            "https://api.exa.ai/search",
            {"x-api-key": exa},
            {"query": query, "numResults": 5, "contents": {"highlights": True}},
        ),
    ):
        if not (tavily if provider == "Tavily" else exa):
            continue
        try:
            data = await request(http, headers, "POST", url, json=body, timeout=12)
        except (httpx.HTTPError, ValueError):
            continue
        if not isinstance(data, dict) or not isinstance(data.get("results"), list):
            continue
        had_success = True
        results = []
        for item in data["results"][:5]:
            if not isinstance(item, dict):
                continue
            try:
                link = public_url(item.get("url", ""))
            except (TypeError, ValueError):
                continue
            highlights = item.get("highlights")
            snippet = item.get("content") or item.get("text")
            if not snippet and isinstance(highlights, list):
                snippet = next((line for line in highlights if isinstance(line, str)), "")
            results.append(
                {
                    "url": link,
                    "title": str(item.get("title") or "")[:200],
                    "snippet": str(snippet or "")[:600],
                    "published": str(item.get("published_date") or item.get("publishedDate") or "")[
                        :40
                    ],
                }
            )
        if results:
            return {"provider": provider, "results": results}
    if had_success:
        return {"results": []}
    raise ValueError("The configured web search providers were unavailable")


async def scrape(http, args):
    url = public_url(args["url"])
    key = os.environ.get(SCRAPER_KEY, "").strip()
    if not key:
        raise ValueError("Configure the page extraction provider on the gateway")
    offset = args.get("offset", 0)
    if not 0 <= offset < 200_000:
        raise ValueError("Page offset must be less than 200000")
    end = min(offset + 2200, 200_000)
    try:
        async with http.stream(
            "GET",
            "https://api.scraperapi.com/",
            headers={"x-sapi-api_key": key},
            params={"url": url, "output_format": "markdown", "follow_redirect": "false"},
            follow_redirects=False,
            timeout=25,
        ) as reply:
            reply.raise_for_status()
            parts, size = [], 0
            async for chunk in reply.aiter_text():
                parts.append(chunk[: end + 1 - size])
                size += len(parts[-1])
                if size > end:
                    break
    except httpx.HTTPError:
        raise ValueError("The page extraction provider could not read that page") from None
    text = "".join(parts)
    return {
        "url": url,
        "text": text[offset:end],
        "nextOffset": end if end < 200_000 and len(text) > end else None,
    }
