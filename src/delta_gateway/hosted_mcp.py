"""Transport and bounded tool helpers for fixed hosted MCP providers."""

import asyncio
from contextlib import asynccontextmanager


async def find_tool(client, provider, name):
    cursor = None
    seen = set()
    for _ in range(20):
        page = await client.list_tools(cursor=cursor)
        for spec in page.tools:
            if spec.name == name:
                return spec
        cursor = page.next_cursor
        if not cursor:
            raise ValueError(f"{provider} did not advertise that tool")
        if cursor in seen:
            break
        seen.add(cursor)
    raise ValueError(f"{provider} returned an incomplete tool catalog")


def result_data(reply, provider, *, may_write=False):
    if reply.is_error:
        suffix = " Check its state before retrying a write." if may_write else ""
        raise ValueError(f"{provider} reported a tool error.{suffix}")
    if reply.structured_content is not None:
        return {"data": reply.structured_content}
    return {
        "text": "\n".join(block.text for block in reply.content if block.type == "text")[:18000],
        "hasOtherContent": any(block.type != "text" for block in reply.content),
    }


@asynccontextmanager
async def hosted_session(url, headers, *, transport=None):
    from httpx2 import AsyncClient, Timeout
    from mcp import Client
    from mcp.client.streamable_http import streamable_http_client

    async with AsyncClient(
        headers=headers,
        timeout=Timeout(20, read=45),
        trust_env=False,
        transport=transport,
    ) as http:
        async with Client(
            streamable_http_client(url, http_client=http, max_sse_event_size=256000),
            read_timeout_seconds=45,
            cache=None,
        ) as client:
            yield client


async def run_hosted(provider, session, bridge, headers, name, args, *, may_write=False):
    try:
        from httpx2 import HTTPError
        from mcp.shared.exceptions import MCPError
        from pydantic import ValidationError
    except ImportError:
        raise ValueError(f"Install delta-gateway[accounts] to use {provider} MCP") from None
    try:
        async with asyncio.timeout(60), session(headers) as client:
            try:
                result = await bridge(client, name, args)
            except ValidationError:
                raise
            except ValueError as exc:
                result = exc
    except (HTTPError, MCPError, ExceptionGroup, TimeoutError, ValidationError):
        suffix = " Check before retrying a write." if may_write else ""
        raise ValueError(f"{provider} MCP did not confirm the result.{suffix}") from None
    if isinstance(result, ValueError):
        raise result
    return result
