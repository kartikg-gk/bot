"""Read-only integration with Wispr Flow's hosted MCP endpoint."""

from delta_gateway.hosted_mcp import find_tool, hosted_session, result_data, run_hosted
from delta_gateway.plugin_schema import TEXT, tool

WISPR_MCP_URL = "https://api.wisprflow.ai/connect/mcp"
WISPR_ISSUER = "https://mcp-auth.wisprflow.com"
WISPR_AUTH_URL = WISPR_ISSUER + "/oauth2/authorize"
WISPR_TOKEN_URL = WISPR_ISSUER + "/oauth2/token"
WISPR_REGISTRATION_URL = WISPR_ISSUER + "/oauth2/register"
WISPR_RESOURCE_METADATA = (
    "https://api.wisprflow.ai/.well-known/oauth-protected-resource/connect/mcp"
)
WISPR_SCOPES = frozenset({"openid", "offline_access"})

WISPR_TOOLS = [
    tool(
        "wispr_list_tools",
        "List Wispr Flow's read-only tools. Pass a tool name to see its input schema.",
        {"cursor": TEXT, "name": TEXT},
        [],
    ),
    tool(
        "wispr_call_tool",
        "Call a discovered read-only Wispr Flow tool for notes, meetings, or calendar data.",
        {"name": TEXT, "arguments": {"type": "object"}},
        ["name"],
    ),
]


async def register_client(http, callback):
    resource = await http.get(WISPR_RESOURCE_METADATA)
    resource.raise_for_status()
    document = resource.json()
    if (
        not isinstance(document, dict)
        or document.get("resource") != WISPR_MCP_URL
        or WISPR_ISSUER not in document.get("authorization_servers", [])
        or not WISPR_SCOPES <= set(document.get("scopes_supported", []))
    ):
        raise ValueError("Wispr Flow returned unexpected resource metadata")
    authorization = await http.get(WISPR_ISSUER + "/.well-known/oauth-authorization-server")
    authorization.raise_for_status()
    document = authorization.json()
    expected = {
        "issuer": WISPR_ISSUER,
        "authorization_endpoint": WISPR_AUTH_URL,
        "token_endpoint": WISPR_TOKEN_URL,
        "registration_endpoint": WISPR_REGISTRATION_URL,
    }
    if (
        not isinstance(document, dict)
        or any(document.get(key) != value for key, value in expected.items())
        or "S256" not in document.get("code_challenge_methods_supported", [])
        or "none" not in document.get("token_endpoint_auth_methods_supported", [])
    ):
        raise ValueError("Wispr Flow returned unexpected authorization metadata")
    registration = await http.post(
        WISPR_REGISTRATION_URL,
        json={
            "client_name": "Delta Gateway",
            "redirect_uris": [callback],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "scope": " ".join(sorted(WISPR_SCOPES)),
        },
    )
    registration.raise_for_status()
    client = registration.json()
    if not isinstance(client, dict):
        raise ValueError("Wispr Flow returned an invalid public client registration")
    identifier = client.get("client_id")
    if (
        not isinstance(identifier, str)
        or not 1 <= len(identifier) <= 1024
        or any(char.isspace() for char in identifier)
        or client.get("token_endpoint_auth_method", "none") != "none"
        or callback not in client.get("redirect_uris", [callback])
    ):
        raise ValueError("Wispr Flow returned an invalid public client registration")
    return identifier


def readable(spec):
    # This provider's fixed endpoint is documented as read-only. Refuse conflicting tool metadata.
    hints = spec.annotations
    return hints is None or (
        hints.read_only_hint is not False and hints.destructive_hint is not True
    )


def summarize(spec):
    return {"name": spec.name, "description": (spec.description or "")[:240]}


async def bridge(client, name, args):
    if name == "wispr_list_tools" and not args.get("name"):
        page = await client.list_tools(cursor=args.get("cursor"))
        return {
            "tools": [summarize(spec) for spec in page.tools if readable(spec)],
            "nextCursor": page.next_cursor,
        }
    spec = await find_tool(client, "Wispr Flow", args["name"])
    if not readable(spec):
        raise ValueError("Wispr Flow tool is not read-only")
    if name == "wispr_list_tools":
        return {**summarize(spec), "inputSchema": spec.input_schema}
    reply = await client.session.call_tool(spec.name, args.get("arguments") or {})
    return result_data(reply, "Wispr Flow")


def wispr_session(headers, *, transport=None):
    return hosted_session(WISPR_MCP_URL, headers, transport=transport)


async def wispr(_http, headers, name, args):
    return await run_hosted("Wispr Flow", wispr_session, bridge, headers, name, args)
