"""Account-scoped client for Todoist's hosted MCP service."""

from delta_gateway.hosted_mcp import find_tool, hosted_session, result_data, run_hosted
from delta_gateway.plugin_schema import TEXT, tool

TODOIST_MCP_URL = "https://ai.todoist.net/mcp"
TODOIST_SCOPE = "data:read_write"
# Use provider-confirmed read operations plus an affirmative live annotation.
TODOIST_READS = frozenset(
    {
        "fetch",
        "fetch-object",
        "find-activity",
        "find-comments",
        "find-completed-tasks",
        "find-filters",
        "find-labels",
        "find-project-collaborators",
        "find-projects",
        "find-reminders",
        "find-sections",
        "find-tasks",
        "find-tasks-by-date",
        "get-overview",
        "get-productivity-stats",
        "get-project-activity-stats",
        "get-project-health",
        "get-user-status",
        "get-workspace-insights",
        "list-workspaces",
        "search",
        "user-info",
        "view-attachment",
    }
)
CALL = {"name": TEXT, "arguments": {"type": "object"}}
TODOIST_TOOLS = [
    tool(
        "todoist_list_tools",
        "List Todoist tools and their access mode. Pass a tool name to see its input schema.",
        {"cursor": TEXT, "name": TEXT},
        [],
    ),
    tool(
        "todoist_read_tool",
        "Call a Todoist tool marked read by todoist_list_tools.",
        CALL,
        ["name"],
    ),
    tool(
        "todoist_write_tool",
        "Call a Todoist tool marked write by todoist_list_tools. Requires interactive approval.",
        CALL,
        ["name"],
        write=True,
    ),
]


def access_mode(spec):
    hints = spec.annotations
    read = (
        spec.name in TODOIST_READS
        and hints is not None
        and hints.read_only_hint is True
        and hints.destructive_hint is not True
    )
    return "read" if read else "write"


def summarize(spec, *, schema=False):
    result = {
        "name": spec.name,
        "mode": access_mode(spec),
        "description": (spec.description or "")[:240],
    }
    if schema:
        result["inputSchema"] = spec.input_schema
    return result


async def bridge(client, name, args):
    if name == "todoist_list_tools" and not args.get("name"):
        page = await client.list_tools(cursor=args.get("cursor"))
        return {"tools": [summarize(spec) for spec in page.tools], "nextCursor": page.next_cursor}

    spec = await find_tool(client, "Todoist", args["name"])
    if name == "todoist_list_tools":
        return summarize(spec, schema=True)
    if name != f"todoist_{access_mode(spec)}_tool":
        raise ValueError(f"Use todoist_{access_mode(spec)}_tool for this operation")
    reply = await client.session.call_tool(spec.name, args.get("arguments") or {})
    return result_data(reply, "Todoist", may_write=name == "todoist_write_tool")


def todoist_session(headers, *, transport=None):
    return hosted_session(TODOIST_MCP_URL, headers, transport=transport)


async def todoist(_http, headers, name, args):
    return await run_hosted(
        "Todoist",
        todoist_session,
        bridge,
        headers,
        name,
        args,
        may_write=name == "todoist_write_tool",
    )
