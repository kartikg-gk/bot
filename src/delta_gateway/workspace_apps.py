"""Account-scoped Notion and Linear tool adapters."""

from delta_gateway.google_api import request
from delta_gateway.plugin_schema import TEXT, tool

NOTION_VERSION = "2026-03-11"
NOTION = "https://api.notion.com/v1"
LINEAR = "https://api.linear.app/graphql"
CURSOR = {"cursor": TEXT}
NOTION_ID = {"page_id": TEXT}
ISSUE_ID = {"issue_id": TEXT}
ISSUE_INPUT = {
    "title": TEXT,
    "description": {"type": "string"},
    "state_id": TEXT,
    "assignee_id": {"type": ["string", "null"]},
    "priority": {"type": "integer", "minimum": 0, "maximum": 4},
    "project_id": {"type": ["string", "null"]},
}

NOTION_TOOLS = [
    tool(
        "notion_search",
        "Search connected Notion pages and data sources by title.",
        {"query": TEXT, **CURSOR},
        [],
    ),
    tool("notion_get_page", "Read the metadata and properties of a Notion page.", NOTION_ID),
    tool(
        "notion_list_blocks",
        "List child blocks of a page or block.",
        {"block_id": TEXT, **CURSOR},
        ["block_id"],
    ),
    tool(
        "notion_create_page",
        "Create a Notion page under a shared page or in a shared data source.",
        {
            "parent_id": TEXT,
            "parent_type": {"type": "string", "enum": ["page", "data_source"]},
            "title": TEXT,
            "text": TEXT,
        },
        ["parent_id", "title"],
        write=True,
    ),
    tool(
        "notion_append_text",
        "Append a plain-text paragraph to a Notion page or block.",
        {"block_id": TEXT, "text": TEXT},
        write=True,
    ),
]
LINEAR_TOOLS = [
    tool(
        "linear_list_teams", "List accessible Linear teams and their workflow states.", CURSOR, []
    ),
    tool(
        "linear_list_issues",
        "List recent Linear issues, optionally scoped to one team.",
        {"team_id": TEXT, **CURSOR},
        [],
    ),
    tool(
        "linear_search_issues",
        "Search Linear issues by text with an optional team filter.",
        {"query": TEXT, "team_id": TEXT, **CURSOR},
        ["query"],
    ),
    tool("linear_get_issue", "Read a Linear issue by UUID or issue identifier.", ISSUE_ID),
    tool(
        "linear_create_issue",
        "Create a Linear issue in a selected team.",
        {"team_id": TEXT, **ISSUE_INPUT},
        ["team_id", "title"],
        write=True,
    ),
    tool(
        "linear_update_issue",
        "Update selected fields of a Linear issue.",
        {**ISSUE_ID, **ISSUE_INPUT},
        ["issue_id"],
        write=True,
    ),
    tool(
        "linear_add_comment",
        "Post a Markdown comment on a Linear issue.",
        {**ISSUE_ID, "body": TEXT},
        write=True,
    ),
]
REVIEW_TOOLS = frozenset(
    spec["name"] for spec in (*NOTION_TOOLS, *LINEAR_TOOLS) if spec.get("approval") is not False
)


def notion_id(value):
    from uuid import UUID

    try:
        return str(UUID(value))
    except ValueError:
        raise ValueError("Use the Notion resource ID returned by search or block listing") from None


def rich_text(value):
    return [
        {"type": "text", "text": {"content": value[i : i + 1000]}}
        for i in range(0, len(value), 1000)
    ]


def paragraph(text):
    return {"type": "paragraph", "paragraph": {"rich_text": rich_text(text)}}


def plain(parts):
    return "".join(
        item.get("plain_text", item.get("text", {}).get("content", "")) for item in parts
    )


def notion_summary(item):
    title = item.get("title")
    if not isinstance(title, list):
        title = next(
            (
                field["title"]
                for field in item.get("properties", {}).values()
                if field.get("type") == "title"
            ),
            [],
        )
    return {
        "id": item["id"],
        "type": item.get("object"),
        "title": plain(title)[:500],
        "url": item.get("url"),
        "parent": item.get("parent"),
    }


def block_summary(item):
    kind = item["type"]
    detail = item.get(kind, {})
    output = {"id": item["id"], "type": kind, "hasChildren": item.get("has_children", False)}
    for field in ("rich_text", "caption"):
        if field in detail:
            text = plain(detail[field])
            output[field] = text[:1200]
            if len(text) > 1200:
                output["textTruncated"] = True
    if "cells" in detail:
        output["cells"] = [plain(cell)[:250] for cell in detail["cells"][:20]]
    for field in ("title", "url", "checked", "language", "expression"):
        if field in detail:
            output[field] = detail[field]
    return output


async def notion(http, headers, name, args):
    headers = {**headers, "Notion-Version": NOTION_VERSION}
    params = {"page_size": 10}
    if args.get("cursor"):
        params["start_cursor"] = args["cursor"]
    if name == "notion_search":
        if args.get("query"):
            params["query"] = args["query"]
        page = await request(http, headers, "POST", NOTION + "/search", json=params)
        return {
            "items": [notion_summary(item) for item in page["results"]],
            "nextCursor": page.get("next_cursor"),
        }
    if name == "notion_get_page":
        return await request(http, headers, "GET", NOTION + "/pages/" + notion_id(args["page_id"]))
    if name == "notion_create_page":
        parent_id = notion_id(args["parent_id"])
        parent_type = args.get("parent_type", "page")
        title_property = "title"
        if parent_type == "data_source":
            source = await request(http, headers, "GET", NOTION + "/data_sources/" + parent_id)
            title_property = next(
                (
                    key
                    for key, value in source["properties"].items()
                    if value.get("type") == "title"
                ),
                None,
            )
            if title_property is None:
                raise ValueError("The selected Notion data source has no title property")
        body = {
            "parent": {"type": parent_type + "_id", parent_type + "_id": parent_id},
            "properties": {title_property: {"type": "title", "title": rich_text(args["title"])}},
        }
        if args.get("text"):
            body["children"] = [paragraph(args["text"])]
        return notion_summary(await request(http, headers, "POST", NOTION + "/pages", json=body))
    url = NOTION + "/blocks/" + notion_id(args["block_id"]) + "/children"
    if name == "notion_list_blocks":
        page = await request(http, headers, "GET", url, params=params)
        return {
            "items": [block_summary(item) for item in page["results"]],
            "nextCursor": page.get("next_cursor"),
        }
    await request(http, headers, "PATCH", url, json={"children": [paragraph(args["text"])]})
    return {"appended": True, "blockId": args["block_id"]}


ISSUE_FIELDS = "id identifier title url priority state { id name } team { id key }"
PAGE_INFO = "pageInfo { hasNextPage endCursor }"
ISSUE_INPUT_NAMES = {
    "title": "title",
    "description": "description",
    "state_id": "stateId",
    "assignee_id": "assigneeId",
    "priority": "priority",
    "project_id": "projectId",
}
LINEAR_QUERIES = {
    "linear_list_teams": (
        "teams",
        f"""
      query Teams($after: String) {{
        teams(first: 10, after: $after) {{
          nodes {{ id key name states(first: 15) {{ nodes {{ id name type }} {PAGE_INFO} }} }}
          {PAGE_INFO}
        }}
      }}""",
    ),
    "linear_list_issues": (
        "issues",
        f"""
      query Issues($after: String, $filter: IssueFilter) {{
        issues(first: 20, after: $after, filter: $filter, orderBy: updatedAt) {{
          nodes {{ {ISSUE_FIELDS} }} {PAGE_INFO}
        }}
      }}""",
    ),
    "linear_search_issues": (
        "searchIssues",
        f"""
      query SearchIssues($term: String!, $after: String, $filter: IssueFilter) {{
        searchIssues(term: $term, first: 20, after: $after, filter: $filter) {{
          nodes {{ {ISSUE_FIELDS} }} {PAGE_INFO}
        }}
      }}""",
    ),
    "linear_get_issue": (
        "issue",
        f"""
      query Issue($id: String!) {{
        issue(id: $id) {{
          {ISSUE_FIELDS} description dueDate assignee {{ id name }} project {{ id name }}
          comments(first: 10) {{ nodes {{ id body user {{ name }} createdAt }} {PAGE_INFO} }}
        }}
      }}""",
    ),
}
LINEAR_MUTATIONS = {
    "linear_create_issue": (
        "issueCreate",
        "issue",
        f"""
      mutation Create($input: IssueCreateInput!) {{
        issueCreate(input: $input) {{ success issue {{ {ISSUE_FIELDS} }} }}
      }}""",
    ),
    "linear_update_issue": (
        "issueUpdate",
        "issue",
        f"""
      mutation Update($id: String!, $input: IssueUpdateInput!) {{
        issueUpdate(id: $id, input: $input) {{ success issue {{ {ISSUE_FIELDS} }} }}
      }}""",
    ),
    "linear_add_comment": (
        "commentCreate",
        "comment",
        """
      mutation Comment($input: CommentCreateInput!) {
        commentCreate(input: $input) { success comment { id url } }
      }""",
    ),
}


async def linear(http, headers, name, args):
    if name in LINEAR_QUERIES:
        field, query = LINEAR_QUERIES[name]
        variables = {"after": args.get("cursor")}
        if name == "linear_get_issue":
            variables = {"id": args["issue_id"]}
        if name in {"linear_list_issues", "linear_search_issues"}:
            team = args.get("team_id")
            variables["filter"] = {"team": {"id": {"eq": team}}} if team else None
        if name == "linear_search_issues":
            variables["term"] = args["query"]
        response = await linear_query(http, headers, field, query, variables)
        return response
    field, node, query = LINEAR_MUTATIONS[name]
    if name == "linear_add_comment":
        payload = {"issueId": args["issue_id"], "body": args["body"]}
    else:
        payload = {target: args[key] for key, target in ISSUE_INPUT_NAMES.items() if key in args}
        if not payload:
            raise ValueError("Provide at least one issue field")
    variables = {"input": payload}
    if name == "linear_create_issue":
        payload["teamId"] = args["team_id"]
    if name == "linear_update_issue":
        variables["id"] = args["issue_id"]
    result = await linear_query(http, headers, field, query, variables)
    if result.get("success") is not True or not isinstance(result.get(node), dict):
        raise ValueError("Linear did not confirm the write. Check the issue before retrying.")
    return result[node]


async def linear_query(http, headers, field, query, variables):
    result = await request(
        http, headers, "POST", LINEAR, json={"query": query, "variables": variables}
    )
    if result.get("errors") or not isinstance((result.get("data") or {}).get(field), dict):
        raise ValueError(
            "Linear did not return a complete result. Check the target before retrying."
        )
    return result["data"][field]
