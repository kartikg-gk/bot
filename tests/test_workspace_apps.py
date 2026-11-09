import json

import httpx
import pytest

from delta_gateway.plugins import UNTRUSTED, PluginRunner

PAGE = "abcdef01-2345-6789-abcd-ef0123456789"
SOURCE = "abcdef01-2345-6789-abcd-ef0123456788"


def value(output):
    assert output.startswith(UNTRUSTED)
    return json.loads(output.removeprefix(UNTRUSTED))


def runner(owner, handler):
    async def token(name):
        assert name == owner
        return "tool-token"

    return PluginRunner(transport=httpx.MockTransport(handler), access_token=token)


async def test_notion_search_read_page_blocks_create_and_append():
    def provider(req):
        assert req.url.host == "api.notion.com"
        assert req.headers["authorization"] == "Bearer tool-token"
        assert req.headers["notion-version"] == "2026-03-11"
        path = req.url.path
        if path == "/v1/search":
            assert json.loads(req.content) == {
                "query": "Notes",
                "start_cursor": "page2",
                "page_size": 10,
            }
            return httpx.Response(
                200,
                json={
                    "results": [
                        {"id": SOURCE, "object": "data_source", "title": [{"plain_text": "Notes"}]}
                    ],
                    "next_cursor": "page3",
                },
            )
        if path == f"/v1/pages/{PAGE}":
            assert req.method == "GET"
            return httpx.Response(
                200, json={"id": PAGE, "properties": {"Status": {"type": "status"}}}
            )
        if path == f"/v1/data_sources/{SOURCE}":
            return httpx.Response(200, json={"properties": {"Task name": {"type": "title"}}})
        if path == "/v1/pages":
            payload = json.loads(req.content)
            assert payload["parent"] == {"type": "data_source_id", "data_source_id": SOURCE}
            assert payload["properties"]["Task name"]["title"][0]["text"]["content"] == "New note"
            assert payload["children"][0]["paragraph"]["rich_text"][0]["text"]["content"] == "Hello"
            return httpx.Response(200, json={"id": PAGE, "object": "page"})
        assert path == f"/v1/blocks/{PAGE}/children"
        if req.method == "PATCH":
            payload = json.loads(req.content)["children"][0]["paragraph"]["rich_text"]
            assert len(payload) == 2
            assert "".join(part["text"]["content"] for part in payload) == "x" * 1001
            return httpx.Response(200, json={"results": []})
        assert req.url.params["start_cursor"] == "b2"
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "table-row",
                        "type": "table_row",
                        "has_children": False,
                        "table_row": {"cells": [[{"plain_text": "Content"}]]},
                    }
                ],
            },
        )

    client = runner("notion", provider)
    try:
        search = value(await client.run("notion_search", {"query": "Notes", "cursor": "page2"}))
        assert search["items"][0]["title"] == "Notes" and search["nextCursor"] == "page3"
        page = value(await client.run("notion_get_page", {"page_id": PAGE}))
        assert page["properties"]["Status"]["type"] == "status"
        blocks = value(await client.run("notion_list_blocks", {"block_id": PAGE, "cursor": "b2"}))
        assert blocks["items"][0]["cells"] == ["Content"]
        created = value(
            await client.run(
                "notion_create_page",
                {
                    "parent_id": SOURCE,
                    "parent_type": "data_source",
                    "title": "New note",
                    "text": "Hello",
                },
            )
        )
        assert created["id"] == PAGE
        assert value(
            await client.run(
                "notion_append_text",
                {
                    "block_id": PAGE,
                    "text": "x" * 1001,
                },
            )
        )["appended"]
    finally:
        await client.close()


async def test_linear_operations_use_variables_and_reject_partial_graphql_results():
    issue = {"id": "issue-id", "identifier": "PROJ-1", "title": "Title"}
    partial = False

    def provider(req):
        assert str(req.url) == "https://api.linear.app/graphql"
        assert req.headers["authorization"] == "Bearer tool-token"
        payload = json.loads(req.content)
        q, variables = payload["query"], payload["variables"]
        if "query Teams" in q:
            assert variables == {"after": "t2"}
            data = {"teams": {"nodes": [{"id": "team"}]}}
        elif "query Issues" in q:
            assert variables["filter"] == {"team": {"id": {"eq": "team"}}}
            data = {
                "issues": {"nodes": [issue], "pageInfo": {"hasNextPage": True, "endCursor": "i2"}}
            }
        elif "query SearchIssues" in q:
            assert variables["term"] == 'quoted " search'
            assert 'quoted " search' not in q
            data = {"searchIssues": {"nodes": [issue]}}
        elif "query Issue(" in q:
            assert variables == {"id": "PROJ-1"}
            data = {"issue": issue}
        elif "mutation Create" in q:
            assert variables == {"input": {"title": "Title", "teamId": "team"}}
            data = {"issueCreate": {"success": True, "issue": issue}}
        elif "mutation Update" in q:
            assert variables == {"id": "PROJ-1", "input": {"stateId": "done", "assigneeId": None}}
            data = {"issueUpdate": {"success": True, "issue": issue}}
        else:
            assert "mutation Comment" in q
            assert variables == {"input": {"issueId": "PROJ-1", "body": "Reviewed"}}
            data = {"commentCreate": {"success": True, "comment": {"id": "comment"}}}
        return httpx.Response(
            200, json={"data": data, **({"errors": [{"message": "error"}]} if partial else {})}
        )

    client = runner("linear", provider)
    try:
        assert (
            value(await client.run("linear_list_teams", {"cursor": "t2"}))["nodes"][0]["id"]
            == "team"
        )
        listed = value(await client.run("linear_list_issues", {"team_id": "team"}))
        assert listed["pageInfo"]["endCursor"] == "i2"
        assert value(await client.run("linear_search_issues", {"query": 'quoted " search'}))[
            "nodes"
        ] == [issue]
        assert value(await client.run("linear_get_issue", {"issue_id": "PROJ-1"})) == issue
        assert (
            value(await client.run("linear_create_issue", {"team_id": "team", "title": "Title"}))
            == issue
        )
        assert (
            value(
                await client.run(
                    "linear_update_issue",
                    {
                        "issue_id": "PROJ-1",
                        "state_id": "done",
                        "assignee_id": None,
                    },
                )
            )
            == issue
        )
        comment = {"issue_id": "PROJ-1", "body": "Reviewed"}
        assert value(await client.run("linear_add_comment", comment))["id"] == "comment"
        partial = True
        with pytest.raises(ValueError, match="complete result"):
            await client.run("linear_add_comment", comment)
    finally:
        await client.close()
