import asyncio
import json

import pytest
from delta_harness.contracts.transcript import CallBlock, ModelEntry
from delta_harness.provider.wire import StreamCloseEvent

from delta_gateway.bots import SELF_TOOLS
from delta_gateway.computer import LocalComputer
from delta_gateway.embedded import EmbeddedClient
from delta_gateway.plugins import PLUGINS
from delta_gateway.skill_tools import TOOLS
from delta_gateway.team import CHIEF_TOOLS
from tests.test_gateway import config
from tests.test_rpc_boundary import until

APP_CALLS = {
    "tinyfish_fetch": {"url": "https://example.test/"},
    "tinyfish_agent": {
        "url": "https://example.test/",
        "goal": "Read heading",
        "duration_seconds": 90,
    },
    "tinyfish_cancel_run": {"run_id": "run_test"},
    "bland_call": {
        "phone_number": "+12025550123",
        "task": "Ask about hours",
        "duration_minutes": 2,
    },
    "todoist_write_tool": {"name": "add-tasks", "arguments": {"tasks": [{"content": "Review"}]}},
    "notion_create_page": {"parent_id": "page1", "title": "Reviewed"},
    "notion_append_text": {"block_id": "block1", "text": "Reviewed"},
    "linear_create_issue": {"team_id": "team1", "title": "Reviewed"},
    "linear_update_issue": {"issue_id": "ISS-1", "title": "Reviewed"},
    "linear_add_comment": {"issue_id": "ISS-1", "body": "Reviewed"},
    "docs_create": {"title": "Review"},
    "docs_append_text": {"document_id": "doc1", "text": "Reviewed"},
    "sheets_append_rows": {"spreadsheet_id": "sheet1", "range": "A1", "values": [["data"]]},
    "sheets_update_range": {
        "spreadsheet_id": "sheet1",
        "range": "A1",
        "values": [["=1+1"]],
        "input_mode": "USER_ENTERED",
    },
    "sheets_create": {"title": "Review"},
    "calendar_create_event": {
        "title": "Review",
        "notify": "none",
        "start": "2026-12-24",
        "end": "2026-12-25",
    },
    "calendar_update_event": {"event_id": "event1", "title": "Change", "notify": "none"},
    "calendar_respond_to_event": {"event_id": "event1", "response": "accepted", "notify": "all"},
    "calendar_delete_event": {"event_id": "event1", "notify": "all"},
    "tasks_create": {"list_id": "list1", "title": "Review"},
    "tasks_update": {"list_id": "list1", "task_id": "task1", "status": "completed"},
    "gmail_send": {"to": "test@example.test", "subject": "Reviewed", "body": "Body"},
    "gmail_modify_labels": {"id": "message1", "add": ["Label_1"]},
    "github_create_pull_request": {
        "repo": "owner/repo",
        "title": "Change",
        "head": "topic",
        "base": "main",
    },
}
REVIEW_CASES = ["create_bot", "update_own_bot", "write_skill", "write_memory", *APP_CALLS]
DEFINITIONS = (
    CHIEF_TOOLS
    + SELF_TOOLS
    + TOOLS
    + [definition for plugin in PLUGINS.values() for definition in plugin.tools]
)


class Provider:
    def __init__(self, tool="create_bot"):
        self.called = False
        self.tool = tool

    async def stream_response(self, *, model, **kwargs):
        if not self.called:
            self.called = True
            message = ModelEntry(
                model=model,
                stop_reason="toolUse",
                content=[
                    CallBlock(
                        id="create",
                        name=self.tool,
                        arguments={
                            "create_bot": {"name": "Helper", "description": "Read documents"},
                            "update_own_bot": {"instructions": "Be precise."},
                            "write_skill": {
                                "name": "review",
                                "files": {
                                    "SKILL.md": (
                                        "---\nname: review\ndescription: Review text\n---\nRead it."
                                    )
                                },
                            },
                            "write_memory": {"file": "PREFERENCES.md", "text": "Be precise."},
                            **APP_CALLS,
                        }[self.tool],
                    )
                ],
            )
            yield StreamCloseEvent(reason="toolUse", message=message)
        else:
            yield StreamCloseEvent(reason="stop", message=ModelEntry(model=model, content="Done"))


@pytest.mark.parametrize("tool", REVIEW_CASES)
@pytest.mark.parametrize("allow", [True, False])
async def test_durable_tool_requires_decision_with_auto_policy(tmp_path, allow, tool):
    provider = Provider(tool)
    async with EmbeddedClient(
        config(tmp_path, policy="auto"),
        computer=LocalComputer(tmp_path),
        provider=lambda _: (provider, "fake", "fake"),
        host_definitions=DEFINITIONS,
    ) as client:
        await client.request("set_session_name", name="Review fixture")
        await client.request("prompt", message="Create a helper")
        review = await until(client, "approval_requested")
        assert review["tool"] == tool
        if tool in APP_CALLS:
            assert json.loads(review["preview"]) == APP_CALLS[tool]
        await client.request("respond_to_approval", approvalId=review["approvalId"], allow=allow)
        seen = []
        async with asyncio.timeout(10):
            while True:
                event = await client.next_event()
                seen.append(event)
                if event["type"] == "host_tool_call":
                    assert allow
                    await client.request(
                        "respond_to_host_tool",
                        callId=event["callId"],
                        output="Created helper",
                        isError=False,
                    )
                if event["type"] == "agent_settled":
                    break
        assert any(e["type"] == "host_tool_call" for e in seen) is allow


@pytest.mark.parametrize("tool", REVIEW_CASES)
async def test_unattended_durable_call_never_waits_or_reaches_host(tmp_path, tool):
    provider = Provider(tool)
    async with EmbeddedClient(
        config(tmp_path, policy="auto"),
        computer=LocalComputer(tmp_path),
        provider=lambda _: (provider, "fake", "fake"),
        host_definitions=DEFINITIONS,
        review_durable=False,
    ) as client:
        await client.request("set_session_name", name="Background fixture")
        await client.request("prompt", message="Create helper")
        async with asyncio.timeout(10):
            while True:
                event = await client.next_event()
                assert event["type"] not in {"approval_requested", "host_tool_call"}
                if event["type"] == "agent_settled":
                    break


async def test_review_context_restored_when_client_creation_fails(tmp_path):
    from delta_gateway.session_manager import DURABLE_REVIEW, SessionManager
    from delta_gateway.store import Store
    from tests.test_gateway import config

    manager = SessionManager(config(tmp_path), Store(tmp_path / "state", "a" * 32))
    bot = manager.store.create_bot("Reader", "Read documents")

    def broken(*args, **kwargs):
        raise ValueError("Workspace unavailable")

    manager.client = broken
    before = DURABLE_REVIEW.get()
    with pytest.raises(ValueError, match="Workspace unavailable"):
        await manager.delegate(bot, "task", review_durable=False)
    assert DURABLE_REVIEW.get() is before
    manager.store.close()
