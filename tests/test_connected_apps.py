import base64
import json
from email import policy
from email.parser import BytesParser

import httpx
import pytest

from delta_gateway.plugins import GMAIL, UNTRUSTED, PluginRunner


def decoded(output):
    assert output.startswith(UNTRUSTED)
    return json.loads(output.removeprefix(UNTRUSTED))


async def test_github_repository_issue_comments_and_pull_request_workflows():
    calls = []

    async def token(app):
        assert app == "github"
        return "test-token"

    def provider(request):
        assert request.url.host == "api.github.com"
        assert request.headers["authorization"] == "Bearer test-token"
        path = request.url.path
        calls.append(path)
        if path == "/user/repos":
            assert request.url.params["page"] == "2"
            return httpx.Response(
                200,
                headers={"Link": '<https://api.github.com/user/repos?page=3>; rel="next"'},
                json=[{"full_name": "octo/project", "private": True}],
            )
        if path == "/repos/octo/project":
            return httpx.Response(200, json={"default_branch": "main"})
        if path.endswith("/issues/3/comments"):
            return httpx.Response(200, json=[{"body": "Untrusted", "user": {"login": "octo"}}])
        if path.endswith("/pulls/3/files"):
            return httpx.Response(
                200, json=[{"filename": "src/main.py", "patch": "+print(1)\n" * 360}]
            )
        if path.endswith("/pulls/3"):
            return httpx.Response(200, json={"head": {"ref": "fix"}, "merged": False})
        assert path == "/repos/octo/project/pulls"
        if request.method == "POST":
            assert json.loads(request.content) == {
                "title": "Fix",
                "head": "octo:fix",
                "base": "main",
                "draft": True,
            }
            return httpx.Response(
                201, json={"number": 3, "html_url": "https://github.com/octo/project/pull/3"}
            )
        assert request.url.params["state"] == "all"
        return httpx.Response(200, json=[{"number": 3}])

    runner = PluginRunner(transport=httpx.MockTransport(provider), access_token=token)
    repo = {"repo": "octo/project"}
    item = {**repo, "number": 3}
    try:
        page = decoded(await runner.run("github_list_repositories", {"page": 2}))
        assert page["nextPage"] == 3 and page["items"][0]["private"]
        assert decoded(await runner.run("github_get_repository", repo))["default_branch"] == "main"
        assert decoded(await runner.run("github_list_issue_comments", item))["items"][0] == {
            "body": "Untrusted",
            "user": "octo",
        }
        assert decoded(await runner.run("github_list_pull_requests", {**repo, "state": "all"}))[
            "items"
        ] == [{"number": 3}]
        assert decoded(await runner.run("github_get_pull_request", item))["head"]["ref"] == "fix"
        listing = decoded(await runner.run("github_list_pull_request_files", item))
        assert listing["items"][0]["patch"].startswith("+print(1)")
        patch = decoded(
            await runner.run("github_list_pull_request_files", {**item, "filename": "src/main.py"})
        )
        tail = decoded(
            await runner.run(
                "github_list_pull_request_files",
                {**item, "filename": "src/main.py", "offset": patch["nextOffset"]},
            )
        )
        assert tail["nextOffset"] is None
        assert patch["patch"] + tail["patch"] == "+print(1)\n" * 360
        created = decoded(
            await runner.run(
                "github_create_pull_request",
                {**repo, "title": "Fix", "head": "octo:fix", "base": "main", "draft": True},
            )
        )
        assert created["number"] == 3
        with pytest.raises(ValueError):
            await runner.run("github_get_repository", {"repo": "octo/../redirect"})
        assert len(calls) == 9
    finally:
        await runner.close()


async def test_gmail_thread_attachments_send_and_labels():
    attachment = bytes(range(256)) * 37
    calls = []

    def encoded(data):
        return base64.urlsafe_b64encode(data).decode().rstrip("=")

    def message(identifier):
        return {
            "id": identifier,
            "threadId": "thread1",
            "payload": {
                "headers": [{"name": "Subject", "value": "Example"}],
                "parts": [
                    {"mimeType": "text/plain", "body": {"data": encoded(b"Hello")}},
                    {
                        "mimeType": "text/plain",
                        "filename": "note.txt",
                        "body": {"data": encoded(b"Inline file")},
                    },
                    {
                        "mimeType": "application/octet-stream",
                        "filename": "bin.dat",
                        "body": {"attachmentId": "blob1", "size": len(attachment)},
                    },
                ],
            },
        }

    async def token(app):
        assert app == "gmail"
        return "test-token"

    def provider(request):
        assert request.url.host == "gmail.googleapis.com"
        assert request.headers["authorization"] == "Bearer test-token"
        prefix = "/gmail/v1/users/me"
        assert request.url.path.startswith(prefix)
        path = request.url.path.removeprefix(prefix)
        calls.append(path)
        if path == "/threads/thread1":
            assert request.url.params["format"] == "full"
            return httpx.Response(
                200, json={"id": "thread1", "messages": [message(f"m{i}") for i in range(6)]}
            )
        if path == "/messages/m0":
            return httpx.Response(200, json=message("m0"))
        if path == "/messages/m0/attachments/blob1":
            return httpx.Response(200, json={"data": encoded(attachment)})
        if path == "/labels":
            return httpx.Response(200, json={"labels": [{"id": "Label_1", "name": "Work"}]})
        if path == "/threads/thread1/modify":
            assert json.loads(request.content) == {
                "addLabelIds": ["Label_1"],
                "removeLabelIds": ["UNREAD"],
            }
            return httpx.Response(200, json={"id": "thread1"})
        assert path == "/messages/send"
        payload = json.loads(request.content)
        mail = BytesParser(policy=policy.default).parsebytes(
            base64.urlsafe_b64decode(payload["raw"])
        )
        assert mail["To"] == "user@example.test"
        assert mail["Subject"] == "Hello"
        assert mail.get_content().strip() == "Sent content"
        return httpx.Response(200, json={"id": "sent1"})

    runner = PluginRunner(transport=httpx.MockTransport(provider), access_token=token)
    try:
        thread = decoded(await runner.run("gmail_read_thread", {"id": "thread1"}))
        assert len(thread["messages"]) == 5 and thread["nextOffset"] == 5
        mail = decoded(await runner.run("gmail_read", {"id": "m0"}))
        assert mail["text"] == "Hello"
        assert [a["part"] for a in mail["attachments"]] == ["0.1", "0.2"]
        inline = {"message_id": "m0", "part": "0.1"}
        assert (
            decoded(await runner.run("gmail_read_attachment", inline))["content"] == "Inline file"
        )
        binary = {"message_id": "m0", "part": "0.2", "format": "base64"}
        part1 = decoded(await runner.run("gmail_read_attachment", binary))
        part2 = decoded(
            await runner.run("gmail_read_attachment", {**binary, "offset": part1["nextOffset"]})
        )
        assert part2["nextOffset"] is None
        assert base64.b64decode(part1["content"]) + base64.b64decode(part2["content"]) == attachment
        assert decoded(await runner.run("gmail_list_labels", {}))[0]["id"] == "Label_1"
        assert (
            await runner.run(
                "gmail_modify_labels",
                {"id": "thread1", "target": "thread", "add": ["Label_1"], "remove": ["UNREAD"]},
            )
            == "Gmail labels updated."
        )
        sent = decoded(
            await runner.run(
                "gmail_send",
                {"to": "user@example.test", "subject": "Hello", "body": "Sent content"},
            )
        )
        assert sent["id"] == "sent1"
        with pytest.raises(ValueError):
            await runner.run("gmail_modify_labels", {"id": "m0", "add": ["A"], "remove": ["A"]})
        assert calls.count("/messages/send") == 1
    finally:
        await runner.close()
    tools = {tool["name"]: tool for tool in GMAIL.tools}
    assert "approval" not in tools["gmail_send"] and "approval" not in tools["gmail_modify_labels"]
    assert tools["gmail_read_attachment"]["approval"] is False
