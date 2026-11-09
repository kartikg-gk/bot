import json

import httpx
import pytest

from delta_gateway.plugins import PluginRunner


def result(output):
    return json.loads(output.split("\n", 1)[1])


def runner(handler, owner):
    async def token(app):
        assert app == owner
        return f"{app}-token"

    return PluginRunner(transport=httpx.MockTransport(handler), access_token=token)


async def test_calendar_reads_and_writes():
    observed = []

    def provider(req):
        assert req.url.host == "www.googleapis.com"
        assert req.headers["authorization"] == "Bearer calendar-token"
        observed.append(req)
        path = req.url.path.removeprefix("/calendar/v3")
        if path == "/users/me/calendarList":
            assert req.url.params["pageToken"] == "page-2"
            return httpx.Response(200, json={"items": [{"id": "cal@example.test"}]})
        if path == "/calendars/cal@example.test/events" and req.method == "GET":
            assert req.url.params["singleEvents"] == "true"
            return httpx.Response(200, json={"items": [{"id": "event1"}], "nextPageToken": "p2"})
        if path == "/calendars/cal@example.test/events" and req.method == "POST":
            assert req.url.params["sendUpdates"] == "all"
            assert json.loads(req.content) == {
                "summary": "Holiday",
                "start": {"date": "2026-12-24"},
                "end": {"date": "2026-12-25"},
                "attendees": [{"email": "me@example.test"}],
            }
            return httpx.Response(200, json={"id": "event1"})
        assert path == "/calendars/cal@example.test/events/event1"
        if req.method == "GET":
            return httpx.Response(
                200,
                json={
                    "etag": '"rsvp-v1"',
                    "attendees": [
                        {"email": "other@example.test"},
                        {"email": "me@example.test", "self": True},
                    ],
                },
            )
        if req.method == "DELETE":
            assert req.url.params["sendUpdates"] == "externalOnly"
            return httpx.Response(204)
        assert req.method == "PATCH"
        payload = json.loads(req.content)
        if "attendees" in payload:
            assert req.url.params["sendUpdates"] == "all"
            assert req.headers["If-Match"] == '"rsvp-v1"'
            assert payload == {
                "attendeesOmitted": True,
                "attendees": [{"email": "me@example.test", "responseStatus": "tentative"}],
            }
        else:
            assert payload == {"summary": "Renamed", "location": ""}
        return httpx.Response(200, json={"id": "event1"})

    client = runner(provider, "calendar")
    calendar = {"calendar_id": "cal@example.test"}
    event = {**calendar, "event_id": "event1"}
    try:
        calendars = result(await client.run("calendar_list_calendars", {"page_token": "page-2"}))
        assert calendars["items"][0]["id"] == calendar["calendar_id"]
        events = result(
            await client.run(
                "calendar_list_events",
                {
                    **calendar,
                    "start": "2026-12-23T09:00:00Z",
                    "end": "2026-12-25T10:00:00Z",
                },
            )
        )
        assert events["nextPageToken"] == "p2"
        holiday = {
            **calendar,
            "title": "Holiday",
            "notify": "all",
            "start": "2026-12-24",
            "end": "2026-12-25",
            "attendees": ["me@example.test"],
        }
        with pytest.raises(ValueError, match="both be dates"):
            await client.run("calendar_create_event", {**holiday, "end": "2026-12-25T12:00:00Z"})
        assert result(await client.run("calendar_create_event", holiday))["id"] == "event1"
        await client.run(
            "calendar_update_event", {**event, "title": "Renamed", "location": "", "notify": "none"}
        )
        await client.run(
            "calendar_respond_to_event", {**event, "response": "tentative", "notify": "all"}
        )
        assert result(
            await client.run("calendar_delete_event", {**event, "notify": "externalOnly"})
        )["deleted"]
        assert len(observed) == 7
    finally:
        await client.close()


async def test_calendar_free_time_merges_busy_intervals_and_refuses_partial_data():
    blocked = False

    def provider(req):
        assert req.method == "POST" and req.url.path == "/calendar/v3/freeBusy"
        assert json.loads(req.content)["items"] == [{"id": "one"}, {"id": "two"}]
        calendars = {
            "one": {"busy": [{"start": "2026-12-24T09:30:00Z", "end": "2026-12-24T10:30:00Z"}]},
            "two": {
                "busy": [{"start": "2026-12-24T12:15:00+02:00", "end": "2026-12-24T13:30:00+02:00"}]
            },
        }
        if blocked:
            calendars["two"] = {"errors": [{"reason": "notFound"}]}
        return httpx.Response(200, json={"calendars": calendars})

    client = runner(provider, "calendar")
    args = {
        "calendar_ids": ["one", "two"],
        "start": "2026-12-24T09:00:00Z",
        "end": "2026-12-24T12:00:00Z",
    }
    try:
        output = result(await client.run("calendar_find_free_time", args))
        assert output["intervals"] == [
            {"start": "2026-12-24T09:00:00+00:00", "end": "2026-12-24T09:30:00+00:00"},
            {"start": "2026-12-24T11:30:00+00:00", "end": "2026-12-24T12:00:00+00:00"},
        ]
        blocked = True
        with pytest.raises(ValueError, match="every requested calendar"):
            await client.run("calendar_find_free_time", args)
    finally:
        await client.close()


async def test_google_tasks_lists_creates_and_reopens():
    def provider(req):
        assert req.url.host == "tasks.googleapis.com"
        assert req.headers["authorization"] == "Bearer tasks-token"
        if req.url.path == "/tasks/v1/users/@me/lists":
            return httpx.Response(200, json={"items": [{"id": "list1"}]})
        if req.method == "GET":
            assert req.url.path == "/tasks/v1/lists/list1/tasks"
            assert req.url.params["showCompleted"] == "false"
            assert req.url.params["pageToken"] == "p2"
            return httpx.Response(200, json={"items": [{"id": "task1"}]})
        if req.method == "POST":
            assert req.url.path == "/tasks/v1/lists/list1/tasks"
            assert json.loads(req.content) == {"title": "Pack", "due": "2026-12-24T00:00:00Z"}
        else:
            assert req.method == "PATCH" and req.url.path == "/tasks/v1/lists/list1/tasks/task1"
            assert json.loads(req.content) == {
                "status": "needsAction",
                "due": None,
                "completed": None,
            }
        return httpx.Response(200, json={"id": "task1"})

    client = runner(provider, "tasks")
    try:
        assert result(await client.run("tasks_list_lists", {}))["items"][0]["id"] == "list1"
        tasks = {"list_id": "list1"}
        assert (
            result(
                await client.run(
                    "tasks_list", {**tasks, "show_completed": False, "page_token": "p2"}
                )
            )["items"][0]["id"]
            == "task1"
        )
        assert (
            result(
                await client.run("tasks_create", {**tasks, "title": "Pack", "due": "2026-12-24"})
            )["id"]
            == "task1"
        )
        await client.run(
            "tasks_update", {**tasks, "task_id": "task1", "status": "needsAction", "due": None}
        )
    finally:
        await client.close()
