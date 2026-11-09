"""Google Calendar and Tasks tool definitions and HTTP adapters."""

import re
from datetime import UTC, date, datetime, timedelta

from delta_gateway.google_api import request, segment
from delta_gateway.plugin_schema import TEXT, tool

CALENDAR = "https://www.googleapis.com/calendar/v3"
TASKS = "https://tasks.googleapis.com/tasks/v1"

PAGE = {"page_token": TEXT}
CALENDAR_ID = {"calendar_id": TEXT}
EVENT_ID = {**CALENDAR_ID, "event_id": TEXT}
TASK_LIST_ID = {"list_id": TEXT}
TASK_ID = {**TASK_LIST_ID, "task_id": TEXT}
NOTIFY = {"notify": {"type": "string", "enum": ["all", "externalOnly", "none"]}}
EVENT_FIELDS = {
    "title": TEXT,
    "start": TEXT,
    "end": TEXT,
    "description": {"type": "string"},
    "location": {"type": "string"},
    "attendees": {"type": "array", "items": TEXT, "maxItems": 20},
    **NOTIFY,
}
TASK_FIELDS = {
    "title": TEXT,
    "notes": {"type": "string"},
    "due": {"type": ["string", "null"]},
    "status": {"type": "string", "enum": ["needsAction", "completed"]},
}

CALENDAR_TOOLS = [
    tool("calendar_list_calendars", "List calendars and their IDs.", PAGE, []),
    tool(
        "calendar_list_events",
        "List event instances overlapping an RFC 3339 time range with explicit offsets.",
        {**CALENDAR_ID, "start": TEXT, "end": TEXT, **PAGE},
        ["start", "end"],
    ),
    tool(
        "calendar_find_free_time",
        "Find common free intervals within 31 days. Default minimum is 30 minutes.",
        {
            "calendar_ids": {"type": "array", "items": TEXT, "minItems": 1, "maxItems": 50},
            "start": TEXT,
            "end": TEXT,
            "minimum_minutes": {"type": "integer", "minimum": 1, "maximum": 44640},
        },
        ["calendar_ids", "start", "end"],
    ),
    tool(
        "calendar_create_event",
        "Create an event with offset timestamps or all-day dates and an exclusive end. "
        "Choose which guests to notify.",
        {**CALENDAR_ID, **EVENT_FIELDS},
        ["title", "start", "end", "notify"],
        write=True,
    ),
    tool(
        "calendar_update_event",
        "Patch selected event fields. Attendees replaces the guest list if supplied. "
        "Choose which guests to notify.",
        {**EVENT_ID, **EVENT_FIELDS},
        ["event_id", "notify"],
        write=True,
    ),
    tool(
        "calendar_respond_to_event",
        "Change your own RSVP and choose which guests to notify.",
        {
            **EVENT_ID,
            **NOTIFY,
            "response": {"type": "string", "enum": ["accepted", "declined", "tentative"]},
        },
        ["event_id", "response", "notify"],
        write=True,
    ),
    tool(
        "calendar_delete_event",
        "Delete an event and choose which guests to notify.",
        {**EVENT_ID, **NOTIFY},
        ["event_id", "notify"],
        write=True,
    ),
]
TASK_TOOLS = [
    tool("tasks_list_lists", "List the connected user's Google Tasks lists.", PAGE, []),
    tool(
        "tasks_list",
        "Read tasks from a list. Completed tasks are included by default.",
        {**TASK_LIST_ID, **PAGE, "show_completed": {"type": "boolean"}},
        ["list_id"],
    ),
    tool(
        "tasks_create",
        "Create a task with optional notes and a due date in YYYY-MM-DD.",
        {**TASK_LIST_ID, **TASK_FIELDS},
        ["list_id", "title"],
        write=True,
    ),
    tool(
        "tasks_update",
        "Patch a task. Set due to null to clear it, or set status to complete or reopen the task.",
        {**TASK_ID, **TASK_FIELDS},
        ["list_id", "task_id"],
        write=True,
    ),
]
REVIEW_TOOLS = frozenset(
    spec["name"] for spec in (*CALENDAR_TOOLS, *TASK_TOOLS) if spec.get("approval") is not False
)


def timestamp(value):
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value
    ):
        raise ValueError("Use an RFC 3339 timestamp with a timezone offset")
    return datetime.fromisoformat(value)


def event_time(value):
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return {"date": date.fromisoformat(value).isoformat()}
    return {"dateTime": timestamp(value).isoformat()}


def window(args):
    start, end = timestamp(args["start"]), timestamp(args["end"])
    if end <= start:
        raise ValueError("End must be after start")
    return start, end


def event_payload(args):
    mapping = {"title": "summary", "description": "description", "location": "location"}
    payload = {field: args[key] for key, field in mapping.items() if key in args}
    payload.update({key: event_time(args[key]) for key in ("start", "end") if key in args})
    if "start" in payload and "end" in payload:
        start, end = payload["start"], payload["end"]
        if start.keys() != end.keys():
            raise ValueError("Event start and end must both be dates or both be timestamps")
        if "date" in start:
            if start["date"] >= end["date"]:
                raise ValueError("End must be after start")
        else:
            window(args)
    if "attendees" in args:
        payload["attendees"] = [{"email": email} for email in args["attendees"]]
    if not payload:
        raise ValueError("Provide at least one event field to update")
    return payload


def task_payload(args):
    payload = {key: args[key] for key in ("title", "notes", "status") if key in args}
    if args.get("status") == "needsAction":
        payload["completed"] = None
    if "due" in args:
        due = args["due"]
        if due is not None:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", due):
                raise ValueError("Use a due date in YYYY-MM-DD")
            due = date.fromisoformat(due).isoformat() + "T00:00:00Z"
        payload["due"] = due
    if not payload:
        raise ValueError("Provide at least one task field to update")
    return payload


async def page(http, headers, url, args, fields, **query):
    params = dict(maxResults=20, fields=f"nextPageToken,items({fields})", **query)
    if args.get("page_token"):
        params["pageToken"] = args["page_token"]
    data = await request(http, headers, "GET", url, params=params)
    return {"items": data.get("items", []), "nextPageToken": data.get("nextPageToken")}


async def find_free(http, headers, args):
    start, end = window(args)
    calendars = list(dict.fromkeys(args["calendar_ids"]))
    if not 1 <= len(calendars) <= 50 or end - start > timedelta(days=31):
        raise ValueError("Use 1 to 50 calendars and a range up to 31 days")
    payload = {
        "timeMin": start.isoformat(),
        "timeMax": end.isoformat(),
        "items": [{"id": identifier} for identifier in calendars],
    }
    data = await request(http, headers, "POST", CALENDAR + "/freeBusy", json=payload)
    busy = []
    for identifier in calendars:
        entry = data.get("calendars", {}).get(identifier)
        if (
            not isinstance(entry, dict)
            or entry.get("errors")
            or not isinstance(entry.get("busy"), list)
        ):
            raise ValueError("Availability could not be verified for every requested calendar")
        for item in entry["busy"]:
            lower = max(start, timestamp(item["start"]))
            upper = min(end, timestamp(item["end"]))
            if lower < upper:
                busy.append((lower, upper))
    threshold = timedelta(minutes=args.get("minimum_minutes", 30))
    free = []
    cursor = start
    for lower, upper in [*sorted(busy), (end, end)]:
        if lower - cursor >= threshold:
            free.append(
                {
                    "start": cursor.astimezone(UTC).isoformat(),
                    "end": lower.astimezone(UTC).isoformat(),
                }
            )
        cursor = max(cursor, upper)
    return {"intervals": free}


async def respond(http, headers, url, value, params):
    event = await request(
        http, headers, "GET", url, params={"fields": "etag,attendees(email,self)"}
    )
    me = [
        guest["email"]
        for guest in event.get("attendees", [])
        if guest.get("self") and guest.get("email")
    ]
    if len(me) != 1 or not event.get("etag"):
        raise ValueError("Could not verify the current account as an attendee on this event")
    return await request(
        http,
        {**headers, "If-Match": event["etag"]},
        "PATCH",
        url,
        params=params,
        json={"attendeesOmitted": True, "attendees": [{"email": me[0], "responseStatus": value}]},
    )


async def calendar(http, headers, name, args):
    if name == "calendar_list_calendars":
        return await page(
            http,
            headers,
            CALENDAR + "/users/me/calendarList",
            args,
            "id,summary,primary,timeZone,accessRole",
        )
    if name == "calendar_find_free_time":
        return await find_free(http, headers, args)
    base = CALENDAR + f"/calendars/{segment(args.get('calendar_id', 'primary'))}/events"
    if name == "calendar_list_events":
        start, end = window(args)
        return await page(
            http,
            headers,
            base,
            args,
            "id,summary,description,start,end,status,location,htmlLink,organizer",
            timeMin=start.isoformat(),
            timeMax=end.isoformat(),
            singleEvents="true",
            orderBy="startTime",
        )
    notify = args.get("notify")
    if notify not in {"all", "externalOnly", "none"}:
        raise ValueError("Choose which guests to notify: all, externalOnly, or none")
    params = {"sendUpdates": notify}
    if name == "calendar_create_event":
        return await request(http, headers, "POST", base, params=params, json=event_payload(args))
    url = base + "/" + segment(args["event_id"])
    if name == "calendar_update_event":
        return await request(http, headers, "PATCH", url, params=params, json=event_payload(args))
    if name == "calendar_respond_to_event":
        return await respond(http, headers, url, args["response"], params)
    if name == "calendar_delete_event":
        await request(http, headers, "DELETE", url, params=params)
        return {"deleted": True, "id": args["event_id"]}
    raise ValueError("Unknown Calendar tool")


async def tasks(http, headers, name, args):
    if name == "tasks_list_lists":
        return await page(http, headers, TASKS + "/users/@me/lists", args, "id,title,updated")
    base = TASKS + f"/lists/{segment(args['list_id'])}/tasks"
    if name == "tasks_list":
        completed = "true" if args.get("show_completed", True) else "false"
        return await page(
            http,
            headers,
            base,
            args,
            "id,title,notes,status,due,completed,parent,webViewLink",
            showCompleted=completed,
            showHidden=completed,
        )
    if name == "tasks_create":
        return await request(http, headers, "POST", base, json=task_payload(args))
    if name == "tasks_update":
        return await request(
            http, headers, "PATCH", base + "/" + segment(args["task_id"]), json=task_payload(args)
        )
    raise ValueError("Unknown Tasks tool")
