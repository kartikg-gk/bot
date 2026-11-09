"""Read notes and paginated transcripts from Granola's public API."""

import json
import re

from delta_gateway.google_api import request
from delta_gateway.plugin_schema import OFFSET, TEXT, tool

GRANOLA_NOTES = "https://public-api.granola.ai/v1/notes"
NOTE_ID = {"note_id": TEXT}
PAGE = {"cursor": TEXT, "page_size": {"type": "integer", "minimum": 1, "maximum": 20}}
FILTERS = {name: TEXT for name in ("created_after", "created_before", "folder_id", "space_id")}
NOTE_FIELDS = ("id", "title", "owner", "created_at", "updated_at", "deleted_at")
GRANOLA_TOOLS = [
    tool(
        "granola_list_notes",
        "List accessible Granola notes, optionally filtered by date, folder or space.",
        {**PAGE, **FILTERS},
        [],
    ),
    tool(
        "granola_get_note",
        "Read note details and a page of its summary and private notes if available.",
        {**NOTE_ID, "offset": OFFSET},
        ["note_id"],
    ),
    tool(
        "granola_get_transcript",
        "Read a page of transcript entries. Pass the returned cursor for the next page.",
        {**NOTE_ID, **PAGE},
        ["note_id"],
    ),
]


def key_valid(key):
    return isinstance(key, str) and re.fullmatch(r"grn_[A-Za-z0-9_-]{8,200}", key) is not None


async def check_key(http, key):
    response = await http.get(
        GRANOLA_NOTES,
        headers={"authorization": "Bearer " + key},
        params={"page_size": 1},
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or not isinstance(body.get("notes"), list):
        raise ValueError("Granola did not return a note list")


def page(data, field):
    return {
        "items": data[field],
        "cursor": data.get("cursor"),
        "hasMore": data.get("hasMore", False),
    }


async def granola(http, headers, name, args):
    if name == "granola_list_notes":
        params = {field: args[field] for field in (*FILTERS, "cursor") if field in args}
        params["page_size"] = args.get("page_size", 10)
        data = await request(http, headers, "GET", GRANOLA_NOTES, params=params)
        output = page(data, "notes")
        output["items"] = [
            {field: note[field] for field in NOTE_FIELDS if field in note}
            for note in output["items"]
        ]
        return output

    note = args["note_id"]
    if not re.fullmatch(r"(?i:not)_[A-Za-z0-9]{14}", note):
        raise ValueError("Use a Granola note ID from the note list")
    url = GRANOLA_NOTES + "/" + note
    if name == "granola_get_transcript":
        params = {"page_size": args.get("page_size", 10)}
        if args.get("cursor"):
            params["cursor"] = args["cursor"]
        data = await request(http, headers, "GET", url + "/transcript", params=params)
        output = page(data, "transcript")
        if len(json.dumps(output, ensure_ascii=False)) > 19000:
            raise ValueError("This transcript page is too large. Use a smaller page_size.")
        return output

    data = await request(http, headers, "GET", url)
    output = {field: data[field] for field in NOTE_FIELDS if field in data}
    output["attendees"] = data.get("attendees", [])[:25]
    offset = args.get("offset", 0)
    end = offset + 1000
    summary = data.get("summary_markdown") or data.get("summary_text") or ""
    private = data.get("private_notes_markdown") or data.get("private_notes_text")
    output["summary"] = summary[offset:end]
    output["privateNotes"] = private[offset:end] if private is not None else None
    output["nextOffset"] = end if end < max(len(summary), len(private or "")) else None
    return output
