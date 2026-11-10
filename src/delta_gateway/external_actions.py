"""Account-scoped TinyFish and Bland provider tools."""

import ipaddress
import re
from urllib.parse import urlsplit

from delta_gateway.google_api import request
from delta_gateway.plugin_schema import OFFSET, TEXT, tool

TINY_SEARCH = "https://api.search.tinyfish.ai"
TINY_FETCH = "https://api.fetch.tinyfish.ai"
TINY_API = "https://agent.tinyfish.ai/v1"
BLAND_CALLS = "https://api.bland.ai/v1/calls"
RUN_ID = {"run_id": TEXT}

TINYFISH_TOOLS = [
    tool(
        "tinyfish_search",
        "Search the public web and return a page of links and excerpts.",
        {"query": TEXT, "page": {"type": "integer", "minimum": 0, "maximum": 10}},
        ["query"],
    ),
    tool(
        "tinyfish_fetch",
        "Read a public page as Markdown. Each fetch may use TinyFish credits.",
        {"url": TEXT, "offset": OFFSET},
        ["url"],
        write=True,
    ),
    tool(
        "tinyfish_agent",
        "Start a reviewed browser task. Returns a run ID. The time limit caps runtime, "
        "and charges can depend on the actions taken.",
        {
            "url": TEXT,
            "goal": TEXT,
            "duration_seconds": {"type": "integer", "minimum": 30, "maximum": 300},
        },
        write=True,
    ),
    tool("tinyfish_get_run", "Read the status and result of a TinyFish browser run.", RUN_ID),
    tool(
        "tinyfish_cancel_run", "Cancel a queued or active TinyFish browser run.", RUN_ID, write=True
    ),
]
BLAND_TOOLS = [
    tool(
        "bland_call",
        "Place a reviewed AI phone call to an E.164 number. The task instructs it to "
        "identify as AI "
        "and audio recording is disabled.",
        {
            "phone_number": TEXT,
            "task": TEXT,
            "duration_minutes": {"type": "integer", "minimum": 1, "maximum": 15},
        },
        write=True,
    ),
    tool(
        "bland_get_call",
        "Read a Bland call's status, summary and paged transcript.",
        {"call_id": TEXT, "offset": OFFSET},
        ["call_id"],
    ),
]
REVIEW_TOOLS = {
    definition["name"]
    for definition in (*TINYFISH_TOOLS, *BLAND_TOOLS)
    if definition.get("approval") is not False
}


def provider_key_valid(value):
    return isinstance(value, str) and re.fullmatch(r"[\x21-\x7e]{8,512}", value) is not None


def id_segment(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        raise ValueError("Use the ID returned by the provider")
    return value


def public_url(value):
    try:
        url = urlsplit(value)
        hostname = url.hostname
        if (
            url.scheme not in {"http", "https"}
            or not hostname
            or url.username is not None
            or url.password is not None
            or len(value) > 4096
            or any(c.isspace() for c in value)
            or "\\" in value
            or (url.port is not None and not 1 <= url.port <= 65535)
        ):
            raise ValueError
        host = hostname.encode("idna").decode("ascii").rstrip(".").lower()
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            numeric_suffix = re.fullmatch(r"(?:[0-9]+|0x[0-9a-f]+)", host.rsplit(".", 1)[-1])
            if (
                "." not in host
                or numeric_suffix
                or host.endswith((".localhost", ".local", ".internal"))
            ):
                raise ValueError from None
        else:
            if not address.is_global or address.is_multicast:
                raise ValueError
    except (ValueError, UnicodeError):
        raise ValueError("Enter a public HTTP or HTTPS URL without embedded credentials") from None
    return value


def bounded(value, lower, upper, unit):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(f"Duration must be between {lower} and {upper} {unit}")
    return value


async def check_tinyfish_key(http, key):
    data = await request(http, {"X-API-Key": key}, "GET", TINY_API + "/runs", params={"limit": 1})
    if not isinstance(data, dict) or not isinstance(data.get("data"), list):
        raise ValueError("TinyFish did not return a run list")


async def check_bland_key(http, key):
    data = await request(http, {"authorization": key}, "GET", BLAND_CALLS, params={"limit": 1})
    if not isinstance(data, dict) or not isinstance(data.get("calls"), list):
        raise ValueError("Bland did not return a call list")


async def tinyfish(http, headers, name, args):
    headers = {"X-API-Key": headers["authorization"].removeprefix("Bearer ")}
    if name == "tinyfish_search":
        data = await request(
            http,
            headers,
            "GET",
            TINY_SEARCH,
            params={"query": args["query"], "page": args.get("page", 0)},
        )
        return {
            **data,
            "results": [
                {**item, "snippet": (item.get("snippet") or "")[:600]}
                for item in data.get("results", [])
            ],
        }
    if name == "tinyfish_fetch":
        data = await request(
            http,
            headers,
            "POST",
            TINY_FETCH,
            json={"urls": [public_url(args["url"])], "format": "markdown"},
        )
        items = data.get("results")
        if not isinstance(items, list) or not items:
            raise ValueError("TinyFish returned no page result")
        item = items[0]
        text = item.get("text") or ""
        offset = args.get("offset", 0)
        end = offset + 2200
        return {
            "url": item.get("url"),
            "title": item.get("title"),
            "error": item.get("error"),
            "text": text[offset:end],
            "nextOffset": end if end < len(text) else None,
        }
    if name == "tinyfish_agent":
        data = await request(
            http,
            headers,
            "POST",
            TINY_API + "/automation/run-async",
            json={
                "url": public_url(args["url"]),
                "goal": args["goal"],
                "agent_config": {
                    "max_duration_seconds": bounded(args["duration_seconds"], 30, 300, "seconds"),
                },
            },
        )
        if not isinstance(data.get("run_id"), str) or not data["run_id"]:
            raise ValueError(
                "TinyFish did not return a run ID. Check provider runs before retrying."
            )
        return {"run_id": data["run_id"], "status": data.get("status")}
    url = TINY_API + "/runs/" + id_segment(args["run_id"])
    if name == "tinyfish_cancel_run":
        return await request(http, headers, "POST", url + "/cancel")
    data = await request(http, headers, "GET", url)
    fields = ("run_id", "status", "result", "error", "created_at", "started_at", "finished_at")
    return {field: data[field] for field in fields if field in data}


async def bland_result(http, key, call_id, offset=0):
    data = await request(
        http,
        {"authorization": key},
        "GET",
        BLAND_CALLS + "/" + id_segment(call_id),
    )
    fields = (
        "call_id",
        "status",
        "completed",
        "call_length",
        "answered_by",
        "started_at",
        "end_at",
    )
    output = {field: data[field] for field in fields if field in data}
    text = data.get("concatenated_transcript") or ""
    end = offset + 2200
    output["summary"] = (data.get("summary") or "")[:900]
    output["transcript"] = text[offset:end]
    output["nextOffset"] = end if end < len(text) else None
    return output


async def bland(http, headers, name, args):
    key = headers["authorization"].removeprefix("Bearer ")
    if name == "bland_get_call":
        return await bland_result(http, key, args["call_id"], args.get("offset", 0))
    number = args["phone_number"]
    if not re.fullmatch(r"\+[1-9][0-9]{6,14}", number):
        raise ValueError("Use an E.164 international phone number")
    data = await request(
        http,
        {"authorization": key},
        "POST",
        BLAND_CALLS,
        json={
            "phone_number": number,
            "task": "At the beginning of the call, identify yourself as an AI assistant. "
            + args["task"],
            "max_duration": bounded(args["duration_minutes"], 1, 15, "minutes"),
            "record": False,
        },
    )
    if (
        data.get("status") == "error"
        or not isinstance(data.get("call_id"), str)
        or not data["call_id"]
    ):
        raise ValueError("Bland did not confirm the call. Check provider calls before retrying.")
    return {"call_id": data["call_id"], "status": data.get("status")}
