"""Drive search and downloads, Docs editing, and Sheets values tools."""

import base64
import codecs
import json
import re

from delta_gateway.google_api import request, segment
from delta_gateway.plugin_schema import OFFSET, TEXT, tool

DRIVE_API = "https://www.googleapis.com/drive/v3/files"
DOCS_API = "https://docs.googleapis.com/v1/documents"
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"
GOOGLE_FILE = "application/vnd.google-apps."
EXPORTS = {
    GOOGLE_FILE + "document": "text/plain",
    GOOGLE_FILE + "spreadsheet": "text/csv",
    GOOGLE_FILE + "presentation": "text/plain",
    GOOGLE_FILE + "drawing": "image/png",
    GOOGLE_FILE + "script": "application/vnd.google-apps.script+json",
}
SHEET_RANGE = {"spreadsheet_id": TEXT, "range": TEXT}
SHEET_VALUES = {
    "values": {
        "type": "array",
        "minItems": 1,
        "maxItems": 100,
        "items": {
            "type": "array",
            "minItems": 1,
            "maxItems": 50,
            "items": {"type": ["string", "number", "boolean", "null"]},
        },
    },
    "input_mode": {"type": "string", "enum": ["RAW", "USER_ENTERED"]},
}

DRIVE_TOOLS = [
    tool(
        "drive_search",
        "Search accessible Drive files using a Drive query expression. Trashed files are excluded.",
        {"query": TEXT, "page_token": TEXT, "drive_id": TEXT},
        [],
    ),
    tool(
        "drive_read_file",
        "Read an uploaded file or export a Google file. Use nextOffset and version for each page. "
        "Choose base64 for binary data. Google Sheets CSV exports the first sheet.",
        {
            "file_id": TEXT,
            "format": {"type": "string", "enum": ["text", "base64"]},
            "export_mime": TEXT,
            "offset": OFFSET,
            "version": TEXT,
        },
        ["file_id"],
    ),
]
DOCS_TOOLS = [
    tool(
        "docs_create",
        "Create an empty Google Doc.",
        {"title": TEXT},
        write=True,
    ),
    tool(
        "docs_append_text",
        "Append plain text to the end of a Google Doc or specified tab.",
        {"document_id": TEXT, "text": TEXT, "tab_id": TEXT},
        ["document_id", "text"],
        write=True,
    ),
]
SHEETS_TOOLS = [
    tool(
        "sheets_read",
        "Read a Google Sheets A1 range or named range. Use a small range for large sheets.",
        SHEET_RANGE,
    ),
    tool(
        "sheets_append_rows",
        "Append rows after the table in the specified range. Values are literal unless "
        "USER_ENTERED is chosen.",
        {**SHEET_RANGE, **SHEET_VALUES},
        ["spreadsheet_id", "range", "values"],
        write=True,
    ),
    tool(
        "sheets_update_range",
        "Update a range. Values are literal unless USER_ENTERED is chosen.",
        {**SHEET_RANGE, **SHEET_VALUES},
        ["spreadsheet_id", "range", "values"],
        write=True,
    ),
    tool(
        "sheets_create",
        "Create a spreadsheet with its default first tab or one named tab.",
        {"title": TEXT, "sheet_title": TEXT},
        ["title"],
        write=True,
    ),
]
REVIEW_TOOLS = frozenset(
    spec["name"] for spec in (*DOCS_TOOLS, *SHEETS_TOOLS) if spec.get("approval") is not False
)


async def search(http, headers, args):
    query = args.get("query")
    params = {
        "q": f"trashed = false and ({query})" if query else "trashed = false",
        "pageSize": 20,
        "fields": "nextPageToken,incompleteSearch,files(id,name,mimeType,size,version,webViewLink)",
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }
    if args.get("drive_id"):
        params.update(corpora="drive", driveId=args["drive_id"])
    if args.get("page_token"):
        params["pageToken"] = args["page_token"]
    return await request(http, headers, "GET", DRIVE_API, params=params)


async def media_page(http, headers, url, params, offset, limit, *, export):
    headers = {**headers, "Accept-Encoding": "identity"}
    if not export:
        headers["Range"] = f"bytes={offset}-{offset + limit - 1}"
    async with http.stream("GET", url, headers=headers, params=params) as reply:
        if reply.status_code == 416:
            raise ValueError("The offset is outside the file")
        reply.raise_for_status()
        total = None
        skip = offset
        if reply.status_code == 206:
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", reply.headers.get("content-range", ""))
            if export or not match or int(match[1]) != offset:
                raise ValueError("Drive returned an unexpected content range")
            start, end, total = map(int, match.groups())
            if not start <= end < total or end >= start + limit:
                raise ValueError("Drive returned an unexpected content range")
            skip = 0
        elif not export and offset:
            raise ValueError("Drive did not honor the requested byte range")
        data = bytearray()
        async for chunk in reply.aiter_bytes():
            if skip:
                consumed = min(skip, len(chunk))
                chunk = chunk[consumed:]
                skip -= consumed
            data.extend(chunk[: limit + 1 - len(data)])
            if len(data) > limit:
                break
        if skip:
            raise ValueError("The offset is outside the file")
        if total is not None and len(data) != end - start + 1:
            raise ValueError("Drive returned an incomplete content range")
        more = offset + len(data) < total if total is not None else len(data) > limit
        return bytes(data[:limit]), more


async def drive(http, headers, name, args):
    if name == "drive_search":
        return await search(http, headers, args)
    file_id = args["file_id"]
    url = DRIVE_API + "/" + segment(file_id)
    metadata = await request(
        http,
        headers,
        "GET",
        url,
        params={
            "fields": (
                "id,name,mimeType,version,size,capabilities(canDownload),shortcutDetails(targetId)"
            ),
            "supportsAllDrives": "true",
        },
    )
    mime = metadata["mimeType"]
    if mime == GOOGLE_FILE + "shortcut":
        return {"shortcut": metadata["shortcutDetails"]["targetId"]}
    version = metadata.get("version")
    if args.get("version") is not None and args["version"] != version:
        raise ValueError("The file changed. Restart from byte zero to read the new version.")
    if metadata.get("capabilities", {}).get("canDownload") is False:
        raise ValueError("Drive does not allow downloading this file")
    export = mime.startswith(GOOGLE_FILE)
    offset = args.get("offset", 0)
    if offset and version and not args.get("version"):
        raise ValueError("Pass the version returned by the first page when continuing a file read")
    output = args.get("format", "text")
    limit = 9000 if output == "base64" else 2500
    if export:
        default = EXPORTS.get(mime)
        if default is None and "export_mime" not in args:
            raise ValueError("This Google file type has no default export format")
        mime = args.get("export_mime", default)
        url += "/export"
        params = {"mimeType": mime}
        if offset > 10 * 1024**2:
            raise ValueError("The offset exceeds the Drive export size limit")
    else:
        if "export_mime" in args:
            raise ValueError("Export MIME type applies only to native Google files")
        params = {"alt": "media", "supportsAllDrives": "true"}
    size = int(metadata["size"]) if not export and "size" in metadata else None
    if size is not None and offset > size:
        raise ValueError("The offset is outside the file")
    if size == offset:
        data, more = b"", False
    else:
        data, more = await media_page(http, headers, url, params, offset, limit, export=export)
    if version and (more or offset):
        current = await request(
            http,
            headers,
            "GET",
            DRIVE_API + "/" + segment(file_id),
            params={"fields": "version", "supportsAllDrives": "true"},
        )
        if current.get("version") != version:
            raise ValueError("The file changed. Restart from byte zero to read the new version.")
    pending = 0
    if output == "base64":
        content = base64.b64encode(data).decode()
    else:
        decoder = codecs.getincrementaldecoder("utf-8")()
        try:
            content = decoder.decode(data, final=not more)
        except UnicodeDecodeError:
            raise ValueError(
                "Use a returned byte cursor or choose base64 for binary data"
            ) from None
        pending = len(decoder.getstate()[0])
    return {
        "id": file_id,
        "name": metadata.get("name"),
        "mimeType": mime,
        "version": version,
        "format": output,
        "content": content,
        "nextOffset": offset + len(data) - pending if more else None,
    }


async def docs(http, headers, name, args):
    if name == "docs_create":
        return await request(http, headers, "POST", DOCS_API, json={"title": args["title"]})
    location = {"tabId": args["tab_id"]} if args.get("tab_id") else {}
    url = DOCS_API + "/" + segment(args["document_id"]) + ":batchUpdate"
    await request(
        http,
        headers,
        "POST",
        url,
        json={
            "requests": [{"insertText": {"endOfSegmentLocation": location, "text": args["text"]}}],
        },
    )
    return {"documentId": args["document_id"], "appended": True}


async def sheet_values(http, headers, url):
    data = bytearray()
    async with http.stream(
        "GET",
        url,
        headers=headers,
        params={
            "valueRenderOption": "UNFORMATTED_VALUE",
            "majorDimension": "ROWS",
        },
    ) as reply:
        reply.raise_for_status()
        async for chunk in reply.aiter_bytes():
            if len(data) + len(chunk) > 18000:
                raise ValueError("The result is too large. Read a smaller spreadsheet range.")
            data.extend(chunk)
    value = json.loads(data)
    if len(json.dumps(value, ensure_ascii=False)) > 19000:
        raise ValueError("The result is too large. Read a smaller spreadsheet range.")
    return value


async def sheets(http, headers, name, args):
    if name == "sheets_create":
        body = {"properties": {"title": args["title"]}}
        if args.get("sheet_title"):
            body["sheets"] = [{"properties": {"title": args["sheet_title"]}}]
        return await request(
            http,
            headers,
            "POST",
            SHEETS_API,
            json=body,
            params={
                "fields": (
                    "spreadsheetId,spreadsheetUrl,properties(title),"
                    "sheets(properties(sheetId,title))"
                ),
            },
        )
    url = SHEETS_API + "/" + segment(args["spreadsheet_id"]) + "/values/" + segment(args["range"])
    if name == "sheets_read":
        return await sheet_values(http, headers, url)
    params = {"valueInputOption": args.get("input_mode", "RAW")}
    body = {"values": args["values"], "majorDimension": "ROWS"}
    if name == "sheets_append_rows":
        params["insertDataOption"] = "INSERT_ROWS"
        return await request(http, headers, "POST", url + ":append", params=params, json=body)
    return await request(http, headers, "PUT", url, params=params, json=body)
