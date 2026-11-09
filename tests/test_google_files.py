import base64
import json
import re

import httpx
import pytest

from delta_gateway.plugins import PluginRunner


def value(output):
    return json.loads(output.split("\n", 1)[1])


def runner(owner, handler):
    async def token(app):
        assert app == owner
        return f"{app}-token"

    return PluginRunner(transport=httpx.MockTransport(handler), access_token=token)


async def test_drive_search_ranges_utf8_exports_and_version():
    uploaded = ("x" * 2499 + "€" + " y").encode()
    native = b"First sheet only"
    version = "5"

    def provider(req):
        assert req.headers["authorization"] == "Bearer drive-token"
        assert req.url.host == "www.googleapis.com"
        path = req.url.path
        if path.endswith("/files"):
            assert req.url.params["q"] == "trashed = false and (name contains 'report')"
            assert req.url.params["pageToken"] == "next"
            assert req.url.params["corpora"] == "drive"
            assert req.url.params["driveId"] == "shared"
            return httpx.Response(200, json={"files": [{"id": "blob"}], "nextPageToken": "later"})
        if path.endswith("/sheet/export"):
            assert req.url.params["mimeType"] == "text/csv"
            assert "range" not in req.headers
            return httpx.Response(200, content=native)
        if req.url.params.get("alt") == "media":
            assert path.endswith("/blob")
            start, stop = map(
                int, re.fullmatch(r"bytes=(\d+)-(\d+)", req.headers["range"]).groups()
            )
            end = min(stop, len(uploaded) - 1)
            return httpx.Response(
                206,
                headers={"Content-Range": f"bytes {start}-{end}/{len(uploaded)}"},
                content=uploaded[start : end + 1],
            )
        if path.endswith("/sheet"):
            return httpx.Response(
                200,
                json={
                    "id": "sheet",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "version": version,
                },
            )
        assert path.endswith("/blob")
        return httpx.Response(
            200,
            json={
                "id": "blob",
                "mimeType": "text/plain",
                "size": str(len(uploaded)),
                "version": version,
            },
        )

    client = runner("drive", provider)
    try:
        search = value(
            await client.run(
                "drive_search",
                {
                    "query": "name contains 'report'",
                    "page_token": "next",
                    "drive_id": "shared",
                },
            )
        )
        assert search["files"] == [{"id": "blob"}] and search["nextPageToken"] == "later"
        first = value(await client.run("drive_read_file", {"file_id": "blob"}))
        assert first["nextOffset"] == 2499
        options = {"file_id": "blob", "version": first["version"], "offset": first["nextOffset"]}
        second = value(await client.run("drive_read_file", options))
        assert second["nextOffset"] is None
        assert (first["content"] + second["content"]).encode() == uploaded
        blob = value(await client.run("drive_read_file", {"file_id": "blob", "format": "base64"}))
        assert base64.b64decode(blob["content"]) == uploaded
        export = value(await client.run("drive_read_file", {"file_id": "sheet"}))
        assert export["mimeType"] == "text/csv" and export["content"] == native.decode()
        version = "6"
        with pytest.raises(ValueError, match="file changed"):
            await client.run("drive_read_file", options)
    finally:
        await client.close()


async def test_docs_create_and_append_to_tab():
    def provider(req):
        assert req.headers["authorization"] == "Bearer docs-token"
        assert req.url.host == "docs.googleapis.com"
        assert req.method == "POST"
        payload = json.loads(req.content)
        if req.url.path == "/v1/documents":
            assert payload == {"title": "Notes"}
            return httpx.Response(200, json={"documentId": "doc1"})
        assert req.url.path == "/v1/documents/doc1:batchUpdate"
        assert payload == {
            "requests": [
                {
                    "insertText": {
                        "endOfSegmentLocation": {"tabId": "tab1"},
                        "text": "\nNext note",
                    }
                }
            ]
        }
        return httpx.Response(200, json={"documentId": "doc1", "replies": [{}]})

    client = runner("docs", provider)
    try:
        assert value(await client.run("docs_create", {"title": "Notes"}))["documentId"] == "doc1"
        appended = value(
            await client.run(
                "docs_append_text",
                {
                    "document_id": "doc1",
                    "tab_id": "tab1",
                    "text": "\nNext note",
                },
            )
        )
        assert appended == {"documentId": "doc1", "appended": True}
    finally:
        await client.close()


async def test_sheets_read_append_update_create_and_large_range():
    selected_range = "'Sales 2026'!A1:B2"
    too_large = False

    def provider(req):
        assert req.headers["authorization"] == "Bearer sheets-token"
        assert req.url.host == "sheets.googleapis.com"
        path = req.url.path
        if path == "/v4/spreadsheets":
            assert req.method == "POST"
            assert json.loads(req.content) == {
                "properties": {"title": "Report"},
                "sheets": [{"properties": {"title": "Sales 2026"}}],
            }
            return httpx.Response(200, json={"spreadsheetId": "sheet1"})
        base = "/v4/spreadsheets/sheet1/values/" + selected_range
        if req.method == "GET":
            assert path == base
            assert req.url.params["valueRenderOption"] == "UNFORMATTED_VALUE"
            return httpx.Response(200, json={"values": [["X" * 20000 if too_large else 7]]})
        payload = json.loads(req.content)
        assert payload == {"majorDimension": "ROWS", "values": [["=1+1", True], [None, 4]]}
        if req.method == "POST":
            assert path == base + ":append"
            assert req.url.params["valueInputOption"] == "RAW"
            assert req.url.params["insertDataOption"] == "INSERT_ROWS"
        else:
            assert req.method == "PUT" and path == base
            assert req.url.params["valueInputOption"] == "USER_ENTERED"
        return httpx.Response(200, json={"spreadsheetId": "sheet1"})

    client = runner("sheets", provider)
    try:
        created = value(
            await client.run("sheets_create", {"title": "Report", "sheet_title": "Sales 2026"})
        )
        assert created["spreadsheetId"] == "sheet1"
        location = {"spreadsheet_id": "sheet1", "range": selected_range}
        assert value(await client.run("sheets_read", location))["values"] == [[7]]
        writes = {**location, "values": [["=1+1", True], [None, 4]]}
        await client.run("sheets_append_rows", writes)
        await client.run("sheets_update_range", {**writes, "input_mode": "USER_ENTERED"})
        too_large = True
        with pytest.raises(ValueError, match="smaller spreadsheet range"):
            await client.run("sheets_read", location)
    finally:
        await client.close()
