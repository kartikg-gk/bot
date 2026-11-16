import asyncio
import base64
import json
import os
import shutil
import struct
import sys
from pathlib import Path

import httpx
import pytest

from delta_gateway import documents
from delta_gateway.plugins import PluginRunner


def simple_pdf():
    stream = b"BT /F1 12 Tf 24 120 Td (Gateway fixture) Tj ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 240 180]"
        b" /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream",
    ]
    pdf = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf.extend(f"{number} 0 obj\n".encode() + body + b"\nendobj\n")
    xref = len(pdf)
    pdf.extend(b"xref\n0 6\n0000000000 65535 f \n")
    for offset in offsets:
        pdf.extend(f"{offset:010} 00000 n \n".encode())
    pdf.extend(f"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    return bytes(pdf)


@pytest.mark.skipif(shutil.which("pdftotext") is None, reason="Poppler is unavailable")
async def test_extract_real_pdf_text():
    extracted = await documents.Documents().read(simple_pdf(), "application/pdf", {})
    assert extracted == {
        "page": 1,
        "method": "pdf-text",
        "markdown": "Gateway fixture",
        "nextOffset": None,
    }


async def test_image_limits_and_ocr_fallback_for_blank_pdf(monkeypatch):
    calls = []
    pdf = simple_pdf()

    async def fake_execute(program, args, data, output_limit):
        calls.append((program, args, data, output_limit))
        if program in {"pdftotext", "pdftoppm"}:
            path = Path(args[-2] if program == "pdftotext" else args[-1])
            assert path.read_bytes() == pdf
            assert data == b""
            if os.name != "nt":
                assert path.stat().st_mode & 0o777 == 0o600
        return {"pdftotext": b"\f", "pdftoppm": b"raster", "tesseract": b"Scanned page"}[program]

    monkeypatch.setattr(documents, "execute", fake_execute)
    reader = documents.Documents()
    first = await reader.read(pdf, "application/pdf", {"page": 3})
    assert first["method"] == "ocr" and first["markdown"] == "Scanned page"
    assert [call[0] for call in calls] == ["pdftotext", "pdftoppm", "tesseract"]
    assert calls[1][1][:4] == ["-f", "3", "-l", "3"]
    assert calls[0][1][-2] == calls[1][1][-1]
    assert not Path(calls[0][1][-2]).exists()
    assert calls[-1][2] == b"raster"
    assert (await reader.read(pdf, "application/pdf", {"page": 3, "offset": 8}))[
        "markdown"
    ] == "page"
    assert len(calls) == 3
    png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR" + struct.pack(">II", 5000, 5000)
    with pytest.raises(ValueError, match="dimension"):
        await reader.read(png, "image/png", {})
    with pytest.raises(ValueError, match="supports PDF"):
        await reader.read(pdf, "image/jpeg", {})


async def test_local_worker_caps_output():
    with pytest.raises(ValueError, match="too much output"):
        await documents.execute(sys.executable, ["-c", "print('a' * 100000)"], b"", 50)


async def test_drive_and_gmail_extract_use_existing_authorized_downloads(monkeypatch):
    pdf = simple_pdf()
    paths = []
    owners = []

    async def token(owner):
        owners.append(owner)
        return "own-" + owner

    async def plain(data, kind, page):
        assert data == pdf and kind == "pdf" and page == 1
        return "a" * 2400 + "last", "pdf-text"

    def respond(request):
        path = request.url.path
        paths.append(path)
        if request.url.host == "www.googleapis.com":
            assert request.headers["authorization"] == "Bearer own-drive"
            if request.url.params.get("alt") == "media":
                return httpx.Response(200, content=pdf)
            return httpx.Response(200, json={"mimeType": "application/pdf", "version": "1"})
        assert request.headers["authorization"] == "Bearer own-gmail"
        if path.endswith("/attachments/part-id"):
            return httpx.Response(200, json={"data": base64.urlsafe_b64encode(pdf).decode()})
        return httpx.Response(
            200,
            json={
                "payload": {
                    "parts": [
                        {
                            "filename": "fixture.pdf",
                            "mimeType": "application/pdf",
                            "body": {"attachmentId": "part-id", "size": len(pdf)},
                        }
                    ]
                }
            },
        )

    monkeypatch.setattr(documents, "extract", plain)
    runner = PluginRunner(transport=httpx.MockTransport(respond), access_token=token)
    try:
        first = await runner.run("drive_extract_document", {"file_id": "file"})
        second = await runner.run(
            "gmail_extract_attachment", {"message_id": "msg", "part": "0.0", "offset": 2400}
        )
    finally:
        await runner.close()
    assert json.loads(first.split("\n", 1)[1])["nextOffset"] == 2400
    assert json.loads(second.split("\n", 1)[1])["markdown"] == "last"
    assert owners == ["drive", "gmail"]
    assert len(paths) == 5


@pytest.mark.parametrize("outcome", ["output limit", "timeout", "cancel"])
async def test_pdf_temp_file_is_removed_when_worker_stops(monkeypatch, outcome):
    original_execute = documents.execute
    paths = []
    started = asyncio.Event()
    pdf = simple_pdf()

    async def execute(program, args, data, output_limit):
        assert program == "pdftotext"
        path = Path(args[-2])
        assert path.read_bytes() == pdf
        assert data == b""
        assert output_limit == documents.MAX_TEXT_BYTES
        paths.append(path)
        started.set()
        command = (
            "import sys; from pathlib import Path; "
            "assert Path(sys.argv[1]).read_bytes().startswith(b'%PDF-'); "
        )
        if outcome == "output limit":
            command += "sys.stdout.write('a' * 200000)"
        else:
            command += "import time; time.sleep(60)"
        return await original_execute(
            sys.executable, ["-c", command, str(path)], data, output_limit
        )

    monkeypatch.setattr(documents, "execute", execute)
    if outcome == "timeout":
        monkeypatch.setattr(documents, "EXTRACTION_TIMEOUT", 0.25)
    task = asyncio.create_task(documents.extract(pdf, "pdf", 1))
    if outcome == "cancel":
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        message = "too much output" if outcome == "output limit" else "timed out"
        with pytest.raises(ValueError, match=message):
            await asyncio.wait_for(task, 10)
    assert len(paths) == 1 and not paths[0].exists()
