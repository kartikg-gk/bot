"""Bounded, local text extraction from authorized attachments."""

import asyncio
import hashlib
import os
import shutil
from collections import OrderedDict
from tempfile import NamedTemporaryFile

MAX_DOCUMENT_BYTES = 8 * 1024**2
MAX_GMAIL_RESPONSE = 12 * 1024**2
MAX_TEXT_BYTES = 128 * 1024
PAGE_CHARS = 2400
EXTRACTION_TIMEOUT = 20
_SLOTS = asyncio.Semaphore(2)


async def bounded_body(reply, limit):
    reply.raise_for_status()
    data = bytearray()
    async for chunk in reply.aiter_bytes():
        if len(data) + len(chunk) > limit:
            raise ValueError("The attachment exceeds the gateway extraction size limit")
        data.extend(chunk)
    return bytes(data)


def document_kind(data, mime):
    if mime == "application/pdf" and data.startswith(b"%PDF-"):
        return "pdf"
    if mime == "image/png" and data.startswith(b"\x89PNG\r\n\x1a\n"):
        if len(data) < 24 or data[12:16] != b"IHDR":
            raise ValueError("Invalid PNG attachment")
        check_dimensions(int.from_bytes(data[16:20]), int.from_bytes(data[20:24]))
        return "image"
    if mime == "image/jpeg" and data.startswith(b"\xff\xd8"):
        check_dimensions(*jpeg_dimensions(data))
        return "image"
    raise ValueError("Document extraction supports PDF, PNG and JPEG attachments")


def check_dimensions(width, height):
    if not 0 < width <= 6000 or not 0 < height <= 6000 or width * height > 16_000_000:
        raise ValueError("The attachment image exceeds the OCR dimension limit")


def jpeg_dimensions(data):
    frame_markers = {0xC0, 0xC1, 0xC2}
    offset = 2
    while offset < len(data) and data[offset] == 0xFF:
        while offset < len(data) and data[offset] == 0xFF:
            offset += 1
        if offset + 2 >= len(data):
            break
        marker = data[offset]
        length = int.from_bytes(data[offset + 1 : offset + 3])
        if marker in {0xDA, 0xD9} or length < 2 or offset + 1 + length > len(data):
            break
        if marker in frame_markers and length >= 8:
            height = int.from_bytes(data[offset + 4 : offset + 6])
            width = int.from_bytes(data[offset + 6 : offset + 8])
            return width, height
        offset += 1 + length
    raise ValueError("Invalid or unsupported JPEG attachment")


async def execute(program, args, data, output_limit):
    path = shutil.which(program)
    if path is None:
        raise ValueError(f"Install {program} on the gateway for document extraction")
    allowed_env = {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TESSDATA_PREFIX", "LANG"}
    env = {key: value for key, value in os.environ.items() if key.upper() in allowed_env}
    child = await asyncio.create_subprocess_exec(
        path,
        *args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=env,
    )
    try:
        async with asyncio.timeout(EXTRACTION_TIMEOUT):

            async def send():
                try:
                    child.stdin.write(data)
                    await child.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    child.stdin.close()

            writer = asyncio.create_task(send())
            result = b""
            try:
                try:
                    result = await child.stdout.readexactly(output_limit + 1)
                except asyncio.IncompleteReadError as incomplete:
                    result = incomplete.partial
                if len(result) <= output_limit:
                    await writer
            finally:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            if len(result) > output_limit:
                raise ValueError("The extraction produced too much output")
            if await child.wait():
                raise ValueError("The gateway could not extract this document page")
            return result
    except TimeoutError:
        raise ValueError("The document extraction timed out") from None
    finally:
        if child.returncode is None:
            try:
                child.kill()
            except ProcessLookupError:
                pass
            await child.stdout.read()
            await child.wait()


async def extract(data, kind, page):
    if kind == "pdf":
        selection = ["-f", str(page), "-l", str(page)]
        # Close the private file before the child opens it on Windows. The context
        # unlinks it after success, errors, and cancellation.
        with NamedTemporaryFile(suffix=".pdf", delete_on_close=False) as source:
            source.write(data)
            source.close()
            text = (
                (
                    await execute(
                        "pdftotext",
                        [*selection, "-layout", "-enc", "UTF-8", source.name, "-"],
                        b"",
                        MAX_TEXT_BYTES,
                    )
                )
                .decode("utf-8", errors="replace")
                .strip()
            )
            if text:
                return text, "pdf-text"
            data = await execute(
                "pdftoppm",
                [*selection, "-singlefile", "-scale-to", "1800", "-png", source.name],
                b"",
                MAX_DOCUMENT_BYTES,
            )
    text = await execute("tesseract", ["stdin", "stdout", "-l", "eng"], data, MAX_TEXT_BYTES)
    return text.decode("utf-8", errors="replace").strip(), "ocr"


class Documents:
    def __init__(self):
        self.cache = OrderedDict()

    async def read(self, data, mime, args):
        if len(data) > MAX_DOCUMENT_BYTES:
            raise ValueError("The attachment exceeds 8 MiB")
        kind = document_kind(data, mime)
        page, offset = args.get("page", 1), args.get("offset", 0)
        if not 1 <= page <= 100 or (kind == "image" and page != 1):
            raise ValueError("Choose page 1 for images or a PDF page from 1 to 100")
        key = (hashlib.sha256(data).digest(), kind, page)
        async with _SLOTS:
            result = self.cache.pop(key, None)
            if result is None:
                result = await extract(data, kind, page)
            self.cache[key] = result
            if len(self.cache) > 8:
                self.cache.popitem(last=False)
        text, method = result
        if not 0 <= offset <= len(text):
            raise ValueError("The offset is outside the extracted page")
        end = offset + PAGE_CHARS
        return {
            "page": page,
            "method": method,
            "markdown": text[offset:end],
            "nextOffset": end if end < len(text) else None,
        }
