"""Versioned, LF-delimited worker records."""

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

MAX_RECORD = 16 * 1024 * 1024


class Request(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    version: Literal[1]
    kind: Literal["request"]
    request_id: str = Field(min_length=1, max_length=128)
    operation: str
    arguments: dict[str, Any] = Field(default_factory=dict)


def encode(record: dict) -> bytes:
    data = json.dumps({"version": 1, **record}, ensure_ascii=True).encode() + b"\n"
    if len(data) > MAX_RECORD:
        raise ValueError("record too large")
    return data


def decode(data: bytes) -> dict:
    if len(data) > MAX_RECORD or not data.endswith(b"\n"):
        raise ValueError("invalid record framing")
    record = json.loads(data)
    if not isinstance(record, dict) or type(record.get("version")) is not int:
        raise ValueError("invalid record")
    if record["version"] != 1:
        raise ValueError("unsupported protocol version")
    return record
