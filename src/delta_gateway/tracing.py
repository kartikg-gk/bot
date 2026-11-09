"""Opt-in, metadata-only export of gateway operation traces."""

import asyncio
import logging
import os
import re
import secrets
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from urllib.parse import urlsplit

import httpx

from delta_gateway.run_usage import RunUsage

PUBLIC_KEY = "DELTA_GATEWAY_LANGFUSE_PUBLIC_KEY"
SECRET_KEY = "DELTA_GATEWAY_LANGFUSE_SECRET_KEY"
BASE_URL = "DELTA_GATEWAY_LANGFUSE_URL"
MAX_SPANS = 256
MAX_PENDING_EXPORTS = 16
IDENTIFIER = re.compile(r"[a-zA-Z0-9._:/-]{1,120}\Z")
logger = logging.getLogger(__name__)
current = ContextVar("gateway_trace", default=None)


def attribute(key, value):
    if isinstance(value, int):
        wrapped = {"intValue": str(value)}
    elif isinstance(value, float):
        wrapped = {"doubleValue": value}
    else:
        wrapped = {"stringValue": value}
    return {"key": key, "value": wrapped}


class Trace:
    def __init__(self, name):
        self.id = secrets.token_hex(16)
        self.name = name
        self.root = self.start(name, "agent", None)
        self.spans = []
        self.open = {}
        self.closed = self.failed = False

    def start(self, name, kind, parent):
        span = {
            "traceId": self.id,
            "spanId": secrets.token_hex(8),
            "name": name,
            "kind": 1,
            "startTimeUnixNano": str(time.time_ns()),
            "attributes": [
                attribute("langfuse.observation.type", kind),
                attribute("langfuse.trace.name", self.name),
            ],
        }
        if parent:
            span["parentSpanId"] = parent["spanId"]
        return span

    def finish(self, span, failed=False, **attrs):
        if len(self.spans) >= MAX_SPANS and span is not self.root:
            return
        span["endTimeUnixNano"] = str(max(time.time_ns(), int(span["startTimeUnixNano"])))
        span["status"] = {"code": 2 if failed else 1}
        span["attributes"].extend(attribute(key, value) for key, value in attrs.items())
        self.spans.append(span)

    def observe(self, parent, source, event):
        if self.closed:
            return
        kind = event.get("type")
        if kind == "agent_settled" and event.get("outcome") != "completed":
            self.failed = True
        if kind in {"message_start", "message_end"}:
            message = event.get("message") or {}
            if message.get("role") != "assistant":
                return
            model = message.get("model", "")
            model = model if IDENTIFIER.fullmatch(model) else "unknown"
            key = source, "generation"
            if kind == "message_start" and len(self.open) < MAX_SPANS:
                self.open[key] = self.start("model", "generation", parent)
            if kind == "message_end":
                span = self.open.pop(key, None) or self.start("model", "generation", parent)
                usage = RunUsage()
                usage.observe(event)
                self.finish(
                    span,
                    message.get("stopReason") == "error",
                    **{
                        "gen_ai.response.model": model,
                        "gen_ai.usage.input_tokens": usage.input,
                        "gen_ai.usage.output_tokens": usage.output,
                        "gen_ai.usage.cache_read.input_tokens": usage.cache_read,
                        "gen_ai.usage.cache_creation.input_tokens": usage.cache_write,
                        "delta.reported_cost_usd": usage.cost_usd,
                    },
                )
        if kind in {"tool_execution_start", "tool_execution_end"}:
            tool = event.get("toolName") or ""
            tool = tool if IDENTIFIER.fullmatch(tool) else "unknown"
            key = source, event.get("toolCallId")
            if kind.endswith("start") and len(self.open) < MAX_SPANS:
                self.open[key] = self.start(tool, "tool", parent)
            elif kind.endswith("end"):
                span = self.open.pop(key, None) or self.start(tool, "tool", parent)
                self.finish(span, bool(event.get("isError")))


class TraceExporter:
    def __init__(self, base, public, secret, *, transport=None):
        url = urlsplit(base)
        local = url.hostname in {"localhost", "127.0.0.1", "::1"}
        if (
            not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or not (url.scheme == "https" or local and url.scheme == "http")
        ):
            raise ValueError("Use an HTTPS Langfuse URL or an HTTP loopback URL")
        if not public or not secret:
            raise ValueError("Provide both gateway Langfuse keys")
        self.endpoint = base.rstrip("/") + "/api/public/otel/v1/traces"
        self.http = httpx.AsyncClient(
            auth=(public, secret),
            headers={"x-langfuse-ingestion-version": "4"},
            timeout=3,
            trust_env=False,
            transport=transport,
        )
        self.pending = set()

    @classmethod
    def from_env(cls):
        if base := os.environ.get(BASE_URL):
            return cls(base, os.environ.get(PUBLIC_KEY), os.environ.get(SECRET_KEY))
        return None

    async def send(self, trace):
        body = {
            "resourceSpans": [
                {
                    "resource": {"attributes": [attribute("service.name", "delta-gateway")]},
                    "scopeSpans": [{"scope": {"name": "delta-gateway"}, "spans": trace.spans}],
                }
            ]
        }
        try:
            response = await self.http.post(self.endpoint, json=body)
            response.raise_for_status()
        except httpx.HTTPError:
            logger.warning("Gateway trace export failed")

    def enqueue(self, trace):
        if len(self.pending) < MAX_PENDING_EXPORTS:
            task = asyncio.create_task(self.send(trace))
            self.pending.add(task)
            task.add_done_callback(self.pending.discard)

    async def close(self):
        if self.pending:
            _, pending = await asyncio.wait(tuple(self.pending), timeout=4)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        await self.http.aclose()


def traced(name):
    def decorate(method):
        @wraps(method)
        async def wrapped(service, *args, **kwargs):
            if service.tracer is None:
                return await method(service, *args, **kwargs)
            label = (
                "compaction"
                if name == "control" and args and args[0].text.split(maxsplit=1)[0] == "/compact"
                else name
            )
            trace = Trace(label)
            token = current.set((trace, trace.root))
            failed = True
            try:
                result = await method(service, *args, **kwargs)
                failed = isinstance(result, dict) and result.get("status") != "completed"
                return result
            finally:
                current.reset(token)
                for span in trace.open.values():
                    trace.finish(span, True)
                trace.finish(trace.root, failed or trace.failed)
                trace.closed = True
                service.tracer.enqueue(trace)

        return wrapped

    return decorate


@contextmanager
def observation(name, kind):
    state = current.get()
    if state is None or state[0].closed:
        yield
        return
    trace, parent = state
    span = trace.start(name, kind, parent)
    token = current.set((trace, span))
    failed = True
    try:
        yield
        failed = False
    finally:
        current.reset(token)
        if not trace.closed:
            trace.finish(span, failed)


def observe(source, event):
    if state := current.get():
        trace, parent = state
        trace.observe(parent, source, event)
