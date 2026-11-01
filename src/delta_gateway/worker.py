"""Headless adapter; only this process imports and owns a Delta session."""

import asyncio
import contextlib
import importlib
import inspect
import os
import sys
import threading
from pathlib import Path
from uuid import uuid4

from delta_gateway.ownership import SessionLock
from delta_gateway.protocol import MAX_RECORD, Request, decode, encode
from delta_gateway.session_controls import (
    ControlError,
    change_model,
    change_thinking,
    clone,
    compact,
    html_export,
    list_models,
    prepare_provider,
    require_text,
    snapshot,
)


async def build_session(options):
    from delta_app.cli.main import _install_safety, _resolve_provider_name
    from delta_app.runtime import build_session as build
    from delta_app.safety import ApprovalPolicy

    options.resume = options.session
    options.no_approve = False
    options.approve = options.trust_project
    # Select credentials for this provider before Delta maps them to shared
    # OpenAI-compatible variables; another provider's inherited key must not win.
    prepare_provider(_resolve_provider_name(options.provider), dict(os.environ))
    session = await build(options)
    _install_safety(
        session.harness,
        ApprovalPolicy.AUTO,
    )
    return session


class Worker:
    def __init__(
        self, session, output, policy="deny", expiry=300, *, directory=None, environment=None
    ):
        from delta_gateway.approvals import ApprovalGate

        self.session = session
        self.output = output
        self.task = None
        self.run_id = None
        self.cancelled = False
        self.operation = None
        self.directory = directory
        self.environment = environment or dict(os.environ)
        self.gate = ApprovalGate(self, policy, expiry)
        self.session.harness.settings.before_tool_call = self.gate.check

    def send(self, **record):
        self.output.write(encode(record))
        self.output.flush()

    def status(self):
        return dict(**snapshot(self.session), run_id=self.run_id, operation=self.operation)

    def watch_task(self, run_id):
        def finished(task):
            # Cancellation can arrive before the task body has entered its finally block.
            if self.run_id == run_id:
                self.run_id = None
                self.operation = None
                self.session.flush_queues()
                self.send(
                    kind="event",
                    event="run.finished",
                    run_id=run_id,
                    outcome="cancelled" if task.cancelled() else "failed",
                    session_id=self.session.session_id,
                    state=self.status(),
                )

        self.task.add_done_callback(finished)

    async def run(self, text, run_id):
        from delta_harness.contracts.transcript import ModelEntry

        outcome = "completed"
        previous = ""
        has_text = False

        async def events():
            stream = self.session.submit(text)
            while True:
                async for event in stream:
                    yield event
                if not self.session.pending.count or self.cancelled or outcome != "completed":
                    break
                stream = self.session.resume_run()

        try:
            async for event in events():
                if event.type == "message_start":
                    if isinstance(event.message, ModelEntry) and has_text:
                        self.send(kind="event", event="assistant.text", run_id=run_id, text="\n\n")
                    previous = ""
                if event.type in ("message_update", "message_end"):
                    message = event.message
                    if isinstance(message, ModelEntry):
                        current = message.text
                        # Each event is a snapshot. Emit only the newly appended text.
                        if current.startswith(previous) and len(current) > len(previous):
                            delta = current[len(previous) :]
                            has_text = True
                            # Bound wire records even when an adapter returns a large snapshot.
                            for index in range(0, len(delta), 64000):
                                self.send(
                                    kind="event",
                                    event="assistant.text",
                                    run_id=run_id,
                                    text=delta[index : index + 64000],
                                )
                        previous = current
                        if event.type == "message_end":
                            if message.stop_reason == "error":
                                outcome = "failed"
                            elif message.stop_reason == "aborted":
                                outcome = "cancelled"
                elif event.type in ("tool_execution_start", "tool_execution_end"):
                    self.send(
                        kind="event",
                        event=("tool.started" if event.type.endswith("start") else "tool.finished"),
                        run_id=run_id,
                        tool=event.tool_name,
                        failed=getattr(event, "is_error", False),
                    )
        except asyncio.CancelledError:
            outcome = "cancelled"
        except Exception:
            outcome = "failed"
        finally:
            if self.cancelled:
                outcome = "cancelled"
            if outcome != "completed":
                self.session.flush_queues()
            self.run_id = None
            self.operation = None
            self.send(
                kind="event",
                event="run.finished",
                run_id=run_id,
                outcome=outcome,
                session_id=self.session.session_id,
                state=self.status(),
            )

    async def control(self, operation, arguments, run_id):
        result, outcome = {}, "completed"
        try:
            if operation == "name":
                title = require_text(arguments, "title", limit=120)
                await self.session.handle_command("/name " + title)
                result = {"message": "Session named: " + title}
            elif operation == "compact":
                result = await compact(
                    self.session, require_text(arguments, "instructions", optional=True)
                )
            elif operation == "clone":
                result = await clone(self.session, self.directory)
            elif operation == "export":
                result = {"document": html_export(self.session)}
            elif operation == "model":
                result = await change_model(
                    self.session, require_text(arguments, "selection", limit=500), self.environment
                )
                result["message"] = f"Model: {self.session.provider_name}/{self.session.model}"
            elif operation == "thinking":
                result = await change_thinking(
                    self.session, require_text(arguments, "level", limit=20)
                )
                result["message"] = "Thinking: " + result["thinking"]
                if result["requested_thinking"] == "max":
                    result["message"] += " (max uses this engine's highest level, xhigh)"
            elif operation == "command":
                text = require_text(arguments, "text")
                if text.split(maxsplit=1)[0] not in {
                    "/system",
                    "/skills",
                    "/tools",
                    "/prompts",
                    "/plan",
                    "/help",
                    "/stats",
                }:
                    raise ControlError(
                        "Use the gateway's session controls or /delta "
                        "with a prompt, skill, or template"
                    )
                result = {
                    "message": await self.session.handle_command(text) or "Command unavailable"
                }
        except asyncio.CancelledError:
            outcome = "cancelled"
        except ControlError as exc:
            outcome, result = "failed", {"message": str(exc)}
        except Exception:
            outcome, result = (
                "failed",
                {"message": "Operation failed; inspect local configuration or session state"},
            )
        finally:
            if self.cancelled:
                outcome = "cancelled"
            self.run_id = None
            self.operation = None
            self.send(
                kind="event",
                event="run.finished",
                run_id=run_id,
                outcome=outcome,
                operation=operation,
                result=result,
                session_id=self.session.session_id,
                state=self.status(),
            )

    async def dispatch(self, record):
        request_id = record.get("request_id")
        try:
            request = Request.model_validate(record)
            args = request.arguments
            operation = request.operation
            fields = {
                "submit": {"text"},
                "approval.respond": {"approval_id", "allow"},
                "name": {"title"},
                "compact": {"instructions"},
                "clone": set(),
                "export": set(),
                "model": {"selection"},
                "thinking": {"level"},
                "queue": {"text"},
                "steer": {"text"},
                "command": {"text"},
                "models": {"provider", "query", "offset"},
                "status": set(),
                "cancel": set(),
                "shutdown": set(),
            }
            if operation not in fields or set(args) - fields[operation]:
                raise ValueError("unexpected arguments")
            if request.operation == "status":
                result = self.status()
            elif operation == "models":
                provider = require_text(args, "provider", optional=True, limit=100)
                query = require_text(args, "query", optional=True, limit=200)
                offset = args.get("offset", 0)
                if type(offset) is not int or offset < 0:
                    raise ControlError("Invalid model page")
                result = list_models(provider, query, offset)
            elif operation in {"queue", "steer"}:
                text = require_text(args, "text")
                if (
                    not self.task
                    or self.task.done()
                    or self.operation != "submit"
                    or not self.session.active
                ):
                    self.send(
                        kind="response",
                        request_id=request_id,
                        ok=False,
                        error={"code": "not_running", "message": "No active prompt"},
                    )
                    return True
                if self.session.pending.count >= 10:
                    raise ControlError("Agent queue is full")
                (self.session.enqueue if operation == "queue" else self.session.inject)(text)
                result = {"run_id": self.run_id, "pending": self.session.pending.count}
            elif request.operation == "approval.respond":
                if set(args) != {"approval_id", "allow"}:
                    raise ValueError("invalid approval arguments")
                self.gate.respond(args["approval_id"], args["allow"])
                result = {}
            elif request.operation == "submit":
                if (
                    set(args) != {"text"}
                    or not isinstance(args["text"], str)
                    or not args["text"].strip()
                ):
                    raise ValueError("text must be nonempty")
                if self.task and not self.task.done():
                    self.send(
                        kind="response",
                        request_id=request_id,
                        ok=False,
                        error={"code": "busy", "message": "A run is active"},
                    )
                    return True
                self.cancelled = False
                self.operation = "submit"
                self.run_id = uuid4().hex
                result = {"run_id": self.run_id}
                self.task = asyncio.create_task(self.run(args["text"], self.run_id))
                self.watch_task(self.run_id)
            elif operation in {
                "name",
                "compact",
                "clone",
                "export",
                "model",
                "thinking",
                "command",
            }:
                if self.task and not self.task.done():
                    self.send(
                        kind="response",
                        request_id=request_id,
                        ok=False,
                        error={
                            "code": "busy",
                            "message": "Use /stop or wait for the active operation",
                        },
                    )
                    return True
                self.cancelled = False
                self.operation = operation
                self.run_id = uuid4().hex
                result = {"run_id": self.run_id}
                self.task = asyncio.create_task(self.control(operation, args, self.run_id))
                self.watch_task(self.run_id)
            elif request.operation == "cancel":
                self.session.flush_queues()
                if self.task and not self.task.done():
                    self.cancelled = True
                    self.session.abort()
                    self.task.cancel()
                result = self.status()
            elif request.operation == "shutdown":
                await self.stop()
                self.send(kind="response", request_id=request_id, ok=True, result={})
                return False
            else:
                raise ValueError("unknown operation")
            self.send(kind="response", request_id=request_id, ok=True, result=result)
        except ControlError as exc:
            self.send(
                kind="response",
                request_id=request_id,
                ok=False,
                error={"code": "invalid_request", "message": str(exc)},
            )
        except (ValueError, TypeError):
            self.send(
                kind="response",
                request_id=request_id if isinstance(request_id, str) else None,
                ok=False,
                error={"code": "invalid_request", "message": "Invalid request"},
            )
        return True

    async def stop(self):
        self.session.flush_queues()
        if self.task and not self.task.done():
            self.cancelled = True
            self.session.abort()
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task


async def serve(options):
    original_environment = dict(os.environ)
    output = sys.stdout.buffer
    # Keep library output, including startup notices, outside the protocol stream.
    sys.stdout = sys.stderr
    os.chdir(options.cwd)
    directory = Path(options.session_dir).resolve()
    lock = None
    session = None
    worker = None
    try:
        if options.session:
            lock = SessionLock(directory / "locks", options.session)
        factory = build_session
        if options.session_factory:
            module, name = options.session_factory.split(":", 1)
            factory = getattr(importlib.import_module(module), name)
        session = await factory(options)
        if Path(session.cwd).resolve() != options.cwd:
            raise ValueError("session working directory differs from worker directory")
        if lock is None:
            lock = SessionLock(directory / "locks", session.session_id)
        policy = "auto" if options.allow_mutations else options.policy
        worker = Worker(
            session,
            output,
            policy,
            options.approval_seconds,
            directory=directory,
            environment=original_environment,
        )
        worker.send(kind="event", event="worker.ready", **worker.status())
        queue = asyncio.Queue(maxsize=32)
        loop = asyncio.get_running_loop()

        def read_input():
            # A daemon reader avoids Windows pipe limitations and executor shutdown hangs.
            while True:
                line = sys.stdin.buffer.readline(MAX_RECORD + 1)
                try:
                    future = asyncio.run_coroutine_threadsafe(queue.put(line), loop)
                    future.result()
                except RuntimeError:
                    return
                if not line or len(line) > MAX_RECORD or not line.endswith(b"\n"):
                    return

        threading.Thread(target=read_input, daemon=True).start()
        while True:
            line = await queue.get()
            if not line:
                break
            if len(line) > MAX_RECORD or not line.endswith(b"\n"):
                break
            try:
                record = decode(line)
            except (ValueError, UnicodeError):
                worker.send(
                    kind="response",
                    request_id=None,
                    ok=False,
                    error={"code": "invalid_request", "message": "Invalid JSON record"},
                )
                continue
            if not await worker.dispatch(record):
                break
    finally:
        if worker:
            await worker.stop()
        if session:
            try:
                await session.shutdown()
            finally:
                close = getattr(session.harness.settings.provider, "close", None)
                if close:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
        if lock:
            lock.close()


def run_worker(options):
    with contextlib.suppress(KeyboardInterrupt):
        try:
            asyncio.run(serve(options))
        except Exception:
            # Provider exceptions may contain credential-bearing URLs or response bodies.
            print("Worker failed; check configuration and session ownership.", file=sys.stderr)
            raise SystemExit(1) from None
