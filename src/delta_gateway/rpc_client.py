"""Correlated JSONL client for a separately installed Delta engine."""

import asyncio
import contextlib
import importlib.util
import logging
import os
import re
from collections import deque
from pathlib import Path
from uuid import uuid4

from delta_gateway.errors import RequestRejected, WorkerLost
from delta_gateway.instances import token_variables

LIMIT = 16 * 1024 * 1024
log = logging.getLogger(__name__)


def engine_command(config, session=None, *, factory=None):
    command = list(config.engine_command)
    if factory:
        module, name = factory.split(":", 1)
        if not re.fullmatch(r"[A-Za-z_][\w.]*", module) or not name.isidentifier():
            raise ValueError("Invalid test factory")
        spec = importlib.util.find_spec(module)
        if not spec or not spec.origin:
            raise ValueError("Test factory module was not found")
        root = str(Path(spec.origin).parents[len(module.split(".")) - 1])
        command = [
            command[0],
            "-c",
            (
                "import importlib,sys; "
                f"sys.path.insert(0,{root!r}); "
                "import delta_app.runtime as runtime; "
                f"factory=getattr(importlib.import_module({module!r}),{name!r}); "
                "original=factory; "
                "exec('async def build(options):\\n options.session=options.resume"
                "\\n return await original(options)'); "
                "runtime.build_session=build; "
                "from delta_app.cli.main import main; raise SystemExit(main())"
            ),
        ]
    command += [
        "--mode",
        "rpc",
        "--cwd",
        str(config.project),
        "--session-dir",
        str(config.sessions_path),
        "--tool-policy",
        config.policy,
        "--approval-seconds",
        str(config.approval_seconds),
    ]
    command += ["--approve" if config.trust_project else "--no-approve"]
    if config.agent_dir:
        command += ["--agent-dir", str(config.agent_dir)]
    if session:
        command += ["--session", str(session)]
    for name, value in (
        ("provider", "scripted" if factory else config.provider),
        ("model", config.model),
        ("thinking", config.thinking),
    ):
        if value:
            command += ["--" + name, value]
    return command + list(config.engine_args)


class RpcClient:
    def __init__(
        self,
        config,
        session=None,
        *,
        factory=None,
        timeout=30,
        grace=10,
        command=None,
        environment=None,
        paths=(),
        on_close=None,
    ):
        self.config, self.timeout, self.grace = config, timeout, grace
        self.command = command or engine_command(config, session, factory=factory)
        self.environment = environment or {}
        self.paths, self.on_close = list(paths), on_close
        self.process = self.reader = self.stderr_reader = None
        self.pending = {}
        self.events = asyncio.Queue()
        self.write_lock = asyncio.Lock()
        self.failure = None
        self.state = {}
        self.diagnostics = deque(maxlen=100)
        self.hidden = {
            value
            for key, value in os.environ.items()
            if any(word in key.upper() for word in ("TOKEN", "KEY", "PASSWORD", "SECRET"))
            and len(value) >= 4
        }
        if config.token():
            self.hidden.add(config.token())

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.close()

    def redact(self, line):
        for value in sorted(self.hidden, key=len, reverse=True):
            line = line.replace(value, "[redacted]")
        line = re.sub(
            r"(?i)(bearer\s+|(?:api[_-]?key|token|password|secret)[\s=:]+)[^\s,;]+",
            r"\1[redacted]",
            line,
        )
        line = re.sub(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@", r"\1[redacted]@", line)
        line = re.sub(
            r"(?i)([?&](?:key|api_key|token|access_token)=)[^&\s]+", r"\1[redacted]", line
        )
        return line[:2000]

    async def start(self):
        try:
            private = token_variables(self.config)
            self.process = await asyncio.create_subprocess_exec(
                *self.command,
                cwd=str(self.config.project),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=LIMIT,
                env={
                    **{k: v for k, v in os.environ.items() if k not in private},
                    **self.environment,
                },
            )
            self.reader = asyncio.create_task(self.read())
            self.stderr_reader = asyncio.create_task(self.read_stderr())
            capabilities = await self.request("get_capabilities")
            required = {
                "prompt",
                "get_state",
                "abort",
                "new_session",
                "clone",
                "export_html",
                "compact",
            }
            if (
                capabilities.get("engine") != "delta"
                or capabilities.get("protocolVersion") != 1
                or not required.issubset(capabilities.get("commands", []))
                or "agent_settled" not in capabilities.get("events", [])
            ):
                raise WorkerLost(
                    "Installed Delta lacks compatible native RPC; install the matching engine build"
                )
            self.state = await self.request("get_state")
            return self
        except BaseException:
            await self.close()
            raise

    def translate(self, value):
        """Rewrite file paths reported from inside a sandbox to their host locations."""
        if isinstance(value, list):
            for item in value:
                self.translate(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                if key in {"sessionFile", "path"} and isinstance(item, str):
                    for inner, outer in self.paths:
                        if item == inner or item.startswith(inner + "/"):
                            value[key] = outer + item[len(inner) :]
                            break
                else:
                    self.translate(item)

    def fail(self):
        if not self.failure:
            self.failure = WorkerLost("Delta RPC connection closed; work was not replayed")
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(self.failure)
            self.events.put_nowait(None)

    async def read(self):
        try:
            while line := await self.process.stdout.readline():
                if len(line) > LIMIT or not line.endswith(b"\n"):
                    raise ValueError("Invalid engine framing")
                record = __import__("json").loads(line.decode("utf-8"))
                if not isinstance(record, dict):
                    raise ValueError("Invalid engine record")
                if self.paths:
                    self.translate(record)
                if record.get("type") == "response":
                    future = self.pending.get(record.get("id"))
                    if future and not future.done():
                        future.set_result(record)
                else:
                    if record.get("type") == "agent_settled" and record.get("state"):
                        self.state = record["state"]
                    self.events.put_nowait(record)
        except (ValueError, UnicodeError, OSError, TypeError, KeyError):
            pass
        finally:
            self.fail()

    async def read_stderr(self):
        try:
            while line := await self.process.stderr.readline():
                safe = self.redact(line.decode("utf-8", errors="replace").strip())
                self.diagnostics.append(safe)
                log.info("Delta: %s", safe)
        except (OSError, ValueError):
            log.info("Delta diagnostics stream closed")

    async def request(self, command, *, timeout="default", **arguments):
        import json

        if self.failure:
            raise self.failure
        identifier = uuid4().hex
        payload = (
            json.dumps(dict(type=command, id=identifier, **arguments), ensure_ascii=True) + "\n"
        ).encode()
        if len(payload) > LIMIT:
            raise ValueError("Request exceeds the engine record limit")
        future = asyncio.get_running_loop().create_future()
        self.pending[identifier] = future
        try:
            deadline = self.timeout if timeout == "default" else timeout
            async with asyncio.timeout(deadline):
                async with self.write_lock:
                    self.process.stdin.write(payload)
                    await self.process.stdin.drain()
                record = await future
            if not record.get("success"):
                raise RequestRejected(
                    {
                        "code": "rejected",
                        "message": self.redact(record.get("error", "Engine rejected request")),
                    }
                )
            return record.get("data")
        except (TimeoutError, BrokenPipeError, ConnectionResetError):
            await self.terminate()
            raise WorkerLost("Delta RPC deadline exceeded; work was not replayed") from None
        finally:
            self.pending.pop(identifier, None)
            if not future.done():
                future.cancel()

    def clear_events(self):
        while not self.events.empty():
            self.events.get_nowait()

    async def next_event(self):
        if self.failure and self.events.empty():
            raise self.failure
        event = await self.events.get()
        if event is None:
            raise self.failure
        return event

    async def abort(self):
        await self.request("abort")
        async with asyncio.timeout(self.grace):
            while True:
                self.state = await self.request("get_state")
                if self.state.get("operation") is None:
                    return
                await asyncio.sleep(0.01)

    async def terminate(self):
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), self.grace)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()

    async def close(self):
        if self.on_close:
            self.on_close()
            self.on_close = None
        if not self.process:
            return
        if self.process.returncode is None:
            with contextlib.suppress(RuntimeError, OSError, TimeoutError):
                await self.request("shutdown", timeout=self.grace)
            if self.process.stdin:
                self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), self.grace)
            except TimeoutError:
                await self.terminate()
        await asyncio.gather(
            *(t for t in (self.reader, self.stderr_reader) if t), return_exceptions=True
        )
