"""Asynchronous client for a single session worker."""

import asyncio
import contextlib
import os
import sys
from uuid import uuid4

from delta_gateway.protocol import MAX_RECORD, decode, encode


class WorkerLost(RuntimeError):
    pass


class RequestRejected(RuntimeError):
    def __init__(self, error):
        self.code = error["code"]
        super().__init__(error["message"])


class WorkerClient:
    def __init__(self, command, *, timeout=30.0, grace=5.0, private_env=()):
        self.command = command
        self.timeout = timeout
        self.grace = grace
        self.process = None
        self.reader = None
        self.pending = {}
        self.events = asyncio.Queue()
        self.failure = None
        self.write_lock = asyncio.Lock()
        self.finished = {}
        self.private_env = {"TELEGRAM_BOT_TOKEN", *private_env}

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.close()

    async def start(self):
        self.ready = asyncio.get_running_loop().create_future()
        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            # Worker diagnostics are generic and do not include provider exception bodies.
            stderr=asyncio.subprocess.DEVNULL,
            limit=MAX_RECORD,
            env={k: v for k, v in os.environ.items() if k not in self.private_env},
        )
        self.reader = asyncio.create_task(self._read())
        try:
            self.state = await asyncio.wait_for(asyncio.shield(self.ready), self.timeout)
        except BaseException:
            if not self.ready.done():
                self.ready.cancel()
            await self._terminate()
            if self.reader:
                await self.reader
            raise
        return self

    def _fail(self):
        if self.failure:
            return
        self.failure = WorkerLost("Worker connection closed; task was not replayed")
        if not self.ready.done():
            self.ready.set_exception(self.failure)
        for future in self.pending.values():
            if not future.done():
                future.set_exception(self.failure)
        self.events.put_nowait(None)

    async def _read(self):
        try:
            while line := await self.process.stdout.readline():
                record = decode(line)
                if record.get("kind") == "response":
                    future = self.pending.get(record.get("request_id"))
                    if future is not None and not future.done():
                        future.set_result(record)
                elif record.get("kind") == "event":
                    if record.get("event") == "worker.ready":
                        if self.ready.done():
                            raise ValueError("duplicate readiness event")
                        self.ready.set_result(record)
                    else:
                        if record.get("event") == "run.finished":
                            if record.get("state"):
                                self.state.update(record["state"])
                            self.finished[record["run_id"]] = record
                            if len(self.finished) > 128:
                                self.finished.pop(next(iter(self.finished)))
                        self.events.put_nowait(record)
                else:
                    raise ValueError("invalid worker record")
        except (ValueError, UnicodeError, OSError, KeyError, TypeError):
            pass
        finally:
            self._fail()

    async def request(self, operation, **arguments):
        if self.failure:
            raise self.failure
        request_id = uuid4().hex
        data = encode(
            dict(kind="request", request_id=request_id, operation=operation, arguments=arguments)
        )
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            async with asyncio.timeout(self.timeout):
                async with self.write_lock:
                    self.process.stdin.write(data)
                    await self.process.stdin.drain()
                record = await future
            if not record["ok"]:
                raise RequestRejected(record["error"])
            return record["result"]
        except (TimeoutError, BrokenPipeError, ConnectionResetError):
            await self._terminate()
            raise WorkerLost("Worker did not respond; task was not replayed") from None
        finally:
            self.pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def next_event(self):
        if self.failure and self.events.empty():
            raise self.failure
        event = await self.events.get()
        if event is None:
            raise self.failure
        return event

    async def cancel(self):
        state = await self.request("cancel")
        run_id = state["run_id"]
        if run_id is None:
            return
        try:
            async with asyncio.timeout(self.grace):
                while True:
                    if run_id in self.finished:
                        return self.finished.pop(run_id)
                    if self.failure:
                        raise self.failure
                    await asyncio.sleep(0.01)
        except TimeoutError:
            await self._terminate()
            raise WorkerLost("Worker did not cancel within the grace period") from None

    async def _terminate(self):
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), self.grace)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()

    async def close(self):
        if not self.process:
            return
        if self.process.returncode is None:
            with contextlib.suppress(WorkerLost, RequestRejected, OSError, TimeoutError):
                await asyncio.wait_for(self.request("shutdown"), self.grace)
            if self.process.stdin:
                self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), self.grace)
            except TimeoutError:
                await self._terminate()
        if self.reader:
            await self.reader


def worker_command(*arguments):
    return [sys.executable, "-m", "delta_gateway", "worker", *map(str, arguments)]
