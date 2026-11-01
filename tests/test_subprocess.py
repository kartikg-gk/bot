import asyncio
import sys

import pytest

from delta_gateway.client import RequestRejected, WorkerClient, WorkerLost, worker_command
from delta_gateway.protocol import MAX_RECORD, decode, encode


def command(tmp_path, session=None):
    args = [
        "--cwd",
        str(tmp_path),
        "--session-dir",
        str(tmp_path / "sessions"),
        "--session-factory",
        "tests.fake_session:create",
    ]
    if session:
        args += ["--session", session]
    return worker_command(*args)


async def finish(client):
    events = []
    async with asyncio.timeout(10):
        while True:
            event = await client.next_event()
            events.append(event)
            if event["event"] == "run.finished":
                return events


async def test_stream_two_turns_and_resume(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        session = client.state["session_id"]
        for count in (1, 2):
            result = await client.request("submit", text="hello")
            events = await finish(client)
            assert events[0]["event"] == "assistant.text"
            assert "".join(e.get("text", "") for e in events) == f"turn {count}: done"
            assert events[-1]["run_id"] == result["run_id"]
            assert events[-1]["outcome"] == "completed"
        assert (await client.request("status"))["run_id"] is None
    assert client.process.returncode == 0
    async with WorkerClient(command(tmp_path, session)) as resumed:
        await resumed.request("submit", text="again")
        assert "".join(e.get("text", "") for e in await finish(resumed)) == "turn 3: done"


async def test_cancel_busy_and_reuse(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        await client.request("submit", text="slow")
        assert (await client.next_event())["event"] == "assistant.text"
        with pytest.raises(RequestRejected) as error:
            await client.request("submit", text="hello")
        assert error.value.code == "busy"
        result = await client.cancel()
        assert result["outcome"] == "cancelled"
        assert (await finish(client))[-1]["outcome"] == "cancelled"
        await client.request("submit", text="hello")
        assert (await finish(client))[-1]["outcome"] == "completed"


async def test_malformed_input_then_valid(tmp_path):
    process = await asyncio.create_subprocess_exec(
        *command(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=MAX_RECORD,
    )
    try:
        assert decode(await process.stdout.readline())["event"] == "worker.ready"
        process.stdin.write(b"not-json\n")
        await process.stdin.drain()
        assert not decode(await process.stdout.readline())["ok"]
        process.stdin.write(encode(dict(kind="request", request_id="1", operation="unknown")))
        await process.stdin.drain()
        assert not decode(await process.stdout.readline())["ok"]
        process.stdin.write(encode(dict(kind="request", request_id="2", operation="status")))
        await process.stdin.drain()
        assert decode(await process.stdout.readline())["ok"]
        process.stdin.close()
        assert await asyncio.wait_for(process.wait(), 5) == 0
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def test_crash_unblocks_event_and_pending_request(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        await client.request("submit", text="slow")
        await client.next_event()
        client.process.kill()
        with pytest.raises(WorkerLost):
            await asyncio.wait_for(client.next_event(), 5)
        with pytest.raises(WorkerLost):
            await client.request("status")


async def test_session_lock_and_release_after_crash(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        session = client.state["session_id"]
        await client.request("submit", text="hello")
        await finish(client)
        with pytest.raises(WorkerLost):
            async with WorkerClient(command(tmp_path, session)):
                pass
        client.process.kill()
        await client.process.wait()
    async with WorkerClient(command(tmp_path, session)) as resumed:
        assert resumed.state["session_id"] == session


async def test_errors_do_not_expose_provider_exception(tmp_path):
    async with WorkerClient(command(tmp_path)) as client:
        await client.request("submit", text="fail")
        events = await finish(client)
        assert events[-1]["outcome"] == "failed"
        assert "secret-that" not in str(events)


async def test_close_active_run_reaps_child(tmp_path):
    client = await WorkerClient(command(tmp_path)).start()
    await client.request("submit", text="slow")
    await client.next_event()
    await client.close()
    assert client.process.returncode == 0


async def test_startup_deadline_reaps_unresponsive_process():
    client = WorkerClient([sys.executable, "-c", "import time; time.sleep(60)"], timeout=0.2)
    with pytest.raises(TimeoutError):
        await client.start()
    assert client.process.returncode is not None


async def test_request_deadline_does_not_leave_child_alive():
    script = (
        "import time; "
        'print(\'{"version":1,"kind":"event","event":"worker.ready"}\', flush=True); '
        "time.sleep(60)"
    )
    async with WorkerClient([sys.executable, "-c", script], timeout=2) as client:
        client.timeout = 0.1
        with pytest.raises(WorkerLost):
            await client.request("status")
        assert client.process.returncode is not None


async def test_crash_fails_an_inflight_request():
    script = (
        "import time; "
        'print(\'{"version":1,"kind":"event","event":"worker.ready"}\', flush=True); '
        "time.sleep(60)"
    )
    async with WorkerClient([sys.executable, "-c", script]) as client:
        pending = asyncio.create_task(client.request("status"))
        await asyncio.sleep(0.03)
        client.process.kill()
        with pytest.raises(WorkerLost):
            await asyncio.wait_for(pending, 3)


async def test_cancel_grace_reaps_unresponsive_worker():
    script = """
import json, sys, time
print(json.dumps(dict(version=1, kind="event", event="worker.ready")), flush=True)
request = json.loads(sys.stdin.readline())
print(json.dumps(dict(version=1, kind="response", request_id=request["request_id"],
                      ok=True, result=dict(run_id="stuck"))), flush=True)
time.sleep(60)
"""
    async with WorkerClient([sys.executable, "-c", script], grace=0.1) as client:
        with pytest.raises(WorkerLost):
            await client.cancel()
        assert client.process.returncode is not None


async def test_request_deadline_covers_blocked_pipe():
    script = (
        "import time; "
        'print(\'{"version":1,"kind":"event","event":"worker.ready"}\', flush=True); '
        "time.sleep(60)"
    )
    async with WorkerClient([sys.executable, "-c", script], timeout=2) as client:
        client.timeout = 0.1
        with pytest.raises(WorkerLost):
            await asyncio.wait_for(client.request("submit", text="x" * 2000000), 3)
        assert client.process.returncode is not None
