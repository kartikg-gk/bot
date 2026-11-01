"""Offline provider exercising real Delta persistence inside a real child process."""

import asyncio
from pathlib import Path

from delta_app.conversation import CodingSession
from delta_harness.contracts.transcript import CallBlock, HumanEntry, ModelEntry
from delta_harness.provider.wire import ContentChunkEvent, StreamCloseEvent, StreamOpenEvent


class Provider:
    async def stream_response(self, *, model, messages, **kwargs):
        count = sum(isinstance(m, HumanEntry) for m in messages)
        prompt = next((m.text for m in reversed(messages) if isinstance(m, HumanEntry)), "")
        if prompt == "write" and isinstance(messages[-1], HumanEntry):
            yield StreamCloseEvent(
                reason="toolUse",
                message=ModelEntry(
                    model=model,
                    stop_reason="toolUse",
                    content=[
                        CallBlock(
                            id="write-1",
                            name="Write",
                            arguments={
                                "file_path": str(Path.cwd() / "approved.txt"),
                                "content": "ok",
                            },
                        )
                    ],
                ),
            )
            return
        if prompt == "fail":
            raise RuntimeError("secret-that-must-not-appear-in-the-protocol")
        text = f"turn {count}: "
        yield StreamOpenEvent(partial=ModelEntry(model=model))
        yield ContentChunkEvent(
            content_index=0, delta=text, partial=ModelEntry(model=model, content=text)
        )
        await asyncio.sleep(30 if prompt == "slow" else 0.03)
        if prompt == "pause":
            while not (Path.cwd() / "release").exists():
                await asyncio.sleep(0.01)
        text += "done"
        yield StreamCloseEvent(reason="stop", message=ModelEntry(model=model, content=text))

    async def close(self):
        pass


async def create(options):
    from delta_app.tools.files import make_write_tool

    # Deliberately print during setup: the worker must keep stdout clean.
    print("fake setup notice")
    kwargs = dict(
        provider=Provider(),
        provider_name="fake",
        model="fake",
        system="",
        sessions_dir=Path(options.session_dir),
        tools=[make_write_tool()],
    )
    if options.session:
        session = await CodingSession.resume(options.session, **kwargs)
    else:
        session = await CodingSession.create(**kwargs)

    async def no_name():
        await asyncio.sleep(0.03)
        return ""

    session.auto_name = no_name
    return session
