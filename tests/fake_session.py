"""Offline provider exercising real Delta persistence inside the gateway process."""

import asyncio
from pathlib import Path

from delta_harness.contracts.transcript import CallBlock, HumanEntry, ModelEntry
from delta_harness.provider.wire import ContentChunkEvent, StreamCloseEvent, StreamOpenEvent


class Provider:
    def __init__(self, root=None):
        self.root = Path(root or Path.cwd())

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
                                "file_path": str(self.root / "approved.txt"),
                                "content": "ok",
                            },
                        )
                    ],
                ),
            )
            return
        if prompt == "plugin" and isinstance(messages[-1], HumanEntry):
            yield StreamCloseEvent(
                reason="toolUse",
                message=ModelEntry(
                    model=model,
                    stop_reason="toolUse",
                    content=[
                        CallBlock(
                            id="plugin-1",
                            name="github_list_issues",
                            arguments={"repo": "a/b"},
                        )
                    ],
                ),
            )
            return
        if prompt.startswith("delegate:") and isinstance(messages[-1], HumanEntry):
            _, bot, task = prompt.split(":", 2)
            yield StreamCloseEvent(
                reason="toolUse",
                message=ModelEntry(
                    model=model,
                    stop_reason="toolUse",
                    content=[
                        CallBlock(
                            id="delegate-1",
                            name="delegate_task",
                            arguments={"bot_name": bot, "task": task},
                        )
                    ],
                ),
            )
            return
        if prompt.startswith("delegate:"):
            text = "delegated"
            yield StreamCloseEvent(reason="stop", message=ModelEntry(model=model, content=text))
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
            while not (self.root / "release").exists():
                await asyncio.sleep(0.01)
        text += "done"
        yield StreamCloseEvent(reason="stop", message=ModelEntry(model=model, content=text))

    async def close(self):
        pass


def create(config):
    """Provider factory used in place of a real model: (provider, provider name, model)."""
    return Provider(config.project), "fake", "fake"
