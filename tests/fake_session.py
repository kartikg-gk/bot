"""Offline provider exercising real Delta persistence inside the gateway process."""

import asyncio
from pathlib import Path

from delta_harness.contracts.transcript import CallBlock, HumanEntry, ModelEntry
from delta_harness.contracts.transcript.diagnostics import CostBreakdown, UsageStats
from delta_harness.provider.wire import ContentChunkEvent, StreamCloseEvent, StreamOpenEvent


class Provider:
    def __init__(self, root=None):
        self.root = Path(root or Path.cwd())
        self.plugin_loading = False

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
        if prompt == "plugin" and (isinstance(messages[-1], HumanEntry) or self.plugin_loading):
            names = {tool.name for tool in kwargs["tools"]}
            loading = "github_list_issues" not in names
            if loading:
                assert not self.plugin_loading and "load_plugin" in names
            self.plugin_loading = loading
            yield StreamCloseEvent(
                reason="toolUse",
                message=ModelEntry(
                    model=model,
                    stop_reason="toolUse",
                    content=[
                        CallBlock(
                            id="load-1" if loading else "plugin-1",
                            name="load_plugin" if loading else "github_list_issues",
                            arguments={"name": "github"} if loading else {"repo": "a/b"},
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
        usage = (
            UsageStats(
                input=11, output=5, cache_read=3, cache_write=2, cost=CostBreakdown(total=0.004)
            )
            if prompt == "usage"
            else UsageStats()
        )
        yield StreamCloseEvent(
            reason="stop", message=ModelEntry(model=model, content=text, usage=usage)
        )

    async def close(self):
        pass


def create(config):
    """Provider factory used in place of a real model: (provider, provider name, model)."""
    return Provider(config.project), "fake", "fake"
