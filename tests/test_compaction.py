"""Compaction must keep the request the bot is currently working on."""

import pytest
from delta_app.context.budget import ContextLimits, plan_compaction
from delta_harness.contracts.transcript import (
    CallBlock,
    HumanEntry,
    ModelEntry,
    TextSegment,
    ToolOutcomeEntry,
)

REQUEST = "Refactor the billing module and keep the tests green"


def tool_steps(count):
    entries = []
    for index in range(count):
        call = CallBlock(id=f"c{index}", name="Read", arguments={"file_path": f"f{index}.py"})
        entries.append(ModelEntry(content=[call]))
        entries.append(ToolOutcomeEntry(tool_call_id=f"c{index}", tool_name="Read", content="x"))
    return entries


def chat(turns):
    entries = []
    for index in range(turns):
        entries.append(HumanEntry(content=f"question {index}"))
        entries.append(ModelEntry(content=[TextSegment(text=f"answer {index}")]))
    return entries


def kept(transcript):
    plan = plan_compaction(transcript, [str(i) for i in range(len(transcript))])
    assert plan is not None
    return plan


def test_latest_request_kept_after_long_tool_turn():
    transcript = [*chat(10), HumanEntry(content=REQUEST), *tool_steps(60)]
    plan = kept(transcript)
    assert transcript[20] in plan.to_keep
    assert transcript[20] not in plan.to_summarize


def test_latest_request_kept_in_ordinary_chat():
    transcript = [*chat(30), HumanEntry(content=REQUEST)]
    plan = kept(transcript)
    assert REQUEST in [entry.content for entry in plan.to_keep if isinstance(entry, HumanEntry)]


@pytest.mark.xfail(
    strict=True,
    reason="Delta summarises a lone opening request; the gateway appends it to the summary",
)
def test_single_request_kept_when_whole_task_is_one_long_turn():
    # Scheduled jobs, delegated work and one-shot tasks start with one request
    # followed only by tool work.
    transcript = [HumanEntry(content=REQUEST), *tool_steps(60)]
    plan = plan_compaction(transcript, [], limits=ContextLimits())
    if plan is None:
        return
    assert REQUEST in [entry.content for entry in plan.to_keep if isinstance(entry, HumanEntry)]
