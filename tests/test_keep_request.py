"""A run's request survives compaction word for word, even when the run is one long turn."""

from delta_harness.contracts.transcript import HumanEntry, ModelEntry, PruneSummaryEntry
from delta_harness.provider.wire import StreamCloseEvent

from delta_gateway.computer import LocalComputer
from delta_gateway.embedded import EmbeddedClient
from tests.test_compaction import REQUEST, chat, tool_steps
from tests.test_gateway import config


class Summarizer:
    def __init__(self):
        self.prompts = []

    async def stream_response(self, *, model, messages, system="", tools=(), **kwargs):
        self.prompts.append(messages[-1].text)
        message = ModelEntry(model=model, content="The bot read many files.")
        yield StreamCloseEvent(reason="stop", message=message)


async def compacted(tmp_path, transcript, times=1):
    async with EmbeddedClient(
        config(tmp_path),
        computer=LocalComputer(tmp_path),
        provider=lambda _: (Summarizer(), "fake", "fake"),
    ) as client:
        session = client.server.session
        session.harness.set_messages(list(transcript))
        for round in range(times):
            if round:
                session.harness.set_messages([*session.transcript, *tool_steps(60)])
            await session.compact()
        return session.transcript


def summary(transcript):
    assert isinstance(transcript[0], PruneSummaryEntry)
    return transcript[0].summary


async def test_lone_request_is_kept_word_for_word(tmp_path):
    transcript = await compacted(tmp_path, [HumanEntry(content=REQUEST), *tool_steps(60)])
    assert not any(isinstance(entry, HumanEntry) for entry in transcript)
    assert REQUEST in summary(transcript)


async def test_request_survives_repeated_compaction(tmp_path):
    transcript = await compacted(tmp_path, [HumanEntry(content=REQUEST), *tool_steps(60)], 3)
    assert summary(transcript).count(REQUEST) == 1


async def test_kept_request_is_not_repeated_in_summary(tmp_path):
    transcript = await compacted(tmp_path, [*chat(30), HumanEntry(content=REQUEST)])
    assert REQUEST not in summary(transcript)
    assert transcript[-1].text == REQUEST
