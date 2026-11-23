"""Keep the request a run is working on through compaction, word for word.

Compaction keeps recent entries from the start of the latest user turn. When a whole
task is one long turn (scheduled jobs, delegated work, one-shot tasks), that turn's
request is the oldest entry and goes into the summary, where the summarizer may
paraphrase or drop it. The gateway appends the request verbatim to the summary in that
case, and carries it forward through later compactions of the same turn.

Only public parts of the engine are used: the session's ``transcript`` and ``compact``,
and the model provider, which the gateway creates and hands to the session.
"""

from delta_app.context.budget import plan_compaction
from delta_harness.contracts.transcript import HumanEntry, PruneSummaryEntry, TextSegment
from delta_harness.provider.wire import StreamCloseEvent

NOTE = "\n\nThe request being worked on, word for word:\n"


def carried_request(transcript):
    """The request the next compaction would summarize away, or ``None``."""
    plan = plan_compaction(transcript, [])
    if plan is None:
        return None
    if any(isinstance(entry, HumanEntry) for entry in plan.to_keep):
        return None
    for entry in reversed(plan.to_summarize):
        if isinstance(entry, HumanEntry):
            return entry.text
        if isinstance(entry, PruneSummaryEntry) and NOTE in entry.summary:
            return entry.summary.rsplit(NOTE, 1)[1]
    return None


def with_request(event, request):
    text = event.message.text.split(NOTE, 1)[0] + NOTE + request
    message = event.message.model_copy(update={"content": [TextSegment(text=text)]})
    return event.model_copy(update={"message": message})


class RequestKeeper:
    """Adds the current request to the summary the provider returns during compaction."""

    def __init__(self, provider):
        self.request = None
        stream = provider.stream_response

        async def stream_response(*args, **kwargs):
            request = self.request
            async for event in stream(*args, **kwargs):
                if request is not None and isinstance(event, StreamCloseEvent):
                    event = with_request(event, request)
                yield event

        provider.stream_response = stream_response

    def install(self, session):
        """Make every compaction of *session* keep the current request verbatim."""
        compact = session.compact

        async def compact_keeping_request(instructions=None):
            self.request = carried_request(session.transcript)
            try:
                return await compact(instructions)
            finally:
                self.request = None

        session.compact = compact_keeping_request
