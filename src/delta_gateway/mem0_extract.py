"""Fact extraction through Mem0, feeding the gateway's own fact lifecycle.

Mem0 reads each exchange and decides what to add, change or drop. The gateway keeps the
history: a new fact is saved as current, while a change or removal of a stored fact is held
as a conflict for the owner, since Mem0 gives no confidence to judge it by.
"""

import asyncio
import os
from threading import Event

import httpx

from delta_gateway.memory import MODEL

OPENAI_STYLE = {"openai": None, "openrouter": "https://openrouter.ai/api/v1", "ollama": None}


def llm_settings(provider, model):
    if provider in OPENAI_STYLE:
        settings = dict(model=model, api_key=os.environ.get(f"{provider.upper()}_API_KEY"))
        if provider == "openrouter":
            settings["openrouter_base_url"] = OPENAI_STYLE[provider]
        elif os.environ.get("OPENAI_BASE_URL"):
            settings["openai_base_url"] = os.environ["OPENAI_BASE_URL"]
        return dict(provider="openai", config=settings)
    if provider == "anthropic":
        return dict(
            provider="anthropic",
            config=dict(model=model, api_key=os.environ.get("ANTHROPIC_API_KEY")),
        )
    raise ValueError("Mem0 extraction supports the openai, openrouter and anthropic providers")


class Mem0Extractor:
    def __init__(self, folder, provider, model, *, memory=None, budget=None):
        self.folder, self.provider, self.model = folder, provider, model
        self.mem0, self.budget = memory, budget
        self.worker = None
        self.lock = asyncio.Lock()

    def load(self):
        if self.mem0 is None:
            try:
                from mem0 import Memory
            except ImportError:
                raise RuntimeError(
                    "Mem0 extraction needs the memory extra: pip install 'delta-gateway[memory]'"
                ) from None
            self.folder.mkdir(parents=True, exist_ok=True)
            self.mem0 = Memory.from_config(
                dict(
                    llm=llm_settings(self.provider, self.model),
                    embedder=dict(
                        provider="fastembed", config=dict(model=MODEL, embedding_dims=384)
                    ),
                    vector_store=dict(
                        provider="qdrant",
                        config=dict(
                            path=str(self.folder / "qdrant"),
                            collection_name="mem0",
                            embedding_model_dims=384,
                            on_disk=True,
                        ),
                    ),
                    history_db_path=str(self.folder / "history.db"),
                )
            )
        return self.mem0

    def metered_add(self, mem0, messages, project, owner, loop, run, stopped):
        original = getattr(mem0.llm, "client", None)
        copy_client = getattr(original, "with_options", None)
        if copy_client is None:
            raise RuntimeError("This Mem0 provider does not support spend limits.")

        def reserve(request):
            async def debit():
                if stopped.is_set():
                    raise RuntimeError("Memory extraction was cancelled.")
                self.budget.reserve_http(self.provider, request, run=run)

            if stopped.is_set() or loop.is_closed():
                raise RuntimeError("Memory extraction was cancelled.")
            reservation = debit()
            try:
                future = asyncio.run_coroutine_threadsafe(reservation, loop)
            except RuntimeError:
                reservation.close()
                raise
            try:
                future.result(timeout=5)
            except TimeoutError:
                future.cancel()
                raise RuntimeError("Could not reserve a charge for memory extraction.") from None

        with httpx.Client(timeout=15, event_hooks={"request": [reserve]}) as client:
            mem0.llm.client = copy_client(http_client=client, timeout=15)
            try:
                return mem0.add(messages, user_id=str(owner), agent_id=project)
            finally:
                mem0.llm.client = original

    async def changes(self, project, owner, user, assistant):
        """Mem0's decisions for one exchange: (event, mem0 id, text) per affected memory."""
        async with self.lock:
            if self.worker is not None:
                # A cancelled caller may have left a synchronous SDK request unwinding.
                await asyncio.shield(asyncio.gather(self.worker, return_exceptions=True))
            mem0 = await asyncio.to_thread(self.load)
            messages = [dict(role="user", content=user), dict(role="assistant", content=assistant)]
            if self.budget is None:
                result = await asyncio.to_thread(
                    mem0.add,
                    messages,
                    user_id=str(owner),
                    agent_id=project,
                )
            else:
                stopped = Event()
                self.worker = asyncio.create_task(
                    asyncio.to_thread(
                        self.metered_add,
                        mem0,
                        messages,
                        project,
                        owner,
                        asyncio.get_running_loop(),
                        self.budget.scope(),
                        stopped,
                    )
                )
                self.worker.add_done_callback(
                    lambda done: None if done.cancelled() else done.exception()
                )
                try:
                    result = await asyncio.shield(self.worker)
                finally:
                    stopped.set()
        return [
            (row.get("event"), row.get("id"), row.get("memory") or "")
            for row in (result or {}).get("results", [])
            if row.get("event") in {"ADD", "UPDATE", "DELETE"} and row.get("id")
        ]


def apply(memory, project, owner, changes):
    """Record Mem0's decisions. Returns the numbers of facts now waiting for the owner."""
    waiting = []
    for event, external, text in changes:
        existing = memory.by_external(external, project, owner)
        if event == "ADD" or (event == "UPDATE" and existing is None):
            if text:
                memory.add(
                    project,
                    owner,
                    "fact",
                    text,
                    source="extracted",
                    tags=("fact",),
                    external=external,
                )
        elif existing is not None:
            current = next(f for f in memory.facts(project, owner) if f.id == existing)
            replacement = text if event == "UPDATE" else f"No longer true: {current.text}"
            _, number = memory.revise(
                existing,
                project,
                owner,
                replacement,
                0.0,
                source="extracted",
                external=external,
            )
            waiting.append(number)
    return waiting
