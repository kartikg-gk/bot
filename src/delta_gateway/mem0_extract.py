"""Fact extraction through Mem0, feeding the gateway's own fact lifecycle.

Mem0 reads each exchange and decides what to add, change or drop. The gateway keeps the
history: a new fact is saved as current, while a change or removal of a stored fact is held
as a conflict for the owner, since Mem0 gives no confidence to judge it by.
"""

import asyncio
import os

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
    def __init__(self, folder, provider, model, *, memory=None):
        self.folder, self.provider, self.model = folder, provider, model
        self.mem0 = memory
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

    async def changes(self, project, owner, user, assistant):
        """Mem0's decisions for one exchange: (event, mem0 id, text) per affected memory."""
        async with self.lock:
            mem0 = await asyncio.to_thread(self.load)
            result = await asyncio.to_thread(
                mem0.add,
                [dict(role="user", content=user), dict(role="assistant", content=assistant)],
                user_id=str(owner),
                agent_id=project,
            )
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
