import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from delta_model.oai_compatible import OpenAIProvider
from delta_model.settings import Credential, OpenAIProfile, RetryPolicy

from delta_gateway.budget import ModelBudget, operation_scope
from delta_gateway.config import load, save
from delta_gateway.mem0_extract import Mem0Extractor
from delta_gateway.store import Store
from delta_gateway.task_cli import run_task
from tests.test_gateway import config


async def test_daily_envelope_survives_restart_and_checks_actual_native_retry(tmp_path):
    cfg = config(
        tmp_path, model_spend_micros_per_day=900, model_attempt_max_micros={"openai/mock": 600}
    )
    path = tmp_path / "gateway.yaml"
    save(path, cfg)
    cfg = load(path)
    attempts = []
    closed_clients = []

    async def factory():
        provider = OpenAIProvider(
            OpenAIProfile(
                name="openai",
                credential=Credential(api_key="fake", base_url="https://models.example/v1"),
                retry=RetryPolicy(max_retries=2),
            )
        )
        closed_clients.append(provider._client)

        def handle(request):
            attempts.append(json.loads(request.content)["model"])
            return httpx.Response(429, json={"error": {"message": "retry"}})

        provider._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        return provider, "openai", "mock"

    for _ in range(2):
        provider = await factory()
        result = await run_task(
            cfg, "hello", factory=lambda _, selected=provider: selected, deadline=12
        )
        assert result["status"] == "failed" and "Daily model spend limit" in result["answer"]
    for client in closed_clients:
        await client.aclose()
    assert attempts == ["mock"]
    store = Store(cfg.database_path, cfg.instance)
    try:
        assert store.db.execute("SELECT micros FROM model_spend").fetchone() == (600,)
        assert store.db.execute("SELECT count FROM model_requests").fetchone() == (1,)
    finally:
        store.close()


async def test_unpriced_and_unsupported_adapters_cannot_send(tmp_path):
    cfg = config(tmp_path, model_spend_micros_per_run=1000)
    provider = OpenAIProvider(
        OpenAIProfile(
            name="openai",
            credential=Credential(api_key="fake", base_url="https://models.example/v1"),
        )
    )
    await provider._client.aclose()

    def forbidden(request):
        pytest.fail("Request escaped the spend guard")

    provider._client = httpx.AsyncClient(transport=httpx.MockTransport(forbidden))
    result = await run_task(cfg, "hello", factory=lambda _: (provider, "openai", "unknown"))
    assert result["status"] == "failed" and "No approved maximum charge" in result["answer"]
    result = await run_task(cfg, "hello", factory="tests.fake_session:create")
    assert result["status"] == "failed" and "does not support spend limits" in result["answer"]


async def test_mem0_sync_sdk_attempts_share_parent_limit(tmp_path, monkeypatch):
    import delta_gateway.mem0_extract as module

    cfg = config(
        tmp_path, model_spend_micros_per_run=1200, model_attempt_max_micros={"openai/mock": 600}
    )
    store = Store(cfg.database_path, cfg.instance)
    budget = ModelBudget(cfg, store)
    sent = []
    client_class = httpx.Client

    def transport(request):
        sent.append(json.loads(request.content)["model"])
        return httpx.Response(200, json={})

    def client(**kw):
        return client_class(transport=httpx.MockTransport(transport), **kw)

    class SDK:
        def with_options(self, *, http_client, timeout):
            return http_client

    class Memory:
        llm = SimpleNamespace(client=SDK())

        def add(self, messages, **kw):
            for _ in range(2):
                self.llm.client.post(
                    "https://models.example/v1/chat/completions", json={"model": "mock"}
                )
            return {"results": []}

    monkeypatch.setattr(module.httpx, "Client", client)
    extractor = Mem0Extractor(tmp_path / "memory", "openai", "mock", memory=Memory(), budget=budget)
    request = httpx.Request("POST", "https://models.example", json={"model": "mock"})
    try:
        with operation_scope(cfg.instance):
            budget.reserve_http("openai", request, run=budget.scope())
            with pytest.raises(RuntimeError, match="spend limit for this run"):
                await asyncio.wait_for(extractor.changes("project", 11, "user", "assistant"), 4)
        assert sent == ["mock"]
        assert store.db.execute("SELECT micros FROM model_spend").fetchone() == (1200,)
        assert isinstance(extractor.mem0.llm.client, SDK)
    finally:
        store.close()
