import asyncio
import time

import pytest

from delta_gateway.bot_profiles import profile, record_chat, save_profile
from delta_gateway.cli import parser
from delta_gateway.config import load, save
from delta_gateway.database_import import migrate_configs
from delta_gateway.instances import OperatorError, config_lease
from delta_gateway.store import Store
from delta_gateway.task_cli import run_task
from tests.test_gateway import config


def populated(root, name):
    root.mkdir()
    cfg = config(root, name=name)
    path = root / "gateway.yaml"
    save(path, cfg)
    store = Store(cfg.database_path, cfg.instance)
    try:
        chief = store.bots()[0]
        bot = store.create_bot("Reader", "Review notes", "Use citations")
        route = store.switch_bot(store.conversation(11, 11), bot)
        with store.db:
            save_profile(store, bot, {"style": "upbeat"})
        record_chat(store, bot.id, "assistant", name)
        store.set_plugin(bot.id, "github", True)
        now = time.time()
        job = store.add_job(11, 11, 11, 0, bot.id, name, "once", now)
        store.add_task(11, 11, 11, 0, bot.id, name, "deny", False)
        store.begin_delegation(route.id, bot, name)
        with store.db:
            store.db.execute("UPDATE jobs SET running=1 WHERE id=?", (job,))
            for status, previous in (("superseded", None), ("active", 1)):
                store.db.execute(
                    "INSERT INTO memory_facts(instance,project,owner,kind,text,tags,confidence,"
                    "source,prompt_version,status,replaces,created,changed,indexed) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
                    (
                        cfg.instance,
                        str(cfg.project),
                        11,
                        "fact",
                        name,
                        "[]",
                        1,
                        "explicit",
                        1,
                        status,
                        previous,
                        now,
                        now,
                    ),
                )
        return cfg, path, chief.id, bot.id
    finally:
        store.close()


def test_populated_instances_keep_bot_identity_and_remap_database_references(tmp_path):
    sources = [populated(tmp_path / name, name) for name in ("alpha", "beta")]
    paths = [item[1] for item in sources]
    target = tmp_path / "shared.sqlite3"
    migrate_configs(paths, destination=target)
    migrate_configs(paths, destination=target)
    fact_ids = []
    for cfg, path, chief_id, bot_id in sources:
        migrated = load(path)
        assert migrated.database_path == target and migrated.state == cfg.state
        store = Store(target, cfg.instance)
        try:
            assert {bot.id for bot in store.bots()} == {chief_id, bot_id}
            assert profile(store, store.bot(bot_id))["style"] == "upbeat"
            assert store.plugins_for(bot_id) == ["github"]
            assert store.jobs()[0].last_status == "interrupted"
            assert store.tasks()[0].status == "interrupted"
            route = store.conversation(11, 11)
            assert route.bot == bot_id
            assert (
                store.db.execute(
                    "SELECT bot FROM route_bots WHERE route=?", (route.id,)
                ).fetchone()[0]
                == chief_id
            )
            assert (
                store.db.execute(
                    "SELECT status FROM delegations WHERE route=?", (route.id,)
                ).fetchone()[0]
                == "interrupted"
            )
            facts = store.db.execute(
                "SELECT id,replaces,indexed FROM memory_facts WHERE instance=? ORDER BY id",
                (cfg.instance,),
            ).fetchall()
            assert len(facts) == 2 and facts[1][1:] == (facts[0][0], 0)
            assert store.memory_index_path(cfg.state).name.startswith("qdrant-")
            fact_ids.extend(row[0] for row in facts)
        finally:
            store.close()
    assert len(set(fact_ids)) == 4


async def test_one_shot_runs_selected_bot_and_reports_provider_usage(tmp_path):
    cfg = config(tmp_path, policy="ask")
    store = Store(cfg.database_path, cfg.instance)
    bot = store.create_bot("Reader", "Read documents")
    store.close()
    options = parser().parse_args(["task", "usage", "--bot", "Reader", "--json"])
    assert options.text == "usage" and options.json
    result = await run_task(
        cfg, options.text, bot_name=options.bot, factory="tests.fake_session:create"
    )
    assert result["bot"] == bot.id and result["status"] == "completed"
    assert result["usage"] == {
        "input": 11,
        "output": 5,
        "cache_read": 3,
        "cache_write": 2,
        "model_messages": 1,
        "cost_usd": 0.004,
    }
    with config_lease(cfg):
        with pytest.raises(OperatorError, match="Stop this gateway"):
            await run_task(cfg, "hello", factory="tests.fake_session:create")
    async with asyncio.timeout(5):
        result = await run_task(
            cfg, "write", bot_name="Reader", factory="tests.fake_session:create"
        )
    assert result["status"] == "completed"
    assert not (cfg.state / "bots" / bot.id / "workspace" / "approved.txt").exists()


async def test_delegated_models_share_the_parent_task_budget(tmp_path):
    cfg = config(tmp_path, model_requests_per_run=2)
    store = Store(cfg.database_path, cfg.instance)
    store.create_bot("Reader", "Read documents")
    store.close()
    result = await run_task(cfg, "delegate:Reader:usage", factory="tests.fake_session:create")
    assert result["status"] == "failed"
    assert "Model request limit" in result["answer"]
    assert result["usage"]["cache_read"] == 3
    store = Store(cfg.database_path, cfg.instance)
    try:
        assert store.db.execute("SELECT count FROM model_requests").fetchone() == (2,)
    finally:
        store.close()


async def test_langfuse_trace_exports_nested_metadata_without_prompt_or_tool_args(
    tmp_path, monkeypatch
):
    import base64
    import json

    import httpx

    from delta_gateway.tracing import TraceExporter

    cfg = config(tmp_path)
    store = Store(cfg.database_path, cfg.instance)
    store.create_bot("Reader", "Read documents")
    store.close()
    sent = []

    def receive(request):
        assert request.url.path == "/api/public/otel/v1/traces"
        assert request.headers["x-langfuse-ingestion-version"] == "4"
        assert request.headers["authorization"] == "Basic " + base64.b64encode(b"pk:sk").decode()
        sent.append(request.content)
        return httpx.Response(200, json={})

    def exporter(cls):
        return cls("https://trace.example", "pk", "sk", transport=httpx.MockTransport(receive))

    monkeypatch.setattr(TraceExporter, "from_env", classmethod(exporter))
    text = "delegate:Reader:private-trace-fixture"
    result = await run_task(cfg, text, factory="tests.fake_session:create")
    assert result["status"] == "completed" and len(sent) == 1
    assert b"private-trace-fixture" not in sent[0] and b"bot_name" not in sent[0]
    spans = json.loads(sent[0])["resourceSpans"][0]["scopeSpans"][0]["spans"]
    root = next(span for span in spans if "parentSpanId" not in span)
    delegate = next(span for span in spans if span["name"] == "delegation")
    assert root["name"] == "one-shot" and delegate["parentSpanId"] == root["spanId"]
    assert any(
        span["name"] == "model" and span.get("parentSpanId") == delegate["spanId"] for span in spans
    )
    assert any(span["name"] == "delegate_task" for span in spans)


async def test_retry_reserves_another_request_before_the_provider_retries(tmp_path):
    from delta_harness.provider.wire import SourceRetryEvent

    attempted = []

    class Provider:
        async def stream_response(self, **kwargs):
            attempted.append(1)
            yield SourceRetryEvent(attempt=1, max_attempts=2, delay_seconds=0, message="retrying")
            attempted.append(2)

    result = await run_task(
        config(tmp_path, model_requests_per_run=1),
        "hello",
        factory=lambda config: (Provider(), "fake", "fake"),
    )
    assert result["status"] == "failed" and "Model request limit" in result["answer"]
    assert attempted == [1]
