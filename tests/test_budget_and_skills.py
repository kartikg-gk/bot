import pytest

from delta_gateway.computer import LocalComputer
from delta_gateway.embedded import EmbeddedClient
from delta_gateway.store import Store
from tests.fake_session import create
from tests.test_gateway import config
from tests.test_rpc_boundary import until


async def test_run_limit_stops_model_after_tool_execution(tmp_path):
    c = config(tmp_path, model_requests_per_run=1, policy="auto")
    store = Store(c.database_path, c.instance)
    try:
        async with EmbeddedClient(
            c, computer=LocalComputer(tmp_path), provider=create, budget_store=store
        ) as client:
            await client.request("set_session_name", name="Budget fixture")
            await client.request("prompt", message="write")
            event = await until(client, "agent_settled")
            assert event["outcome"] == "failed" and "for this run" in event["error"]
            await client.request("prompt", message="hello")
            assert (await until(client, "agent_settled"))["outcome"] == "completed"
    finally:
        store.close()


async def test_daily_limit_survives_database_reopen(tmp_path):
    c = config(tmp_path, model_requests_per_day=1)
    for expected in ("completed", "failed"):
        store = Store(c.database_path, c.instance)
        try:
            async with EmbeddedClient(
                c, computer=LocalComputer(tmp_path), provider=create, budget_store=store
            ) as client:
                await client.request("set_session_name", name="Budget fixture")
                await client.request("prompt", message="hello")
                event = await until(client, "agent_settled")
                assert event["outcome"] == expected
                if expected == "failed":
                    assert "Daily model request limit" in event["error"]
        finally:
            store.close()


@pytest.mark.parametrize("disabled", [False, True])
async def test_bot_skill_discovery_and_model_access(tmp_path, disabled):
    from delta_gateway.bot_skills import load_bot_skills, skill_reader

    skills_dir = tmp_path / "skills"
    source = skills_dir / "make-note" / "SKILL.md"
    source.parent.mkdir(parents=True)
    source.write_text(
        "---\nname: make-note\ndescription: Write a note\n"
        f"disable-model-invocation: {str(disabled).lower()}\n---\nPrivate instructions.\n",
        encoding="utf-8",
    )
    skills = load_bot_skills(config(tmp_path), skills_dir)
    skill = next(skill for skill in skills if skill.name == "make-note")
    reader = skill_reader([skill])
    if disabled:
        assert reader is None
    else:
        assert "Private instructions." in (await reader.run("1", {"name": skill.name})).text
