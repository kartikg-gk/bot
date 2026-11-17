"""Hosted bots keep notes and skills in the database; the local folder is a working copy."""

import shutil

from delta_gateway.bots import bot_root
from delta_gateway.file_tables import hosted, pull, push
from delta_gateway.store import Store
from tests.test_gateway import config

INSTANCE = "a" * 32
SKILL = "---\nname: cite\ndescription: Cite sources\n---\nAlways cite.\n"


async def setup(tmp_path, database):
    store = await Store.open(database, INSTANCE)
    bot = await store.create_bot("Reader", "Reads")
    return config(tmp_path), store, bot


async def test_notes_and_skills_survive_a_wiped_folder(tmp_path, database):
    c, store, bot = await setup(tmp_path, database)
    root = bot_root(c, bot)
    (root / "memory").mkdir(parents=True)
    (root / "memory" / "KNOWLEDGE.md").write_text("Prefers metric units.", encoding="utf-8")
    (root / "skills" / "cite").mkdir(parents=True)
    (root / "skills" / "cite" / "SKILL.md").write_text(SKILL, encoding="utf-8")
    await push(store, c, bot)
    shutil.rmtree(root)
    await pull(store, c, bot)
    assert (root / "memory" / "KNOWLEDGE.md").read_text(encoding="utf-8") == "Prefers metric units."
    assert (root / "skills" / "cite" / "SKILL.md").read_text(encoding="utf-8") == SKILL


async def test_removed_skill_is_removed_from_the_table(tmp_path, database):
    c, store, bot = await setup(tmp_path, database)
    root = bot_root(c, bot)
    (root / "skills" / "cite").mkdir(parents=True)
    (root / "skills" / "cite" / "SKILL.md").write_text(SKILL, encoding="utf-8")
    await push(store, c, bot)
    shutil.rmtree(root / "skills" / "cite")
    await push(store, c, bot)
    shutil.rmtree(root)
    await pull(store, c, bot)
    assert not (root / "skills" / "cite").exists()


async def test_shared_skills_are_kept_apart_from_bot_skills(tmp_path, database):
    c, store, bot = await setup(tmp_path, database)
    (c.state / "skills" / "cite").mkdir(parents=True)
    (c.state / "skills" / "cite" / "SKILL.md").write_text(SKILL, encoding="utf-8")
    await push(store, c, None)
    await pull(store, c, bot)
    assert not (bot_root(c, bot) / "skills" / "cite").exists()
    shutil.rmtree(c.state / "skills")
    await pull(store, c, None)
    assert (c.state / "skills" / "cite" / "SKILL.md").is_file()


def test_only_a_store_without_a_local_file_is_hosted(tmp_path):
    local = Store(tmp_path / "state", INSTANCE)
    try:
        assert not hosted(local)
    finally:
        local.close()


def test_hosted_config_keeps_notes_only_and_needs_daytona(tmp_path):
    import pytest

    hosted_config = config(tmp_path, hosted=True, executor="daytona", memory_enabled=True)
    with pytest.raises(ValueError, match="^Hosted mode keeps notes only; turn off memory$"):
        hosted_config.validate_paths()
    with pytest.raises(ValueError, match="^Hosted mode needs the Daytona executor$"):
        config(tmp_path, hosted=True, executor="local").validate_paths()
    config(tmp_path, hosted=True, executor="daytona").validate_paths()
