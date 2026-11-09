import asyncio
import json
from types import SimpleNamespace

import pytest

from delta_gateway.bot_skills import load_bot_skills, skill_reader
from delta_gateway.cli import parser
from delta_gateway.computer import LocalComputer, ManagedComputer
from delta_gateway.skill_store import SkillStore, page
from delta_gateway.skills_cli import command
from delta_gateway.team import Team
from delta_gateway.tools import computer_tools
from tests.test_web import web as web


def package(name="research", body="Read carefully."):
    return {
        "SKILL.md": f"---\nname: {name}\ndescription: Review evidence\n---\n{body}\n",
        "references/guide.txt": "\n".join(f"line {i}" for i in range(1, 501)),
    }


@pytest.mark.parametrize(
    "path", ["../escape", "/escape", "a/../../escape", "C:/escape", "a\\escape"]
)
def test_skill_paths_cannot_escape(tmp_path, path):
    files = package()
    files[path] = "bad"
    with pytest.raises(ValueError):
        SkillStore(tmp_path / "skills").write("research", files)
    assert not (tmp_path / "escape").exists()


def test_skill_replacement_validation_and_stale_files(tmp_path):
    store = SkillStore(tmp_path / "skills")
    first = package()
    store.write("research", first)
    with pytest.raises(ValueError, match="256 KB"):
        store.write("research", {**package(), "large.txt": "x" * 256001})
    assert store.read("research") == first
    replacement = {"SKILL.md": package(body="Updated.")["SKILL.md"]}
    store.write("research", replacement)
    assert store.read("research") == replacement
    store.remove("research")
    assert not store.package("research").exists()


def test_package_names_and_binary_content_are_rejected(tmp_path):
    store = SkillStore(tmp_path / "skills")
    for name, files in [
        ("../oops", package()),
        ("alias", package()),
        ("research", {**package(), "binary.txt": "a\0b"}),
    ]:
        with pytest.raises(ValueError):
            store.write(name, files)


def test_supporting_text_is_paged(tmp_path):
    from delta_app.skillset import load_skills

    root = tmp_path / "skills"
    SkillStore(root).write("research", package())
    skill = load_skills([root])[0]
    first = json.loads(page(skill, "references/guide.txt", 1, 200))
    assert first["nextOffset"] == 201 and first["text"].endswith("line 200")
    last = json.loads(page(skill, "references/guide.txt", 401, 200))
    assert last["nextOffset"] is None and "line 500" in last["text"]
    with pytest.raises(ValueError):
        page(skill, "../../outside")


def test_supporting_file_symlinks_are_rejected(tmp_path):
    from delta_app.skillset import load_skills

    root = tmp_path / "skills"
    SkillStore(root).write("research", package())
    skill = load_skills([root])[0]
    link = root / "research" / "linked.txt"
    outside = tmp_path / "outside.txt"
    outside.write_text("private")
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Creating symlinks requires host permissions")
    with pytest.raises(ValueError, match="Linked"):
        page(skill, "linked.txt")


async def test_web_skill_catalog_editor_and_notes(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    path = "/api/bots/" + bot.id
    assert (
        await client.post(path + "/skills", json={"name": "research", "files": package()})
    ).status_code == 200
    catalog = (await client.get(path + "/skills")).json()
    assert any(s["name"] == "research" and s["managed"] for s in catalog)
    assert any(s["name"] == "skill-authoring" for s in catalog)
    assert (await client.get(path + "/skills/research")).json()["files"] == package()
    assert (
        await client.post("/api/skills", json={"name": "shared", "files": package("shared")})
    ).status_code == 200
    assert any(s["name"] == "shared" for s in (await client.get(path + "/skills")).json())
    assert (
        await client.put(path + "/memory", json={"file": "KNOWLEDGE.md", "text": "A checked fact."})
    ).status_code == 200
    assert (await client.get(path + "/memory")).json()["KNOWLEDGE.md"] == "A checked fact."
    assert (
        await client.put(path + "/memory", json={"file": "../secret", "text": "bad"})
    ).status_code == 400
    assert (await client.delete(path + "/skills/research")).status_code == 200


async def test_web_cannot_mutate_busy_bot_resources(web):
    service, client, _ = web
    from delta_gateway.bot_profiles import reserve

    bot = service.store.create_bot("Reader", "Read documents")
    path = "/api/bots/" + bot.id
    with reserve(service.sessions, bot.id):
        assert (
            await client.post(path + "/skills", json={"name": "research", "files": package()})
        ).status_code == 409
        assert (
            await client.put(path + "/memory", json={"file": "PREFERENCES.md", "text": "bad"})
        ).status_code == 409


async def test_skill_and_notes_changes_refresh_next_engine(web):
    service, _, _ = web
    manager, store = service.sessions, service.store
    bot = store.create_bot("Reader", "Read documents")
    route = store.switch_bot(store.conversation(11, 11), bot)
    await manager.prompt(route, "hello")
    old = next(iter(manager.clients.values()))
    team = Team(store, manager)
    await team.run("write_skill", {"name": "research", "files": package()}, route, bot=bot)
    await team.run(
        "write_memory", {"file": "PREFERENCES.md", "text": "Prefer citations."}, route, bot=bot
    )
    assert "Prefer citations" in await team.run(
        "read_memory", {"file": "PREFERENCES.md"}, route, bot=bot
    )
    await manager.prompt(route, "again")
    fresh = next(iter(manager.clients.values()))
    assert fresh is not old
    skills = load_bot_skills(fresh.config, service.config.state / "bots" / bot.id / "skills")
    result = await skill_reader(skills).run(
        "1", {"name": "research", "file": "references/guide.txt", "offset": 201}
    )
    assert "line 201" in result.text


async def test_skill_cli_add_list_remove(web, tmp_path, capsys):
    service, _, _ = web
    source = tmp_path / "incoming" / "research"
    SkillStore(source.parent).write("research", package())
    command(service.config, parser().parse_args(["skills", "add", str(source)]))
    assert "Installed research" in capsys.readouterr().out
    command(service.config, parser().parse_args(["skills", "list"]))
    assert "research" in capsys.readouterr().out
    command(service.config, parser().parse_args(["skills", "remove", "research"]))
    assert "Removed research" in capsys.readouterr().out


class CountComputer(LocalComputer):
    def __init__(self, root):
        super().__init__(root)
        self.starts = self.stops = 0

    async def start(self):
        self.starts += 1
        await super().start()

    async def stop(self):
        self.stops += 1


async def test_computer_is_lazy_and_stops_at_task_end(web):
    service, _, _ = web
    service.config.policy = "auto"
    bot = service.store.bots()[0]
    route = service.store.conversation(11, 11)
    base = CountComputer(service.config.project)
    base.memory_path = str(service.config.state / "bots" / bot.id / "memory")
    managed = ManagedComputer(base)
    service.sessions.computers[bot.id] = managed
    await service.sessions.prompt(route, "hello")
    assert base.starts == base.stops == 0
    await service.sessions.prompt(route, "write")
    assert base.starts == base.stops == 1
    assert not managed.started
    await service.sessions.prompt(route, "hello")
    assert base.starts == base.stops == 1


async def test_computer_releases_on_cancel_and_handles_parallel_acquisition(web):
    service, _, _ = web
    bot = service.store.bots()[0]
    base = CountComputer(service.config.project)
    base.memory_path = str(service.config.state / "bots" / bot.id / "memory")
    managed = ManagedComputer(base)
    service.sessions.computers[bot.id] = managed
    await asyncio.gather(managed.start(), managed.start())
    assert base.starts == 1
    task = asyncio.create_task(service.sessions.prompt(service.store.conversation(11, 11), "slow"))
    async with asyncio.timeout(5):
        while bot.id not in service.sessions.active_bots:
            await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert base.stops == 1 and not managed.started


async def test_relative_tool_paths_resolve_against_computer(tmp_path):
    computer = LocalComputer(tmp_path)
    read, write, edit, _ = computer_tools(computer)
    assert "Created" in (await write.run("1", {"file_path": "folder/a.txt", "content": "one"})).text
    await edit.run("2", {"file_path": "folder/a.txt", "old_string": "one", "new_string": "two"})
    assert "two" in (await read.run("3", {"file_path": "folder/a.txt"})).text
    assert (tmp_path / "folder" / "a.txt").read_text() == "two"
    remote = SimpleNamespace(posix=True, root="/workspace")
    from delta_gateway.tools import absolute

    assert absolute(remote, "folder/../a.txt") == "/workspace/a.txt"


@pytest.mark.parametrize("alias", ["skill.md", "SKILL.md ", "references./guide.txt"])
def test_skill_file_aliases_cannot_replace_validated_content(tmp_path, alias):
    with pytest.raises(ValueError):
        SkillStore(tmp_path / "skills").write("research", {**package(), alias: "Bad alias"})


def test_package_replacement_failure_restores_previous_version(tmp_path, monkeypatch):
    import delta_gateway.skill_store as module

    store = SkillStore(tmp_path / "skills")
    original = package()
    store.write("research", original)
    replace = module.replace_directory

    def fail_new_version(source, target):
        if source.name.startswith(".skill-"):
            raise OSError("Replacement failed")
        return replace(source, target)

    monkeypatch.setattr(module, "replace_directory", fail_new_version)
    with pytest.raises(OSError, match="Replacement failed"):
        store.write("research", package(body="Updated"))
    assert store.read("research") == original


async def test_partial_computer_start_is_cleaned_up(tmp_path):
    class Broken(CountComputer):
        async def start(self):
            self.starts += 1
            raise RuntimeError("Could not prepare workspace")

    base = Broken(tmp_path)
    computer = ManagedComputer(base)
    with pytest.raises(RuntimeError, match="prepare workspace"):
        await computer.start()
    assert base.starts == base.stops == 1
    assert not computer.started and not computer.cleanup_needed


async def test_managed_note_write_is_mirrored_to_acquired_remote_memory(web, tmp_path):
    from delta_gateway.bots import bot_root

    service, _, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    base = CountComputer(tmp_path / "computer")
    base.memory_path = str(tmp_path / "remote-notes")
    managed = ManagedComputer(base)
    service.sessions.computers[bot.id] = managed
    await managed.start()
    await Team(service.store, service.sessions).run(
        "write_memory",
        {"file": "KNOWLEDGE.md", "text": "Reviewed fact"},
        service.store.conversation(11, 11),
        bot=bot,
    )
    assert (tmp_path / "remote-notes" / "KNOWLEDGE.md").read_text() == "Reviewed fact"
    assert (
        bot_root(service.config, bot) / "memory" / "KNOWLEDGE.md"
    ).read_text() == "Reviewed fact"


async def test_failed_container_stop_remains_retryable(tmp_path):
    from delta_gateway.computer import ComputerError, DockerComputer

    base = DockerComputer("delta-stop-fixture", "unused", tmp_path)
    calls = []

    async def docker(*arguments, **kwargs):
        calls.append(arguments)
        return (1, b"", b"Daemon unavailable") if len(calls) == 1 else (0, b"", b"")

    base.docker = docker
    computer = ManagedComputer(base)
    computer.started = computer.cleanup_needed = True
    with pytest.raises(ComputerError, match="Could not stop"):
        await computer.stop()
    assert computer.started and computer.cleanup_needed
    await computer.stop()
    assert not computer.started and not computer.cleanup_needed
