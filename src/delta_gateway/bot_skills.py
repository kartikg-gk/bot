"""Load operator-managed skill definitions for an embedded bot."""

from dataclasses import replace

from delta_app.discovery import default_paths, skill_search_paths
from delta_app.skillset import load_skills
from delta_app.tools._shared import fault, text_outcome
from delta_harness.contracts.tooling import ToolSpec


def load_bot_skills(config, bot_directory):
    paths = default_paths(home=config.agent_dir, project=config.project)
    paths = replace(paths, project_enabled=paths.project_enabled and config.trust_project)
    from delta_gateway.starter_skills import ensure_starters

    directories = [config.state / "skills", *skill_search_paths(paths), ensure_starters(config)]
    if bot_directory is not None:
        directories.insert(0, bot_directory)
    return load_skills(directories)


def skill_reader(skills):
    available = {skill.name: skill for skill in skills if not skill.disable_model_invocation}
    if not available:
        return None

    async def run(_call_id, arguments, signal=None, on_update=None):
        name = arguments.get("name")
        if not isinstance(name, str) or name not in available:
            return fault("Unknown skill. Choose a name from the skill catalog.")
        from delta_gateway.skill_store import page

        try:
            return text_outcome(
                page(
                    available[name],
                    arguments.get("file", "SKILL.md"),
                    arguments.get("offset", 1),
                    arguments.get("limit", 200),
                )
            )
        except (ValueError, OSError) as exc:
            return fault(str(exc))

    return ToolSpec(
        name="LoadSkill",
        label="Load skill instructions",
        description="Read paged skill instructions or supporting text; continue with nextOffset.",
        parameters={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "file": {"type": "string"},
                "offset": {"type": "integer", "minimum": 1},
                "limit": {"type": "integer", "minimum": 1, "maximum": 400},
            },
            "required": ["name"],
            "additionalProperties": False,
        },
        run=run,
    )
