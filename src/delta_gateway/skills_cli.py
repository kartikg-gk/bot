"""Operator commands for shared and bot-local skill folders."""

import json

from delta_gateway.bot_skills import load_bot_skills
from delta_gateway.bots import bot_root
from delta_gateway.instances import OperatorError
from delta_gateway.ownership import SessionLock
from delta_gateway.skill_store import SkillStore
from delta_gateway.store import Store


def command(config, options):
    store = Store(config.database_path, config.instance)
    owner = None
    try:
        bot = None
        if options.bot:
            bot = store.bot(options.bot) or store.find_bot(options.bot)
            if bot is None:
                raise OperatorError("No bot with that name or ID")
        root = bot_root(config, bot) / "skills" if bot else config.state / "skills"
        managed = SkillStore(root)
        if options.action == "list":
            skills = load_bot_skills(config, root if bot else None)
            print(
                json.dumps(
                    [
                        dict(
                            name=s.name,
                            description=s.description,
                            disabled=s.disable_model_invocation,
                            managed=s.source.parent.parent == root,
                        )
                        for s in skills
                    ],
                    indent=2,
                )
            )
            return
        if not options.target:
            raise OperatorError("Supply a skill folder for add, or a skill name for remove")
        if bot:
            owner = SessionLock(config.state / "bot-locks", bot.id)
        if options.action == "add":
            print("Installed " + managed.add(options.target))
        else:
            managed.remove(options.target)
            print("Removed " + options.target)
    except (ValueError, OSError, RuntimeError) as error:
        if isinstance(error, OperatorError):
            raise
        raise OperatorError(str(error)) from None
    finally:
        if owner is not None:
            owner.close()
        store.close()
