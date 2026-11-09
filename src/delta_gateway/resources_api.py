"""Web catalog management for local skill packages and bot notes."""

from aiohttp import web

from delta_gateway.bot_profiles import reserve
from delta_gateway.bot_skills import load_bot_skills
from delta_gateway.bots import bot_root
from delta_gateway.skill_store import NOTES, SkillStore, safe_file, text_file, write_note
from delta_gateway.web import ResourceScope


class ResourcesAPI(ResourceScope):
    def install(self, app):
        for prefix in ("/api/skills", "/api/bots/{bot}/skills"):
            app.router.add_get(prefix, self.catalog)
            app.router.add_post(prefix, self.save)
            app.router.add_get(prefix + "/{name}", self.read)
            app.router.add_delete(prefix + "/{name}", self.remove)
        app.router.add_get("/api/bots/{bot}/memory", self.notes)
        app.router.add_put("/api/bots/{bot}/memory", self.notes)

    def bot(self, request):
        identifier = request.match_info.get("bot")
        if identifier is None:
            return None
        bot = self.store.bot(identifier)
        if bot is None or bot.temporary:
            raise web.HTTPNotFound(text="Unknown bot")
        return bot

    def root(self, bot):
        return bot_root(self.config, bot) / "skills" if bot else self.config.state / "skills"

    def skills(self, bot):
        return load_bot_skills(self.config, self.root(bot) if bot else None)

    async def catalog(self, request):
        bot = self.bot(request)
        root = self.root(bot)
        return web.json_response(
            [
                dict(
                    name=s.name,
                    description=s.description,
                    disabled=s.disable_model_invocation,
                    managed=s.source.parent.parent == root and not s.source.parent.is_symlink(),
                )
                for s in self.skills(bot)
            ]
        )

    async def read(self, request):
        bot = self.bot(request)
        skill = next((s for s in self.skills(bot) if s.name == request.match_info["name"]), None)
        if skill is None:
            raise web.HTTPNotFound(text="Unknown skill")
        try:
            files = SkillStore(skill.source.parent.parent).read(skill.source.parent.name)
        except (ValueError, OSError):
            raise web.HTTPBadRequest(
                text="This skill cannot be opened as a managed text package"
            ) from None
        return web.json_response(dict(name=skill.name, files=files))

    async def save(self, request):
        bot = self.bot(request)
        try:
            body = await request.json()
            if not isinstance(body, dict) or set(body) != {"name", "files"}:
                raise ValueError("Supply a skill name and files")
            if bot:
                with reserve(self.sessions, bot.id):
                    SkillStore(self.root(bot)).write(body["name"], body["files"])
                    await self.sessions.restart_idle(bot.id)
            else:
                SkillStore(self.root(None)).write(body["name"], body["files"])
        except (ValueError, OSError) as error:
            raise web.HTTPBadRequest(text=str(error)) from None
        except RuntimeError:
            raise web.HTTPConflict(text="This resource is busy") from None
        return web.json_response(dict(saved=body["name"]))

    async def remove(self, request):
        bot = self.bot(request)
        try:
            if bot:
                with reserve(self.sessions, bot.id):
                    SkillStore(self.root(bot)).remove(request.match_info["name"])
                    await self.sessions.restart_idle(bot.id)
            else:
                SkillStore(self.root(None)).remove(request.match_info["name"])
        except (ValueError, OSError) as error:
            raise web.HTTPBadRequest(text=str(error)) from None
        except RuntimeError:
            raise web.HTTPConflict(text="This resource is busy") from None
        return web.json_response(dict(deleted=request.match_info["name"]))

    async def notes(self, request):
        bot = self.bot(request)
        root = bot_root(self.config, bot) / "memory"
        if request.method == "GET":
            try:
                return web.json_response(
                    {
                        name: text_file(safe_file(root, name))
                        if safe_file(root, name).exists()
                        else ""
                        for name in NOTES
                    }
                )
            except (ValueError, OSError):
                raise web.HTTPBadRequest(text="Cannot read this bot's note files") from None
        try:
            body = await request.json()
            if not isinstance(body, dict) or set(body) != {"file", "text"}:
                raise ValueError("Supply a note filename and text")
            with reserve(self.sessions, bot.id):
                write_note(root, body["file"], body["text"])
                await self.sessions.restart_idle(bot.id)
        except (ValueError, OSError) as error:
            raise web.HTTPBadRequest(text=str(error)) from None
        except RuntimeError:
            raise web.HTTPConflict(text="This bot is busy") from None
        return web.json_response(dict(saved=body["file"]))
