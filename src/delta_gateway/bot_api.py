"""Authenticated web operations for bot profiles and their own conversations."""

import asyncio
from dataclasses import asdict

from aiohttp import web
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from delta_gateway.bot_profiles import (
    profile,
    record_chat,
    reserve,
    save_profile,
    validate_profile,
)
from delta_gateway.database import ConstraintError
from delta_gateway.web import ResourceScope, reply_markup


class BotChanges(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=100)
    role: str | None = Field(default=None, min_length=1, max_length=10000)
    instructions: str | None = Field(default=None, max_length=10000)
    style: str | None = Field(default=None, pattern="^(direct|thoughtful|upbeat|custom)$")
    voice: str | None = Field(default=None, max_length=2000)
    color: str | None = Field(default=None, pattern="^#[0-9a-fA-F]{6}$")
    shape: str | None = Field(default=None, pattern="^(orb|diamond|square)$")


class BotAPI(ResourceScope):
    def install(self, app):
        app.router.add_get("/api/bots", self.list)
        app.router.add_get("/api/bots/unread-counts", self.reply_counts)
        app.router.add_post("/api/bots", self.create)
        app.router.add_patch("/api/bots/{bot}", self.update)
        app.router.add_delete("/api/bots/{bot}", self.delete)
        app.router.add_get("/api/bots/{bot}/messages", self.messages)
        app.router.add_get("/api/bots/{bot}/export", self.export)
        app.router.add_post("/api/bots/{bot}/select", self.select)
        app.router.add_post("/api/bots/{bot}/greeting", self.greeting)

    async def bot(self, request):
        bot = await self.store.bot(request.match_info["bot"])
        if bot is None:
            raise web.HTTPNotFound(text="Unknown bot")
        return bot

    async def changes(self, request):
        try:
            data = await request.json()
            result = BotChanges.model_validate(data)
        except (ValueError, ValidationError):
            raise web.HTTPBadRequest(text="Invalid bot settings") from None
        values = result.model_dump(exclude_unset=True)
        if any(value is None for value in values.values()):
            raise web.HTTPBadRequest(text="Bot settings cannot be null")
        if any(key in values and not values[key].strip() for key in ("name", "role")):
            raise web.HTTPBadRequest(text="A name and purpose must contain text")
        return values

    async def view(self, bot):
        return {
            **asdict(bot),
            **await profile(self.store, bot),
            "busy": bot.id in self.sessions.active_bots,
        }

    async def list(self, request):
        from delta_gateway.web import WEB_CHAT

        selected = (await self.store.conversation(WEB_CHAT, self.server.actor)).bot
        return web.json_response(
            [
                {**await self.view(bot), "selected": bot.id == selected}
                for bot in await self.store.bots()
            ]
        )

    async def reply_counts(self, request):
        rows = await self.store.shared.query(
            "SELECT bot,count(*) AS n FROM bot_chat WHERE instance=? AND role='assistant' "
            "GROUP BY bot",
            (self.store.instance,),
        )
        return web.json_response(
            {row["bot"]: row["n"] for row in rows}, headers={"Cache-Control": "no-store"}
        )

    async def create(self, request):
        values = await self.changes(request)
        if not values.get("name") or not values.get("role"):
            raise web.HTTPBadRequest(text="Provide a name and purpose")
        try:
            bot = await self.store.create_bot(
                values["name"], values["role"], values.get("instructions", "")
            )
            await save_profile(
                self.store,
                bot,
                {k: v for k, v in values.items() if k in {"style", "voice", "color", "shape"}},
            )
        except (ValueError, ConstraintError):
            raise web.HTTPConflict(text="That bot name is already in use") from None
        return web.json_response(await self.view(bot), status=201)

    async def update(self, request):
        bot = await self.bot(request)
        values = await self.changes(request)
        try:
            async with reserve(self.sessions, bot.id):
                await self.sessions.restart_idle(bot.id)
                # Profile values are checked first, so a rejected profile leaves the bot as it was.
                changes = {
                    k: v for k, v in values.items() if k in {"style", "voice", "color", "shape"}
                }
                check = {**await profile(self.store, bot), **changes}
                validate_profile(check)
                await self.store.shared.run(
                    "UPDATE bots SET name=?,name_key=?,role=?,instructions=?"
                    " WHERE instance=? AND id=?",
                    (
                        values.get("name", bot.name).strip(),
                        values.get("name", bot.name).strip().casefold(),
                        values.get("role", bot.role),
                        values.get("instructions", bot.instructions),
                        self.store.instance,
                        bot.id,
                    ),
                )
                await save_profile(self.store, bot, changes)
        except (ValueError, RuntimeError, ConstraintError) as error:
            raise web.HTTPConflict(text=str(error)) from None
        return web.json_response(await self.view(await self.store.bot(bot.id)))

    async def delete(self, request):
        bot = await self.bot(request)
        if bot.chief:
            raise web.HTTPConflict(text="The chief can be edited but cannot be deleted")
        pending = await self.store.shared.query(
            "SELECT 1 FROM inbox i JOIN routes r ON r.instance=i.instance AND r.chat=i.chat "
            "AND r.user=coalesce(i.route_user,i.user) AND r.thread=i.thread "
            "WHERE r.instance=? AND r.bot=? "
            "AND i.status IN ('pending','prompt','scheduled') LIMIT 1",
            (self.store.instance, bot.id),
        )
        routes = await self.store.shared.query(
            "SELECT chat,user,thread FROM routes WHERE instance=? AND bot=?",
            (self.store.instance, bot.id),
        )
        queued = any(tuple(row.values()) in self.service.jobs for row in routes)
        if pending or queued:
            raise web.HTTPConflict(
                text="Finish or cancel this bot's queued messages before deleting it"
            )
        try:
            async with reserve(self.sessions, bot.id):
                await self.sessions.restart_idle(bot.id)
                computer = self.sessions.computer_for(bot)
                if hasattr(computer, "remove"):
                    await computer.remove()
                chief = (await self.store.bots())[0].id
                shared, instance = self.store.shared, self.store.instance
                # Each step is safe to repeat; the bot row goes last, so an interrupted
                # delete can simply be run again.
                for table in ("bot_profiles", "bot_sessions", "bot_chat", "bot_plugins"):
                    await shared.run(
                        f"DELETE FROM {table} WHERE instance=? AND bot=?", (instance, bot.id)
                    )
                await shared.run(
                    "DELETE FROM route_bots WHERE route IN (SELECT id FROM routes WHERE "
                    "instance=?) AND bot=?",
                    (instance, bot.id),
                )
                await shared.run(
                    "UPDATE jobs SET status='cancelled',next_run=NULL,version=version+1"
                    " WHERE instance=? AND bot=? AND status IN ('active','paused')",
                    (instance, bot.id),
                )
                await shared.run(
                    "UPDATE routes SET bot=?,session=NULL,session_file=NULL"
                    " WHERE instance=? AND bot=?",
                    (chief, instance, bot.id),
                )
                await shared.run("DELETE FROM bots WHERE instance=? AND id=?", (instance, bot.id))
                self.sessions.computers.pop(bot.id, None)
        except (ValueError, RuntimeError, OSError) as error:
            raise web.HTTPConflict(text=str(error)) from None
        return web.json_response({"deleted": bot.id})

    async def select(self, request):
        bot = await self.bot(request)
        from delta_gateway.web import WEB_CHAT

        route = await self.store.conversation(WEB_CHAT, self.server.actor)
        pending = any(
            row[1] == route.chat and row[2] == route.user and row[5] == route.thread
            for row in await self.store.pending()
        )
        if (route.chat, route.user, route.thread) in self.service.jobs or pending:
            raise web.HTTPConflict(text="Wait for the current conversation to finish")
        await self.sessions.switch_bot(route, bot, resume_bot=True)
        return web.json_response(await self.view(bot))

    async def messages(self, request):
        bot = await self.bot(request)
        rows = await self.store.shared.query(
            "SELECT role,text FROM bot_chat WHERE instance=? AND bot=? ORDER BY id DESC LIMIT 200",
            (self.store.instance, bot.id),
        )
        messages = [dict(row) for row in reversed(rows)]
        return web.json_response(
            [reply_markup(row) if row["role"] == "assistant" else row for row in messages]
        )

    async def export(self, request):
        bot = await self.bot(request)
        rows = await self.store.shared.query(
            "SELECT role,text FROM bot_chat WHERE instance=? AND bot=? ORDER BY id",
            (self.store.instance, bot.id),
        )
        return web.json_response(
            [dict(row) for row in rows],
            headers={"Cache-Control": "no-store"},
        )

    async def greeting(self, request):
        bot = await self.bot(request)
        saved = (await profile(self.store, bot))["greeting"]
        if saved:
            return web.json_response({"text": saved})
        if bot.id in self.sessions.active_bots:
            raise web.HTTPConflict(text="Wait for this bot to finish")
        # Generate through the same budgeted, read-only execution path as other bot work.
        try:
            async with asyncio.timeout(30):
                text, outcome = await self.sessions.delegate(
                    bot,
                    "Greet the user in one short sentence and say what you can help with. "
                    "Do not use tools.",
                    policy="deny",
                    review_durable=False,
                    persist=False,
                )
        except TimeoutError:
            raise web.HTTPServiceUnavailable(text="Greeting generation timed out") from None
        except (ValueError, RuntimeError):
            raise web.HTTPConflict(text="This bot is unavailable or working") from None
        if outcome != "completed":
            raise web.HTTPServiceUnavailable(text="Greeting generation failed")
        settings = await profile(self.store, bot)
        await save_profile(self.store, bot, settings)
        await self.store.shared.run(
            "UPDATE bot_profiles SET greeting=? WHERE instance=? AND bot=?",
            (text, self.store.instance, bot.id),
        )
        await record_chat(self.store, bot.id, "assistant", text)
        return web.json_response({"text": text})
