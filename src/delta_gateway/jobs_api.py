"""Schedule management for the authenticated web actor."""

from dataclasses import asdict

from aiohttp import web
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from delta_gateway import wakeups
from delta_gateway.schedule import parse_when
from delta_gateway.web import ResourceScope


class JobChanges(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bot: str | None = Field(default=None, min_length=1, max_length=64)
    task: str | None = Field(default=None, min_length=1, max_length=16000)
    when: str | None = Field(default=None, min_length=1, max_length=80)
    missed: str | None = Field(default=None, pattern="^(run|skip)$")


class JobsAPI(ResourceScope):
    def install(self, app):
        app.router.add_get("/api/jobs", self.list)
        app.router.add_post("/api/jobs", self.create)
        app.router.add_put("/api/jobs/automation", self.automation)
        app.router.add_post("/api/jobs/events", self.event)
        path = r"/api/jobs/{job:\d+}"
        app.router.add_get(path, self.details)
        app.router.add_patch(path, self.edit)
        app.router.add_post(path + "/{action:pause|resume|cancel}", self.change_status)

    def view(self, job, *, answer=False):
        result = asdict(job)
        for key in ("chat", "user", "thread", "actor"):
            result.pop(key)
        if not answer:
            result.pop("last_answer")
        result["bot_name"] = getattr(self.store.bot(job.bot), "name", "Deleted bot")
        return result

    def job(self, request):
        identifier = request.match_info["job"]
        job = self.store.job(int(identifier)) if len(identifier) <= 18 else None
        if not job or job.actor != self.server.actor:
            raise web.HTTPNotFound(text="Unknown job")
        return job

    async def changes(self, request):
        try:
            result = JobChanges.model_validate(await request.json()).model_dump(exclude_unset=True)
        except (ValueError, ValidationError):
            raise web.HTTPBadRequest(text="Invalid job settings") from None
        if any(value is None or not value.strip() for value in result.values()):
            raise web.HTTPBadRequest(text="Job settings must contain text")
        return result

    def bot(self, identifier):
        bot = self.store.bot(identifier)
        if bot is None or bot.temporary:
            raise web.HTTPBadRequest(text="Choose a persistent bot")
        return bot

    async def list(self, request):
        heartbeat = self.store.db.execute(
            "SELECT heartbeat,expires FROM scheduler_leases WHERE instance=?",
            (self.store.instance,),
        ).fetchone()
        return web.json_response(
            {
                "automation": wakeups.enabled(self.store.db, self.store.instance),
                "jobs": [
                    self.view(job) for job in self.store.jobs(actor=self.server.actor, history=True)
                ],
                "scheduler": dict(zip(("heartbeat", "expires"), heartbeat, strict=True))
                if heartbeat
                else None,
            }
        )

    async def details(self, request):
        job = self.job(request)
        data = self.view(job, answer=True)
        data["wakeups"] = wakeups.history(self.store.db, self.store.instance, job.id)
        return web.json_response(data)

    async def automation(self, request):
        try:
            body = await request.json()
        except ValueError:
            body = None
        if (
            not isinstance(body, dict)
            or set(body) != {"enabled"}
            or type(body["enabled"]) is not bool
        ):
            raise web.HTTPBadRequest(text="Provide a Boolean enabled setting")
        wakeups.set_enabled(self.store.db, self.store.instance, body["enabled"])
        if body["enabled"]:
            self.service.run_due()
        return web.json_response(body)

    async def event(self, request):
        try:
            body = await request.json()
            if not isinstance(body, dict) or set(body) != {"event", "delivery"}:
                raise ValueError("Provide an event name and a stable delivery ID")
            result = wakeups.receive(
                self.store.db,
                self.store.instance,
                self.server.actor,
                body["event"],
                body["delivery"],
            )
        except ValueError as error:
            raise web.HTTPBadRequest(text=str(error)) from None
        except RuntimeError as error:
            raise web.HTTPConflict(text=str(error)) from None
        if result["accepted"]:
            self.service.run_due()
        return web.json_response(result, status=202)

    async def create(self, request):
        from delta_gateway.web import WEB_CHAT

        values = await self.changes(request)
        if not {"bot", "when", "task"} <= values.keys():
            raise web.HTTPBadRequest(text="Choose a bot, timing, and task")
        bot = self.bot(values["bot"])
        route = self.store.conversation(WEB_CHAT, self.server.actor)
        try:
            spec, first = parse_when(values["when"])
            identifier = self.store.add_job(
                *route.key[:2],
                self.server.actor,
                route.thread,
                bot.id,
                values["task"].strip(),
                spec,
                first,
                values.get("missed", "run"),
            )
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from None
        return web.json_response(self.view(self.store.job(identifier)), status=201)

    async def edit(self, request):
        values = await self.changes(request)
        job = self.job(request)
        bot = self.bot(values.get("bot", job.bot))
        try:
            spec, first = parse_when(values["when"]) if "when" in values else (None, None)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from None
        try:
            self.store.edit_job(
                job.id,
                self.server.actor,
                bot=bot.id,
                task=values.get("task", job.task).strip(),
                spec=spec,
                first=first,
                missed=values.get("missed", job.missed),
            )
        except ValueError as exc:
            raise web.HTTPConflict(text=str(exc)) from None
        return web.json_response(self.view(self.store.job(job.id)))

    async def change_status(self, request):
        job = self.job(request)
        action = request.match_info["action"]
        try:
            if action == "cancel":
                self.service.cancel_job(job.id, self.server.actor)
            else:
                self.store.set_job_paused(job.id, self.server.actor, action == "pause")
        except ValueError as exc:
            raise web.HTTPConflict(text=str(exc)) from None
        return web.json_response(self.view(self.store.job(job.id)))
