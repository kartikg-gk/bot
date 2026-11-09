"""Submit, follow and stop an individual web run."""

import asyncio
import json

from aiohttp import web

from delta_gateway.web import WEB_CHAT, WEB_IDS, ResourceScope


class RunAPI(ResourceScope):
    def install(self, app):
        app.router.add_get("/api/runs", self.active)
        app.router.add_post("/api/runs", self.create)
        app.router.add_get("/api/runs/{run}", self.get)
        app.router.add_get("/api/runs/{run}/events", self.stream)
        app.router.add_post("/api/runs/{run}/stop", self.stop)
        app.router.add_post("/api/bots/{bot}/runs/{run}/stop", self.stop)
        app.router.add_get("/api/approvals", self.approvals)

    @property
    def journal(self):
        return self.service.runs

    def lookup(self, run):
        result = self.journal.get(run, self.server.actor)
        if result is None:
            raise web.HTTPNotFound(text="Unknown run")
        return result

    async def create(self, request):
        try:
            body = await request.json()
        except ValueError:
            raise web.HTTPBadRequest(text="Send a JSON run request") from None
        if not isinstance(body, dict) or set(body) - {"text", "bot"}:
            raise web.HTTPBadRequest(text="Provide text and an optional bot ID")
        text = body.get("text")
        if not isinstance(text, str) or not text.strip() or len(text) > 16000:
            raise web.HTTPBadRequest(text="Send text up to 16000 characters")
        text = text.strip()
        if text.startswith("/") and not text.startswith("/skill:"):
            raise web.HTTPBadRequest(text="Use the messages endpoint for slash commands")
        bot = None
        if "bot" in body:
            if not isinstance(body["bot"], str):
                raise web.HTTPBadRequest(text="Send a bot ID")
            bot = self.store.bot(body["bot"])
            if bot is None or bot.temporary:
                raise web.HTTPNotFound(text="Unknown bot")
        if self.server.needs_model(request):
            raise web.HTTPConflict(text="Choose a model and API key in Account settings")
        route = self.journal.route(WEB_CHAT, self.server.actor, bot)
        run, _ = self.journal.queue(route, text, WEB_IDS)
        await self.service.dispatch()
        if "text/event-stream" in request.headers.get("Accept", ""):
            return await self.follow(request, run, 0)
        return web.json_response(
            self.lookup(run), status=202, headers={"Location": f"/api/runs/{run}"}
        )

    async def active(self, request):
        bot = request.query.get("bot")
        if request.query.get("recent") == "1":
            return web.json_response(self.journal.recent(self.server.actor, bot))
        runs = self.journal.active(self.server.actor)
        return web.json_response([run for run in runs if not bot or bot == run["bot"]])

    async def get(self, request):
        return web.json_response(self.lookup(request.match_info["run"]))

    async def stop(self, request):
        run = request.match_info["run"]
        current = self.lookup(run)
        bot = request.match_info.get("bot")
        if bot is not None and current["bot"] != bot:
            raise web.HTTPNotFound(text="Unknown run for this bot")
        if not await self.service.stop_web_run(run, self.server.actor):
            raise web.HTTPConflict(text="This run has already stopped or is stopping")
        return web.json_response(self.lookup(run))

    async def approvals(self, request):
        run = request.query.get("run")
        if run:
            self.lookup(run)
        return web.json_response(self.service.web_approvals(self.server.actor, run))

    async def stream(self, request):
        run = request.match_info["run"]
        self.lookup(run)
        raw = request.headers.get("Last-Event-ID", request.query.get("after", "0"))
        if not raw.isascii() or not raw.isdigit() or len(raw) > 16:
            raise web.HTTPBadRequest(text="Invalid event cursor")
        return await self.follow(request, run, int(raw))

    async def follow(self, request, run, after):
        journal = self.journal
        event = journal.subscribe(run)
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-store",
                "X-Accel-Buffering": "no",
                "X-Run-ID": run,
            }
        )
        try:
            await response.prepare(request)
            while not journal.closed:
                if self.server.google and not self.server.account_from_cookie(request):
                    break
                event.clear()
                result = journal.get(run, self.server.actor)
                rows = journal.events(run, after)
                if rows and rows[0][0] > after + 1:
                    gap = json.dumps({"type": "replay_gap", "run": run, "next": rows[0][0]})
                    await response.write(f"event: replay_gap\ndata: {gap}\n\n".encode())
                for seq, kind, data in rows:
                    if self.server.google and not self.server.account_from_cookie(request):
                        return response
                    payload = json.dumps({"type": kind, "run": run, **data}, ensure_ascii=False)
                    await response.write(f"id: {seq}\nevent: {kind}\ndata: {payload}\n\n".encode())
                    after = seq
                if result is None or result["finished"] is not None:
                    break
                try:
                    await asyncio.wait_for(event.wait(), 2)
                except TimeoutError:
                    await response.write(b": keep-alive\n\n")
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            journal.unsubscribe(run, event)
        return response
