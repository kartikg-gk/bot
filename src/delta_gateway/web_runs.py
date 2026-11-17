"""Durable state and bounded event replay for authenticated web runs."""

import asyncio
import json
import time
from collections import defaultdict
from uuid import uuid4

from delta_gateway.file_tables import hosted
from delta_gateway.hosted_runs import save_context

ACTIVE = ("pending", "prompt", "scheduled", "dispatching", "running", "delivering", "cancelling")
RUN_STATUS = {
    "pending": "queued",
    "prompt": "queued",
    "scheduled": "queued",
    "dispatching": "running",
    "delivering": "running",
}
REPLAY_LIMIT = 512


class RunJournal:
    def __init__(self, store):
        self.store, self.db, self.instance = store, store.shared, store.instance
        self.listeners = defaultdict(set)
        self.closed = False
        self.recovered = False
        self.recovering = asyncio.Lock()

    async def ready(self):
        """Settle runs a stopped gateway left open, once, before the journal is first used."""
        if self.recovered:
            return
        async with self.recovering:
            if not self.recovered:
                await self.recover()

    async def recover(self):
        self.recovered = True
        rows = await self.db.query(
            "SELECT r.id FROM web_runs r JOIN inbox i ON i.instance=r.instance AND i.id=r.inbox "
            "WHERE r.instance=? AND r.finished IS NULL AND i.status NOT IN (?,?,?,?,?,?,?)",
            (self.instance, *ACTIVE),
        )
        for row in rows:
            await self.finish(row["id"])

    async def route(self, chat, actor, bot):
        await self.ready()
        if bot is None:
            return await self.store.conversation(chat, actor)
        await self.db.run(
            "INSERT OR IGNORE INTO web_bot_routes(instance,actor,bot,thread) "
            "SELECT ?,?,?,coalesce(max(thread),0)+1 FROM web_bot_routes "
            "WHERE instance=? AND actor=?",
            (self.instance, actor, bot.id, self.instance, actor),
        )
        rows = await self.db.query(
            "SELECT thread FROM web_bot_routes WHERE instance=? AND actor=? AND bot=?",
            (self.instance, actor, bot.id),
        )
        route = await self.store.conversation(chat, actor, rows[0]["thread"])
        if route.bot != bot.id:
            await self.db.run(
                "UPDATE routes SET bot=? WHERE instance=? AND id=?",
                (bot.id, self.instance, route.id),
            )
            route = await self.store.get_route(route.id)
        return route

    async def queue(self, route, text, base, conversation=None):
        await self.ready()
        run = uuid4().hex
        inbox = await self.store.admit_web(
            route.chat, route.user, text, base=base, route=route, run=run
        )
        if conversation and hosted(self.store):
            await save_context(self.store, inbox, conversation)
        return run, inbox

    async def _rows(self, condition, parameters):
        await self.ready()
        rows = await self.db.query(
            "SELECT r.id,r.bot,r.actor,r.inbox,i.status,i.created,r.finished,r.answer "
            "FROM web_runs r JOIN inbox i ON i.instance=r.instance AND i.id=r.inbox "
            f"WHERE r.instance=? AND {condition}",
            (self.instance, *parameters),
        )
        return [tuple(row.values()) for row in rows]

    async def _row(self, condition, parameters):
        rows = await self._rows(condition, parameters)
        return rows[0] if rows else None

    async def _view(self, row):
        run, bot, _actor, _inbox, status, created, finished, answer = row
        result = dict(
            id=run,
            bot=bot,
            status=RUN_STATUS.get(status, status),
            created=created,
            finished=finished,
            answer=answer,
        )
        if finished is not None:
            data = await self.db.query(
                "SELECT data FROM web_run_events WHERE instance=? AND run=? AND kind='run_usage' "
                "ORDER BY seq DESC LIMIT 1",
                (self.instance, run),
            )
            if data:
                result["usage"] = json.loads(data[0]["data"])
        return result

    async def get(self, run, actor):
        row = await self._row("r.id=? AND r.actor=?", (run, actor))
        return await self._view(row) if row else None

    async def active(self, actor):
        rows = await self._rows(
            "r.actor=? AND r.finished IS NULL AND i.status IN (?,?,?,?,?,?,?) ORDER BY i.id",
            (actor, *ACTIVE),
        )
        return [await self._view(row) for row in rows]

    async def recent(self, actor, bot):
        rows = await self._rows(
            "r.actor=? AND r.finished IS NOT NULL AND (? IS NULL OR r.bot=?) "
            "ORDER BY r.finished DESC LIMIT 10",
            (actor, bot, bot),
        )
        return [await self._view(row) for row in rows]

    async def bot(self, run):
        row = await self._row("r.id=?", (run,))
        return row[1] if row else None

    async def status(self, run):
        row = await self._row("r.id=?", (run,))
        return row[4] if row else None

    async def answer(self, run, answer):
        await self.db.run(
            "UPDATE web_runs SET answer=?,version=version+1 WHERE instance=? AND id=?",
            (answer, self.instance, run),
        )

    async def _append(self, run, kind, details):
        # The next sequence number is taken in the same statement that inserts the event.
        rows = await self.db.query(
            "INSERT INTO web_run_events SELECT ?,?,coalesce(max(seq),0)+1,?,? FROM web_run_events "
            "WHERE instance=? AND run=? RETURNING seq",
            (
                self.instance,
                run,
                kind,
                json.dumps(details, ensure_ascii=False),
                self.instance,
                run,
            ),
        )
        seq = rows[0]["seq"]
        if seq > REPLAY_LIMIT:
            await self.db.run(
                "DELETE FROM web_run_events WHERE instance=? AND run=? AND seq<=?",
                (self.instance, run, seq - REPLAY_LIMIT),
            )

    def _wake(self, run):
        for event in self.listeners.get(run, ()):
            event.set()

    async def emit(self, run, kind, **details):
        await self._append(run, kind, details)
        self._wake(run)

    async def finish(self, run):
        row = await self._row("r.id=?", (run,))
        if not row or row[6] is not None:
            return
        status = row[4]
        if status in ACTIVE and status != "cancelling":
            return
        if status == "cancelling":
            await self.db.run(
                "UPDATE inbox SET status='cancelled',version=version+1 WHERE instance=? AND id=?",
                (self.instance, row[3]),
            )
        # Only the first finisher sets the time and writes the final event.
        if not await self.db.run(
            "UPDATE web_runs SET finished=?,version=version+1 WHERE instance=? AND id=? "
            "AND finished IS NULL",
            (time.time(), self.instance, run),
        ):
            return
        result = await self.get(run, row[2])
        await self._append(run, "run_finished", result)
        self._wake(run)

    async def cancel(self, run, actor):
        row = await self._row("r.id=? AND r.actor=?", (run, actor))
        if not row or row[4] not in ACTIVE or row[4] == "cancelling":
            return False
        await self.store.mark(row[3], "cancelling")
        return True

    async def events(self, run, after):
        await self.ready()
        rows = await self.db.query(
            "SELECT seq,kind,data FROM web_run_events WHERE instance=? AND run=? "
            "AND seq>? ORDER BY seq",
            (self.instance, run, after),
        )
        return [(row["seq"], row["kind"], json.loads(row["data"])) for row in rows]

    async def activity(self, run):
        await self.ready()
        rows = await self.db.query(
            "SELECT seq,kind,data FROM web_run_events WHERE instance=? AND run=? "
            "AND kind IN ('tool_started','tool_finished') ORDER BY seq",
            (self.instance, run),
        )
        events = []
        for row in rows:
            data = json.loads(row["data"])
            events.append(
                dict(
                    seq=row["seq"],
                    type=row["kind"],
                    id=data.get("id"),
                    bot=data.get("bot"),
                    tool=data.get("tool"),
                    failed=bool(data.get("failed")),
                )
            )
        return events

    def subscribe(self, run):
        event = asyncio.Event()
        self.listeners[run].add(event)
        return event

    def unsubscribe(self, run, event):
        listeners = self.listeners[run]
        listeners.discard(event)
        if not listeners:
            self.listeners.pop(run)

    def close(self):
        self.closed = True
        for run in self.listeners:
            self._wake(run)
