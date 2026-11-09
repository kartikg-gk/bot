"""Durable state and bounded event replay for authenticated web runs."""

import asyncio
import json
import time
from collections import defaultdict
from uuid import uuid4

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
        self.store, self.db, self.instance = store, store.db, store.instance
        self.listeners = defaultdict(set)
        self.closed = False
        self.recover()

    def recover(self):
        rows = self.db.execute(
            "SELECT r.id FROM web_runs r JOIN inbox i ON i.instance=r.instance AND i.id=r.inbox "
            "WHERE r.instance=? AND r.finished IS NULL AND i.status NOT IN (?,?,?,?,?,?,?)",
            (self.instance, *ACTIVE),
        ).fetchall()
        for (run,) in rows:
            self.finish(run)

    def route(self, chat, actor, bot):
        if bot is None:
            return self.store.conversation(chat, actor)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO web_bot_routes(instance,actor,bot,thread) "
                "SELECT ?,?,?,coalesce(max(thread),0)+1 FROM web_bot_routes "
                "WHERE instance=? AND actor=?",
                (self.instance, actor, bot.id, self.instance, actor),
            )
            (thread,) = self.db.execute(
                "SELECT thread FROM web_bot_routes WHERE instance=? AND actor=? AND bot=?",
                (self.instance, actor, bot.id),
            ).fetchone()
            route = self.store.conversation(chat, actor, thread)
            if route.bot != bot.id:
                self.db.execute("UPDATE routes SET bot=? WHERE id=?", (bot.id, route.id))
                route = self.store.get_route(route.id)
        return route

    def queue(self, route, text, base):
        run = uuid4().hex
        inbox = self.store.admit_web(route.chat, route.user, text, base=base, route=route, run=run)
        return run, inbox

    def _rows(self, condition, parameters):
        return self.db.execute(
            "SELECT r.id,r.bot,r.actor,r.inbox,i.status,i.created,r.finished,r.answer "
            "FROM web_runs r JOIN inbox i ON i.instance=r.instance AND i.id=r.inbox "
            f"WHERE r.instance=? AND {condition}",
            (self.instance, *parameters),
        )

    def _view(self, row):
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
            data = self.db.execute(
                "SELECT data FROM web_run_events WHERE instance=? AND run=? AND kind='run_usage' "
                "ORDER BY seq DESC LIMIT 1",
                (self.instance, run),
            ).fetchone()
            if data:
                result["usage"] = json.loads(data[0])
        return result

    def get(self, run, actor):
        row = self._rows("r.id=? AND r.actor=?", (run, actor)).fetchone()
        return self._view(row) if row else None

    def active(self, actor):
        rows = self._rows(
            "r.actor=? AND r.finished IS NULL AND i.status IN (?,?,?,?,?,?,?) ORDER BY i.id",
            (actor, *ACTIVE),
        )
        return [self._view(row) for row in rows]

    def recent(self, actor, bot):
        rows = self._rows(
            "r.actor=? AND r.finished IS NOT NULL AND (? IS NULL OR r.bot=?) "
            "ORDER BY r.finished DESC LIMIT 10",
            (actor, bot, bot),
        )
        return [self._view(row) for row in rows]

    def bot(self, run):
        row = self._rows("r.id=?", (run,)).fetchone()
        return row[1] if row else None

    def status(self, run):
        row = self._rows("r.id=?", (run,)).fetchone()
        return row[4] if row else None

    def answer(self, run, answer):
        with self.db:
            self.db.execute(
                "UPDATE web_runs SET answer=? WHERE instance=? AND id=?",
                (answer, self.instance, run),
            )

    def _append(self, run, kind, details):
        (seq,) = self.db.execute(
            "SELECT coalesce(max(seq),0)+1 FROM web_run_events WHERE instance=? AND run=?",
            (self.instance, run),
        ).fetchone()
        self.db.execute(
            "INSERT INTO web_run_events VALUES(?,?,?,?,?)",
            (self.instance, run, seq, kind, json.dumps(details, ensure_ascii=False)),
        )
        if seq > REPLAY_LIMIT:
            self.db.execute(
                "DELETE FROM web_run_events WHERE instance=? AND run=? AND seq<=?",
                (self.instance, run, seq - REPLAY_LIMIT),
            )

    def _wake(self, run):
        for event in self.listeners.get(run, ()):
            event.set()

    def emit(self, run, kind, **details):
        with self.db:
            self._append(run, kind, details)
        self._wake(run)

    def finish(self, run):
        row = self._rows("r.id=?", (run,)).fetchone()
        if not row or row[6] is not None:
            return
        status = row[4]
        if status in ACTIVE and status != "cancelling":
            return
        with self.db:
            if status == "cancelling":
                self.db.execute(
                    "UPDATE inbox SET status='cancelled' WHERE instance=? AND id=?",
                    (self.instance, row[3]),
                )
            self.db.execute(
                "UPDATE web_runs SET finished=? WHERE instance=? AND id=?",
                (time.time(), self.instance, run),
            )
            result = self.get(run, row[2])
            self._append(run, "run_finished", result)
        self._wake(run)

    def cancel(self, run, actor):
        row = self._rows("r.id=? AND r.actor=?", (run, actor)).fetchone()
        if not row or row[4] not in ACTIVE or row[4] == "cancelling":
            return False
        self.store.mark(row[3], "cancelling")
        return True

    def events(self, run, after):
        return [
            (seq, kind, json.loads(data))
            for seq, kind, data in self.db.execute(
                "SELECT seq,kind,data FROM web_run_events WHERE instance=? AND run=? "
                "AND seq>? ORDER BY seq",
                (self.instance, run, after),
            )
        ]

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
