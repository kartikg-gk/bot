"Durable intake receipts, conversation ownership, and minimal approval audit."

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from delta_gateway.bots import Bot
from delta_gateway.migrations import SCHEMA, migrate
from delta_gateway.routing import admit_message
from delta_gateway.schedule import next_run


@dataclass
class Conversation:
    id: int
    instance: str
    chat: int
    user: int
    thread: int
    session: str | None
    session_file: str | None
    title: str | None
    provider: str | None
    model: str | None
    thinking: str | None
    created: float
    updated: float
    bot: str | None = None

    @property
    def key(self):
        return self.chat, self.user, self.thread


@dataclass
class Job:
    id: int
    chat: int
    user: int
    actor: int
    thread: int
    bot: str
    task: str
    spec: str
    next_run: float | None
    missed: str
    status: str
    running: int
    last_status: str | None
    last_run: float | None

    @property
    def key(self):
        return self.chat, self.user, self.thread


JOB_FIELDS = (
    "id,chat,user,actor,thread,bot,task,spec,next_run,missed,status,running,last_status,last_run"
)
MAX_ACTIVE_JOBS = 50
MISSED_GRACE = 300


@dataclass
class InboxItem:
    id: int
    chat: int
    actor: int
    text: str
    status: str
    thread: int
    user: int

    @property
    def key(self):
        return self.chat, self.user, self.thread


class Store:
    def __init__(self, directory, instance, *, retention_days=None):
        directory = Path(directory)
        path = (
            directory if directory.suffix in {".db", ".sqlite3"} else directory / "gateway.sqlite3"
        )
        directory = path.parent
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
        self.path = path.resolve()
        self.retention_days = retention_days
        self.db = sqlite3.connect(path, timeout=5)
        self.instance = instance
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA:
            self.db.close()
            raise ValueError("Database schema is newer than this gateway")
        if version < SCHEMA and path.stat().st_size:
            backup_path = directory / f"gateway.before-v{SCHEMA}-{time.time_ns()}.sqlite3"
            backup = sqlite3.connect(backup_path)
            try:
                self.db.backup(backup)
                backup_path.chmod(0o600)
            finally:
                backup.close()
        deadline = time.monotonic() + 5
        while True:
            try:
                self.db.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    self.db.close()
                    raise
                if time.monotonic() >= deadline:
                    self.db.close()
                    raise
                time.sleep(0.05)
        self.db.execute("PRAGMA foreign_keys=ON")
        try:
            migrate(self.db, version)
        except BaseException:
            self.db.close()
            raise
        path.chmod(0o600)
        with self.db:
            self.db.execute(
                "UPDATE inbox SET status='pending' WHERE instance=? AND status='scheduled'",
                (instance,),
            )
            self.db.execute("INSERT OR IGNORE INTO instances(id) VALUES(?)", (instance,))
            self.db.execute(
                "UPDATE inbox SET status='uncertain' WHERE instance=? AND status IN "
                "('dispatching','running','delivering')",
                (instance,),
            )
            self.db.execute(
                "UPDATE jobs SET running=0,last_status='interrupted' "
                "WHERE instance=? AND running=1",
                (instance,),
            )
            self.db.execute(
                "UPDATE delegations SET status='interrupted',finished=? "
                "WHERE instance=? AND status='running'",
                (time.time(), instance),
            )
            self.db.execute(
                "UPDATE approvals SET decision='denied',source='restart' "
                "WHERE instance=? AND decision IS NULL",
                (instance,),
            )

        self.seed_routes(directory / "sessions")
        self.ensure_chief()

    def ensure_chief(self):
        with self.db:
            if not self.db.execute(
                "SELECT 1 FROM bots WHERE instance=? AND chief=1", (self.instance,)
            ).fetchone():
                self.db.execute(
                    "INSERT INTO bots(id,instance,name,name_key,role,chief,created) "
                    "VALUES(?,?,?,?,?,1,?)",
                    (
                        uuid4().hex,
                        self.instance,
                        "Chief of Staff",
                        "chief of staff",
                        "The user's primary bot.",
                        time.time(),
                    ),
                )

    def bots(self):
        return [
            Bot(row[0], row[1], row[2], row[3], bool(row[4]))
            for row in self.db.execute(
                "SELECT id,name,role,instructions,chief FROM bots WHERE instance=? "
                "ORDER BY chief DESC,created",
                (self.instance,),
            ).fetchall()
        ]

    def bot(self, identifier):
        return next((bot for bot in self.bots() if bot.id == identifier), None)

    def find_bot(self, name):
        key = name.strip().casefold()
        return next((bot for bot in self.bots() if bot.name.casefold() == key), None)

    def create_bot(self, name, role, instructions=""):
        name, role = name.strip(), role.strip()
        if not name or not role:
            raise ValueError("A bot needs a name and a purpose")
        if self.find_bot(name):
            raise ValueError(f"A bot named {name} already exists")
        identifier = uuid4().hex
        with self.db:
            self.db.execute(
                "INSERT INTO bots(id,instance,name,name_key,role,instructions,created) "
                "VALUES(?,?,?,?,?,?,?)",
                (identifier, self.instance, name, name.casefold(), role, instructions, time.time()),
            )
        return self.bot(identifier)

    def add_job(self, chat, user, actor, thread, bot, task, spec, first, missed="run"):
        with self.db:
            active = self.db.execute(
                "SELECT count(*) FROM jobs WHERE instance=? AND status='active'", (self.instance,)
            ).fetchone()[0]
            if active >= MAX_ACTIVE_JOBS:
                raise ValueError(f"At most {MAX_ACTIVE_JOBS} jobs can be active")
            return self.db.execute(
                "INSERT INTO jobs(instance,chat,user,actor,thread,bot,task,spec,next_run,missed,"
                "created) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.instance,
                    chat,
                    user,
                    actor,
                    thread,
                    bot,
                    task,
                    spec,
                    first,
                    missed,
                    time.time(),
                ),
            ).lastrowid

    def job(self, identifier):
        row = self.db.execute(
            f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? AND id=?", (self.instance, identifier)
        ).fetchone()
        return Job(*row) if row else None

    def jobs(self):
        return [
            Job(*row)
            for row in self.db.execute(
                f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? AND status='active' ORDER BY id",
                (self.instance,),
            ).fetchall()
        ]

    def claim_due(self, now):
        """Advance each due job before it runs, so a crash can never run it twice."""
        claimed = []
        with self.db:
            for row in self.db.execute(
                f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? AND status='active' "
                "AND running=0 AND next_run<=?",
                (self.instance, now),
            ).fetchall():
                job = Job(*row)
                upcoming = next_run(job.spec, now)
                status = "active" if upcoming is not None else "done"
                if job.missed == "skip" and now - job.next_run > MISSED_GRACE:
                    self.db.execute(
                        "UPDATE jobs SET next_run=?,status=?,last_status='missed' WHERE id=?",
                        (upcoming, status, job.id),
                    )
                    continue
                self.db.execute(
                    "UPDATE jobs SET next_run=?,status=?,running=1,last_run=? WHERE id=?",
                    (upcoming, status, now, job.id),
                )
                job.next_run, job.status, job.running = upcoming, status, 1
                claimed.append(job)
        return claimed

    def finish_job(self, identifier, status):
        with self.db:
            self.db.execute(
                "UPDATE jobs SET running=0,last_status=? WHERE instance=? AND id=?",
                (status, self.instance, identifier),
            )

    def cancel_job(self, identifier):
        with self.db:
            changed = self.db.execute(
                "UPDATE jobs SET status='cancelled',next_run=NULL "
                "WHERE instance=? AND id=? AND status='active'",
                (self.instance, identifier),
            ).rowcount
        if not changed:
            raise ValueError("No active job with that number")

    def update_bot(self, identifier, role, instructions):
        with self.db:
            self.db.execute(
                "UPDATE bots SET role=?,instructions=? WHERE instance=? AND id=?",
                (role, instructions, self.instance, identifier),
            )

    def begin_delegation(self, route, bot, task):
        try:
            with self.db:
                return self.db.execute(
                    "INSERT INTO delegations(instance,route,bot,task,status,created) "
                    "VALUES(?,?,?,?,'running',?)",
                    (self.instance, route, bot.id, task, time.time()),
                ).lastrowid
        except sqlite3.IntegrityError:
            raise ValueError(f"{bot.name} is busy with another task.") from None

    def finish_delegation(self, identifier, status, answer):
        with self.db:
            self.db.execute(
                "UPDATE delegations SET status=?,answer=?,finished=? WHERE instance=? AND id=?",
                (status, answer, time.time(), self.instance, identifier),
            )

    def switch_bot(self, route, bot):
        """Keep each bot's session on this route so switching back resumes it."""
        fields = "session,session_file,title,provider,model,thinking"
        route = self.get_route(route.id)
        with self.db:
            self.db.execute(
                f"INSERT OR REPLACE INTO route_bots(route,bot,{fields}) VALUES(?,?,?,?,?,?,?,?)",
                (
                    route.id,
                    route.bot,
                    route.session,
                    route.session_file,
                    route.title,
                    route.provider,
                    route.model,
                    route.thinking,
                ),
            )
            saved = (
                self.db.execute(
                    f"SELECT {fields} FROM route_bots WHERE route=? AND bot=?", (route.id, bot.id)
                ).fetchone()
                or (None,) * 6
            )
            self.db.execute(
                "UPDATE routes SET bot=?,session=?,session_file=?,title=?,provider=?,model=?,"
                "thinking=?,updated=? WHERE instance=? AND id=?",
                (bot.id, *saved, time.time(), self.instance, route.id),
            )
        return self.get_route(route.id)

    def bind_bot(self, bot_id):
        old = self.db.execute(
            "SELECT bot_id FROM instances WHERE id=?", (self.instance,)
        ).fetchone()[0]
        if old is not None and old != bot_id:
            raise ValueError("Bot identity differs from configured instance")
        with self.db:
            self.db.execute("UPDATE instances SET bot_id=? WHERE id=?", (bot_id, self.instance))

    @property
    def offset(self):
        row = self.db.execute(
            "SELECT offset FROM poll_state WHERE instance=?", (self.instance,)
        ).fetchone()
        return row[0] if row else 0

    def admit(self, updates, config, bot_id=None, username=""):
        with self.db:
            for update in updates:
                route = admit_message(update.get("message", {}), config, bot_id, username)
                if route:
                    chat, user, thread, text = route
                    route_user = config.route(chat, user, thread)[1]
                    count = self.db.execute(
                        "SELECT count(*) FROM inbox WHERE instance=? AND chat=? "
                        "AND route_user=? AND thread=? AND status IN ('pending','prompt')",
                        (self.instance, chat, route_user, thread),
                    ).fetchone()[0]
                    control = (text.split(maxsplit=1) or [""])[0].split("@")[0] in (
                        "/bot",
                        "/jobs",
                        "/schedule",
                        "/unschedule",
                        "/stop",
                        "/status",
                        "/help",
                        "/start",
                        "/new",
                        "/last",
                        "/sessions",
                        "/switch",
                        "/clone",
                        "/compact",
                        "/export",
                        "/name",
                        "/models",
                        "/model",
                        "/thinking",
                    )
                    status = (
                        "pending"
                        if config.backlog is None or count < config.backlog or control
                        else "rejected"
                    )
                    inserted = self.db.execute(
                        "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?,?,NULL,?,?,?)",
                        (
                            self.instance,
                            update["update_id"],
                            chat,
                            user,
                            text,
                            status,
                            time.time(),
                            thread,
                            route_user,
                        ),
                    ).rowcount
                    if inserted:
                        conversation = self.conversation(chat, route_user, thread)
                        self.db.execute(
                            (
                                "INSERT OR IGNORE INTO messages(instance,route,direction,text,platf"
                                "orm_id,created) VALUES(?,?,'inbound',?,?,?)"
                            ),
                            (
                                self.instance,
                                conversation.id,
                                text,
                                str(update["update_id"]),
                                time.time(),
                            ),
                        )
                self.db.execute(
                    "INSERT INTO poll_state VALUES(?,?) ON CONFLICT(instance) "
                    "DO UPDATE SET offset=max(offset,excluded.offset)",
                    (self.instance, update["update_id"] + 1),
                )
            total = self.db.execute(
                "SELECT count(*) FROM inbox WHERE instance=? "
                "AND status IN ('pending','prompt','rejected')",
                (self.instance,),
            ).fetchone()[0]
            if config.backlog is not None and total > 1000:
                raise RuntimeError(
                    "Inbox capacity reached; resolve pending items before restarting"
                )

    def pending(self):
        return self.db.execute(
            "SELECT id,chat,user,text,status,thread FROM inbox WHERE instance=? "
            "AND status IN ('pending','prompt','rejected') ORDER BY id",
            (self.instance,),
        ).fetchall()

    def mark(self, update, status, run=None):
        with self.db:
            self.db.execute(
                "UPDATE inbox SET status=?,run=coalesce(?,run) WHERE instance=? AND id=?",
                (status, run, self.instance, update),
            )

    def is_pending(self, update):
        return (
            self.db.execute(
                "SELECT 1 FROM inbox WHERE instance=? AND id=? "
                "AND status IN ('pending','prompt','rejected')",
                (self.instance, update),
            ).fetchone()
            is not None
        )

    def mapping(self, chat, user, thread=0):
        return self.db.execute(
            "SELECT session,persisted FROM conversations WHERE instance=? "
            "AND chat=? AND user=? AND thread=?",
            (self.instance, chat, user, thread),
        ).fetchone()

    def map(self, chat, user, thread, session, persisted=False):
        with self.db:
            if session:
                owner = self.db.execute(
                    "SELECT chat,user,thread FROM session_owners WHERE instance=? AND session=?",
                    (self.instance, session),
                ).fetchone()
                if owner and owner != (chat, user, thread):
                    raise ValueError("Session belongs to another conversation")
                self.db.execute(
                    "INSERT OR IGNORE INTO session_owners"
                    "(instance,session,chat,user,created,updated,thread) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (self.instance, session, chat, user, time.time(), time.time(), thread),
                )
            self.db.execute(
                "INSERT INTO conversations VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(instance,chat,user,thread) "
                "DO UPDATE SET session=excluded.session,persisted=excluded.persisted",
                (self.instance, chat, user, session, int(persisted), thread),
            )

    def clear_queue(self, chat, user, thread=0):
        with self.db:
            self.db.execute(
                "UPDATE inbox SET status='cancelled' WHERE instance=? "
                "AND chat=? AND user=? AND thread=? "
                "AND status IN ('pending','prompt','scheduled') AND (text NOT LIKE '/%' "
                "OR status IN ('prompt','scheduled') OR text LIKE '/queue %' "
                "OR text LIKE '/steer %' OR text LIKE '/delta %')",
                (self.instance, chat, user, thread),
            )

    def audit(self, event, chat, user, thread=0):
        with self.db:
            self.db.execute(
                "INSERT INTO approvals VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL,?)",
                (
                    self.instance,
                    event["approval_id"],
                    chat,
                    user,
                    event["run_id"],
                    event["tool"],
                    event["digest"],
                    time.time(),
                    event["expires_at"],
                    thread,
                ),
            )

    def decide(self, approval, allow, source):
        with self.db:
            self.db.execute(
                "UPDATE approvals SET decision=?,source=? WHERE instance=? AND id=? "
                "AND decision IS NULL",
                ("allowed" if allow else "denied", source, self.instance, approval),
            )

    def retain(self, days=None):
        days = days or self.retention_days
        if days is None:
            return
        with self.db:
            self.db.execute(
                "UPDATE inbox SET text=NULL WHERE instance=? AND created<? "
                "AND status IN ('completed','cancelled','dismissed','rejected_done')",
                (self.instance, time.time() - days * 86400),
            )
            self.db.execute(
                ("DELETE FROM approvals WHERE instance=? AND created<? AND decision IS NOT NULL"),
                (self.instance, time.time() - 30 * 86400),
            )
            self.db.execute(
                "DELETE FROM messages WHERE instance=? AND created<?",
                (self.instance, time.time() - days * 86400),
            )

    def seed_routes(self, session_directory):
        with self.db:
            for chat, user, thread, session in self.db.execute(
                "SELECT chat,user,thread,session FROM conversations WHERE instance=?",
                (self.instance,),
            ).fetchall():
                saved = self.saved(session) if session else {}
                self.db.execute(
                    (
                        "INSERT OR IGNORE INTO routes(instance,chat,user,thread,session,ses"
                        "sion_file,title,provider,model,thinking,created,updated) VALUES(?,"
                        "?,?,?,?,?,?,?,?,?,?,?)"
                    ),
                    (
                        self.instance,
                        chat,
                        user,
                        thread,
                        session,
                        str(Path(session_directory) / f"{session}.jsonl") if session else None,
                        saved.get("title"),
                        saved.get("provider"),
                        saved.get("model"),
                        saved.get("thinking"),
                        0,
                        time.time(),
                    ),
                )

    def conversation(self, chat, user, thread=0):
        if not self.db.in_transaction:
            with self.db:
                return self._conversation(chat, user, thread)
        return self._conversation(chat, user, thread)

    def _conversation(self, chat, user, thread):
        self.db.execute(
            (
                "INSERT OR IGNORE INTO routes(instance,chat,user,thread,created,upd"
                "ated) VALUES(?,?,?,?,?,?)"
            ),
            (self.instance, chat, user, thread, time.time(), time.time()),
        )
        self.db.execute(
            "UPDATE routes SET bot=(SELECT id FROM bots WHERE instance=? AND chief=1) "
            "WHERE instance=? AND chat=? AND user=? AND thread=? AND bot IS NULL",
            (self.instance, self.instance, chat, user, thread),
        )
        row = self.db.execute(
            "SELECT * FROM routes WHERE instance=? AND chat=? AND user=? AND thread=?",
            (self.instance, chat, user, thread),
        ).fetchone()
        return Conversation(*row)

    def get_route(self, identifier):
        row = self.db.execute(
            "SELECT * FROM routes WHERE instance=? AND id=?", (self.instance, identifier)
        ).fetchone()
        return Conversation(*row) if row else None

    def list_routes(self, actor, *, groups=True):
        return [
            Conversation(*row)
            for row in self.db.execute(
                (
                    "SELECT * FROM routes WHERE instance=? AND (user=? OR (? AND user=0"
                    ")) ORDER BY updated DESC,id DESC LIMIT 10"
                ),
                (self.instance, actor, int(groups)),
            )
        ]

    def persist_route(self, route, state):
        model = state.get("model") or {}
        with self.db:
            if state.get("sessionFile"):
                self.db.execute(
                    (
                        "UPDATE routes SET title=?,provider=?,model=?,thinking=? WHERE inst"
                        "ance=? AND session_file=?"
                    ),
                    (
                        state.get("sessionName"),
                        model.get("provider"),
                        model.get("id"),
                        state.get("thinkingLevel"),
                        self.instance,
                        state["sessionFile"],
                    ),
                )
            self.db.execute(
                (
                    "UPDATE routes SET session=?,session_file=?,title=?,provider=?,mode"
                    "l=?,thinking=?,updated=? WHERE instance=? AND id=?"
                ),
                (
                    state.get("sessionId"),
                    state.get("sessionFile"),
                    state.get("sessionName"),
                    model.get("provider"),
                    model.get("id"),
                    state.get("thinkingLevel"),
                    time.time(),
                    self.instance,
                    route.id,
                ),
            )
            self.db.execute(
                (
                    "INSERT INTO conversations VALUES(?,?,?,?,?,?) ON CONFLICT(instance"
                    ",chat,user,thread) DO UPDATE SET session=excluded.session,persiste"
                    "d=excluded.persisted"
                ),
                (
                    self.instance,
                    route.chat,
                    route.user,
                    state.get("sessionId"),
                    int(bool(state.get("sessionFile") and Path(state["sessionFile"]).is_file())),
                    route.thread,
                ),
            )
            if state.get("sessionId"):
                self.db.execute(
                    (
                        "INSERT OR IGNORE INTO session_owners(instance,session,chat,user,th"
                        "read,created,updated) VALUES(?,?,?,?,?,?,?)"
                    ),
                    (
                        self.instance,
                        state["sessionId"],
                        route.chat,
                        route.user,
                        route.thread,
                        time.time(),
                        time.time(),
                    ),
                )
                self.remember(
                    route.chat,
                    route.user,
                    route.thread,
                    dict(
                        session_id=state["sessionId"],
                        title=state.get("sessionName"),
                        provider=model.get("provider"),
                        model=model.get("id"),
                        thinking=state.get("thinkingLevel"),
                    ),
                ) if self.owns(route.chat, route.user, route.thread, state["sessionId"]) else None
        for name in ("session", "session_file", "title", "provider", "model", "thinking"):
            setattr(route, name, getattr(self.get_route(route.id), name))

    def point(self, route, source):
        with self.db:
            self.db.execute(
                (
                    "UPDATE routes SET session=?,session_file=?,title=?,provider=?,mode"
                    "l=?,thinking=?,bot=?,updated=? WHERE instance=? AND id=?"
                ),
                (
                    source.session,
                    source.session_file,
                    source.title,
                    source.provider,
                    source.model,
                    source.thinking,
                    source.bot,
                    time.time(),
                    self.instance,
                    route.id,
                ),
            )
        return self.get_route(route.id)

    def items(self):
        return [
            InboxItem(*row)
            for row in self.db.execute(
                (
                    "SELECT id,chat,user,text,status,thread,coalesce(route_user,user) F"
                    "ROM inbox WHERE instance=? AND status IN ('pending','prompt','reje"
                    "cted') ORDER BY id"
                ),
                (self.instance,),
            )
        ]

    def clear_route_queue(self, key):
        with self.db:
            self.db.execute(
                (
                    "UPDATE inbox SET status='cancelled' WHERE instance=? AND chat=? AN"
                    "D route_user=? AND thread=? AND status IN ('pending','prompt','sch"
                    "eduled') AND (text NOT LIKE '/%' OR text LIKE '/delta %' OR text L"
                    "IKE '/queue %' OR text LIKE '/steer %' OR status IN ('prompt','sch"
                    "eduled'))"
                ),
                (self.instance, *key),
            )

    def log_message(self, route, text):
        with self.db:
            self.db.execute(
                (
                    "INSERT INTO messages(instance,route,direction,text,created) VALUES"
                    "(?,?,'outbound',?,?)"
                ),
                (self.instance, route.id, text, time.time()),
            )

    def archive(self):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO instance_archive VALUES(?,?)", (self.instance, time.time())
            )

    def activate(self):
        with self.db:
            self.db.execute("DELETE FROM instance_archive WHERE instance=?", (self.instance,))

    def issues(self):
        return self.db.execute(
            "SELECT id,chat,status,user,thread FROM inbox WHERE instance=? "
            "AND status IN ('uncertain','failed','delivery_failed')",
            (self.instance,),
        ).fetchall()

    def close(self):
        self.db.close()

    def owns(self, chat, user, thread, session):
        return (
            self.db.execute(
                "SELECT 1 FROM session_owners WHERE instance=? AND session=? "
                "AND chat=? AND user=? AND thread=?",
                (self.instance, session, chat, user, thread),
            ).fetchone()
            is not None
        )

    def remember(self, chat, user, thread, details):
        session = details["session_id"]
        if not self.owns(chat, user, thread, session):
            raise ValueError("Unknown session owner")
        with self.db:
            self.db.execute(
                "UPDATE session_owners SET title=?,provider=?,model=?,thinking=?,updated=? "
                "WHERE instance=? AND session=? AND chat=? AND user=? AND thread=?",
                (
                    details.get("title"),
                    details.get("provider"),
                    details.get("model"),
                    details.get("thinking"),
                    time.time(),
                    self.instance,
                    session,
                    chat,
                    user,
                    thread,
                ),
            )
            self.db.execute(
                "INSERT INTO session_status VALUES(?,?,?) ON CONFLICT(instance,session) "
                "DO UPDATE SET data=excluded.data",
                (self.instance, session, json.dumps(details)),
            )

    def saved(self, session):
        self.db.row_factory = sqlite3.Row
        try:
            row = self.db.execute(
                "SELECT * FROM session_owners WHERE instance=? AND session=?",
                (self.instance, session),
            ).fetchone()
            result = dict(row) if row else {}
            if row:
                extra = self.db.execute(
                    "SELECT data FROM session_status WHERE instance=? AND session=?",
                    (self.instance, session),
                ).fetchone()
                if extra:
                    result.update(json.loads(extra[0]))
            return result
        finally:
            self.db.row_factory = None

    def sessions(self, chat, user, thread=0, offset=0):
        return self.db.execute(
            (
                "SELECT session,title,provider,model FROM session_owners WHERE inst"
                "ance=? AND chat=? AND user=? AND thread=? ORDER BY updated DESC,se"
                "ssion LIMIT 21 OFFSET ?"
            ),
            (self.instance, chat, user, thread, offset),
        ).fetchall()

    def replace_prompt(self, update, text):
        with self.db:
            self.db.execute(
                "UPDATE inbox SET text=?,status='prompt' WHERE instance=? AND id=?",
                (text, self.instance, update),
            )

    def finish_run(self, chat, user, thread, run, status):
        with self.db:
            self.db.execute(
                "UPDATE inbox SET status=? WHERE instance=? AND chat=? AND user=? "
                "AND thread=? AND run=? AND status='running'",
                (status, self.instance, chat, user, thread, run),
            )
