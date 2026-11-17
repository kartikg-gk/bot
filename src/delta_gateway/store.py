"Durable intake receipts, conversation ownership, and minimal approval audit."

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from delta_gateway import wakeups
from delta_gateway.bots import Bot
from delta_gateway.database import ConstraintError, SqliteDatabase
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
    last_answer: str | None

    @property
    def key(self):
        return self.chat, self.user, self.thread


JOB_FIELDS = (
    "id,chat,user,actor,thread,bot,task,spec,next_run,missed,status,running,last_status,last_run,"
    "last_answer"
)
MAX_ACTIVE_JOBS = 50
MISSED_GRACE = 300


@dataclass
class Task:
    id: int
    chat: int
    user: int
    actor: int
    thread: int
    bot: str
    goal: str
    policy: str
    follow_up: int
    status: str
    answer: str | None

    @property
    def key(self):
        return self.chat, self.user, self.thread


TASK_FIELDS = "id,chat,user,actor,thread,bot,goal,policy,follow_up,status,answer"


@dataclass
class InboxItem:
    id: int
    chat: int
    actor: int
    text: str
    status: str
    thread: int
    user: int
    run: str | None = None

    @property
    def key(self):
        return self.chat, self.user, self.thread


INBOX = "inbox(instance,id,chat,user,text,status,run,created,thread,route_user)"
LEASES = "scheduler_leases(instance,owner,heartbeat,expires)"
APPROVALS = (
    "approvals(instance,id,chat,user,run,tool,digest,created,expires,decision,source,thread)"
)

CONTROL_COMMANDS = (
    "/bot",
    "/tasks",
    "/cancel",
    "/keep",
    "/discard",
    "/memory",
    "/plugin",
    "/plugins",
    "/jobs",
    "/schedule",
    "/unschedule",
    "/pausejob",
    "/resumejob",
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


ENSURE_CHIEF = (
    "INSERT INTO bots(id,instance,name,name_key,role,chief,created) "
    "SELECT ?,?,?,?,?,1,? WHERE NOT EXISTS (SELECT 1 FROM bots WHERE instance=? AND chief=1)"
)


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
                "('dispatching','running','delivering','cancelling')",
                (instance,),
            )
            self.db.execute(
                "UPDATE tasks SET status='interrupted',finished=? "
                "WHERE instance=? AND status='running'",
                (time.time(), instance),
            )
            self.db.execute(
                "UPDATE jobs SET running=0,last_status='interrupted',last_answer=NULL "
                "WHERE instance=? AND running=1 AND NOT EXISTS ("
                "SELECT 1 FROM scheduler_leases WHERE instance=? AND expires>?)",
                (instance, instance, time.time()),
            )
            wakeups.interrupted(self.db, instance, time.time())
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
        self.db.execute(ENSURE_CHIEF, self.chief_values())
        self.db.commit()
        # Methods move to this shared interface one group at a time; both use the same file.
        self.shared = SqliteDatabase(self.path)
        self.shared.connection.execute("PRAGMA foreign_keys=ON")

    def memory_index_path(self, state):
        row = self.db.execute(
            "SELECT epoch FROM memory_index_epochs WHERE instance=?", (self.instance,)
        ).fetchone()
        return state / "memory" / (f"qdrant-{row[0]}" if row else "qdrant")

    def chief_values(self):
        return (
            uuid4().hex,
            self.instance,
            "Chief of Staff",
            "chief of staff",
            "The user's primary bot.",
            time.time(),
            self.instance,
        )

    async def ensure_chief(self):
        await self.shared.run(ENSURE_CHIEF, self.chief_values())

    async def bots(self, *, temporary=False):
        rows = await self.shared.query(
            "SELECT id,name,role,instructions,chief,temporary FROM bots WHERE instance=? "
            "AND (? OR temporary=0) ORDER BY chief DESC,created",
            (self.instance, int(temporary)),
        )
        return [
            Bot(
                row["id"],
                row["name"],
                row["role"],
                row["instructions"],
                bool(row["chief"]),
                bool(row["temporary"]),
            )
            for row in rows
        ]

    async def bot(self, identifier):
        return next((bot for bot in await self.bots(temporary=True) if bot.id == identifier), None)

    async def find_bot(self, name):
        key = name.strip().casefold()
        return next((bot for bot in await self.bots() if bot.name.casefold() == key), None)

    async def create_bot(self, name, role, instructions="", *, temporary=False):
        name, role = name.strip(), role.strip()
        if not name or not role:
            raise ValueError("A bot needs a name and a purpose")
        if await self.find_bot(name):
            raise ValueError(f"A bot named {name} already exists")
        identifier = uuid4().hex
        await self.shared.run(
            "INSERT INTO bots(id,instance,name,name_key,role,instructions,temporary,created) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (
                identifier,
                self.instance,
                name,
                name.casefold(),
                role,
                instructions,
                int(temporary),
                time.time(),
            ),
        )
        return await self.bot(identifier)

    async def keep_bot(self, identifier, name):
        name = name.strip()
        if not name:
            raise ValueError("Give the agent a name")
        if await self.find_bot(name):
            raise ValueError(f"A bot named {name} already exists")
        await self.shared.run(
            "UPDATE bots SET name=?,name_key=?,temporary=0 WHERE instance=? AND id=?",
            (name, name.casefold(), self.instance, identifier),
        )

    async def remove_bot(self, identifier):
        await self.shared.run(
            "DELETE FROM bots WHERE instance=? AND id=? AND temporary=1",
            (self.instance, identifier),
        )

    async def update_bot(self, identifier, role, instructions):
        await self.shared.run(
            "UPDATE bots SET role=?,instructions=? WHERE instance=? AND id=?",
            (role, instructions, self.instance, identifier),
        )
        await self.shared.run(
            "UPDATE bot_profiles SET greeting=NULL WHERE instance=? AND bot=?",
            (self.instance, identifier),
        )

    def seed_routes(self, session_directory):
        with self.db:
            for chat, user, thread, session in self.db.execute(
                "SELECT chat,user,thread,session FROM conversations WHERE instance=?",
                (self.instance,),
            ).fetchall():
                saved = self._saved_now(session) if session else {}
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
        if hasattr(self, "shared"):
            self.shared.close_now()
        self.db.close()

    def conversation_now(self, chat, user, thread=0):
        """conversation() for offline operator steps that already hold a write transaction."""
        self.db.execute(
            "INSERT OR IGNORE INTO routes(instance,chat,user,thread,created,updated) "
            "VALUES(?,?,?,?,?,?)",
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

    def _saved_now(self, session):
        """Startup copy of saved(), used before the event loop runs."""
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

    async def take_unreported(self, *, chat=None):
        # One statement claims the rows, so two callers never report the same task twice.
        rows = await self.shared.query(
            "UPDATE tasks SET reported=1 WHERE instance=? AND status='interrupted' "
            f"AND reported=0 AND (? IS NULL OR chat=?) RETURNING {TASK_FIELDS}",
            (self.instance, chat, chat),
        )
        return sorted((Task(*row.values()) for row in rows), key=lambda task: task.id)

    async def add_report(self, task, text):
        """Queue a prompt for the chat's session, using IDs Telegram updates never reach."""
        await self.shared.run(
            f"INSERT INTO {INBOX} SELECT ?,max(coalesce((SELECT max(id) FROM inbox "
            "WHERE instance=?),0),CAST(? AS INTEGER))+1,?,?,?,'prompt',NULL,?,?,?",
            (
                self.instance,
                self.instance,
                2**60,
                task.chat,
                task.actor,
                text,
                time.time(),
                task.thread,
                task.user,
            ),
        )

    async def admit_web(self, chat, user, text, *, base, route=None, run=None):
        """Queue a message from the web page, with IDs Telegram updates never reach."""
        route = route or await self.conversation(chat, user)
        # The next ID is taken in the same statement that inserts it, so two writers
        # cannot pick the same one.
        rows = await self.shared.query(
            f"INSERT INTO {INBOX} SELECT ?,max(coalesce((SELECT max(id) FROM inbox "
            "WHERE instance=? AND id>=CAST(? AS INTEGER) AND id<CAST(? AS INTEGER)),0),"
            "CAST(? AS INTEGER))+1,?,?,?,?,?,?,?,? RETURNING id",
            (
                self.instance,
                self.instance,
                base,
                2**60,
                base,
                chat,
                user,
                text,
                "prompt" if run else "pending",
                run,
                time.time(),
                route.thread,
                user,
            ),
        )
        identifier = rows[0]["id"]
        if run:
            await self.shared.run(
                "INSERT INTO web_runs(instance,id,inbox,bot,actor) VALUES(?,?,?,?,?)",
                (self.instance, run, identifier, route.bot, user),
            )
            await self.shared.run(
                "INSERT INTO web_run_events VALUES(?,?,1,'run_queued',?)",
                (self.instance, run, json.dumps({"bot": route.bot})),
            )
        await self.shared.run(
            "INSERT INTO messages(instance,route,direction,text,platform_id,created) "
            "VALUES(?,?,'inbound',?,?,?)",
            (self.instance, route.id, text, f"web-{identifier}", time.time()),
        )
        return identifier

    async def history(self, chat, user, limit=100):
        route = await self.conversation(chat, user, 0)
        rows = await self.shared.query(
            "SELECT direction,text,created FROM messages WHERE instance=? AND route=? "
            "AND text IS NOT NULL ORDER BY id DESC LIMIT ?",
            (self.instance, route.id, limit),
        )
        return [dict(row) for row in reversed(rows)]

    async def switch_bot(self, route, bot):
        """Keep each bot's session on this route so switching back resumes it."""
        fields = "session,session_file,title,provider,model,thinking"
        route = await self.get_route(route.id)
        await self.shared.run(
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
        rows = await self.shared.query(
            f"SELECT {fields} FROM route_bots WHERE route=? AND bot=?", (route.id, bot.id)
        )
        saved = tuple(rows[0].values()) if rows else (None,) * 6
        await self.shared.run(
            "UPDATE routes SET bot=?,session=?,session_file=?,title=?,provider=?,model=?,"
            "thinking=?,updated=? WHERE instance=? AND id=?",
            (bot.id, *saved, time.time(), self.instance, route.id),
        )
        return await self.get_route(route.id)

    async def bind_bot(self, bot_id):
        rows = await self.shared.query("SELECT bot_id FROM instances WHERE id=?", (self.instance,))
        old = rows[0]["bot_id"]
        if old is not None and old != bot_id:
            raise ValueError("Bot identity differs from configured instance")
        await self.shared.run("UPDATE instances SET bot_id=? WHERE id=?", (bot_id, self.instance))

    async def offset(self):
        rows = await self.shared.query(
            "SELECT offset FROM poll_state WHERE instance=?", (self.instance,)
        )
        return rows[0]["offset"] if rows else 0

    async def admit(self, updates, config, bot_id=None, username=""):
        routed = []
        for update in updates:
            route = admit_message(update.get("message", {}), config, bot_id, username)
            routed.append((update, route))
        # Check capacity before writing anything, as the old all-or-nothing batch did.
        if config.backlog is not None and routed:
            total = (
                await self.shared.query(
                    "SELECT count(*) AS n FROM inbox WHERE instance=? "
                    "AND status IN ('pending','prompt','rejected')",
                    (self.instance,),
                )
            )[0]["n"]
            fresh = [update["update_id"] for update, route in routed if route]
            known = set()
            if fresh:
                marks = ",".join("?" * len(fresh))
                known = {
                    row["id"]
                    for row in await self.shared.query(
                        f"SELECT id FROM inbox WHERE instance=? AND id IN ({marks})",
                        (self.instance, *fresh),
                    )
                }
            if total + len(set(fresh) - known) > 1000:
                raise RuntimeError(
                    "Inbox capacity reached; resolve pending items before restarting"
                )
        for update, route in routed:
            if route:
                chat, user, thread, text = route
                route_user = config.route(chat, user, thread)[1]
                count = (
                    await self.shared.query(
                        "SELECT count(*) AS n FROM inbox WHERE instance=? AND chat=? "
                        "AND route_user=? AND thread=? AND status IN ('pending','prompt')",
                        (self.instance, chat, route_user, thread),
                    )
                )[0]["n"]
                control = (text.split(maxsplit=1) or [""])[0].split("@")[0] in CONTROL_COMMANDS
                status = (
                    "pending"
                    if config.backlog is None or count < config.backlog or control
                    else "rejected"
                )
                inserted = await self.shared.run(
                    f"INSERT OR IGNORE INTO {INBOX} VALUES(?,?,?,?,?,?,NULL,?,?,?)",
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
                )
                if inserted:
                    conversation = await self.conversation(chat, route_user, thread)
                    await self.shared.run(
                        "INSERT OR IGNORE INTO messages(instance,route,direction,text,platf"
                        "orm_id,created) VALUES(?,?,'inbound',?,?,?)",
                        (
                            self.instance,
                            conversation.id,
                            text,
                            str(update["update_id"]),
                            time.time(),
                        ),
                    )
            await self.shared.run(
                "INSERT INTO poll_state VALUES(?,?) ON CONFLICT(instance) "
                "DO UPDATE SET offset=max(offset,excluded.offset)",
                (self.instance, update["update_id"] + 1),
            )

    async def pending(self):
        rows = await self.shared.query(
            "SELECT id,chat,user,text,status,thread FROM inbox WHERE instance=? "
            "AND status IN ('pending','prompt','rejected') ORDER BY id",
            (self.instance,),
        )
        return [tuple(row.values()) for row in rows]

    async def mark(self, update, status, run=None):
        await self.shared.run(
            "UPDATE inbox SET status=?,run=coalesce(?,run),version=version+1 "
            "WHERE instance=? AND id=?",
            (status, run, self.instance, update),
        )

    async def is_pending(self, update):
        rows = await self.shared.query(
            "SELECT 1 FROM inbox WHERE instance=? AND id=? "
            "AND status IN ('pending','prompt','rejected')",
            (self.instance, update),
        )
        return bool(rows)

    async def mapping(self, chat, user, thread=0):
        rows = await self.shared.query(
            "SELECT session,persisted FROM conversations WHERE instance=? "
            "AND chat=? AND user=? AND thread=?",
            (self.instance, chat, user, thread),
        )
        return tuple(rows[0].values()) if rows else None

    async def map(self, chat, user, thread, session, persisted=False):
        if session:
            rows = await self.shared.query(
                "SELECT chat,user,thread FROM session_owners WHERE instance=? AND session=?",
                (self.instance, session),
            )
            if rows and tuple(rows[0].values()) != (chat, user, thread):
                raise ValueError("Session belongs to another conversation")
            await self.shared.run(
                "INSERT OR IGNORE INTO session_owners"
                "(instance,session,chat,user,created,updated,thread) "
                "VALUES(?,?,?,?,?,?,?)",
                (self.instance, session, chat, user, time.time(), time.time(), thread),
            )
        await self.shared.run(
            "INSERT INTO conversations VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(instance,chat,user,thread) "
            "DO UPDATE SET session=excluded.session,persisted=excluded.persisted",
            (self.instance, chat, user, session, int(persisted), thread),
        )

    async def clear_queue(self, chat, user, thread=0):
        await self.shared.run(
            "UPDATE inbox SET status='cancelled',version=version+1 WHERE instance=? "
            "AND chat=? AND user=? AND thread=? "
            "AND status IN ('pending','prompt','scheduled') AND (text NOT LIKE '/%' "
            "OR status IN ('prompt','scheduled') OR text LIKE '/queue %' "
            "OR text LIKE '/steer %' OR text LIKE '/delta %')",
            (self.instance, chat, user, thread),
        )

    async def audit(self, event, chat, user, thread=0):
        await self.shared.run(
            f"INSERT INTO {APPROVALS} VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL,?)",
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

    async def decide(self, approval, allow, source):
        await self.shared.run(
            "UPDATE approvals SET decision=?,source=?,version=version+1 WHERE instance=? "
            "AND id=? AND decision IS NULL",
            ("allowed" if allow else "denied", source, self.instance, approval),
        )

    async def retain(self, days=None):
        days = days or self.retention_days
        if days is None:
            return
        cutoff = time.time() - days * 86400
        await self.shared.run(
            "UPDATE inbox SET text=NULL WHERE instance=? AND created<? "
            "AND status IN ('completed','cancelled','dismissed','rejected_done')",
            (self.instance, cutoff),
        )
        await self.shared.run(
            "DELETE FROM approvals WHERE instance=? AND created<? AND decision IS NOT NULL",
            (self.instance, time.time() - 30 * 86400),
        )
        await self.shared.run(
            "DELETE FROM web_runs WHERE instance=? AND finished<?", (self.instance, cutoff)
        )
        await self.shared.run(
            "DELETE FROM messages WHERE instance=? AND created<?", (self.instance, cutoff)
        )

    async def conversation(self, chat, user, thread=0):
        # Each step is safe to repeat, so no transaction is needed around them.
        await self.shared.run(
            "INSERT OR IGNORE INTO routes(instance,chat,user,thread,created,updated) "
            "VALUES(?,?,?,?,?,?)",
            (self.instance, chat, user, thread, time.time(), time.time()),
        )
        await self.shared.run(
            "UPDATE routes SET bot=(SELECT id FROM bots WHERE instance=? AND chief=1) "
            "WHERE instance=? AND chat=? AND user=? AND thread=? AND bot IS NULL",
            (self.instance, self.instance, chat, user, thread),
        )
        rows = await self.shared.query(
            "SELECT * FROM routes WHERE instance=? AND chat=? AND user=? AND thread=?",
            (self.instance, chat, user, thread),
        )
        return Conversation(*rows[0].values())

    async def get_route(self, identifier):
        rows = await self.shared.query(
            "SELECT * FROM routes WHERE instance=? AND id=?", (self.instance, identifier)
        )
        return Conversation(*rows[0].values()) if rows else None

    async def list_routes(self, actor, *, groups=True):
        rows = await self.shared.query(
            "SELECT * FROM routes WHERE instance=? AND (user=? OR (? AND user=0)) "
            "ORDER BY updated DESC,id DESC LIMIT 10",
            (self.instance, actor, int(groups)),
        )
        return [Conversation(*row.values()) for row in rows]

    async def persist_route(self, route, state):
        model = state.get("model") or {}
        if state.get("sessionFile"):
            await self.shared.run(
                "UPDATE routes SET title=?,provider=?,model=?,thinking=? WHERE instance=? "
                "AND session_file=?",
                (
                    state.get("sessionName"),
                    model.get("provider"),
                    model.get("id"),
                    state.get("thinkingLevel"),
                    self.instance,
                    state["sessionFile"],
                ),
            )
        await self.shared.run(
            "UPDATE routes SET session=?,session_file=?,title=?,provider=?,model=?,thinking=?,"
            "updated=? WHERE instance=? AND id=?",
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
        await self.shared.run(
            "INSERT INTO conversations VALUES(?,?,?,?,?,?) ON CONFLICT(instance,chat,user,"
            "thread) DO UPDATE SET session=excluded.session,persisted=excluded.persisted",
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
            await self.shared.run(
                "INSERT OR IGNORE INTO session_owners(instance,session,chat,user,thread,"
                "created,updated) VALUES(?,?,?,?,?,?,?)",
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
            if await self.owns(route.chat, route.user, route.thread, state["sessionId"]):
                await self.remember(
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
                )
        current = await self.get_route(route.id)
        for name in ("session", "session_file", "title", "provider", "model", "thinking"):
            setattr(route, name, getattr(current, name))

    async def point(self, route, source):
        await self.shared.run(
            "UPDATE routes SET session=?,session_file=?,title=?,provider=?,model=?,thinking=?,"
            "bot=?,updated=? WHERE instance=? AND id=?",
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
        return await self.get_route(route.id)

    async def items(self):
        rows = await self.shared.query(
            "SELECT id,chat,user,text,status,thread,coalesce(route_user,user),run FROM inbox "
            "WHERE instance=? AND status IN ('pending','prompt','rejected') ORDER BY id",
            (self.instance,),
        )
        return [InboxItem(*row.values()) for row in rows]

    async def clear_route_queue(self, key):
        await self.shared.run(
            "UPDATE inbox SET status='cancelled',version=version+1 WHERE instance=? AND chat=? "
            "AND route_user=? AND thread=? AND status IN ('pending','prompt','scheduled') AND "
            "(text NOT LIKE '/%' OR text LIKE '/delta %' OR text LIKE '/queue %' OR text "
            "LIKE '/steer %' OR status IN ('prompt','scheduled'))",
            (self.instance, *key),
        )

    async def log_message(self, route, text):
        await self.shared.run(
            "INSERT INTO messages(instance,route,direction,text,created) VALUES"
            "(?,?,'outbound',?,?)",
            (self.instance, route.id, text, time.time()),
        )

    async def owns(self, chat, user, thread, session):
        rows = await self.shared.query(
            "SELECT 1 FROM session_owners WHERE instance=? AND session=? "
            "AND chat=? AND user=? AND thread=?",
            (self.instance, session, chat, user, thread),
        )
        return bool(rows)

    async def remember(self, chat, user, thread, details):
        session = details["session_id"]
        if not await self.owns(chat, user, thread, session):
            raise ValueError("Unknown session owner")
        await self.shared.run(
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
        await self.shared.run(
            "INSERT INTO session_status VALUES(?,?,?) ON CONFLICT(instance,session) "
            "DO UPDATE SET data=excluded.data",
            (self.instance, session, json.dumps(details)),
        )

    async def saved(self, session):
        rows = await self.shared.query(
            "SELECT * FROM session_owners WHERE instance=? AND session=?",
            (self.instance, session),
        )
        if not rows:
            return {}
        result = dict(rows[0])
        extra = await self.shared.query(
            "SELECT data FROM session_status WHERE instance=? AND session=?",
            (self.instance, session),
        )
        if extra:
            result.update(json.loads(extra[0]["data"]))
        return result

    async def sessions(self, chat, user, thread=0, offset=0):
        rows = await self.shared.query(
            "SELECT session,title,provider,model FROM session_owners WHERE instance=? AND chat=? "
            "AND user=? AND thread=? ORDER BY updated DESC,session LIMIT 21 OFFSET ?",
            (self.instance, chat, user, thread, offset),
        )
        return [tuple(row.values()) for row in rows]

    async def replace_prompt(self, update, text):
        await self.shared.run(
            "UPDATE inbox SET text=?,status='prompt',version=version+1 WHERE instance=? AND id=?",
            (text, self.instance, update),
        )

    async def finish_run(self, chat, user, thread, run, status):
        await self.shared.run(
            "UPDATE inbox SET status=?,version=version+1 WHERE instance=? AND chat=? AND user=? "
            "AND thread=? AND run=? AND status='running'",
            (status, self.instance, chat, user, thread, run),
        )

    async def add_task(self, chat, user, actor, thread, bot, goal, policy, follow_up):
        rows = await self.shared.query(
            "INSERT INTO tasks(instance,chat,user,actor,thread,bot,goal,policy,follow_up,"
            "status,created) VALUES(?,?,?,?,?,?,?,?,?,'running',?) RETURNING id",
            (
                self.instance,
                chat,
                user,
                actor,
                thread,
                bot,
                goal,
                policy,
                int(follow_up),
                time.time(),
            ),
        )
        return rows[0]["id"]

    async def task(self, identifier):
        rows = await self.shared.query(
            f"SELECT {TASK_FIELDS} FROM tasks WHERE instance=? AND id=?",
            (self.instance, identifier),
        )
        return Task(*rows[0].values()) if rows else None

    async def tasks(self, limit=20):
        rows = await self.shared.query(
            f"SELECT {TASK_FIELDS} FROM tasks WHERE instance=? ORDER BY id DESC LIMIT ?",
            (self.instance, limit),
        )
        return [Task(*row.values()) for row in rows]

    async def running_tasks(self):
        rows = await self.shared.query(
            "SELECT count(*) AS n FROM tasks WHERE instance=? AND status='running'",
            (self.instance,),
        )
        return rows[0]["n"]

    async def finish_task(self, identifier, status, answer):
        await self.shared.run(
            "UPDATE tasks SET status=?,answer=?,finished=?,reported=1 WHERE instance=? AND id=?",
            (status, answer, time.time(), self.instance, identifier),
        )

    async def plugins_for(self, bot):
        rows = await self.shared.query(
            "SELECT plugin FROM bot_plugins WHERE instance=? AND bot=? ORDER BY plugin",
            (self.instance, bot),
        )
        return [row["plugin"] for row in rows]

    async def set_plugin(self, bot, plugin, enabled):
        await self.shared.run(
            "INSERT OR IGNORE INTO bot_plugins VALUES(?,?,?)"
            if enabled
            else "DELETE FROM bot_plugins WHERE instance=? AND bot=? AND plugin=?",
            (self.instance, bot, plugin),
        )

    async def add_job(self, chat, user, actor, thread, bot, task, spec, first, missed="run"):
        if missed not in {"run", "skip"}:
            raise ValueError("Choose run or skip for missed jobs")
        # The quota check and the insert are one statement, so two requests cannot both
        # slip under the limit.
        rows = await self.shared.query(
            "INSERT INTO jobs(instance,chat,user,actor,thread,bot,task,spec,next_run,missed,"
            "created) SELECT ?,?,?,?,?,?,?,?,?,?,? WHERE (SELECT count(*) FROM jobs "
            "WHERE instance=? AND actor=? AND status IN ('active','paused')) "
            "< CAST(? AS INTEGER) RETURNING id",
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
                self.instance,
                actor,
                MAX_ACTIVE_JOBS,
            ),
        )
        if not rows:
            raise ValueError(f"Each person can have at most {MAX_ACTIVE_JOBS} scheduled jobs")
        return rows[0]["id"]

    async def job(self, identifier):
        if type(identifier) is not int or not 0 < identifier < 2**63:
            return None
        rows = await self.shared.query(
            f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? AND id=?", (self.instance, identifier)
        )
        return Job(*rows[0].values()) if rows else None

    async def jobs(self, *, actor=None, bot=None, history=False):
        rows = await self.shared.query(
            f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? "
            "AND (? IS NULL OR actor=?) AND (? IS NULL OR bot=?) "
            "AND (CAST(? AS INTEGER) OR status IN ('active','paused') OR running=1) "
            "ORDER BY CASE WHEN status IN ('active','paused') OR running=1 "
            "THEN 0 ELSE 1 END,id DESC LIMIT 100",
            (self.instance, actor, actor, bot, bot, int(history)),
        )
        return [Job(*row.values()) for row in rows]

    async def scheduler_heartbeat(self, owner, now, ttl):
        """Take or renew the scheduler lease; False while another owner holds it."""
        if await self.shared.run(
            f"INSERT INTO {LEASES} VALUES(?,?,?,?) ON CONFLICT(instance) DO NOTHING",
            (self.instance, owner, now, now + ttl),
        ):
            return True
        rows = await self.shared.query(
            "SELECT owner,expires,version FROM scheduler_leases WHERE instance=?",
            (self.instance,),
        )
        if not rows:
            return False
        lease = rows[0]
        if lease["owner"] != owner and lease["expires"] > now:
            return False
        # Compare-and-swap on the version read, so only one of two racing owners wins.
        taken = await self.shared.run(
            "UPDATE scheduler_leases SET owner=?,heartbeat=?,expires=?,version=version+1 "
            "WHERE instance=? AND version=?",
            (owner, now, now + ttl, self.instance, lease["version"]),
        )
        if not taken:
            return False
        if lease["owner"] != owner:
            await self.shared.run(
                "UPDATE jobs SET running=0,last_status='interrupted',last_answer=NULL,"
                "version=version+1 WHERE instance=? AND running=1",
                (self.instance,),
            )
            await wakeups.mark_interrupted(self.shared, self.instance, now)
        return True

    async def release_scheduler(self, owner):
        await self.shared.run(
            "DELETE FROM scheduler_leases WHERE instance=? AND owner=?", (self.instance, owner)
        )

    async def claim_due(self, now, *, owner=None, chat=None, busy_bots=None):
        """Advance each due job before executing it so an interrupted slot is never replayed."""
        claimed = []
        occupied = set(busy_bots) if busy_bots is not None else None
        lease = await self.shared.query(
            "SELECT owner FROM scheduler_leases WHERE instance=? AND expires>?",
            (self.instance, now),
        )
        if (lease[0]["owner"] if lease else None) != owner or not await wakeups.automation_on(
            self.shared, self.instance
        ):
            return claimed
        rows = await self.shared.query(
            f"SELECT {JOB_FIELDS},version FROM jobs WHERE instance=? AND status='active' "
            "AND running=0 AND (? IS NULL OR chat=?) AND (next_run<=? OR EXISTS ("
            "SELECT 1 FROM job_wakeups WHERE instance=jobs.instance AND job=jobs.id "
            "AND status='queued'))",
            (self.instance, chat, chat, now),
        )
        for row in rows:
            version = row.pop("version")
            job = Job(*row.values())
            if occupied is not None and job.bot in occupied:
                continue
            # Each claim only lands on the version read, so a job is never claimed twice.
            guard = "WHERE instance=? AND id=? AND version=? AND running=0 AND status='active'"
            if job.spec.startswith("event:"):
                if not await self.shared.run(
                    f"UPDATE jobs SET running=1,last_run=?,version=version+1 {guard}",
                    (now, self.instance, job.id, version),
                ):
                    continue
                await self.shared.run(
                    "UPDATE job_wakeups SET status='running',claimed=? WHERE id=("
                    "SELECT id FROM job_wakeups WHERE instance=? AND job=? "
                    "AND status='queued' ORDER BY id LIMIT 1)",
                    (now, self.instance, job.id),
                )
                job.running, job.last_run = 1, now
                claimed.append(job)
                if occupied is not None:
                    occupied.add(job.bot)
                continue
            upcoming = next_run(job.spec, now)
            status = "active" if upcoming is not None else "done"
            if job.missed == "skip" and now - job.next_run > MISSED_GRACE:
                await self.shared.run(
                    "UPDATE jobs SET next_run=?,status=?,last_status='missed',last_answer=NULL,"
                    f"version=version+1 {guard}",
                    (upcoming, status, self.instance, job.id, version),
                )
                continue
            if not await self.shared.run(
                "UPDATE jobs SET next_run=?,status=?,running=1,last_run=?,version=version+1 "
                + guard,
                (upcoming, status, now, self.instance, job.id, version),
            ):
                continue
            job.next_run, job.status, job.running, job.last_run = upcoming, status, 1, now
            claimed.append(job)
            if occupied is not None:
                occupied.add(job.bot)
        return claimed

    async def finish_job(self, identifier, status, answer=None, *, claimed_at=None):
        changed = await self.shared.run(
            "UPDATE jobs SET running=0,last_status=?,last_answer=?,version=version+1 "
            "WHERE instance=? AND id=? AND (? IS NULL OR (running=1 AND last_run=?))",
            (status, answer, self.instance, identifier, claimed_at, claimed_at),
        )
        if changed:
            await self.shared.run(
                "UPDATE job_wakeups SET status=?,finished=? WHERE instance=? AND job=? "
                "AND claimed=? AND status='running'",
                (status, time.time(), self.instance, identifier, claimed_at),
            )

    async def set_job_paused(self, identifier, actor, paused):
        before, after = ("active", "paused") if paused else ("paused", "active")
        changed = await self.shared.run(
            "UPDATE jobs SET status=?,version=version+1 WHERE instance=? AND id=? AND actor=? "
            "AND status=?",
            (after, self.instance, identifier, actor, before),
        )
        if not changed:
            raise ValueError(f"No {before} job with that number")

    async def edit_job(self, identifier, actor, *, bot, task, spec, first, missed):
        changed = await self.shared.run(
            "UPDATE jobs SET bot=?,task=?,spec=coalesce(?,spec),"
            "next_run=CASE WHEN ? IS NULL THEN next_run ELSE ? END,missed=?,version=version+1 "
            "WHERE instance=? AND id=? AND actor=? AND status IN ('active','paused') "
            "AND running=0",
            (bot, task, spec, spec, first, missed, self.instance, identifier, actor),
        )
        if not changed:
            raise ValueError("Only idle active or paused jobs can be edited")

    async def cancel_job(self, identifier, actor=None):
        changed = await self.shared.run(
            "UPDATE jobs SET status='cancelled',next_run=NULL,version=version+1 WHERE instance=? "
            "AND id=? AND (? IS NULL OR actor=?) AND (status IN ('active','paused') OR running=1)",
            (self.instance, identifier, actor, actor),
        )
        if not changed:
            raise ValueError("No cancellable job with that number")

    async def begin_delegation(self, route, bot, task):
        try:
            rows = await self.shared.query(
                "INSERT INTO delegations(instance,route,bot,task,status,created) "
                "VALUES(?,?,?,?,'running',?) RETURNING id",
                (self.instance, route, bot.id, task, time.time()),
            )
        except ConstraintError:
            raise ValueError(f"{bot.name} is busy with another task.") from None
        return rows[0]["id"]

    async def finish_delegation(self, identifier, status, answer):
        await self.shared.run(
            "UPDATE delegations SET status=?,answer=?,finished=?,version=version+1 "
            "WHERE instance=? AND id=?",
            (status, answer, time.time(), self.instance, identifier),
        )
