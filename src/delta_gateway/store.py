"Durable intake receipts, conversation ownership, and minimal approval audit."

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from delta_gateway import wakeups
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
        self.ensure_chief()

    def memory_index_path(self, state):
        row = self.db.execute(
            "SELECT epoch FROM memory_index_epochs WHERE instance=?", (self.instance,)
        ).fetchone()
        return state / "memory" / (f"qdrant-{row[0]}" if row else "qdrant")

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

    def bots(self, *, temporary=False):
        return [
            Bot(row[0], row[1], row[2], row[3], bool(row[4]), bool(row[5]))
            for row in self.db.execute(
                "SELECT id,name,role,instructions,chief,temporary FROM bots WHERE instance=? "
                "AND (? OR temporary=0) ORDER BY chief DESC,created",
                (self.instance, int(temporary)),
            ).fetchall()
        ]

    def bot(self, identifier):
        return next((bot for bot in self.bots(temporary=True) if bot.id == identifier), None)

    def find_bot(self, name):
        key = name.strip().casefold()
        return next((bot for bot in self.bots() if bot.name.casefold() == key), None)

    def create_bot(self, name, role, instructions="", *, temporary=False):
        name, role = name.strip(), role.strip()
        if not name or not role:
            raise ValueError("A bot needs a name and a purpose")
        if self.find_bot(name):
            raise ValueError(f"A bot named {name} already exists")
        identifier = uuid4().hex
        with self.db:
            self.db.execute(
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
        return self.bot(identifier)

    def keep_bot(self, identifier, name):
        name = name.strip()
        if not name:
            raise ValueError("Give the agent a name")
        if self.find_bot(name):
            raise ValueError(f"A bot named {name} already exists")
        with self.db:
            self.db.execute(
                "UPDATE bots SET name=?,name_key=?,temporary=0 WHERE instance=? AND id=?",
                (name, name.casefold(), self.instance, identifier),
            )

    def remove_bot(self, identifier):
        with self.db:
            self.db.execute(
                "DELETE FROM bots WHERE instance=? AND id=? AND temporary=1",
                (self.instance, identifier),
            )

    def add_task(self, chat, user, actor, thread, bot, goal, policy, follow_up):
        with self.db:
            return self.db.execute(
                "INSERT INTO tasks(instance,chat,user,actor,thread,bot,goal,policy,follow_up,"
                "status,created) VALUES(?,?,?,?,?,?,?,?,?,'running',?)",
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
            ).lastrowid

    def task(self, identifier):
        row = self.db.execute(
            f"SELECT {TASK_FIELDS} FROM tasks WHERE instance=? AND id=?",
            (self.instance, identifier),
        ).fetchone()
        return Task(*row) if row else None

    def tasks(self, limit=20):
        return [
            Task(*row)
            for row in self.db.execute(
                f"SELECT {TASK_FIELDS} FROM tasks WHERE instance=? ORDER BY id DESC LIMIT ?",
                (self.instance, limit),
            ).fetchall()
        ]

    def running_tasks(self):
        return self.db.execute(
            "SELECT count(*) FROM tasks WHERE instance=? AND status='running'", (self.instance,)
        ).fetchone()[0]

    def finish_task(self, identifier, status, answer):
        with self.db:
            self.db.execute(
                "UPDATE tasks SET status=?,answer=?,finished=?,reported=1 "
                "WHERE instance=? AND id=?",
                (status, answer, time.time(), self.instance, identifier),
            )

    def take_unreported(self, *, chat=None):
        with self.db:
            rows = self.db.execute(
                f"SELECT {TASK_FIELDS} FROM tasks WHERE instance=? AND status='interrupted' "
                "AND reported=0 AND (? IS NULL OR chat=?)",
                (self.instance, chat, chat),
            ).fetchall()
            self.db.execute(
                "UPDATE tasks SET reported=1 WHERE instance=? AND status='interrupted' "
                "AND (? IS NULL OR chat=?)",
                (self.instance, chat, chat),
            )
        return [Task(*row) for row in rows]

    def add_report(self, task, text):
        """Queue a prompt for the chat's session, using IDs Telegram updates never reach."""
        with self.db:
            newest = self.db.execute(
                "SELECT max(id) FROM inbox WHERE instance=?", (self.instance,)
            ).fetchone()[0]
            identifier = max(newest or 0, 2**60) + 1
            self.db.execute(
                "INSERT INTO inbox VALUES(?,?,?,?,?,'prompt',NULL,?,?,?)",
                (
                    self.instance,
                    identifier,
                    task.chat,
                    task.actor,
                    text,
                    time.time(),
                    task.thread,
                    task.user,
                ),
            )

    def admit_web(self, chat, user, text, *, base, route=None, run=None):
        """Queue a message from the web page, with IDs Telegram updates never reach."""
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            newest = self.db.execute(
                "SELECT max(id) FROM inbox WHERE instance=? AND id>=? AND id<?",
                (self.instance, base, 2**60),
            ).fetchone()[0]
            identifier = max(newest or 0, base) + 1
            route = route or self.conversation(chat, user)
            self.db.execute(
                "INSERT INTO inbox VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    self.instance,
                    identifier,
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
            if run:
                self.db.execute(
                    "INSERT INTO web_runs(instance,id,inbox,bot,actor) VALUES(?,?,?,?,?)",
                    (self.instance, run, identifier, route.bot, user),
                )
                self.db.execute(
                    "INSERT INTO web_run_events VALUES(?,?,1,'run_queued',?)",
                    (self.instance, run, json.dumps({"bot": route.bot})),
                )
            self.db.execute(
                "INSERT INTO messages(instance,route,direction,text,platform_id,created) "
                "VALUES(?,?,'inbound',?,?,?)",
                (self.instance, route.id, text, f"web-{identifier}", time.time()),
            )
        return identifier

    def history(self, chat, user, limit=100):
        route = self.conversation(chat, user, 0)
        rows = self.db.execute(
            "SELECT direction,text,created FROM messages WHERE instance=? AND route=? "
            "AND text IS NOT NULL ORDER BY id DESC LIMIT ?",
            (self.instance, route.id, limit),
        ).fetchall()
        return [dict(direction=d, text=t, created=c) for d, t, c in reversed(rows)]

    def plugins_for(self, bot):
        return [
            row[0]
            for row in self.db.execute(
                "SELECT plugin FROM bot_plugins WHERE instance=? AND bot=? ORDER BY plugin",
                (self.instance, bot),
            ).fetchall()
        ]

    def set_plugin(self, bot, plugin, enabled):
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO bot_plugins VALUES(?,?,?)"
                if enabled
                else "DELETE FROM bot_plugins WHERE instance=? AND bot=? AND plugin=?",
                (self.instance, bot, plugin),
            )

    def add_job(self, chat, user, actor, thread, bot, task, spec, first, missed="run"):
        if missed not in {"run", "skip"}:
            raise ValueError("Choose run or skip for missed jobs")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            active = self.db.execute(
                "SELECT count(*) FROM jobs WHERE instance=? AND actor=? "
                "AND status IN ('active','paused')",
                (self.instance, actor),
            ).fetchone()[0]
            if active >= MAX_ACTIVE_JOBS:
                raise ValueError(f"Each person can have at most {MAX_ACTIVE_JOBS} scheduled jobs")
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
        if type(identifier) is not int or not 0 < identifier < 2**63:
            return None
        row = self.db.execute(
            f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? AND id=?", (self.instance, identifier)
        ).fetchone()
        return Job(*row) if row else None

    def jobs(self, *, actor=None, bot=None, history=False):
        return [
            Job(*row)
            for row in self.db.execute(
                f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? "
                "AND (? IS NULL OR actor=?) AND (? IS NULL OR bot=?) "
                "AND (? OR status IN ('active','paused') OR running=1) "
                "ORDER BY CASE WHEN status IN ('active','paused') OR running=1 "
                "THEN 0 ELSE 1 END,id DESC LIMIT 100",
                (self.instance, actor, actor, bot, bot, history),
            )
        ]

    def scheduler_heartbeat(self, owner, now, ttl):
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            lease = self.db.execute(
                "SELECT owner,expires FROM scheduler_leases WHERE instance=?", (self.instance,)
            ).fetchone()
            if lease and lease[1] > now and lease[0] != owner:
                return False
            if lease and lease[0] != owner and lease[1] <= now:
                self.db.execute(
                    "UPDATE jobs SET running=0,last_status='interrupted',last_answer=NULL "
                    "WHERE instance=? AND running=1",
                    (self.instance,),
                )
                wakeups.interrupted(self.db, self.instance, now)
            self.db.execute(
                "INSERT INTO scheduler_leases VALUES(?,?,?,?) ON CONFLICT(instance) DO UPDATE "
                "SET owner=excluded.owner,heartbeat=excluded.heartbeat,expires=excluded.expires",
                (self.instance, owner, now, now + ttl),
            )
            return True

    def release_scheduler(self, owner):
        with self.db:
            self.db.execute(
                "DELETE FROM scheduler_leases WHERE instance=? AND owner=?", (self.instance, owner)
            )

    def claim_due(self, now, *, owner=None, chat=None, busy_bots=None):
        """Advance each due job before executing it so an interrupted slot is never replayed."""
        claimed = []
        occupied = set(busy_bots) if busy_bots is not None else None
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            lease = self.db.execute(
                "SELECT owner FROM scheduler_leases WHERE instance=? AND expires>?",
                (self.instance, now),
            ).fetchone()
            if (lease[0] if lease else None) != owner or not wakeups.enabled(
                self.db, self.instance
            ):
                return claimed
            for row in self.db.execute(
                f"SELECT {JOB_FIELDS} FROM jobs WHERE instance=? AND status='active' "
                "AND running=0 AND (? IS NULL OR chat=?) AND (next_run<=? OR EXISTS ("
                "SELECT 1 FROM job_wakeups WHERE instance=jobs.instance AND job=jobs.id "
                "AND status='queued'))",
                (self.instance, chat, chat, now),
            ).fetchall():
                job = Job(*row)
                if occupied is not None and job.bot in occupied:
                    continue
                if job.spec.startswith("event:"):
                    self.db.execute(
                        "UPDATE job_wakeups SET status='running',claimed=? WHERE id=("
                        "SELECT id FROM job_wakeups WHERE instance=? AND job=? "
                        "AND status='queued' ORDER BY id LIMIT 1)",
                        (now, self.instance, job.id),
                    )
                    self.db.execute(
                        "UPDATE jobs SET running=1,last_run=? WHERE id=?", (now, job.id)
                    )
                    job.running, job.last_run = 1, now
                    claimed.append(job)
                    if occupied is not None:
                        occupied.add(job.bot)
                    continue
                upcoming = next_run(job.spec, now)
                status = "active" if upcoming is not None else "done"
                if job.missed == "skip" and now - job.next_run > MISSED_GRACE:
                    self.db.execute(
                        "UPDATE jobs SET next_run=?,status=?,last_status='missed',last_answer=NULL "
                        "WHERE id=?",
                        (upcoming, status, job.id),
                    )
                    continue
                self.db.execute(
                    "UPDATE jobs SET next_run=?,status=?,running=1,last_run=? WHERE id=?",
                    (upcoming, status, now, job.id),
                )
                job.next_run, job.status, job.running, job.last_run = upcoming, status, 1, now
                claimed.append(job)
                if occupied is not None:
                    occupied.add(job.bot)
        return claimed

    def finish_job(self, identifier, status, answer=None, *, claimed_at=None):
        with self.db:
            changed = self.db.execute(
                "UPDATE jobs SET running=0,last_status=?,last_answer=? WHERE instance=? AND id=? "
                "AND (? IS NULL OR (running=1 AND last_run=?))",
                (status, answer, self.instance, identifier, claimed_at, claimed_at),
            ).rowcount
            if changed:
                self.db.execute(
                    "UPDATE job_wakeups SET status=?,finished=? WHERE instance=? AND job=? "
                    "AND claimed=? AND status='running'",
                    (status, time.time(), self.instance, identifier, claimed_at),
                )

    def set_job_paused(self, identifier, actor, paused):
        before, after = ("active", "paused") if paused else ("paused", "active")
        with self.db:
            changed = self.db.execute(
                "UPDATE jobs SET status=? WHERE instance=? AND id=? AND actor=? AND status=?",
                (after, self.instance, identifier, actor, before),
            ).rowcount
        if not changed:
            raise ValueError(f"No {before} job with that number")

    def edit_job(self, identifier, actor, *, bot, task, spec, first, missed):
        with self.db:
            changed = self.db.execute(
                "UPDATE jobs SET bot=?,task=?,spec=coalesce(?,spec),"
                "next_run=CASE WHEN ? IS NULL THEN next_run ELSE ? END,missed=? "
                "WHERE instance=? AND id=? AND actor=? AND status IN ('active','paused') "
                "AND running=0",
                (bot, task, spec, spec, first, missed, self.instance, identifier, actor),
            ).rowcount
        if not changed:
            raise ValueError("Only idle active or paused jobs can be edited")

    def cancel_job(self, identifier, actor=None):
        with self.db:
            changed = self.db.execute(
                "UPDATE jobs SET status='cancelled',next_run=NULL WHERE instance=? AND id=? "
                "AND (? IS NULL OR actor=?) AND (status IN ('active','paused') OR running=1)",
                (self.instance, identifier, actor, actor),
            ).rowcount
        if not changed:
            raise ValueError("No cancellable job with that number")

    def update_bot(self, identifier, role, instructions):
        with self.db:
            self.db.execute(
                "UPDATE bots SET role=?,instructions=? WHERE instance=? AND id=?",
                (role, instructions, self.instance, identifier),
            )
            self.db.execute(
                "UPDATE bot_profiles SET greeting=NULL WHERE instance=? AND bot=?",
                (self.instance, identifier),
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
                "DELETE FROM web_runs WHERE instance=? AND finished<?",
                (self.instance, time.time() - days * 86400),
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
                    "SELECT id,chat,user,text,status,thread,coalesce(route_user,user),run F"
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
