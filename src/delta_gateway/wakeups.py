"""Durable event intake for unattended jobs."""

import re
import time

EVENT_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
DELIVERY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
MAX_QUEUED = 1000


def enabled(db, instance):
    row = db.execute("SELECT enabled FROM job_automation WHERE instance=?", (instance,)).fetchone()
    return bool(row[0]) if row else True


async def set_enabled(db, instance, value):
    await db.run(
        "INSERT INTO job_automation VALUES(?,?) ON CONFLICT(instance) "
        "DO UPDATE SET enabled=excluded.enabled",
        (instance, int(value)),
    )


async def receive(db, instance, actor, name, delivery):
    if not isinstance(name, str) or not EVENT_NAME.fullmatch(name):
        raise ValueError("Provide an event name using lowercase letters, numbers, dots or dashes")
    if not isinstance(delivery, str) or not DELIVERY_ID.fullmatch(delivery):
        raise ValueError("Provide a stable delivery ID up to 128 characters")
    if not await automation_on(db, instance):
        raise RuntimeError("Automatic wake-ups are disabled")
    now = time.time()
    # The receipt is the deduplication: only the first delivery of an ID inserts it.
    if not await db.run(
        "INSERT OR IGNORE INTO job_event_receipts VALUES(?,?,?,?,?)",
        (instance, actor, name, delivery, now),
    ):
        return {"accepted": 0, "duplicate": True}
    queued = (
        await db.query(
            "SELECT count(*) AS n FROM job_wakeups WHERE instance=? AND actor=? "
            "AND status='queued'",
            (instance, actor),
        )
    )[0]["n"]
    jobs = await db.query(
        "SELECT id FROM jobs WHERE instance=? AND actor=? AND status='active' AND spec=?",
        (instance, actor, "event:" + name),
    )
    if queued + len(jobs) > MAX_QUEUED:
        # Forget the receipt so the same delivery can be retried once there is room.
        await db.run(
            "DELETE FROM job_event_receipts WHERE instance=? AND actor=? AND event=? "
            "AND delivery=?",
            (instance, actor, name, delivery),
        )
        raise RuntimeError("The automatic wake-up queue is full")
    for job in jobs:
        await db.run(
            "INSERT INTO job_wakeups(instance,actor,job,event,delivery,received) "
            "VALUES(?,?,?,?,?,?)",
            (instance, actor, job["id"], name, delivery, now),
        )
    return {"accepted": len(jobs), "duplicate": False}


async def history(db, instance, job):
    return await db.query(
        "SELECT event,delivery,received,claimed,finished,status FROM job_wakeups "
        "WHERE instance=? AND job=? ORDER BY id DESC LIMIT 20",
        (instance, job),
    )


def interrupted(db, instance, now):
    db.execute(
        "UPDATE job_wakeups SET status='interrupted',finished=? "
        "WHERE instance=? AND status='running' "
        "AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.instance=job_wakeups.instance "
        "AND jobs.id=job_wakeups.job AND jobs.running=1 AND jobs.last_run=job_wakeups.claimed)",
        (now, instance),
    )


async def automation_on(db, instance):
    """enabled() for the shared database interface."""
    rows = await db.query("SELECT enabled FROM job_automation WHERE instance=?", (instance,))
    return bool(rows[0]["enabled"]) if rows else True


async def mark_interrupted(db, instance, now):
    """interrupted() for the shared database interface."""
    await db.run(
        "UPDATE job_wakeups SET status='interrupted',finished=? "
        "WHERE instance=? AND status='running' "
        "AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.instance=job_wakeups.instance "
        "AND jobs.id=job_wakeups.job AND jobs.running=1 AND jobs.last_run=job_wakeups.claimed)",
        (now, instance),
    )
