"""Durable event intake for unattended jobs."""

import re
import time

EVENT_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
DELIVERY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
MAX_QUEUED = 1000


def enabled(db, instance):
    row = db.execute("SELECT enabled FROM job_automation WHERE instance=?", (instance,)).fetchone()
    return bool(row[0]) if row else True


def set_enabled(db, instance, value):
    with db:
        db.execute(
            "INSERT INTO job_automation VALUES(?,?) ON CONFLICT(instance) "
            "DO UPDATE SET enabled=excluded.enabled",
            (instance, int(value)),
        )


def receive(db, instance, actor, name, delivery):
    if not isinstance(name, str) or not EVENT_NAME.fullmatch(name):
        raise ValueError("Provide an event name using lowercase letters, numbers, dots or dashes")
    if not isinstance(delivery, str) or not DELIVERY_ID.fullmatch(delivery):
        raise ValueError("Provide a stable delivery ID up to 128 characters")
    with db:
        db.execute("BEGIN IMMEDIATE")
        if not enabled(db, instance):
            raise RuntimeError("Automatic wake-ups are disabled")
        previous = db.execute(
            "SELECT 1 FROM job_event_receipts WHERE instance=? AND actor=? "
            "AND event=? AND delivery=?",
            (instance, actor, name, delivery),
        ).fetchone()
        if previous:
            return {"accepted": 0, "duplicate": True}
        (queued,) = db.execute(
            "SELECT count(*) FROM job_wakeups WHERE instance=? AND actor=? AND status='queued'",
            (instance, actor),
        ).fetchone()
        jobs = db.execute(
            "SELECT id FROM jobs WHERE instance=? AND actor=? AND status='active' AND spec=?",
            (instance, actor, "event:" + name),
        ).fetchall()
        if queued + len(jobs) > MAX_QUEUED:
            raise RuntimeError("The automatic wake-up queue is full")
        now = time.time()
        db.execute(
            "INSERT INTO job_event_receipts VALUES(?,?,?,?,?)",
            (instance, actor, name, delivery, now),
        )
        db.executemany(
            "INSERT INTO job_wakeups(instance,actor,job,event,delivery,received) "
            "VALUES(?,?,?,?,?,?)",
            [(instance, actor, job, name, delivery, now) for (job,) in jobs],
        )
        return {"accepted": len(jobs), "duplicate": False}


def history(db, instance, job):
    rows = db.execute(
        "SELECT event,delivery,received,claimed,finished,status FROM job_wakeups "
        "WHERE instance=? AND job=? ORDER BY id DESC LIMIT 20",
        (instance, job),
    )
    keys = ("event", "delivery", "received", "claimed", "finished", "status")
    return [dict(zip(keys, row, strict=True)) for row in rows]


def interrupted(db, instance, now):
    db.execute(
        "UPDATE job_wakeups SET status='interrupted',finished=? "
        "WHERE instance=? AND status='running' "
        "AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.instance=job_wakeups.instance "
        "AND jobs.id=job_wakeups.job AND jobs.running=1 AND jobs.last_run=job_wakeups.claimed)",
        (now, instance),
    )
