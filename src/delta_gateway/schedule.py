"""Job timing: `every 30m`, `daily 08:00`, or `at 2026-11-02T08:00+05:30`."""

import re
import time
from datetime import datetime, timedelta

from delta_gateway.wakeups import EVENT_NAME

MIN_INTERVAL = 60
UNITS = {"m": 60, "h": 3600, "d": 86400}


def parse_when(text, now=None):
    """Return the stored schedule and first run time."""
    now = time.time() if now is None else now
    text = text.strip().lower()
    if text.startswith("on ") and EVENT_NAME.fullmatch(text[3:].strip()):
        return "event:" + text[3:].strip(), None
    if match := re.fullmatch(r"every\s+(\d{1,6})\s*([mhd])", text):
        seconds = int(match[1]) * UNITS[match[2]]
        if seconds < MIN_INTERVAL:
            raise ValueError("Jobs can repeat at most once a minute")
        spec = f"every:{seconds}"
    elif match := re.fullmatch(r"daily\s+([01]?\d|2[0-3]):([0-5]\d)", text):
        spec = f"daily:{int(match[1]):02}:{match[2]}"
    elif match := re.fullmatch(r"at\s+(\S+)", text):
        try:
            first = datetime.fromisoformat(match[1].upper()).timestamp()
        except ValueError:
            raise ValueError("Use an ISO time such as at 2026-11-02T08:00+05:30") from None
        if first <= now:
            raise ValueError("That time has already passed")
        return "once", first
    else:
        raise ValueError("Use every 30m, daily 08:00, at ISO-time, or on event-name")
    return spec, next_run(spec, now)


def next_run(spec, now):
    """The first run after now, or None when the job does not repeat."""
    kind, _, value = spec.partition(":")
    if kind == "every":
        return now + int(value)
    if kind == "daily":
        hour, minute = map(int, value.split(":"))
        current = datetime.fromtimestamp(now).astimezone()
        target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= current:
            target += timedelta(days=1)
        return target.timestamp()
    return None
