"""Collect provider-reported usage from finished model messages."""

from dataclasses import asdict, dataclass
from math import isfinite


@dataclass
class RunUsage:
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    model_messages: int = 0
    cost_usd: float = 0.0

    def observe(self, event):
        if event.get("type") != "message_end":
            return
        message = event.get("message") or {}
        if message.get("role") != "assistant" or not isinstance(message.get("usage"), dict):
            return
        usage = message["usage"]
        self.model_messages += 1
        for key, wire_key in (
            ("input", "input"),
            ("output", "output"),
            ("cache_read", "cacheRead"),
            ("cache_write", "cacheWrite"),
        ):
            value = usage.get(wire_key)
            if isinstance(value, int) and value >= 0:
                setattr(self, key, getattr(self, key) + value)
        cost = (usage.get("cost") or {}).get("total", 0)
        if isinstance(cost, (float, int)) and isfinite(cost) and cost >= 0:
            self.cost_usd += cost

    def result(self):
        return asdict(self)
