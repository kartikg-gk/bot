"""Worker-owned, single-use tool decisions."""

import asyncio
import hashlib
import json
import re
import time
from uuid import uuid4


class ApprovalGate:
    def __init__(self, worker, policy, expiry):
        self.worker, self.policy, self.expiry = worker, policy, expiry
        self.pending = {}

    async def check(self, call):
        from delta_app.safety import is_dangerous_command

        command = call.arguments.get("command")
        if isinstance(command, str) and is_dangerous_command(command):
            return True, "Command rejected by safety policy"
        if call.name in {"Read", "Glob", "Grep"}:
            return False, None
        if self.policy == "auto":
            return False, None
        if self.policy == "deny":
            return True, "Tool execution denied by operator policy"
        preview = json.dumps(call.arguments, ensure_ascii=False, sort_keys=True)
        if len(preview) > 2800 or re.search(
            ("(?i)(api[_-]?key|password|secret|bearer\\s|access[_-]?token|sk-[a-z0-9]{12})"),
            preview,
        ):
            return True, "Cannot safely display the complete action for approval"
        approval = uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[approval] = future
        self.worker.send(
            kind="event",
            event="approval.requested",
            run_id=self.worker.run_id,
            approval_id=approval,
            tool=call.name,
            preview=preview,
            digest=hashlib.sha256(preview.encode()).hexdigest(),
            expires_at=time.time() + self.expiry,
        )
        allow = False
        try:
            allow = await asyncio.wait_for(future, self.expiry)
        except TimeoutError:
            pass
        finally:
            self.pending.pop(approval, None)
            self.worker.send(
                kind="event",
                event="approval.resolved",
                approval_id=approval,
                run_id=self.worker.run_id,
                allow=allow,
            )
        return (False, None) if allow else (True, "User denied action or approval expired")

    def respond(self, approval, allow):
        if type(allow) is not bool or not isinstance(approval, str):
            raise ValueError("invalid approval response")
        future = self.pending.get(approval)
        if future is None or future.done():
            raise ValueError("approval is no longer pending")
        future.set_result(allow)
