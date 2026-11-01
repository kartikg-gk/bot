"""OS-backed session ownership; locks are released even after a crash."""

import os
import string
from pathlib import Path


class SessionLock:
    def __init__(self, directory: Path, session_id: str):
        allowed = string.ascii_letters + string.digits + "-_"
        if not session_id or any(c not in allowed for c in session_id):
            raise ValueError("invalid session ID")
        directory.mkdir(parents=True, exist_ok=True)
        self.file = (directory / f"{session_id}.lock").open("a+b")
        self.file.seek(0, 2)
        if self.file.tell() == 0:
            self.file.write(b"0")
            self.file.flush()
        self.file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError("session is already owned by another worker") from None

    def close(self):
        self.file.close()
