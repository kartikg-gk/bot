"""Private identity, session and credential records for web accounts."""

import base64
import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

SESSION_SECONDS = 12 * 60 * 60
FLOW_SECONDS = 600
VALIDATION_SECONDS = 300
VALIDATION_LIMIT = 5


def digest(value):
    return hashlib.sha256(value.encode()).digest()


class AccountStore:
    def __init__(self, path, realm, encoded_key):
        try:
            key = base64.b64decode(encoded_key, altchars=b"-_", validate=True)
            if len(key) != 32:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError(
                "DELTA_GATEWAY_MASTER_KEY must encode 32 random bytes as base64"
            ) from None
        self.cipher, self.realm = AESGCM(key), realm
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        path.chmod(0o600)
        self.db.executescript("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS accounts(
                id TEXT PRIMARY KEY, subject TEXT NOT NULL UNIQUE, email TEXT NOT NULL,
                domain TEXT, provider TEXT, model TEXT);
            CREATE TABLE IF NOT EXISTS flows(
                state BLOB PRIMARY KEY, browser BLOB NOT NULL, verifier TEXT NOT NULL,
                nonce TEXT NOT NULL, expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS app_flows(
                account TEXT NOT NULL REFERENCES accounts(id), app TEXT NOT NULL,
                session BLOB NOT NULL, state BLOB NOT NULL, verifier TEXT NOT NULL,
                expires REAL NOT NULL, PRIMARY KEY(account,app,session));
            CREATE TABLE IF NOT EXISTS sessions(
                token BLOB PRIMARY KEY, account TEXT NOT NULL REFERENCES accounts(id),
                expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS credentials(
                account TEXT NOT NULL REFERENCES accounts(id), kind TEXT NOT NULL,
                name TEXT NOT NULL, sealed BLOB NOT NULL, PRIMARY KEY(account,kind,name));
            CREATE TABLE IF NOT EXISTS attempts(
                account TEXT NOT NULL REFERENCES accounts(id), attempted REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS attempts_account ON attempts(account,attempted);
        """)

    def close(self):
        self.db.close()

    def start_flow(self, state, browser, verifier, nonce):
        now = time.time()
        with self.db:
            self.db.execute(
                "DELETE FROM flows WHERE expires<=? OR browser=?", (now, digest(browser))
            )
            if self.db.execute("SELECT count(*) FROM flows").fetchone()[0] >= 1024:
                raise ValueError("Sign-in is busy. Try again shortly.")
            self.db.execute(
                "INSERT INTO flows VALUES(?,?,?,?,?)",
                (digest(state), digest(browser), verifier, nonce, now + FLOW_SECONDS),
            )

    def consume_flow(self, state, browser):
        with self.db:
            return self.db.execute(
                "DELETE FROM flows WHERE state=? AND browser=? AND expires>? "
                "RETURNING verifier,nonce",
                (digest(state), digest(browser), time.time()),
            ).fetchone()

    def sign_in(self, subject, email, domain):
        token = secrets.token_urlsafe(32)
        with self.db:
            self.db.execute("DELETE FROM sessions WHERE expires<=?", (time.time(),))
            account = self.db.execute(
                "INSERT INTO accounts(id,subject,email,domain) VALUES(?,?,?,?) "
                "ON CONFLICT(subject) DO UPDATE SET email=excluded.email,domain=excluded.domain "
                "RETURNING id",
                (uuid4().hex, subject, email, domain),
            ).fetchone()[0]
            self.db.execute(
                "INSERT INTO sessions VALUES(?,?,?)",
                (digest(token), account, time.time() + SESSION_SECONDS),
            )
        return token

    def session(self, token):
        if not token or len(token) != 43:
            return None
        return self.db.execute(
            "SELECT a.* FROM accounts a JOIN sessions s ON s.account=a.id "
            "WHERE s.token=? AND s.expires>?",
            (digest(token), time.time()),
        ).fetchone()

    def sign_out(self, token):
        if token:
            with self.db:
                self.db.execute("DELETE FROM sessions WHERE token=?", (digest(token),))

    def account(self, account):
        return self.db.execute("SELECT * FROM accounts WHERE id=?", (account,)).fetchone()

    def all_accounts(self):
        return self.db.execute("SELECT * FROM accounts").fetchall()

    def start_app_flow(self, account, app, session, state, verifier):
        now = time.time()
        with self.db:
            self.db.execute("DELETE FROM app_flows WHERE expires<=?", (now,))
            self.db.execute(
                "INSERT OR REPLACE INTO app_flows VALUES(?,?,?,?,?,?)",
                (account, app, digest(session), digest(state), verifier, now + FLOW_SECONDS),
            )

    def consume_app_flow(self, account, app, session, state):
        with self.db:
            row = self.db.execute(
                "DELETE FROM app_flows WHERE account=? AND app=? AND session=? AND state=? "
                "AND expires>? RETURNING verifier",
                (account, app, digest(session), digest(state), time.time()),
            ).fetchone()
            return row[0] if row else None

    def delete_credential(self, account, kind, name):
        self.db.execute(
            "DELETE FROM credentials WHERE account=? AND kind=? AND name=?", (account, kind, name)
        )

    def delete_app_token(self, account, app):
        self.delete_credential(account, "app", app)

    def remove_app(self, account, app):
        with self.db:
            self.delete_app_token(account, app)
            self.db.execute("DELETE FROM app_flows WHERE account=? AND app=?", (account, app))

    def associated_data(self, account, kind, name):
        return json.dumps([self.realm, account, kind, name]).encode()

    def credential(self, account, kind, name):
        row = self.db.execute(
            "SELECT sealed FROM credentials WHERE account=? AND kind=? AND name=?",
            (account, kind, name),
        ).fetchone()
        if row is None:
            return None
        sealed = row[0]
        return self.cipher.decrypt(
            sealed[:12], sealed[12:], self.associated_data(account, kind, name)
        ).decode()

    def names(self, account, kind):
        return [
            row[0]
            for row in self.db.execute(
                "SELECT name FROM credentials WHERE account=? AND kind=? ORDER BY name",
                (account, kind),
            )
        ]

    def put_credential(self, account, kind, name, value):
        nonce = os.urandom(12)
        sealed = nonce + self.cipher.encrypt(
            nonce, value.encode(), self.associated_data(account, kind, name)
        )
        self.db.execute(
            "INSERT OR REPLACE INTO credentials VALUES(?,?,?,?)", (account, kind, name, sealed)
        )

    def set_model(self, account, provider, model, key=None):
        with self.db:
            if key is not None:
                self.put_credential(account, "model", provider, key)
            self.db.execute(
                "UPDATE accounts SET provider=?,model=? WHERE id=?", (provider, model, account)
            )

    def remove_model_key(self, account, provider):
        with self.db:
            self.db.execute(
                "DELETE FROM credentials WHERE account=? AND kind='model' AND name=?",
                (account, provider),
            )
            self.db.execute(
                "UPDATE accounts SET provider=NULL,model=NULL WHERE id=? AND provider=?",
                (account, provider),
            )

    def validation_attempt(self, account, now=None):
        now = time.time() if now is None else now
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            self.db.execute("DELETE FROM attempts WHERE attempted<=?", (now - VALIDATION_SECONDS,))
            count = self.db.execute(
                "SELECT count(*) FROM attempts WHERE account=?", (account,)
            ).fetchone()[0]
            if count >= VALIDATION_LIMIT:
                return False
            self.db.execute("INSERT INTO attempts VALUES(?,?)", (account, now))
        return True
