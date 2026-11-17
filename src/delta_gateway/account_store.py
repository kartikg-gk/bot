"""Private identity, session and credential records for web accounts."""

import base64
import hashlib
import json
import os
import secrets
import time
from pathlib import Path
from uuid import uuid4

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from delta_gateway.database import SqliteDatabase

SESSION_SECONDS = 12 * 60 * 60
FLOW_SECONDS = 600
VALIDATION_SECONDS = 300
VALIDATION_LIMIT = 5

SCHEMA = (
    "CREATE TABLE IF NOT EXISTS accounts(id TEXT PRIMARY KEY, subject TEXT NOT NULL UNIQUE, "
    "email TEXT NOT NULL, domain TEXT, provider TEXT, model TEXT)",
    "CREATE TABLE IF NOT EXISTS flows(state BLOB PRIMARY KEY, browser BLOB NOT NULL, "
    "verifier TEXT NOT NULL, nonce TEXT NOT NULL, expires REAL NOT NULL)",
    "CREATE TABLE IF NOT EXISTS app_flows(account TEXT NOT NULL REFERENCES accounts(id), "
    "app TEXT NOT NULL, session BLOB NOT NULL, state BLOB NOT NULL, verifier TEXT NOT NULL, "
    "expires REAL NOT NULL, PRIMARY KEY(account,app,session))",
    "CREATE TABLE IF NOT EXISTS sessions(token BLOB PRIMARY KEY, "
    "account TEXT NOT NULL REFERENCES accounts(id), expires REAL NOT NULL)",
    "CREATE TABLE IF NOT EXISTS credentials(account TEXT NOT NULL REFERENCES accounts(id), "
    "kind TEXT NOT NULL, name TEXT NOT NULL, sealed BLOB NOT NULL, "
    "PRIMARY KEY(account,kind,name))",
    "CREATE TABLE IF NOT EXISTS attempts(account TEXT NOT NULL REFERENCES accounts(id), "
    "attempted REAL NOT NULL)",
    "CREATE INDEX IF NOT EXISTS attempts_account ON attempts(account,attempted)",
)


def digest(value):
    return hashlib.sha256(value.encode()).digest()


def cipher_for(encoded_key):
    try:
        key = base64.b64decode(encoded_key, altchars=b"-_", validate=True)
        if len(key) != 32:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("DELTA_GATEWAY_MASTER_KEY must encode 32 random bytes as base64") from None
    return AESGCM(key)


class AccountStore:
    """Web accounts on a local file by default, or on any database interface via open()."""

    def __init__(self, path, realm, encoded_key):
        self.cipher, self.realm = cipher_for(encoded_key), realm
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = SqliteDatabase(path)
        self.owns_db = True
        path.chmod(0o600)
        with self.db.lock:
            self.db.connection.execute("PRAGMA foreign_keys=ON")
            for statement in SCHEMA:
                self.db.connection.execute(statement)

    @classmethod
    async def open(cls, database, realm, encoded_key):
        self = cls.__new__(cls)
        self.cipher, self.realm = cipher_for(encoded_key), realm
        self.db, self.owns_db = database, False
        for statement in SCHEMA:
            await database.run(statement)
        return self

    def close(self):
        if self.owns_db:
            self.db.close_now()

    async def start_flow(self, state, browser, verifier, nonce):
        now = time.time()
        await self.db.run("DELETE FROM flows WHERE expires<=? OR browser=?", (now, digest(browser)))
        # The size check and the insert are one statement, so the cap holds under load.
        if not await self.db.run(
            "INSERT INTO flows SELECT ?,?,?,?,? WHERE (SELECT count(*) FROM flows) < 1024",
            (digest(state), digest(browser), verifier, nonce, now + FLOW_SECONDS),
        ):
            raise ValueError("Sign-in is busy. Try again shortly.")

    async def consume_flow(self, state, browser):
        rows = await self.db.query(
            "DELETE FROM flows WHERE state=? AND browser=? AND expires>? RETURNING verifier,nonce",
            (digest(state), digest(browser), time.time()),
        )
        return rows[0] if rows else None

    async def sign_in(self, subject, email, domain):
        token = secrets.token_urlsafe(32)
        await self.db.run("DELETE FROM sessions WHERE expires<=?", (time.time(),))
        rows = await self.db.query(
            "INSERT INTO accounts(id,subject,email,domain) VALUES(?,?,?,?) "
            "ON CONFLICT(subject) DO UPDATE SET email=excluded.email,domain=excluded.domain "
            "RETURNING id",
            (uuid4().hex, subject, email, domain),
        )
        await self.db.run(
            "INSERT INTO sessions VALUES(?,?,?)",
            (digest(token), rows[0]["id"], time.time() + SESSION_SECONDS),
        )
        return token

    async def session(self, token):
        if not token or len(token) != 43:
            return None
        rows = await self.db.query(
            "SELECT a.* FROM accounts a JOIN sessions s ON s.account=a.id "
            "WHERE s.token=? AND s.expires>?",
            (digest(token), time.time()),
        )
        return rows[0] if rows else None

    async def sign_out(self, token):
        if token:
            await self.db.run("DELETE FROM sessions WHERE token=?", (digest(token),))

    async def account(self, account):
        rows = await self.db.query("SELECT * FROM accounts WHERE id=?", (account,))
        return rows[0] if rows else None

    async def all_accounts(self):
        return await self.db.query("SELECT * FROM accounts")

    async def start_app_flow(self, account, app, session, state, verifier):
        now = time.time()
        await self.db.run("DELETE FROM app_flows WHERE expires<=?", (now,))
        await self.db.run(
            "INSERT OR REPLACE INTO app_flows VALUES(?,?,?,?,?,?)",
            (account, app, digest(session), digest(state), verifier, now + FLOW_SECONDS),
        )

    async def consume_app_flow(self, account, app, session, state):
        rows = await self.db.query(
            "DELETE FROM app_flows WHERE account=? AND app=? AND session=? AND state=? "
            "AND expires>? RETURNING verifier",
            (account, app, digest(session), digest(state), time.time()),
        )
        return rows[0]["verifier"] if rows else None

    async def delete_credential(self, account, kind, name):
        await self.db.run(
            "DELETE FROM credentials WHERE account=? AND kind=? AND name=?", (account, kind, name)
        )

    async def delete_app_token(self, account, app):
        await self.delete_credential(account, "app", app)

    async def remove_app(self, account, app):
        await self.delete_app_token(account, app)
        await self.db.run("DELETE FROM app_flows WHERE account=? AND app=?", (account, app))

    def associated_data(self, account, kind, name):
        return json.dumps([self.realm, account, kind, name]).encode()

    async def credential(self, account, kind, name):
        rows = await self.db.query(
            "SELECT sealed FROM credentials WHERE account=? AND kind=? AND name=?",
            (account, kind, name),
        )
        if not rows:
            return None
        sealed = rows[0]["sealed"]
        return self.cipher.decrypt(
            sealed[:12], sealed[12:], self.associated_data(account, kind, name)
        ).decode()

    async def names(self, account, kind):
        rows = await self.db.query(
            "SELECT name FROM credentials WHERE account=? AND kind=? ORDER BY name",
            (account, kind),
        )
        return [row["name"] for row in rows]

    async def put_credential(self, account, kind, name, value):
        nonce = os.urandom(12)
        sealed = nonce + self.cipher.encrypt(
            nonce, value.encode(), self.associated_data(account, kind, name)
        )
        await self.db.run(
            "INSERT OR REPLACE INTO credentials VALUES(?,?,?,?)", (account, kind, name, sealed)
        )

    async def set_model(self, account, provider, model, key=None):
        if key is not None:
            await self.put_credential(account, "model", provider, key)
        await self.db.run(
            "UPDATE accounts SET provider=?,model=? WHERE id=?", (provider, model, account)
        )

    async def remove_model_key(self, account, provider):
        await self.db.run(
            "DELETE FROM credentials WHERE account=? AND kind='model' AND name=?",
            (account, provider),
        )
        await self.db.run(
            "UPDATE accounts SET provider=NULL,model=NULL WHERE id=? AND provider=?",
            (account, provider),
        )

    async def validation_attempt(self, account, now=None):
        now = time.time() if now is None else now
        await self.db.run("DELETE FROM attempts WHERE attempted<=?", (now - VALIDATION_SECONDS,))
        # The count and the insert are one statement, so parallel attempts cannot pass the cap.
        return bool(
            await self.db.run(
                "INSERT INTO attempts SELECT ?,? WHERE (SELECT count(*) FROM attempts "
                "WHERE account=?) < CAST(? AS INTEGER)",
                (account, now, account, VALIDATION_LIMIT),
            )
        )
