"""A stand-in for the D1 HTTP query endpoint, answering from an in-memory SQLite database."""

import json
import sqlite3

import httpx

from delta_gateway.database import D1Database

ACCOUNT, DATABASE = "account-1", "database-1"
URL = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/d1/database/{DATABASE}/query"


class FakeD1:
    def __init__(self):
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.requests = []
        self.fault = None

    def handle(self, request):
        assert str(request.url) == URL
        assert request.headers["authorization"] == "Bearer token-1"
        body = json.loads(request.content)
        self.requests.append(body)
        if self.fault is not None:
            return self.fault(request)
        try:
            before = self.connection.total_changes
            rows = self.connection.execute(body["sql"], body.get("params", [])).fetchall()
            changes = self.connection.total_changes - before
        except sqlite3.Error as error:
            return httpx.Response(
                400,
                json={"success": False, "errors": [{"code": 7500, "message": str(error)}]},
            )
        result = {
            "success": True,
            "results": [dict(row) for row in rows],
            "meta": {"changes": changes},
        }
        return httpx.Response(200, json={"success": True, "errors": [], "result": [result]})

    def database(self):
        http = httpx.AsyncClient(transport=httpx.MockTransport(self.handle))
        return D1Database("token-1", ACCOUNT, DATABASE, http=http)
