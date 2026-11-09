import json

import httpx
import pytest

from delta_gateway.granola import GRANOLA_NOTES
from delta_gateway.plugins import UNTRUSTED
from tests.test_web_accounts import accounts as accounts
from tests.test_web_accounts import login

NOTE = "not_1d3tmYTlCICgjy"
KEY = "grn_offline-demo-key"
INVALID = "grn_rejected-demo-key"


async def test_granola_saved_key_is_private_and_reads_notes_and_transcripts(accounts):
    server, browser, begin, _ = accounts
    route = "/api/apps/granola"
    calls = []

    def provider(req):
        assert req.method == "GET" and req.url.host == "public-api.granola.ai"
        assert "grn_" not in str(req.url)
        token = req.headers["authorization"]
        calls.append((req.url.path, dict(req.url.params)))
        if token == "Bearer " + INVALID:
            return httpx.Response(403, json={"error": "rejected"})
        assert token == "Bearer " + KEY
        if str(req.url).startswith(GRANOLA_NOTES + "?"):
            return httpx.Response(
                200,
                json={
                    "notes": [{"id": NOTE, "title": "Review"}],
                    "hasMore": True,
                    "cursor": "n2",
                },
            )
        if req.url.path.endswith("/transcript"):
            assert req.url.params["cursor"] == "page-two"
            return httpx.Response(
                200,
                json={
                    "transcript": [{"speaker": {"name": "Speaker"}, "text": "Next steps"}],
                    "hasMore": True,
                    "cursor": "page-three",
                },
            )
        assert req.url.path == "/v1/notes/" + NOTE
        return httpx.Response(
            200,
            json={
                "id": NOTE,
                "title": "Review",
                "summary_text": "x" * 1001,
                "private_notes_markdown": "Review actions",
            },
        )

    def decoded(text):
        assert text.startswith(UNTRUSTED)
        return json.loads(text.removeprefix(UNTRUSTED))

    transport = httpx.MockTransport(provider)
    server.apps.transport = transport
    await server.apps.http.aclose()
    server.apps.http = httpx.AsyncClient(transport=transport)
    alice, bob = browser(), browser()
    await login(alice, begin, "alice", "alice@example.test")
    await login(bob, begin, "bob", "bob@team.test", hd="team.test")
    assert (
        await alice.put(
            route + "/key", headers={"Origin": "https://outside.test"}, json={"key": KEY}
        )
    ).status_code == 403
    assert not calls
    assert (await alice.put(route + "/key", json={"key": KEY})).json() == {"connected": True}
    assert calls[0] == ("/v1/notes", {"page_size": "1"})
    assert (await alice.put(route + "/key", json={"key": INVALID})).status_code == 422

    alice_apps = (await alice.get("/api/apps")).json()
    bob_apps = (await bob.get("/api/apps")).json()
    assert next(app for app in alice_apps if app["id"] == "granola")["connected"]
    assert not next(app for app in bob_apps if app["id"] == "granola")["saved"]
    assert KEY not in json.dumps(alice_apps)
    sealed = server.accounts.db.execute(
        "SELECT sealed FROM credentials WHERE kind='app'"
    ).fetchone()[0]
    assert KEY.encode() not in sealed
    principal = server.google.identity(alice.cookies["delta_session"])
    runtime = server.runtimes.get(principal)
    bot = runtime.store.create_bot("Note reader", "Read meeting notes")
    grant = route + f"/bots/{bot.id}"
    assert (await bob.put(grant, json={"enabled": True})).status_code == 404
    with pytest.raises(ValueError, match="not enabled"):
        await runtime.team.call_plugin("granola_list_notes", {}, bot)
    assert (await alice.put(grant, json={"enabled": True})).status_code == 200
    notes = decoded(
        await runtime.team.call_plugin(
            "granola_list_notes",
            {"cursor": "n1", "created_after": "2026-01-01T00:00:00Z"},
            bot,
        )
    )
    assert notes["items"][0]["id"] == NOTE and notes["cursor"] == "n2"
    details = decoded(await runtime.team.call_plugin("granola_get_note", {"note_id": NOTE}, bot))
    assert details["nextOffset"] == 1000 and details["privateNotes"] == "Review actions"
    tail = decoded(
        await runtime.team.call_plugin(
            "granola_get_note",
            {"note_id": NOTE, "offset": details["nextOffset"]},
            bot,
        )
    )
    assert tail["summary"] == "x" and tail["nextOffset"] is None
    transcript = decoded(
        await runtime.team.call_plugin(
            "granola_get_transcript",
            {"note_id": NOTE, "cursor": "page-two"},
            bot,
        )
    )
    assert transcript["cursor"] == "page-three" and transcript["items"][0]["text"] == "Next steps"
    assert (await alice.delete(route)).json() == {"connected": False, "providerRevoked": False}
    assert not runtime.store.plugins_for(bot.id)
