import asyncio

from tests.test_web import web as web


async def test_bot_crud_profile_and_chief_protection(web):
    service, client, _ = web
    chief = (await client.get("/api/bots")).json()[0]
    response = await client.post(
        "/api/bots",
        json={
            "name": "Reader",
            "role": "Read documents",
            "style": "custom",
            "voice": "Explain with care.",
            "shape": "diamond",
            "color": "#458899",
        },
    )
    assert response.status_code == 201
    bot = response.json()
    path = "/api/bots/" + bot["id"]
    changed = await client.patch(path, json={"name": "Researcher", "instructions": "Cite sources."})
    assert changed.status_code == 200
    assert changed.json()["voice"] == "Explain with care."
    assert changed.json()["name"] == "Researcher"
    assert (await client.delete("/api/bots/" + chief["id"])).status_code == 409
    assert (await client.patch(path, json={"color": "bad"})).status_code == 400
    assert (await client.delete(path)).status_code == 200
    assert service.store.bot(bot["id"]) is None


async def test_delegation_can_be_followed_in_bot_chat(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    answer, outcome = await service.sessions.delegate(bot, "First question", policy="deny")
    assert outcome == "completed"
    assert "turn 1" in answer
    path = "/api/bots/" + bot.id
    assert (await client.post(path + "/select")).status_code == 200
    reply = await client.get(path + "/messages")
    assert reply.json()[0]["text"] == "First question"
    await client.post("/api/messages", json={"text": "Follow up"})
    from tests.test_gateway import settle

    await settle(service)
    messages = (await client.get(path + "/messages")).json()
    assert "turn 2" in messages[-1]["text"]


async def test_busy_bot_cannot_be_edited_and_other_bots_still_run(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    other = service.store.create_bot("Other", "Different workspace")
    task = asyncio.create_task(service.sessions.delegate(bot, "slow", policy="deny"))
    try:
        async with asyncio.timeout(5):
            while bot.id not in service.sessions.active_bots:
                await asyncio.sleep(0.01)
        assert (
            await client.patch("/api/bots/" + bot.id, json={"role": "Changed"})
        ).status_code == 409
        assert (await service.sessions.delegate(other, "hello", policy="deny"))[1] == "completed"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_greeting_is_generated_and_cached(web):
    service, client, _ = web
    bot = service.store.create_bot("Reader", "Read documents")
    path = "/api/bots/" + bot.id + "/greeting"
    first = await client.post(path)
    assert first.status_code == 200 and first.json()["text"]
    before = service.store.db.execute("SELECT count(*) FROM bot_chat").fetchone()[0]
    second = await client.post(path)
    assert second.json() == first.json()
    assert service.store.db.execute("SELECT count(*) FROM bot_chat").fetchone()[0] == before
