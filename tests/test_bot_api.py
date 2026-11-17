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
    assert await service.store.bot(bot["id"]) is None


async def test_delegation_can_be_followed_in_bot_chat(web):
    service, client, _ = web
    bot = await service.store.create_bot("Reader", "Read documents")
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
    bot = await service.store.create_bot("Reader", "Read documents")
    other = await service.store.create_bot("Other", "Different workspace")
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
    bot = await service.store.create_bot("Reader", "Read documents")
    path = "/api/bots/" + bot.id + "/greeting"
    first = await client.post(path)
    assert first.status_code == 200 and first.json()["text"]
    before = service.store.db.execute("SELECT count(*) FROM bot_chat").fetchone()[0]
    second = await client.post(path)
    assert second.json() == first.json()
    assert service.store.db.execute("SELECT count(*) FROM bot_chat").fetchone()[0] == before


async def test_export_includes_older_messages_and_only_the_requested_bot(web):
    from delta_gateway.bot_profiles import record_chat

    service, client, _ = web
    bot = await service.store.create_bot("Reader", "Read documents")
    other = await service.store.create_bot("Other", "Different workspace")
    for number in range(201):
        record_chat(service.store, bot.id, "user", f"Question {number}")
    record_chat(service.store, other.id, "user", "Other bot")
    path = f"/api/bots/{bot.id}"
    assert len((await client.get(path + "/messages")).json()) == 200
    exported = await client.get(path + "/export")
    assert exported.headers["Cache-Control"] == "no-store"
    assert exported.json() == [
        {"role": "user", "text": f"Question {number}"} for number in range(201)
    ]


async def test_reply_counts_exclude_prompts_and_count_each_bot(web):
    from delta_gateway.bot_profiles import record_chat

    service, client, _ = web
    bot = await service.store.create_bot("Reader", "Read documents")
    other = await service.store.create_bot("Other", "Different workspace")
    record_chat(service.store, bot.id, "user", "Prompt")
    record_chat(service.store, bot.id, "assistant", "Answer")
    record_chat(service.store, bot.id, "assistant", "Follow-up")
    record_chat(service.store, other.id, "assistant", "Other answer")
    response = await client.get("/api/bots/unread-counts")
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {bot.id: 2, other.id: 1}
