"""Web accounts behave the same on a local SQLite file and on D1."""

import base64

import pytest
from cryptography.exceptions import InvalidTag

from delta_gateway.account_store import VALIDATION_LIMIT, AccountStore

KEY = base64.urlsafe_b64encode(bytes(range(32))).decode()


async def opened(database):
    return await AccountStore.open(database, "realm-1", KEY)


async def test_sign_in_session_and_sign_out(database):
    store = await opened(database)
    await store.start_flow("s" * 43, "b" * 43, "verifier", "nonce")
    flow = await store.consume_flow("s" * 43, "b" * 43)
    assert flow == {"verifier": "verifier", "nonce": "nonce"}
    assert await store.consume_flow("s" * 43, "b" * 43) is None
    token = await store.sign_in("subject-1", "a@example.com", "example.com")
    account = await store.session(token)
    assert account["email"] == "a@example.com"
    await store.sign_out(token)
    assert await store.session(token) is None


async def test_sealed_credentials_are_bound_to_their_account(database):
    store = await opened(database)
    first = (await store.session(await store.sign_in("s1", "a@example.com", None)))["id"]
    second = (await store.session(await store.sign_in("s2", "b@example.com", None)))["id"]
    await store.set_model(first, "openai", "model-1", "secret-key")
    assert await store.credential(first, "model", "openai") == "secret-key"
    assert await store.names(first, "model") == ["openai"]
    [row] = await database.query("SELECT sealed FROM credentials")
    assert isinstance(row["sealed"], bytes) and b"secret-key" not in row["sealed"]
    await database.run(
        "INSERT INTO credentials VALUES(?,?,?,?)", (second, "model", "openai", row["sealed"])
    )
    with pytest.raises(InvalidTag):
        await store.credential(second, "model", "openai")


async def test_validation_attempts_are_capped(database):
    store = await opened(database)
    account = (await store.session(await store.sign_in("s1", "a@example.com", None)))["id"]
    results = [await store.validation_attempt(account, now=1000) for _ in range(VALIDATION_LIMIT)]
    assert all(results)
    assert not await store.validation_attempt(account, now=1000)
    assert await store.validation_attempt(account, now=2000)
