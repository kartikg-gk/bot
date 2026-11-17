import httpx
import pytest

from delta_gateway.database import (
    D1Database,
    DatabaseConfigError,
    SqliteDatabase,
    create_database,
)
from tests.fake_d1 import FakeD1
from tests.test_gateway import config

D1_ENV = {
    "CLOUDFLARE_API_TOKEN": "token-1",
    "CLOUDFLARE_ACCOUNT_ID": "account-1",
    "CLOUDFLARE_D1_DATABASE_ID": "database-1",
}


@pytest.fixture(params=["sqlite", "d1"])
async def database(request, tmp_path):
    if request.param == "sqlite":
        db = SqliteDatabase(tmp_path / "test.sqlite3")
    else:
        db = FakeD1().database()
    yield db
    await db.close()


async def test_query_and_run_round_trip(database):
    await database.run("CREATE TABLE items (id TEXT PRIMARY KEY, size INTEGER NOT NULL)")
    await database.run("INSERT INTO items (id, size) VALUES (?, ?)", ["a", 3])
    rows = await database.query("SELECT id, size FROM items WHERE size > ?", [2])
    assert rows == [{"id": "a", "size": 3}]


async def test_run_reports_changed_rows(database):
    await database.run("CREATE TABLE items (id TEXT PRIMARY KEY, size INTEGER NOT NULL)")
    for name in "abc":
        await database.run("INSERT INTO items (id, size) VALUES (?, 1)", [name])
    assert await database.run("UPDATE items SET size = 2 WHERE id != ?", ["a"]) == 2
    assert await database.run("UPDATE items SET size = 2 WHERE id = ?", ["missing"]) == 0


async def test_check_succeeds(database):
    await database.check()


async def test_null_parameter_refused(database):
    with pytest.raises(TypeError, match="NULLIF"):
        await database.query("SELECT ?", [None])


async def test_numbers_sent_as_text_to_d1():
    fake = FakeD1()
    db = fake.database()
    await db.query("SELECT ? + ? AS total, ? AS flag", [2, 1.5, True])
    assert fake.requests[-1]["params"] == ["2", "1.5", "1"]
    await db.close()


def failing(response):
    def fault(_request):
        if isinstance(response, Exception):
            raise response
        return response

    return fault


@pytest.mark.parametrize(
    "response,detail",
    [
        (
            httpx.Response(500, json={"success": False, "errors": [{"message": "overloaded"}]}),
            r"\(HTTP 500\): overloaded",
        ),
        (httpx.Response(502, text="<html>bad gateway</html>"), "response was not JSON"),
        (httpx.Response(200, json={"unexpected": True}), "response was not understood"),
        (httpx.ReadTimeout("slow"), "timed out after 30 s"),
    ],
)
async def test_d1_error_names_d1(response, detail):
    fake = FakeD1()
    fake.fault = failing(response)
    db = fake.database()
    with pytest.raises(RuntimeError, match=r"^D1 query failed.*" + detail):
        await db.query("SELECT 1")
    await db.close()


async def test_d1_statement_error_is_reported():
    db = FakeD1().database()
    with pytest.raises(RuntimeError, match="^D1 query failed.*no such table"):
        await db.query("SELECT * FROM missing")
    await db.close()


@pytest.mark.parametrize("missing", ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"])
def test_missing_d1_variable_is_named(tmp_path, missing):
    env = {key: value for key, value in D1_ENV.items() if key != missing}
    with pytest.raises(DatabaseConfigError, match=f"^Missing D1 configuration: {missing}$"):
        create_database(config(tmp_path), env)


def test_sqlite_chosen_without_d1_id(tmp_path):
    env = {key: value for key, value in D1_ENV.items() if key != "CLOUDFLARE_D1_DATABASE_ID"}
    db = create_database(config(tmp_path), env)
    assert isinstance(db, SqliteDatabase)
    assert db.path == config(tmp_path).database_path


def test_d1_chosen_with_all_variables(tmp_path):
    assert isinstance(create_database(config(tmp_path), D1_ENV), D1Database)
