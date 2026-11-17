import pytest


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_GATEWAY_HOME", str(tmp_path / "profile"))
    monkeypatch.setenv("DELTA_CONFIG_DIR", str(tmp_path / "delta-config"))
    monkeypatch.setenv("DELTA_HOME", str(tmp_path / "delta-config"))
    monkeypatch.setenv("DELTA_OFFLINE", "1")


@pytest.fixture(params=["sqlite", "d1"])
async def database(request, tmp_path):
    from delta_gateway.database import SqliteDatabase
    from tests.fake_d1 import FakeD1

    if request.param == "sqlite":
        db = SqliteDatabase(tmp_path / "shared.sqlite3")
    else:
        db = FakeD1().database()
    yield db
    await db.close()
