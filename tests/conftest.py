import pytest


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_GATEWAY_HOME", str(tmp_path / "profile"))
    monkeypatch.setenv("DELTA_CONFIG_DIR", str(tmp_path / "delta-config"))
    monkeypatch.setenv("DELTA_HOME", str(tmp_path / "delta-config"))
    monkeypatch.setenv("DELTA_OFFLINE", "1")
