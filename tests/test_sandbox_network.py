import pytest

from delta_gateway import config as gateway_config
from delta_gateway.computer import ComputerError, DockerComputer
from delta_gateway.daytona import DaytonaComputer
from tests import test_daytona
from tests.test_gateway import config


def test_sandbox_network_defaults_open_and_round_trips(tmp_path):
    assert config(tmp_path).sandbox_network == "open"
    path = tmp_path / "gateway.yaml"
    gateway_config.save(path, config(tmp_path, sandbox_network="closed"))
    assert gateway_config.load(path).sandbox_network == "closed"
    gateway_config.save(path, config(tmp_path))
    assert gateway_config.load(path).sandbox_network == "open"


def test_sandbox_network_rejects_unknown_value(tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, sandbox_network="sometimes")


def fake_docker(computer, mode=None):
    calls = []

    async def docker(*arguments, stdin=None, timeout=60):
        calls.append(arguments)
        if arguments[:2] == ("inspect", "--format"):
            if mode is None:
                return 1, b"", b"missing"
            if "NetworkMode" in arguments[2]:
                return 0, mode.encode() + b"\n", b""
            if "Running" in arguments[2]:
                return 0, b"true\n", b""
            return 0, b"[]", b""
        return 0, b"", b""

    computer.docker = docker
    return calls


def run_arguments(calls):
    return next(arguments for arguments in calls if arguments[0] == "run")


@pytest.mark.parametrize("network,closed", [("open", False), ("closed", True)])
async def test_docker_run_arguments_follow_network_setting(tmp_path, network, closed):
    computer = DockerComputer("delta-test", "image", tmp_path / "w", network=network)
    calls = fake_docker(computer)
    await computer.start()
    arguments = run_arguments(calls)
    pairs = list(zip(arguments, arguments[1:], strict=False))
    assert (("--network", "none") in pairs) is closed


@pytest.mark.parametrize("network,mode", [("open", "none"), ("closed", "bridge")])
async def test_docker_refuses_reused_container_with_other_network(tmp_path, network, mode):
    computer = DockerComputer("delta-test", "image", tmp_path / "w", network=network)
    fake_docker(computer, mode)
    with pytest.raises(ComputerError, match="needs to be recreated.*network"):
        await computer.start()


async def test_docker_reuses_container_with_matching_network(tmp_path):
    computer = DockerComputer("delta-test", "image", tmp_path / "w", network="open")
    calls = fake_docker(computer, "bridge")
    await computer.start()
    assert not any(arguments[0] == "run" for arguments in calls)


@pytest.mark.parametrize("network,blocked", [("open", False), ("closed", True)])
async def test_daytona_create_follows_network_setting(tmp_path, monkeypatch, network, blocked):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    computer = DaytonaComputer(
        "delta-bot-n", config(tmp_path, sandbox_network=network), None, sdk=test_daytona.sdk()
    )
    await computer.start()
    sandbox = test_daytona.Client.sandboxes["delta-bot-n"]
    assert sandbox.params.network_block_all is blocked


async def test_daytona_reused_sandbox_is_updated_to_match(tmp_path, monkeypatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    fake = test_daytona.sdk()
    closed = DaytonaComputer(
        "delta-bot-r", config(tmp_path, sandbox_network="closed"), None, sdk=fake
    )
    await closed.start()
    sandbox = test_daytona.Client.sandboxes["delta-bot-r"]
    updates = []

    async def update_network_settings(**settings):
        updates.append(settings)
        sandbox.network_block_all = settings["network_block_all"]

    sandbox.network_block_all = True
    sandbox.update_network_settings = update_network_settings
    opened = DaytonaComputer("delta-bot-r", config(tmp_path), None, sdk=fake)
    await opened.start()
    assert updates == [{"network_block_all": False}]
    await opened.start()
    assert len(updates) == 1


async def test_daytona_reused_sandbox_without_update_must_be_recreated(tmp_path, monkeypatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    fake = test_daytona.sdk()
    await DaytonaComputer(
        "delta-bot-o", config(tmp_path, sandbox_network="closed"), None, sdk=fake
    ).start()
    test_daytona.Client.sandboxes["delta-bot-o"].network_block_all = True
    with pytest.raises(ComputerError, match="needs to be recreated.*network"):
        await DaytonaComputer("delta-bot-o", config(tmp_path), None, sdk=fake).start()
