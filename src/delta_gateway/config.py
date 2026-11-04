"""Namespaced YAML configuration with explicit legacy JSON migration."""

import json
import os
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictInt


def profile_home():
    return (
        Path(os.environ.get("DELTA_GATEWAY_HOME", Path.home() / ".delta-gateway"))
        .expanduser()
        .resolve()
    )


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instance: str = Field(default_factory=lambda: uuid4().hex, pattern=r"^[a-f0-9]{32}$")
    users: list[StrictInt] = Field(min_length=1)
    project: Path
    state: Path
    database: Path | None = None
    provider: str | None = None
    model: str | None = None
    thinking: str | None = Field(default=None, pattern=r"^(off|minimal|low|medium|high|xhigh|max)$")
    agent_dir: Path | None = None
    session_dir: Path | None = None
    workers: int | None = Field(default=None, ge=1, le=256)
    backlog: int | None = Field(default=None, ge=1, le=10000)
    idle_seconds: int = Field(default=1800, ge=1)
    trust_project: bool = False
    policy: str = Field(default="ask", pattern=r"^(engine|ask|deny|auto)$")
    approval_seconds: int = Field(default=300, ge=1, le=3600)
    name: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9-]{0,63}$")
    token_env: str = Field(default="TELEGRAM_BOT_TOKEN", pattern=r"^[A-Z][A-Z0-9_]*$")
    bot_token: SecretStr | None = None
    allow_groups: bool = False
    groups: list[StrictInt] = Field(default_factory=list)
    group_mentions_only: bool = False
    per_user_groups: bool = False
    draft_previews: bool = True
    retention_days: int | None = Field(default=None, ge=1)
    shutdown_timeout: float = Field(default=10, gt=0, le=300)
    release_url: str | None = None
    executor: str = Field(default="local", pattern=r"^(local|docker|daytona)$")
    docker_image: str | None = None
    docker_memory: str = "2g"
    daytona_target: str | None = None
    daytona_snapshot: str | None = None
    memory_enabled: bool = False
    memory_extract: bool = False
    memory_extractor: str = Field(default="mem0", pattern=r"^(mem0|delta)$")
    memory_model: str | None = None
    memory_search_limit: int = Field(default=10, ge=1, le=50)
    memory_search_floor: float = Field(default=0.3, ge=0, le=1)
    memory_token_budget: int = Field(default=2000, ge=100)
    memory_extraction_seconds: float = Field(default=120, gt=0)

    @property
    def database_path(self):
        return self.database or profile_home() / "state" / "gateway.sqlite3"

    @property
    def sessions_path(self):
        return self.session_dir or self.state / "sessions"

    def token(self):
        return os.environ.get(self.token_env) or (
            self.bot_token.get_secret_value() if self.bot_token else None
        )

    def route(self, chat, user, thread=0):
        return chat, user if chat > 0 or self.per_user_groups else 0, thread

    def validate_paths(self, *, require_project=True):
        if any(user <= 0 for user in self.users):
            raise ValueError("User IDs must be positive")
        if any(group >= 0 for group in self.groups):
            raise ValueError("Group IDs must be negative Telegram chat IDs")
        if not self.project.is_absolute() or (require_project and not self.project.is_dir()):
            raise ValueError("Project must be an existing absolute directory")
        if any(not p.is_absolute() for p in (self.state, self.database_path, self.sessions_path)):
            raise ValueError("State, database and session paths must be absolute")
        if self.agent_dir and not self.agent_dir.is_absolute():
            raise ValueError("Agent directory must be absolute")
        if self.executor == "docker" and not self.docker_image:
            raise ValueError("The docker executor needs a container image")
        if self.release_url:
            from urllib.parse import urlparse

            parsed = urlparse(self.release_url)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
            ):
                raise ValueError("Release channel must be an HTTPS URL without credentials")


def document(config):
    return dict(
        version=1,
        instanceId=config.instance,
        name=config.name,
        stateDirectory=str(config.state),
        databasePath=str(config.database_path),
        telegram=dict(
            tokenEnvironment=config.token_env,
            botToken=config.bot_token.get_secret_value() if config.bot_token else None,
            allowedUserIds=config.users,
            allowGroups=config.allow_groups,
            allowedChatIds=config.groups,
            mentionsOnly=config.group_mentions_only,
            includeUserInGroupSessionKey=config.per_user_groups,
            draftPreviews=config.draft_previews,
        ),
        delta=dict(
            cwd=str(config.project),
            sessionDirectory=str(config.sessions_path),
            agentDirectory=str(config.agent_dir) if config.agent_dir else None,
            defaultProvider=config.provider,
            defaultModel=config.model,
            defaultThinking=config.thinking,
            idleSeconds=config.idle_seconds,
            trustProject=config.trust_project,
            toolPolicy=config.policy,
            approvalSeconds=config.approval_seconds,
            executor=config.executor,
            dockerImage=config.docker_image,
            dockerMemory=config.docker_memory,
            daytonaTarget=config.daytona_target,
            daytonaSnapshot=config.daytona_snapshot,
        ),
        limits=dict(workers=config.workers, backlog=config.backlog),
        memory=dict(
            enabled=config.memory_enabled,
            extract=config.memory_extract,
            extractor=config.memory_extractor,
            model=config.memory_model,
            searchLimit=config.memory_search_limit,
            searchFloor=config.memory_search_floor,
            tokenBudget=config.memory_token_budget,
            extractionSeconds=config.memory_extraction_seconds,
        ),
        retentionDays=config.retention_days,
        shutdownTimeout=config.shutdown_timeout,
        releaseUrl=config.release_url,
    )


def load(path, *, require_project=True):
    path = Path(path).expanduser().resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Configuration must be a mapping")
    if "telegram" not in raw:
        raw.setdefault("instance", uuid5(NAMESPACE_URL, path.as_uri()).hex)
        raw.setdefault("database", str(Path(raw["state"]) / "gateway.sqlite3"))
        raw.setdefault("per_user_groups", True)
        raw.setdefault("group_mentions_only", True)
        raw.setdefault("retention_days", 7)
        raw.setdefault("workers", 2)
        raw.setdefault("backlog", 10)
        raw.setdefault("policy", "ask")
        config = Config.model_validate(raw)
    else:
        if raw.get("version", 1) != 1 or set(raw) - {
            "version",
            "instanceId",
            "name",
            "stateDirectory",
            "databasePath",
            "telegram",
            "delta",
            "limits",
            "memory",
            "retentionDays",
            "shutdownTimeout",
            "releaseUrl",
        }:
            raise ValueError("Unknown configuration version or fields")
        t, d, limits = raw["telegram"], raw.get("delta", {}), raw.get("limits", {})
        m = raw.get("memory", {})
        for value, allowed in (
            (
                t,
                {
                    "tokenEnvironment",
                    "botToken",
                    "allowedUserIds",
                    "allowGroups",
                    "allowedChatIds",
                    "mentionsOnly",
                    "includeUserInGroupSessionKey",
                    "draftPreviews",
                },
            ),
            (
                d,
                {
                    "cwd",
                    "sessionDirectory",
                    "agentDirectory",
                    "defaultProvider",
                    "defaultModel",
                    "defaultThinking",
                    "idleSeconds",
                    "trustProject",
                    "toolPolicy",
                    "approvalSeconds",
                    "executor",
                    "dockerImage",
                    "dockerMemory",
                    "daytonaTarget",
                    "daytonaSnapshot",
                },
            ),
            (limits, {"workers", "backlog"}),
            (
                m,
                {
                    "enabled",
                    "extract",
                    "extractor",
                    "model",
                    "searchLimit",
                    "searchFloor",
                    "tokenBudget",
                    "extractionSeconds",
                },
            ),
        ):
            if not isinstance(value, dict) or set(value) - allowed:
                raise ValueError("Unknown nested configuration fields")
        identity = raw.get("instanceId") or uuid5(NAMESPACE_URL, path.as_uri()).hex
        mapping = dict(
            instance=identity,
            name=raw.get("name"),
            users=t.get("allowedUserIds", []),
            project=d.get("cwd", str(path.parent.parent)),
            state=raw.get("stateDirectory", str(profile_home() / "state" / identity)),
            database=raw.get("databasePath"),
            provider=d.get("defaultProvider"),
            model=d.get("defaultModel"),
            thinking=d.get("defaultThinking"),
            agent_dir=d.get("agentDirectory"),
            session_dir=d.get("sessionDirectory"),
            idle_seconds=d.get("idleSeconds", 1800),
            trust_project=d.get("trustProject", False),
            policy=d.get("toolPolicy", "ask"),
            approval_seconds=d.get("approvalSeconds", 300),
            executor=d.get("executor", "local"),
            docker_image=d.get("dockerImage"),
            docker_memory=d.get("dockerMemory", "2g"),
            daytona_target=d.get("daytonaTarget"),
            daytona_snapshot=d.get("daytonaSnapshot"),
            memory_enabled=m.get("enabled", False),
            memory_extract=m.get("extract", False),
            memory_extractor=m.get("extractor", "mem0"),
            memory_model=m.get("model"),
            memory_search_limit=m.get("searchLimit", 10),
            memory_search_floor=m.get("searchFloor", 0.3),
            memory_token_budget=m.get("tokenBudget", 2000),
            memory_extraction_seconds=m.get("extractionSeconds", 120),
            token_env=t.get("tokenEnvironment", "TELEGRAM_BOT_TOKEN"),
            bot_token=t.get("botToken"),
            allow_groups=t.get("allowGroups", False),
            groups=t.get("allowedChatIds", []),
            group_mentions_only=t.get("mentionsOnly", False),
            per_user_groups=t.get("includeUserInGroupSessionKey", False),
            draft_previews=t.get("draftPreviews", True),
            workers=limits.get("workers"),
            backlog=limits.get("backlog"),
            retention_days=raw.get("retentionDays"),
            shutdown_timeout=raw.get("shutdownTimeout", 10),
            release_url=raw.get("releaseUrl"),
        )
        config = Config.model_validate(mapping)
    for field in ("project", "state", "database", "agent_dir", "session_dir"):
        value = getattr(config, field)
        if value is not None:
            setattr(config, field, value.expanduser())
    config.validate_paths(require_project=require_project)
    return config


def save(path, config, *, require_project=True):
    config.validate_paths(require_project=require_project)
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        value = config.model_dump(mode="json")
        if config.bot_token:
            value["bot_token"] = config.bot_token.get_secret_value()
        text = (
            json.dumps(value, indent=2)
            if path.suffix == ".json"
            else yaml.safe_dump(document(config), sort_keys=False)
        )
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as target:
            target.write(text)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
