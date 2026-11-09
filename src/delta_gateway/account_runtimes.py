"""Independent workspaces and service lifecycles for admitted web accounts."""

import asyncio
import logging
from uuid import UUID

from delta_gateway.account_models import AccountProvider


class AccountRuntimes:
    def __init__(self, parent, accounts, login, apps):
        self.parent, self.accounts, self.login = parent, accounts, login
        self.apps = apps
        self.services = {}
        self.runner = None

    def get(self, account):
        from delta_gateway.service import Service

        identity = UUID(account["id"]).hex
        if identity in self.services:
            return self.services[identity]
        root = self.parent.config.state / "web-users" / identity
        project = root / "workspace"
        project.mkdir(parents=True, exist_ok=True)
        config = self.parent.config.model_copy(
            update=dict(
                instance=identity,
                users=[1],
                project=project,
                state=root,
                database=root / "gateway.sqlite3",
                agent_dir=root / "agent",
                session_dir=root / "sessions",
                trust_project=False,
                bot_token=None,
                web_port=0,
                web_only=True,
                memory_enabled=False,
                memory_extract=False,
                provider=None,
                model=None,
            )
        )
        factory = self.parent.sessions.factory or AccountProvider(self.accounts, identity)
        service = Service(config, None, factory=factory, plugins=self.apps.runner(identity))
        self.services[identity] = service
        return service

    def start(self):
        # Reopen existing accounts so persisted jobs can run before anyone signs in again.
        for account in self.accounts.all_accounts():
            if self.login.permitted(account["email"], account["domain"]):
                self.get(account)
        self.runner = asyncio.create_task(self.tick())

    async def tick(self):
        while True:
            for service in tuple(self.services.values()):
                try:
                    await service.dispatch()
                    service.run_due()
                    await service.cleanup_idle()
                    service.store.retain()
                except Exception:
                    logging.getLogger(__name__).exception("Could not advance an account runtime")
            await asyncio.sleep(2)

    async def close(self):
        if self.runner is not None:
            self.runner.cancel()
            await asyncio.gather(self.runner, return_exceptions=True)
        await asyncio.gather(*(service.close() for service in self.services.values()))
        self.services.clear()
