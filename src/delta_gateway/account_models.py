"""Provider credentials and model selection owned by a web account."""

import re

import httpx
from aiohttp import web
from cryptography.exceptions import InvalidTag

PROVIDERS = {
    "openai": ("https://api.openai.com/v1", "/models"),
    "anthropic": ("https://api.anthropic.com", "/v1/models"),
    "openrouter": ("https://openrouter.ai/api/v1", "/key"),
}


class AccountProvider:
    def __init__(self, store, account):
        self.store, self.account = store, account

    def revision(self):
        row = self.store.db.execute(
            "SELECT a.provider,a.model,c.sealed FROM accounts a LEFT JOIN credentials c "
            "ON c.account=a.id AND c.kind='model' AND c.name=a.provider WHERE a.id=?",
            (self.account,),
        ).fetchone()
        return tuple(row)

    def __call__(self, config):
        from delta_model.claude import AnthropicProvider
        from delta_model.oai_compatible import OpenAIProvider
        from delta_model.settings import AnthropicProfile, Credential, OpenAIProfile

        row = self.store.account(self.account)
        name, model = row["provider"], row["model"]
        if name not in PROVIDERS or not model:
            raise RuntimeError("Choose a model and save an API key in Account settings")
        try:
            key = self.store.credential(self.account, "model", name)
        except InvalidTag:
            raise RuntimeError(
                "Saved key could not be decrypted with the gateway master key"
            ) from None
        if key is None:
            raise RuntimeError("Save an API key in Account settings")
        credential = Credential(api_key=key, base_url=PROVIDERS[name][0])
        if name == "anthropic":
            provider = AnthropicProvider(AnthropicProfile(name=name, credential=credential))
        else:
            provider = OpenAIProvider(OpenAIProfile(name=name, credential=credential))
        return provider, name, model


class AccountModels:
    def __init__(self, server, *, transport=None):
        self.server = server
        self.store = server.accounts
        self.http = httpx.AsyncClient(transport=transport, timeout=10, trust_env=False)

    def settings(self, account):
        row = self.store.account(account)
        return web.json_response(
            {
                "provider": row["provider"],
                "model": row["model"],
                "keys": self.store.names(account, "model"),
            },
            headers={"Cache-Control": "no-store"},
        )

    async def read(self, request):
        return self.settings(self.server.account(request)["id"])

    async def validate(self, provider, key):
        base, path = PROVIDERS[provider]
        headers = {"Authorization": "Bearer " + key}
        if provider == "anthropic":
            headers = {"x-api-key": key, "anthropic-version": "2023-06-01"}
        try:
            response = await self.http.get(base + path, headers=headers)
        except httpx.HTTPError:
            raise web.HTTPBadGateway(
                text="Could not reach the provider to verify this key"
            ) from None
        if response.status_code in {401, 403}:
            raise web.HTTPBadRequest(text="The provider rejected this key")
        if not response.is_success:
            raise web.HTTPBadGateway(text="The provider could not verify this key")

    def require_idle(self):
        if self.server.service.sessions.active_bots:
            raise web.HTTPConflict(text="Finish active bot work before changing model credentials")

    async def save(self, request):
        account = self.server.account(request)["id"]
        try:
            data = await request.json()
        except ValueError:
            raise web.HTTPBadRequest(text="Invalid model settings") from None
        if not isinstance(data, dict) or set(data) - {"provider", "model", "key"}:
            raise web.HTTPBadRequest(text="Invalid model settings")
        provider, model, key = data.get("provider"), data.get("model"), data.get("key")
        if (
            not isinstance(provider, str)
            or provider not in PROVIDERS
            or not isinstance(model, str)
            or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:/@+-]{0,199}", model)
        ):
            raise web.HTTPBadRequest(text="Choose a supported provider and a valid model ID")
        if "key" in data and (not isinstance(key, str) or not re.fullmatch(r"[!-~]{1,4096}", key)):
            raise web.HTTPBadRequest(
                text="The key must contain printable characters without spaces"
            )
        self.require_idle()
        if key is not None:
            if not self.store.validation_attempt(account):
                raise web.HTTPTooManyRequests(
                    text="Too many key checks. Try again in five minutes."
                )
            await self.validate(provider, key)
            # A session may have signed out while the provider was checking the key.
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text="Sign in again")
        elif provider not in self.store.names(account, "model"):
            raise web.HTTPBadRequest(text="Save a key for this provider first")
        self.require_idle()
        self.store.set_model(account, provider, model, key)
        return self.settings(account)

    async def remove(self, request):
        account = self.server.account(request)["id"]
        provider = request.match_info["provider"]
        if provider not in PROVIDERS:
            raise web.HTTPNotFound(text="Unknown provider")
        self.require_idle()
        self.store.remove_model_key(account, provider)
        return self.settings(account)
