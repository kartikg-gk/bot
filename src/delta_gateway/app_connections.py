"""Per-account OAuth connections for gateway app tools."""

import asyncio
import base64
import json
import math
import os
import re
import secrets
import time
from dataclasses import dataclass
from urllib.parse import quote, urlencode

import httpx
from aiohttp import web
from cryptography.exceptions import InvalidTag

from delta_gateway.account_store import digest
from delta_gateway.external_actions import (
    bland_result,
    check_bland_key,
    check_tinyfish_key,
    provider_key_valid,
)
from delta_gateway.granola import check_key, key_valid
from delta_gateway.plugins import GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, PluginRunner
from delta_gateway.todoist import TODOIST_MCP_URL, TODOIST_SCOPE
from delta_gateway.wispr import (
    WISPR_AUTH_URL,
    WISPR_MCP_URL,
    WISPR_SCOPES,
    WISPR_TOKEN_URL,
    register_client,
)
from delta_gateway.workspace_apps import NOTION_VERSION

GITHUB_CLIENT_ID = "DELTA_GATEWAY_GITHUB_CLIENT_ID"
GITHUB_CLIENT_SECRET = "DELTA_GATEWAY_GITHUB_CLIENT_SECRET"
GMAIL_SCOPES = frozenset({"https://www.googleapis.com/auth/gmail.modify"})
LEGACY_GMAIL_SCOPES = frozenset(
    {
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/gmail.compose",
    }
)


@dataclass(frozen=True)
class OAuthApp:
    auth_url: str
    token_url: str
    client_id: str
    secret: str
    scopes: frozenset[str] = frozenset()


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


APPS = {
    "github": OAuthApp(
        "https://github.com/login/oauth/authorize",
        "https://github.com/login/oauth/access_token",
        GITHUB_CLIENT_ID,
        GITHUB_CLIENT_SECRET,
    ),
    "gmail": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        GMAIL_CLIENT_ID,
        GMAIL_CLIENT_SECRET,
        GMAIL_SCOPES,
    ),
    "calendar": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        "DELTA_GATEWAY_CALENDAR_CLIENT_ID",
        "DELTA_GATEWAY_CALENDAR_CLIENT_SECRET",
        frozenset(
            {
                "https://www.googleapis.com/auth/calendar.calendarlist.readonly",
                "https://www.googleapis.com/auth/calendar.events",
                "https://www.googleapis.com/auth/calendar.events.freebusy",
            }
        ),
    ),
    "tasks": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        "DELTA_GATEWAY_TASKS_CLIENT_ID",
        "DELTA_GATEWAY_TASKS_CLIENT_SECRET",
        frozenset({"https://www.googleapis.com/auth/tasks"}),
    ),
    "drive": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        "DELTA_GATEWAY_DRIVE_CLIENT_ID",
        "DELTA_GATEWAY_DRIVE_CLIENT_SECRET",
        frozenset({"https://www.googleapis.com/auth/drive.readonly"}),
    ),
    "docs": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        "DELTA_GATEWAY_DOCS_CLIENT_ID",
        "DELTA_GATEWAY_DOCS_CLIENT_SECRET",
        frozenset({"https://www.googleapis.com/auth/documents"}),
    ),
    "sheets": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        "DELTA_GATEWAY_SHEETS_CLIENT_ID",
        "DELTA_GATEWAY_SHEETS_CLIENT_SECRET",
        frozenset({"https://www.googleapis.com/auth/spreadsheets"}),
    ),
    "contacts": OAuthApp(
        GOOGLE_AUTH_URL,
        GOOGLE_TOKEN_URL,
        "DELTA_GATEWAY_CONTACTS_CLIENT_ID",
        "DELTA_GATEWAY_CONTACTS_CLIENT_SECRET",
        frozenset({"https://www.googleapis.com/auth/contacts.readonly"}),
    ),
    "notion": OAuthApp(
        "https://api.notion.com/v1/oauth/authorize",
        "https://api.notion.com/v1/oauth/token",
        "DELTA_GATEWAY_NOTION_CLIENT_ID",
        "DELTA_GATEWAY_NOTION_CLIENT_SECRET",
    ),
    "linear": OAuthApp(
        "https://linear.app/oauth/authorize",
        "https://api.linear.app/oauth/token",
        "DELTA_GATEWAY_LINEAR_CLIENT_ID",
        "DELTA_GATEWAY_LINEAR_CLIENT_SECRET",
        frozenset({"read", "issues:create", "comments:create"}),
    ),
    "todoist": OAuthApp(
        "https://todoist.com/oauth/authorize",
        "https://todoist.com/oauth/access_token",
        "DELTA_GATEWAY_TODOIST_CLIENT_ID",
        "DELTA_GATEWAY_TODOIST_CLIENT_SECRET",
        frozenset({TODOIST_SCOPE}),
    ),
    "wispr": OAuthApp(WISPR_AUTH_URL, WISPR_TOKEN_URL, "", "", WISPR_SCOPES),
    "granola": OAuthApp("", "", "", ""),
    "tinyfish": OAuthApp("", "", "", ""),
    "bland": OAuthApp("", "", "", ""),
}
KEY_APPS = {
    "granola": ("Granola", key_valid, check_key),
    "tinyfish": ("TinyFish", provider_key_valid, check_tinyfish_key),
    "bland": ("Bland AI", provider_key_valid, check_bland_key),
}


class AppAuthorizationExpired(ValueError):
    pass


class AppClientExpired(AppAuthorizationExpired):
    pass


class AppConnections:
    def __init__(self, server, *, transport=None):
        self.server, self.store = server, server.accounts
        self.http = httpx.AsyncClient(transport=transport, timeout=15, trust_env=False)
        self.transport = transport
        self.locks = {}

    def install(self, app):
        app.router.add_get("/api/apps", self.list)
        app.router.add_put("/api/apps/{app}/key", self.save_app_key)
        app.router.add_get("/api/apps/bland/calls/{call}", self.bland_call_result)
        app.router.add_post("/api/apps/{app}/connect", self.connect)
        app.router.add_delete("/api/apps/{app}", self.disconnect)
        app.router.add_put("/api/apps/{app}/bots/{bot}", self.grant)
        app.router.add_get("/auth/apps/{app}/callback", self.callback)

    def configured(self, name):
        if name == "wispr" or name in KEY_APPS:
            return True
        app = APPS[name]
        return bool(os.environ.get(app.client_id) and os.environ.get(app.secret))

    def connected(self, account, name):
        return name in APPS and self.configured(name) and name in self.store.names(account, "app")

    def runner(self, account):
        async def token(name):
            return await self.token(account, name)

        async def recover(name, rejected):
            return await self.token(account, name, rejected=rejected)

        return PluginRunner(
            transport=self.transport,
            access_token=token,
            recover_token=recover,
            available=lambda name: self.connected(account, name),
        )

    def lock(self, account, name):
        return self.locks.setdefault((account, name), asyncio.Lock())

    def credential(self, account, name):
        try:
            raw = self.store.credential(account, "app", name)
            return json.loads(raw) if raw is not None else None
        except (InvalidTag, ValueError):
            raise ValueError(f"Reconnect {name} to restore its credentials") from None

    def save(self, account, name, token):
        with self.store.db:
            self.store.put_credential(account, "app", name, json.dumps(token))

    def client_fields(self, name):
        app = APPS[name]
        return {"client_id": os.environ[app.client_id], "client_secret": os.environ[app.secret]}

    def callback_url(self, name):
        return self.server.google.origin + f"/auth/apps/{name}/callback"

    async def wispr_client(self, account, callback):
        try:
            raw = self.store.credential(account, "app-client", "wispr")
            saved = json.loads(raw) if raw else None
        except (InvalidTag, ValueError):
            saved = None
        if isinstance(saved, dict) and saved.get("callback") == callback and saved.get("client_id"):
            return saved["client_id"]
        identifier = await register_client(self.http, callback)
        with self.store.db:
            self.store.put_credential(
                account,
                "app-client",
                "wispr",
                json.dumps({"callback": callback, "client_id": identifier}),
            )
        return identifier

    async def exchange(self, name, **data):
        if name == "wispr":
            fields = {"client_id": data.pop("client_id"), "resource": WISPR_MCP_URL}
        else:
            fields = self.client_fields(name)
        headers = {"Accept": "application/json"}
        if name == "todoist":
            data["resource"] = TODOIST_MCP_URL
        if name == "notion":
            data.pop("code_verifier", None)
            response = await self.http.post(
                APPS[name].token_url,
                headers={**headers, "Notion-Version": NOTION_VERSION},
                auth=(fields["client_id"], fields["client_secret"]),
                json=data,
            )
        else:
            response = await self.http.post(
                APPS[name].token_url,
                headers=headers,
                data={**fields, **data},
            )
        if response.status_code in {200, 400, 401}:
            try:
                error = response.json()
            except ValueError:
                error = None
            code = error.get("error") if isinstance(error, dict) else None
            if name == "wispr" and code == "invalid_client":
                raise AppClientExpired("Reconnect Wispr Flow to register a new OAuth client")
            if data.get("grant_type") == "refresh_token" and code in (
                "invalid_grant",
                "bad_refresh_token",
            ):
                raise AppAuthorizationExpired(f"Reconnect {name} to renew access")
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or result.get("error"):
            raise ValueError(f"{name} did not authorize this connection")
        return result

    def normalize(self, name, result, previous=None):
        access = result.get("access_token")
        refresh = result.get("refresh_token", previous.get("refresh") if previous else None)
        if not isinstance(access, str) or not access or any(c.isspace() for c in access):
            raise ValueError(f"{name} did not return an access token")
        if refresh is not None and (not isinstance(refresh, str) or not refresh):
            raise ValueError(f"{name} returned an invalid refresh token")
        required_scopes = APPS[name].scopes
        if required_scopes:
            scopes = result.get("scope")
            if scopes is not None:
                granted = (
                    set(re.split(r"[,\s]+", scopes.strip())) if isinstance(scopes, str) else set()
                )
                if name == "linear" and isinstance(scopes, list):
                    granted = {scope for scope in scopes if isinstance(scope, str)}
                legacy = name == "gmail" and previous is not None and LEGACY_GMAIL_SCOPES <= granted
                if name == "wispr" and refresh:
                    granted.add("offline_access")
                if not (required_scopes <= granted or legacy):
                    message = (
                        "Gmail requires mailbox and label permissions"
                        if name == "gmail"
                        else f"{name} requires its requested permissions"
                    )
                    raise ValueError(message)
        if (required_scopes or name == "notion") and refresh is None:
            raise ValueError(f"{name} did not provide offline authorization. Reconnect.")
        expiry = result.get("expires_in")
        if expiry is None and required_scopes:
            raise ValueError(f"{name} did not report the access token lifetime")
        if expiry is not None:
            expiry = float(expiry)
            if not math.isfinite(expiry) or expiry <= 0:
                raise ValueError(f"{name} returned an invalid token lifetime")
        return {
            "access": access,
            "refresh": refresh,
            "expires": time.time() + expiry if expiry is not None else None,
        }

    async def refresh(self, account, name, token, *, force=False):
        if not force and (token["expires"] is None or token["expires"] > time.time() + 30):
            return token
        if not token["refresh"]:
            raise AppAuthorizationExpired(f"Reconnect {name} to renew access")
        fields = {"client_id": token["client_id"]} if name == "wispr" else {}
        reply = await self.exchange(
            name,
            grant_type="refresh_token",
            refresh_token=token["refresh"],
            **fields,
        )
        updated = {**self.normalize(name, reply, token), **fields}
        self.save(account, name, updated)
        return updated

    async def token(self, account, name, *, rejected=None):
        async with self.lock(account, name):
            if not self.connected(account, name):
                raise ValueError(f"Connect {name} in Apps first")
            try:
                current = self.credential(account, name)
                token = await self.refresh(
                    account, name, current, force=rejected == current["access"]
                )
            except AppAuthorizationExpired as exc:
                self.clear_grants(account, name)
                with self.store.db:
                    self.store.delete_app_token(account, name)
                    if isinstance(exc, AppClientExpired):
                        self.store.delete_credential(account, "app-client", name)
                raise
            return token["access"]

    def name(self, request):
        name = request.match_info["app"]
        if name not in APPS:
            raise web.HTTPNotFound(text="Unknown app")
        return name

    def identity(self, request):
        account = self.server.account(request)
        if account is None:
            raise web.HTTPUnauthorized(text="Sign in before connecting an app")
        return account["id"]

    def clear_grants(self, account, name):
        runtime = self.server.runtimes.get(self.store.account(account))
        with runtime.store.db:
            runtime.store.db.execute(
                "DELETE FROM bot_plugins WHERE instance=? AND plugin=?",
                (runtime.store.instance, name),
            )

    async def list(self, request):
        account = self.identity(request)
        bots = self.server.service.store.bots()
        saved = set(self.store.names(account, "app"))
        return web.json_response(
            [
                {
                    "id": name,
                    "configured": self.configured(name),
                    "connected": name in saved and self.configured(name),
                    "saved": name in saved,
                    "keyAuth": name in KEY_APPS,
                    "bots": [
                        {
                            "id": bot.id,
                            "name": bot.name,
                            "enabled": name in self.server.service.store.plugins_for(bot.id),
                        }
                        for bot in bots
                    ],
                }
                for name in APPS
            ]
        )

    async def save_app_key(self, request):
        name, account = self.name(request), self.identity(request)
        if name not in KEY_APPS:
            raise web.HTTPNotFound(text="Unknown API key connection")
        label, valid, check = KEY_APPS[name]
        try:
            payload = await request.json()
        except ValueError:
            raise web.HTTPBadRequest(text=f"Expected a {label} API key") from None
        key = payload.get("key") if isinstance(payload, dict) else None
        if not valid(key):
            raise web.HTTPBadRequest(text=f"Enter a {label} API key")
        async with self.lock(account, name):
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text=f"Sign in again before connecting {label}")
            if not self.store.validation_attempt(account):
                raise web.HTTPTooManyRequests(text="Try validating the key again in a few minutes")
            try:
                await check(self.http, key)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {401, 403}:
                    raise web.HTTPUnprocessableEntity(text=f"{label} rejected that key") from None
                raise web.HTTPBadGateway(text=f"Could not validate the {label} key") from None
            except (httpx.HTTPError, ValueError):
                raise web.HTTPBadGateway(text=f"Could not validate the {label} key") from None
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text=f"Sign in again before connecting {label}")
            self.clear_grants(account, name)
            self.save(account, name, {"access": key, "refresh": None, "expires": None})
        return web.json_response({"connected": True})

    async def bland_call_result(self, request):
        account = self.identity(request)
        if not self.connected(account, "bland"):
            raise web.HTTPConflict(text="Connect Bland AI in Apps first")
        raw = request.query.get("offset", "0")
        if not raw.isascii() or not raw.isdigit() or len(raw) > 8:
            raise web.HTTPBadRequest(text="Invalid transcript offset")
        try:
            token = await self.token(account, "bland")
            result = await bland_result(self.http, token, request.match_info["call"], int(raw))
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise web.HTTPNotFound(text="Bland did not find that call") from None
            raise web.HTTPBadGateway(text="Could not load the Bland call") from None
        except httpx.HTTPError:
            raise web.HTTPBadGateway(text="Could not load the Bland call") from None
        except ValueError:
            raise web.HTTPBadRequest(
                text="Invalid call ID or unavailable Bland connection"
            ) from None
        return web.json_response(result)

    async def connect(self, request):
        name, account = self.name(request), self.identity(request)
        if name in KEY_APPS:
            raise web.HTTPConflict(text=f"Connect {KEY_APPS[name][0]} with an API key in Apps")
        if not self.configured(name):
            raise web.HTTPConflict(text=f"Configure the {name} OAuth client on the gateway")
        from delta_gateway.google_oidc import SESSION_COOKIE

        state, verifier = (secrets.token_urlsafe(32) for _ in range(2))
        session = request.cookies[SESSION_COOKIE]
        callback = self.callback_url(name)
        async with self.lock(account, name):
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text="Sign in again before connecting an app")
            flow = verifier
            if name == "wispr":
                try:
                    identifier = await self.wispr_client(account, callback)
                except (httpx.HTTPError, ValueError, TypeError):
                    raise web.HTTPBadGateway(
                        text="Could not register the Wispr Flow OAuth client"
                    ) from None
                if self.server.account_from_cookie(request) is None:
                    raise web.HTTPUnauthorized(text="Sign in again before connecting an app")
                flow = json.dumps(
                    {"verifier": verifier, "client_id": identifier, "callback": callback}
                )
            else:
                identifier = os.environ[APPS[name].client_id]
            self.store.start_app_flow(account, name, session, state, flow)
        query = dict(
            client_id=identifier,
            redirect_uri=callback,
            response_type="code",
            state=state,
        )
        if name == "notion":
            query["owner"] = "user"
        else:
            query.update(
                code_challenge_method="S256",
                code_challenge=base64.urlsafe_b64encode(digest(verifier)).decode().rstrip("="),
            )
        app = APPS[name]
        if app.token_url == GOOGLE_TOKEN_URL:
            query.update(
                scope=" ".join(sorted(app.scopes)), access_type="offline", prompt="consent"
            )
        elif name == "linear":
            query.update(scope=",".join(sorted(app.scopes)), actor="user")
        elif name == "todoist":
            query.update(scope=TODOIST_SCOPE, resource=TODOIST_MCP_URL)
        elif name == "wispr":
            query.update(
                scope=" ".join(sorted(app.scopes)),
                resource=WISPR_MCP_URL,
                prompt="consent",
            )
        return web.json_response({"url": APPS[name].auth_url + "?" + urlencode(query)})

    async def callback(self, request):
        from delta_gateway.google_oidc import SESSION_COOKIE

        name, account = self.name(request), self.identity(request)
        state, code = request.query.get("state", ""), request.query.get("code", "")
        if name in KEY_APPS or len(state) != 43 or not self.configured(name):
            raise web.HTTPBadRequest(text="Start a new app connection")
        async with self.lock(account, name):
            flow = self.store.consume_app_flow(
                account, name, request.cookies[SESSION_COOKIE], state
            )
            if flow is None or not 1 <= len(code) <= 4096:
                raise web.HTTPBadRequest(text="App authorization expired or was declined")
            try:
                redirect_uri = self.callback_url(name)
                fields = {}
                if name == "wispr":
                    bound = json.loads(flow)
                    if bound["callback"] != redirect_uri:
                        raise ValueError("Wispr Flow redirect changed during authorization")
                    fields["client_id"] = bound["client_id"]
                    flow = bound["verifier"]
                reply = await self.exchange(
                    name,
                    grant_type="authorization_code",
                    code=code,
                    code_verifier=flow,
                    redirect_uri=redirect_uri,
                    **fields,
                )
                token = {**self.normalize(name, reply), **fields}
            except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
                if isinstance(exc, AppClientExpired):
                    with self.store.db:
                        self.store.delete_credential(account, "app-client", name)
                raise web.HTTPBadGateway(
                    text=f"Could not connect {name}. Start a new connection."
                ) from None
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text="Sign in again before connecting an app")
            self.clear_grants(account, name)
            self.save(account, name, token)
        raise web.HTTPFound(
            "/?apps=1", headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}
        )

    async def revoke(self, account, name, token):
        if name == "wispr" or name in KEY_APPS or not self.configured(name):
            return False
        if name == "notion":
            fields = self.client_fields(name)
            reply = await self.http.post(
                "https://api.notion.com/v1/oauth/revoke",
                auth=(fields["client_id"], fields["client_secret"]),
                headers={"Notion-Version": NOTION_VERSION},
                json={"token": token["access"]},
            )
        elif name == "linear":
            key = "refresh" if token["refresh"] else "access"
            reply = await self.http.post(
                "https://api.linear.app/oauth/revoke",
                data={"token": token[key], "token_type_hint": key + "_token"},
            )
        elif name == "todoist":
            current = await self.refresh(account, name, token)
            fields = self.client_fields(name)
            reply = await self.http.post(
                "https://todoist.com/api/v1/revoke",
                auth=(fields["client_id"], fields["client_secret"]),
                data={"token": current["access"], "token_type_hint": "access_token"},
            )
        elif APPS[name].token_url == GOOGLE_TOKEN_URL:
            reply = await self.http.post(
                "https://oauth2.googleapis.com/revoke",
                data={"token": token["refresh"] or token["access"]},
            )
        else:
            token = await self.refresh(account, name, token)
            fields = self.client_fields(name)
            client_id = quote(fields["client_id"], safe="")
            reply = await self.http.request(
                "DELETE",
                f"https://api.github.com/applications/{client_id}/grant",
                auth=(fields["client_id"], fields["client_secret"]),
                json={"access_token": token["access"]},
            )
        return reply.is_success

    async def disconnect(self, request):
        name, account = self.name(request), self.identity(request)
        revoked = False
        async with self.lock(account, name):
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text="Sign in again before disconnecting an app")
            self.clear_grants(account, name)
            try:
                token = self.credential(account, name)
                revoked = token is None or await self.revoke(account, name, token)
            except (httpx.HTTPError, ValueError, TypeError):
                pass
            finally:
                self.store.remove_app(account, name)
        return web.json_response({"connected": False, "providerRevoked": revoked})

    async def grant(self, request):
        name, account = self.name(request), self.identity(request)
        try:
            body = await request.json()
        except ValueError:
            raise web.HTTPBadRequest(text="Expected a Boolean enabled setting") from None
        if (
            not isinstance(body, dict)
            or set(body) != {"enabled"}
            or type(body["enabled"]) is not bool
        ):
            raise web.HTTPBadRequest(text="Expected a Boolean enabled setting")
        async with self.lock(account, name):
            if self.server.account_from_cookie(request) is None:
                raise web.HTTPUnauthorized(text="Sign in again before changing app access")
            store = self.server.service.store
            bot = store.bot(request.match_info["bot"])
            if bot is None or bot.temporary:
                raise web.HTTPNotFound(text="Unknown bot")
            if body["enabled"] and not self.connected(account, name):
                raise web.HTTPConflict(text=f"Connect {name} before granting bot access")
            store.set_plugin(bot.id, name, body["enabled"])
        return web.json_response(body)
