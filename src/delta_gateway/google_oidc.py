"""Authorization-code sign-in with browser binding, PKCE and verified ID tokens."""

import base64
import hmac
import re
import secrets
import sqlite3
import time
from urllib.parse import urlencode, urlsplit

import httpx
import jwt
from aiohttp import web

from delta_gateway.account_store import FLOW_SECONDS, SESSION_SECONDS, digest

LOGIN_COOKIE = "delta_login"
SESSION_COOKIE = "delta_session"
ACCOUNT_REQUEST = web.RequestKey("delta.account", sqlite3.Row)
AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
KEYS_URL = "https://www.googleapis.com/oauth2/v3/certs"


def canonical_origin(value):
    parsed = urlsplit(value or "")
    try:
        valid_port = parsed.port is None or 1 <= parsed.port <= 65535
    except ValueError:
        valid_port = False
    local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if (
        not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or not valid_port
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or not (parsed.scheme == "https" or local and parsed.scheme == "http")
    ):
        raise ValueError("Google mode requires an HTTPS web origin or an HTTP loopback origin")
    hostname = parsed.hostname.lower()
    host = f"[{hostname}]" if ":" in hostname else hostname
    standard_port = 443 if parsed.scheme == "https" else 80
    port = f":{parsed.port}" if parsed.port is not None and parsed.port != standard_port else ""
    return f"{parsed.scheme}://{host}{port}"


class GoogleLogin:
    def __init__(self, store, origin, client_id, client_secret, emails, domains, *, transport=None):
        self.origin = canonical_origin(origin)
        self.host = urlsplit(self.origin).netloc
        if not client_id or not client_secret:
            raise ValueError(
                "Set DELTA_GATEWAY_GOOGLE_CLIENT_ID and DELTA_GATEWAY_GOOGLE_CLIENT_SECRET"
            )
        self.store, self.client_id, self.client_secret = store, client_id, client_secret
        self.emails = {email.strip().casefold() for email in emails if email.strip()}
        self.domains = {domain.strip().casefold() for domain in domains if domain.strip()}
        if not self.emails and not self.domains:
            raise ValueError("Configure an allowed Google email address or Workspace domain")
        self.http = httpx.AsyncClient(transport=transport, timeout=10, trust_env=False)
        self.keys, self.keys_until = {}, 0

    def permitted(self, email, domain):
        email = email.casefold()
        return email in self.emails or (
            domain is not None
            and domain.casefold() in self.domains
            and email.rpartition("@")[2] == domain.casefold()
        )

    def request_allowed(self, request, *, trusted_proxy=False):
        if not trusted_proxy and request.headers.get("Host", "").lower() != self.host:
            return False
        origin = request.headers.get("Origin")
        return origin == self.origin or (origin is None and request.method in {"GET", "HEAD"})

    def identity(self, token):
        account = self.store.session(token)
        if account is not None and self.permitted(account["email"], account["domain"]):
            return account
        return None

    def cookie(self, response, name, value, age):
        response.set_cookie(
            name,
            value,
            max_age=age,
            httponly=True,
            samesite="Lax",
            secure=self.origin.startswith("https:"),
            path="/",
        )

    async def start(self, request):
        state, nonce, verifier = (secrets.token_urlsafe(32) for _ in range(3))
        browser = request.cookies.get(LOGIN_COOKIE, "")
        if len(browser) != 43:
            browser = secrets.token_urlsafe(32)
        try:
            self.store.start_flow(state, browser, verifier, nonce)
        except ValueError as exc:
            raise web.HTTPTooManyRequests(text=str(exc)) from None
        challenge = base64.urlsafe_b64encode(digest(verifier)).decode().rstrip("=")
        query = urlencode(
            dict(
                client_id=self.client_id,
                redirect_uri=self.origin + "/auth/callback",
                response_type="code",
                scope="openid email",
                state=state,
                nonce=nonce,
                code_challenge=challenge,
                code_challenge_method="S256",
            )
        )
        response = web.HTTPFound(AUTH_URL + "?" + query)
        response.headers["Cache-Control"] = "no-store"
        self.cookie(response, LOGIN_COOKIE, browser, FLOW_SECONDS)
        raise response

    async def claims(self, token, nonce):
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if header.get("alg") != "RS256" or not isinstance(kid, str) or not kid:
            raise ValueError("Invalid Google token header")
        if time.monotonic() >= self.keys_until or kid not in self.keys:
            response = await self.http.get(KEYS_URL)
            response.raise_for_status()
            self.keys = {key["kid"]: key for key in response.json()["keys"]}
            ttl = re.search(r"max-age=(\d+)", response.headers.get("Cache-Control", ""))
            self.keys_until = time.monotonic() + min(3600, int(ttl[1]) if ttl else 300)
        key = jwt.PyJWK.from_dict(self.keys[kid], algorithm="RS256").key
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=self.client_id,
            issuer=("https://accounts.google.com", "accounts.google.com"),
            options={"require": ["sub", "iss", "aud", "exp", "iat", "nonce", "email"]},
        )
        actual = claims["nonce"]
        if not isinstance(actual, str) or not hmac.compare_digest(actual.encode(), nonce.encode()):
            raise ValueError("Google token nonce mismatch")
        if claims.get("azp", self.client_id) != self.client_id:
            raise ValueError("Google token belongs to another client")
        if isinstance(claims["aud"], list) and len(claims["aud"]) > 1 and "azp" not in claims:
            raise ValueError("Google token has no authorized party")
        return claims

    async def callback(self, request):
        state, code = request.query.get("state", ""), request.query.get("code", "")
        browser = request.cookies.get(LOGIN_COOKIE, "")
        flow = self.store.consume_flow(state, browser) if len(state) == len(browser) == 43 else None
        if flow is None or not code or len(code) > 4096:
            raise web.HTTPBadRequest(text="Sign-in expired or was declined. Start again.")
        try:
            response = await self.http.post(
                TOKEN_URL,
                data=dict(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    code=code,
                    grant_type="authorization_code",
                    redirect_uri=self.origin + "/auth/callback",
                    code_verifier=flow["verifier"],
                ),
            )
            response.raise_for_status()
            claims = await self.claims(response.json()["id_token"], flow["nonce"])
        except (httpx.HTTPError, jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise web.HTTPUnauthorized(text="Google sign-in could not be verified") from None
        email, domain, subject = claims["email"], claims.get("hd"), claims["sub"]
        if (
            claims.get("email_verified") is not True
            or not isinstance(email, str)
            or len(email) > 320
            or not isinstance(subject, str)
            or not 1 <= len(subject) <= 255
            or (domain is not None and not isinstance(domain, str))
            or not self.permitted(email, domain)
        ):
            raise web.HTTPForbidden(text="This Google account is not allowed")
        self.store.sign_out(request.cookies.get(SESSION_COOKIE))
        token = self.store.sign_in(subject, email, domain)
        response = web.HTTPFound("/")
        response.headers["Cache-Control"] = "no-store"
        response.del_cookie(LOGIN_COOKIE, path="/")
        self.cookie(response, SESSION_COOKIE, token, SESSION_SECONDS)
        raise response

    async def logout(self, request):
        self.store.sign_out(request.cookies.get(SESSION_COOKIE))
        response = web.json_response({"signedOut": True}, headers={"Cache-Control": "no-store"})
        response.del_cookie(SESSION_COOKIE, path="/")
        return response
