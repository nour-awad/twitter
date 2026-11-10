"""Zero-trust security (Week 12): "never trust, always verify".

* Every service verifies every token itself -- being "behind the gateway"
  grants nothing. The gateway check is only an early reject.
* Tokens are RS256 JWTs signed by auth-service; verifiers fetch the public
  keys from its JWKS endpoint (key rotation without redeploying anyone).
* End-user tokens have audience `twitter-api`; service tokens (OAuth2
  client-credentials) have audience `twitter-internal`. Internal endpoints
  only accept service tokens that carry the right *scope* (least privilege).
* Transport identity (mTLS) and network policy are added by the mesh
  (deploy/helm/.../istio-*.yaml, networkpolicy.yaml) -- defence in depth.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import grpc
import httpx
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from twitter_common.config import AUTH_URL, env

log = logging.getLogger(__name__)

ISSUER = "twitter-auth"
AUD_PUBLIC = "twitter-api"
AUD_INTERNAL = "twitter-internal"


@dataclass
class Principal:
    sub: str
    username: str | None = None
    roles: list[str] = field(default_factory=list)
    scopes: set[str] = field(default_factory=set)
    is_service: bool = False
    token: str | None = None

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes or "admin" in self.roles


class TokenVerifier:
    """Verifies JWTs against the auth-service JWKS, with caching and rotation support."""

    def __init__(self, jwks_url: str, ttl: float = 300.0) -> None:
        self.jwks_url = jwks_url
        self.ttl = ttl
        self._keys: dict[str, Any] = {}
        self._fetched_at = 0.0
        self._lock = asyncio.Lock()

    async def _refresh(self) -> None:
        async with self._lock:
            if time.monotonic() - self._fetched_at < 1.0 and self._keys:
                return
            async with httpx.AsyncClient(timeout=3) as client:
                resp = await client.get(self.jwks_url)
                resp.raise_for_status()
            jwks = jwt.PyJWKSet.from_dict(resp.json())
            self._keys = {k.key_id: k.key for k in jwks.keys}
            self._fetched_at = time.monotonic()

    async def _key_for(self, kid: str) -> Any:
        if kid not in self._keys or time.monotonic() - self._fetched_at > self.ttl:
            await self._refresh()  # unknown kid => keys were rotated
        if kid not in self._keys:
            raise jwt.InvalidTokenError("unknown signing key")
        return self._keys[kid]

    async def verify(self, token: str, audience: str) -> Principal:
        header = jwt.get_unverified_header(token)
        key = await self._key_for(header.get("kid", ""))
        claims = jwt.decode(token, key, algorithms=["RS256"], audience=audience, issuer=ISSUER)
        return Principal(
            sub=claims["sub"],
            username=claims.get("username"),
            roles=list(claims.get("roles", [])),
            scopes=set(str(claims.get("scope", "")).split()),
            is_service=claims.get("token_use") == "service",
            token=token,
        )


_verifier: TokenVerifier | None = None


def get_verifier() -> TokenVerifier:
    global _verifier
    if _verifier is None:
        _verifier = TokenVerifier(env("AUTH_JWKS_URL") or f"{AUTH_URL()}/.well-known/jwks.json")
    return _verifier


_bearer = HTTPBearer(auto_error=False)


async def _principal(token: str, audience: str) -> Principal:
    try:
        return await get_verifier().verify(token, audience)
    except (jwt.InvalidTokenError, httpx.HTTPError) as exc:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, f"invalid token: {exc}", headers={"WWW-Authenticate": "Bearer"}
        ) from exc


async def optional_user(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Principal | None:
    if creds is None:
        return None
    return await _principal(creds.credentials, AUD_PUBLIC)


async def require_user(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Principal:
    if creds is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "authentication required",
                            headers={"WWW-Authenticate": "Bearer"})
    return await _principal(creds.credentials, AUD_PUBLIC)


def require_scope(scope: str) -> Callable[..., Any]:
    async def dep(user: Principal = Depends(require_user)) -> Principal:
        if not user.has_scope(scope):
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"missing scope '{scope}'")
        return user

    return dep


def require_service(scope: str) -> Callable[..., Any]:
    """Guard for /internal endpoints: only service identities with `scope`."""

    async def dep(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> Principal:
        if creds is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "service token required")
        principal = await _principal(creds.credentials, AUD_INTERNAL)
        if not principal.is_service or scope not in principal.scopes:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"service lacks scope '{scope}'")
        return principal

    return dep


class ServiceTokenProvider:
    """OAuth2 client-credentials: obtains & caches this service's own identity token."""

    def __init__(self, client_id: str, client_secret: str | None = None, auth_url: str | None = None) -> None:
        self.client_id = client_id
        self.client_secret = client_secret or env("SERVICE_CLIENT_SECRET") or f"dev-secret-{client_id}"
        self.auth_url = auth_url or AUTH_URL()
        self._token: str | None = None
        self._exp = 0.0
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        async with self._lock:
            if self._token and time.time() < self._exp - 30:
                return self._token
            async with httpx.AsyncClient(timeout=3) as client:
                resp = await client.post(
                    f"{self.auth_url}/v1/auth/service-token",
                    json={"client_id": self.client_id, "client_secret": self.client_secret},
                )
                resp.raise_for_status()
            body = resp.json()
            self._token = body["access_token"]
            self._exp = time.time() + int(body.get("expires_in", 300))
            return self._token


async def authorize_grpc(context: grpc.aio.ServicerContext, scope: str) -> Principal:
    """Zero-trust for gRPC: verify the caller's service token from metadata."""
    md = dict(context.invocation_metadata() or ())
    auth = md.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing bearer token")
    try:
        principal = await get_verifier().verify(auth[7:], AUD_INTERNAL)
    except (jwt.InvalidTokenError, httpx.HTTPError) as exc:
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, f"invalid token: {exc}")
        raise  # unreachable, abort raises
    if scope not in principal.scopes:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, f"missing scope '{scope}'")
    return principal
