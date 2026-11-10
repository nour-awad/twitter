"""auth-service: identity provider / security token service (Week 12).

* Users authenticate with username+password (OAuth2 password grant) and get
  a short-lived RS256 JWT (audience `twitter-api`).
* Services authenticate with client credentials and get a service token
  (audience `twitter-internal`) carrying least-privilege scopes.
* Public keys are published as a JWKS so every other service can verify
  tokens locally -- no call to auth-service per request, no shared secret.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

import jwt
from jwt.algorithms import RSAAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends, Form, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, DateTime, String, delete, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.bus import create_event_bus
from twitter_common.config import database_url, env, env_int
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.events import CloudEvent, EventTypes, Topics
from twitter_common.outbox import claim_event, make_inbox_model
from twitter_common.security import AUD_INTERNAL, AUD_PUBLIC, ISSUER, Principal, require_service, require_user
from twitter_common.service import create_service_app

SERVICE = "auth-service"
ACCESS_TTL = env_int("ACCESS_TOKEN_TTL_SECONDS", 3600)
SERVICE_TTL = env_int("SERVICE_TOKEN_TTL_SECONDS", 600)
USER_SCOPES = "tweets:read tweets:write profile:write engagement:write notifications:read ai:use"

# Least privilege: each workload gets exactly the scopes it needs.
SERVICE_CLIENT_SCOPES: dict[str, list[str]] = {
    "api-gateway": ["users:read"],
    "tweet-service": ["users:read"],
    "timeline-service": ["users:read"],
    "saga-orchestrator": ["auth:provision", "users:provision", "timeline:provision", "notifications:send"],
    "aiops-service": ["metrics:read"],
    "ai-agent-service": ["users:read"],
}


def _client_secrets() -> dict[str, str]:
    configured = json.loads(env("SERVICE_CLIENTS_JSON", "{}") or "{}")
    return {name: configured.get(name, f"dev-secret-{name}") for name in SERVICE_CLIENT_SCOPES}


# ------------------------------------------------------------------ storage
class Base(DeclarativeBase):
    pass


class Credential(Base):
    __tablename__ = "credentials"
    user_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    username: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(255))
    password_hash: Mapped[str] = mapped_column(String(255))
    roles: Mapped[str] = mapped_column(String(255), default="user")
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


Inbox = make_inbox_model(Base)
engine = make_engine(database_url("auth"))
Session = make_session_factory(engine)
bus = create_event_bus(SERVICE)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=2**14, r=8, p=1)
    return f"scrypt${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}"


def verify_password(password: str, stored: str) -> bool:
    _, salt_b64, digest_b64 = stored.split("$")
    digest = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt_b64), n=2**14, r=8, p=1)
    return hmac.compare_digest(digest, base64.b64decode(digest_b64))


# ------------------------------------------------------------------ keys
class KeyRing:
    """Signing keys with rotation: new tokens use the newest key, old keys stay in the JWKS."""

    def __init__(self) -> None:
        self.keys: list[tuple[str, rsa.RSAPrivateKey]] = []
        pem_path = env("AUTH_PRIVATE_KEY_PATH")
        if pem_path and os.path.exists(pem_path):
            with open(pem_path, "rb") as f:
                key = serialization.load_pem_private_key(f.read(), password=None)
            self._add(key)  # type: ignore[arg-type]
        else:
            self.rotate()

    def _add(self, key: rsa.RSAPrivateKey) -> str:
        der = key.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        kid = hashlib.sha256(der).hexdigest()[:16]
        self.keys.append((kid, key))
        self.keys = self.keys[-3:]  # keep the last 3 public keys for verification
        return kid

    def rotate(self) -> str:
        return self._add(rsa.generate_private_key(public_exponent=65537, key_size=2048))

    @property
    def current(self) -> tuple[str, rsa.RSAPrivateKey]:
        return self.keys[-1]

    def jwks(self) -> dict:
        out = []
        for kid, key in self.keys:
            jwk = json.loads(RSAAlgorithm.to_jwk(key.public_key()))
            jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
            out.append(jwk)
        return {"keys": out}

    def sign(self, claims: dict) -> str:
        kid, key = self.current
        return jwt.encode(claims, key, algorithm="RS256", headers={"kid": kid})


keyring = KeyRing()


def issue_user_token(cred: Credential) -> dict:
    now = int(time.time())
    token = keyring.sign({
        "iss": ISSUER, "aud": AUD_PUBLIC, "sub": cred.user_id, "username": cred.username,
        "roles": cred.roles.split(","), "scope": USER_SCOPES, "token_use": "user",
        "iat": now, "exp": now + ACCESS_TTL, "jti": str(uuid4()),
    })
    return {"access_token": token, "token_type": "bearer", "expires_in": ACCESS_TTL,
            "user_id": cred.user_id, "username": cred.username}


# ------------------------------------------------------------------ events
async def on_user_event(event: CloudEvent) -> None:
    """Choreography: when a user deletes their account, revoke their credentials."""
    if event.type != EventTypes.USER_DELETED:
        return
    async with Session() as s, s.begin():
        if not await claim_event(s, Inbox, event.id, "auth"):
            return
        await s.execute(delete(Credential).where(Credential.user_id == event.data["user_id"]))


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await create_schema(engine, Base)
    await bus.start()
    await bus.subscribe(Topics.USERS, on_user_event)
    yield
    await app.state.runner.stop()
    await bus.stop()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="Auth Service",
    description="Identity provider: user login, service client-credentials, JWKS.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


# ------------------------------------------------------------------ API
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user_id: str | None = None
    username: str | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class ServiceTokenRequest(BaseModel):
    client_id: str
    client_secret: str


class ProvisionCredentials(BaseModel):
    user_id: str
    username: str = Field(min_length=3, max_length=32, pattern=r"^[A-Za-z0-9_]+$")
    email: str
    password: str = Field(min_length=8)


async def _login(username: str, password: str) -> dict:
    async with Session() as s:
        cred = (await s.execute(select(Credential).where(Credential.username == username))).scalar_one_or_none()
    if cred is None or cred.disabled or not verify_password(password, cred.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid username or password")
    return issue_user_token(cred)


@app.get("/.well-known/jwks.json", tags=["keys"])
async def jwks() -> dict:
    return keyring.jwks()


@app.post("/v1/auth/token", response_model=TokenResponse, tags=["auth"])
async def token(username: str = Form(...), password: str = Form(...)) -> dict:
    """OAuth2 password grant (form-encoded). Use this in Swagger's *Authorize* dialog."""
    return await _login(username, password)


@app.post("/v1/auth/login", response_model=TokenResponse, tags=["auth"])
async def login(body: LoginRequest) -> dict:
    """Same as /token but JSON, convenient for SPAs, GraphQL clients and AI agents."""
    return await _login(body.username, body.password)


@app.post("/v1/auth/service-token", response_model=TokenResponse, tags=["auth"])
async def service_token(body: ServiceTokenRequest) -> dict:
    """OAuth2 client-credentials grant for workloads (service-to-service identity)."""
    expected = _client_secrets().get(body.client_id)
    if expected is None or not hmac.compare_digest(expected, body.client_secret):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid client credentials")
    now = int(time.time())
    tok = keyring.sign({
        "iss": ISSUER, "aud": AUD_INTERNAL, "sub": f"svc:{body.client_id}",
        "scope": " ".join(SERVICE_CLIENT_SCOPES[body.client_id]), "token_use": "service",
        "iat": now, "exp": now + SERVICE_TTL,
    })
    return {"access_token": tok, "token_type": "bearer", "expires_in": SERVICE_TTL}


@app.get("/v1/auth/me", tags=["auth"])
async def me(user: Principal = Depends(require_user)) -> dict:
    return {"user_id": user.sub, "username": user.username, "roles": user.roles, "scopes": sorted(user.scopes)}


@app.post("/internal/keys/rotate", tags=["internal"])
async def rotate_keys(_: Principal = Depends(require_service("auth:provision"))) -> dict:
    return {"kid": keyring.rotate()}


# ---- saga participant endpoints (called by saga-orchestrator) -------------
@app.post("/internal/credentials", status_code=201, tags=["internal"])
async def create_credentials(
    body: ProvisionCredentials, _: Principal = Depends(require_service("auth:provision"))
) -> dict:
    async with Session() as s, s.begin():
        existing = await s.get(Credential, body.user_id)
        if existing is not None:  # idempotent retry of the same saga step
            return {"user_id": existing.user_id, "username": existing.username}
        clash = (await s.execute(select(Credential).where(Credential.username == body.username))).scalar_one_or_none()
        if clash:
            raise HTTPException(status.HTTP_409_CONFLICT, "username already taken")
        s.add(Credential(user_id=body.user_id, username=body.username, email=body.email,
                         password_hash=hash_password(body.password)))
    return {"user_id": body.user_id, "username": body.username}


@app.delete("/internal/credentials/{user_id}", status_code=204, tags=["internal"])
async def delete_credentials(user_id: str, _: Principal = Depends(require_service("auth:provision"))) -> None:
    """Compensating action of the registration saga."""
    async with Session() as s, s.begin():
        await s.execute(delete(Credential).where(Credential.user_id == user_id))
