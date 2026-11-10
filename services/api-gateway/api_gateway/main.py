"""api-gateway: the single entry point for clients (Weeks 2, 3, 10, 12, 13).

Responsibilities (and only these -- no business logic, to avoid the
"smart gateway / ESB" anti-pattern):
  * routing          /api/v1/... -> owning microservice (streaming proxy)
  * GraphQL          /graphql composed from several services
  * BFF aggregation  /api/v1/bff/... fan-out + partial-failure tolerance
  * edge security    early JWT rejection (services still verify: zero trust)
  * rate limiting    per user/IP token buckets
  * resilience       per-upstream circuit breakers, timeouts, retries
  * self-adaptation  MAPE-K controller: brownout & load shedding
"""

from __future__ import annotations

import asyncio
import re
import time
from contextlib import asynccontextmanager

import jwt
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse

from api_gateway.adaptation import AdaptiveController, RateLimiter
from api_gateway.graphql_api import build_graphql_router
from api_gateway.upstreams import Upstream, UpstreamError, build_upstreams, proxy
from twitter_common.clients.user_grpc import UserGrpcClient
from twitter_common.config import env_float
from twitter_common.resilience import CircuitBreaker, DependencyUnavailable
from twitter_common.security import AUD_PUBLIC, ServiceTokenProvider, get_verifier
from twitter_common.service import create_service_app

SERVICE = "api-gateway"

# Ordered routing table: first match wins.
ROUTES: list[tuple[re.Pattern[str], str, str | None]] = [
    (re.compile(r"^/v1/register$"), "saga", "/v1/sagas/registration"),
    (re.compile(r"^/v1/sagas"), "saga", None),
    (re.compile(r"^/v1/auth/"), "auth", None),
    (re.compile(r"^/v1/tweets/[^/]+/(like|retweet|engagement)"), "engagement", None),
    (re.compile(r"^/v[12]/tweets"), "tweet", None),
    (re.compile(r"^/v1/users/[^/]+/tweets"), "tweet", None),
    (re.compile(r"^/v1/users"), "user", None),
    (re.compile(r"^/v1/timelines"), "timeline", None),
    (re.compile(r"^/v1/notifications"), "notification", None),
    (re.compile(r"^/v1/(search|trends)"), "search", None),
    (re.compile(r"^/v1/aiops"), "aiops", None),
    (re.compile(r"^/v1/ai/"), "ai", None),
]
PUBLIC_PREFIXES = ("/v1/auth/token", "/v1/auth/login", "/v1/register", "/v1/auth/service-token")

upstreams: dict[str, Upstream] = build_upstreams()
tokens = ServiceTokenProvider(SERVICE)
users_grpc: UserGrpcClient | None = None
limiter = RateLimiter(rate=env_float("RATE_LIMIT_RPS", 20), burst=env_float("RATE_LIMIT_BURST", 40))
controller = AdaptiveController(limiter)


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    global users_grpc
    users_grpc = UserGrpcClient(SERVICE, tokens)  # created inside the running event loop
    app.state.runner.spawn("mape-k-controller", controller.run)
    yield
    await app.state.runner.stop()
    for u in upstreams.values():
        await u.client.aclose()
    if users_grpc:
        await users_grpc.close()


app = create_service_app(
    name=SERVICE,
    title="Twitter API Gateway",
    description=(
        "Single entry point. REST under /api, GraphQL at /graphql, MCP (AI agents) at /mcp. "
        "Get a token from POST /api/v1/auth/login, then click *Authorize*."
    ),
    lifespan=lifespan,
)
app.include_router(build_graphql_router(upstreams, lambda: users_grpc),  # type: ignore[arg-type, return-value]
 prefix="/graphql", tags=["graphql"])


@app.middleware("http")
async def edge(request: Request, call_next):  # type: ignore[no-untyped-def]
    path = request.url.path
    if not (path.startswith("/api") or path.startswith("/graphql") or path.startswith("/mcp")):
        return await call_next(request)

    # 1) load shedding decided by the adaptive controller
    if controller.is_shed(path):
        return JSONResponse({"error": "shed", "detail": "temporarily disabled to protect core features"},
                            status_code=503, headers={"Retry-After": "30"})

    # 2) edge authentication (early reject; downstream services verify again)
    key = request.client.host if request.client else "anon"
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer ") and not path.removeprefix("/api").startswith(PUBLIC_PREFIXES):
        try:
            principal = await get_verifier().verify(auth[7:], AUD_PUBLIC)
            key = f"user:{principal.sub}"
        except (jwt.InvalidTokenError, Exception):  # noqa: BLE001
            return JSONResponse({"error": "invalid_token", "detail": "invalid or expired token"}, status_code=401,
                                headers={"WWW-Authenticate": "Bearer"})

    # 3) rate limiting
    if not limiter.allow(key):
        return JSONResponse({"error": "rate_limited", "detail": "too many requests"}, status_code=429,
                            headers={"Retry-After": "1"})

    start = time.perf_counter()
    response = await call_next(request)
    controller.record(time.perf_counter() - start, response.status_code)  # Monitor
    response.headers["x-adaptation-mode"] = controller.mode
    return response


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse("/docs")


@app.get("/api/v1/adaptation", tags=["self-adaptation"])
async def adaptation_status() -> dict:
    return controller.status()


@app.get("/api/v1/gateway/upstreams", tags=["ops"])
async def upstream_status() -> dict:
    return {name: {"url": u.base_url, **u.breaker.snapshot()} for name, u in upstreams.items()} | {
        "user-grpc": CircuitBreaker.registry.get(f"{SERVICE}:user-grpc").snapshot()  # type: ignore[union-attr]
        if f"{SERVICE}:user-grpc" in CircuitBreaker.registry else None}


@app.get("/api/v1/bff/tweets/{tweet_id}", tags=["bff"])
async def tweet_details(tweet_id: str, request: Request) -> dict:
    """Backend-for-Frontend aggregation: tweet + author + engagement + replies in ONE round-trip.

    Calls run in parallel; optional parts degrade to null instead of failing the
    whole page (and are skipped entirely when the controller is in brownout).
    """
    auth = request.headers.get("authorization", "")
    tok = auth[7:] if auth.lower().startswith("bearer ") else None
    try:
        tweet = await upstreams["tweet"].json("GET", f"/v2/tweets/{tweet_id}", tok)
    except UpstreamError as e:
        raise HTTPException(e.status if e.status < 500 else 503, e.detail) from e

    warnings: list[str] = []

    async def optional(coro):  # type: ignore[no-untyped-def]
        try:
            return await coro
        except (UpstreamError, DependencyUnavailable) as exc:
            warnings.append(str(exc))
            return None

    if controller.enrichment_enabled:
        engagement, replies, authors = await asyncio.gather(
            optional(upstreams["engagement"].json("GET", f"/v1/tweets/{tweet_id}/engagement")),
            optional(upstreams["tweet"].json("GET", f"/v1/tweets/{tweet_id}/replies")),
            optional(users_grpc.batch_get_users([tweet["author"]["id"]])),  # type: ignore[union-attr]
        )
    else:
        engagement = replies = authors = None
        warnings.append(f"brownout: enrichment disabled in {controller.mode} mode")
    return {
        "tweet": tweet,
        "author": (authors or {}).get(tweet["author"]["id"]),
        "engagement": engagement,
        "replies": replies,
        "warnings": warnings,
    }


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], include_in_schema=False)
async def route(path: str, request: Request):  # type: ignore[no-untyped-def]
    target = "/" + path
    for pattern, name, rewrite in ROUTES:
        if pattern.match(target):
            return await proxy(upstreams[name], request, rewrite or target)
    return JSONResponse({"error": "not_found", "detail": f"no route for {target}"}, status_code=404)


@app.api_route("/mcp", methods=["GET", "POST", "DELETE"], include_in_schema=False)
async def mcp(request: Request):  # type: ignore[no-untyped-def]
    """Model Context Protocol endpoint for AI agents (served by ai-agent-service)."""
    return await proxy(upstreams["ai"], request, "/mcp")
