"""Upstream registry + a resilient streaming reverse proxy."""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

from twitter_common import config
from twitter_common.observability import outgoing_headers
from twitter_common.resilience import CircuitBreaker, resilience_mode

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade", "host", "content-length"}


class Upstream:
    def __init__(self, name: str, base_url: str, timeout: float = 10.0) -> None:
        self.name = name
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=httpx.Timeout(timeout, connect=2.0))
        self.breaker = CircuitBreaker(name, service="api-gateway", failure_threshold=5, recovery_timeout=10)

    async def json(self, method: str, path: str, token: str | None = None, **kw: Any) -> Any:
        """Non-streaming helper used by the BFF and GraphQL resolvers."""
        headers = {**outgoing_headers(), **({"authorization": f"Bearer {token}"} if token else {})}
        if not self.breaker.allow():
            raise UpstreamError(self.name, 503, "circuit open")
        try:
            resp = await self.client.request(method, path, headers=headers, **kw)
        except httpx.TransportError as exc:
            self.breaker.record_failure()
            raise UpstreamError(self.name, 503, str(exc)) from exc
        if resp.status_code >= 500:
            self.breaker.record_failure()
        else:
            self.breaker.record_success()
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except ValueError:
                detail = resp.text
            raise UpstreamError(self.name, resp.status_code, str(detail))
        return resp.json() if resp.content else None


class UpstreamError(Exception):
    def __init__(self, upstream: str, status: int, detail: str) -> None:
        super().__init__(f"{upstream}: {status} {detail}")
        self.upstream, self.status, self.detail = upstream, status, detail


def build_upstreams() -> dict[str, Upstream]:
    return {
        "auth": Upstream("auth", config.AUTH_URL()),
        "user": Upstream("user", config.USER_URL()),
        "tweet": Upstream("tweet", config.TWEET_URL()),
        "timeline": Upstream("timeline", config.TIMELINE_URL()),
        "engagement": Upstream("engagement", config.ENGAGEMENT_URL()),
        "notification": Upstream("notification", config.NOTIFICATION_URL(), timeout=3600),  # SSE
        "search": Upstream("search", config.SEARCH_URL()),
        "saga": Upstream("saga", config.SAGA_URL(), timeout=30),
        "aiops": Upstream("aiops", config.AIOPS_URL()),
        "ai": Upstream("ai", config.AI_AGENT_URL(), timeout=300),
    }


async def proxy(upstream: Upstream, request: Request, path: str) -> Response:
    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
    headers.update(outgoing_headers())
    headers["x-forwarded-for"] = request.client.host if request.client else "unknown"
    headers["x-forwarded-prefix"] = "/api"
    body = await request.body()

    if not upstream.breaker.allow():
        return JSONResponse({"error": "circuit_open", "detail": f"{upstream.name} is unavailable, failing fast"},
                            status_code=503, headers={"Retry-After": str(int(upstream.breaker.recovery_timeout))})

    attempts = 2 if request.method in ("GET", "HEAD") and resilience_mode() == "app" else 1
    resp: httpx.Response | None = None
    for attempt in range(attempts):
        req = upstream.client.build_request(request.method, path, headers=headers, content=body,
                                            params=request.query_params)
        try:
            resp = await upstream.client.send(req, stream=True)
            break
        except httpx.TransportError as exc:
            if attempt == attempts - 1:
                upstream.breaker.record_failure()
                return JSONResponse({"error": "bad_gateway", "detail": f"{upstream.name}: {exc!s}"}, status_code=502)
    assert resp is not None
    if resp.status_code >= 500:
        upstream.breaker.record_failure()
    else:
        upstream.breaker.record_success()
    out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}
    out_headers["x-upstream"] = upstream.name
    return StreamingResponse(resp.aiter_raw(), status_code=resp.status_code, headers=out_headers,
                             background=BackgroundTask(resp.aclose))
