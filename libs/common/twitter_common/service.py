"""Service chassis: every microservice is built the same way.

(The "Microservice Chassis" pattern - Week 2.) Gives each service:
  * /health/live and /health/ready probes (Kubernetes self-healing, Week 10)
  * /metrics + tracing + JSON logs (Week 11)
  * opt-in fault injection for chaos experiments (Week 10)
  * supervised background tasks (outbox relay, consumers, control loops)
  * a uniform error envelope
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Awaitable, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from twitter_common.config import env_bool, env_float
from twitter_common.observability import setup_observability
from twitter_common.resilience import CircuitBreaker, DependencyUnavailable, supervise

log = logging.getLogger(__name__)

ReadyCheck = Callable[[], Awaitable[None]]


class ChaosConfig(BaseModel):
    error_rate: float = 0.0  # 0..1 fraction of requests answered with 503
    latency_ms: int = 0  # added latency per request


class BackgroundRunner:
    def __init__(self) -> None:
        self.tasks: list[asyncio.Task[None]] = []

    def spawn(self, name: str, factory: Callable[[], Awaitable[None]]) -> None:
        self.tasks.append(asyncio.create_task(supervise(name, factory), name=name))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()


def create_service_app(
    *,
    name: str,
    title: str,
    description: str,
    lifespan: Any = None,
    ready_checks: dict[str, ReadyCheck] | None = None,
    version: str = "1.0.0",
) -> FastAPI:
    app = FastAPI(title=title, description=description, version=version, lifespan=lifespan)
    app.state.service_name = name
    app.state.ready_checks = ready_checks or {}
    app.state.chaos = ChaosConfig(
        error_rate=env_float("CHAOS_ERROR_RATE", 0.0), latency_ms=int(env_float("CHAOS_LATENCY_MS", 0))
    )
    app.state.runner = BackgroundRunner()

    @app.middleware("http")
    async def _chaos(request: Request, call_next):  # type: ignore[no-untyped-def]
        chaos: ChaosConfig = app.state.chaos
        path = request.url.path
        if not (path.startswith("/health") or path.startswith("/metrics") or path.startswith("/internal/chaos")):
            if chaos.latency_ms:
                await asyncio.sleep(chaos.latency_ms / 1000)
            if chaos.error_rate and random.random() < chaos.error_rate:
                return JSONResponse({"error": "chaos: injected failure"}, status_code=503)
        return await call_next(request)

    setup_observability(app, name)  # outermost middleware -> also measures chaos

    @app.get("/health/live", tags=["ops"])
    async def live() -> dict[str, str]:
        """Liveness: is the process able to serve at all? (failure => restart pod)"""
        return {"status": "alive", "service": name}

    @app.get("/health/ready", tags=["ops"])
    async def ready() -> JSONResponse:
        """Readiness: are dependencies usable? (failure => stop routing traffic here)"""
        results: dict[str, str] = {}
        ok = True
        for check_name, check in app.state.ready_checks.items():
            try:
                await asyncio.wait_for(check(), timeout=2)
                results[check_name] = "ok"
            except Exception as exc:  # noqa: BLE001
                ok = False
                results[check_name] = f"fail: {exc}"
        breakers = {
            k.split(":", 1)[1]: b.snapshot()["state"]
            for k, b in CircuitBreaker.registry.items()
            if k.startswith(f"{name}:")
        }
        return JSONResponse(
            {"status": "ready" if ok else "not_ready", "checks": results, "circuit_breakers": breakers},
            status_code=200 if ok else 503,
        )

    if env_bool("CHAOS_ENABLED", True):

        @app.get("/internal/chaos", tags=["ops"])
        async def get_chaos() -> ChaosConfig:
            return app.state.chaos

        @app.put("/internal/chaos", tags=["ops"])
        async def set_chaos(cfg: ChaosConfig) -> ChaosConfig:
            """Fault injection for chaos experiments (disable with CHAOS_ENABLED=false)."""
            app.state.chaos = cfg
            log.warning("chaos config changed: %s", cfg)
            return cfg

    @app.exception_handler(DependencyUnavailable)
    async def _dep_unavailable(_: Request, exc: DependencyUnavailable) -> JSONResponse:
        return JSONResponse({"error": "dependency_unavailable", "detail": str(exc)}, status_code=503,
                            headers={"Retry-After": "5"})

    @app.exception_handler(HTTPException)
    async def _http_exc(_: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse({"error": exc.detail, "detail": exc.detail}, status_code=exc.status_code,
                            headers=getattr(exc, "headers", None))

    return app
