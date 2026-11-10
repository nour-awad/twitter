"""Resilience engineering & self-healing (Week 10).

Patterns implemented here (application-level):
  * Timeout          - never wait forever on a dependency.
  * Retry            - exponential backoff + full jitter, idempotent calls only.
  * Circuit breaker  - stop hammering a failing dependency, fail fast, probe
                       again after a cool-down (closed -> open -> half-open).
  * Bulkhead         - cap concurrent calls per dependency so one slow
                       dependency cannot exhaust all workers.
  * Supervisor       - restart crashed background loops with backoff
                       (process-level self-healing; Kubernetes restarts pods).
  * Fallback         - callers catch `DependencyUnavailable` and degrade.

RESILIENCE_MODE=mesh turns off application retries so they don't multiply
with the Istio/Linkerd retries configured in deploy/ (Week 8 trade-off).
"""

from __future__ import annotations

import asyncio
import enum
import logging
import random
import time
from typing import Any, Awaitable, Callable, TypeVar

import httpx

from twitter_common.config import env
from twitter_common.observability import CIRCUIT_STATE, outgoing_headers

log = logging.getLogger(__name__)
T = TypeVar("T")


def resilience_mode() -> str:
    return (env("RESILIENCE_MODE", "app") or "app").lower()


class DependencyUnavailable(Exception):
    """Raised when a dependency cannot be used (open circuit, bulkhead full, exhausted retries)."""


class CircuitOpenError(DependencyUnavailable):
    pass


class BulkheadFullError(DependencyUnavailable):
    pass


class CircuitState(enum.IntEnum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


class CircuitBreaker:
    registry: dict[str, "CircuitBreaker"] = {}

    def __init__(
        self,
        name: str,
        *,
        service: str = "unknown",
        failure_threshold: int = 5,
        recovery_timeout: float = 10.0,
        half_open_max_calls: int = 1,
    ) -> None:
        self.name = name
        self.service = service
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.opened_at = 0.0
        self._half_open_calls = 0
        CircuitBreaker.registry[f"{service}:{name}"] = self
        self._export()

    def _export(self) -> None:
        CIRCUIT_STATE.labels(self.service, self.name).set(int(self.state))

    def _transition(self, state: CircuitState) -> None:
        if state != self.state:
            log.warning("circuit %s: %s -> %s", self.name, self.state.name, state.name)
        self.state = state
        self._export()

    def allow(self) -> bool:
        if self.state == CircuitState.OPEN:
            if time.monotonic() - self.opened_at >= self.recovery_timeout:
                self._transition(CircuitState.HALF_OPEN)
                self._half_open_calls = 0
            else:
                return False
        if self.state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.half_open_max_calls:
                return False
            self._half_open_calls += 1
        return True

    def record_success(self) -> None:
        self.failures = 0
        self._transition(CircuitState.CLOSED)

    def record_failure(self) -> None:
        self.failures += 1
        if self.state == CircuitState.HALF_OPEN or self.failures >= self.failure_threshold:
            self.opened_at = time.monotonic()
            self._transition(CircuitState.OPEN)

    async def call(self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        if not self.allow():
            raise CircuitOpenError(f"circuit '{self.name}' is open")
        try:
            result = await fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def snapshot(self) -> dict[str, Any]:
        return {"name": self.name, "state": self.state.name, "failures": self.failures}


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    if resilience_mode() == "mesh":
        attempts = 1  # the sidecar proxy retries for us
    last: BaseException | None = None
    for attempt in range(attempts):
        try:
            return await fn()
        except CircuitOpenError:
            raise
        except retry_on as exc:  # type: ignore[misc]
            last = exc
            if attempt == attempts - 1:
                break
            # "Full jitter" backoff avoids synchronized retry storms.
            await asyncio.sleep(random.uniform(0, min(max_delay, base_delay * 2**attempt)))
    assert last is not None
    raise last


class Bulkhead:
    def __init__(self, name: str, max_concurrent: int = 20, max_wait: float = 0.5) -> None:
        self.name = name
        self._sem = asyncio.Semaphore(max_concurrent)
        self.max_wait = max_wait

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        try:
            await asyncio.wait_for(self._sem.acquire(), self.max_wait)
        except asyncio.TimeoutError as exc:
            raise BulkheadFullError(f"bulkhead '{self.name}' is full") from exc
        try:
            return await fn()
        finally:
            self._sem.release()


async def supervise(
    name: str,
    factory: Callable[[], Awaitable[None]],
    *,
    min_backoff: float = 0.5,
    max_backoff: float = 30.0,
) -> None:
    """Keep a background loop alive: restart it with exponential backoff if it crashes."""
    backoff = min_backoff
    while True:
        started = time.monotonic()
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("background task '%s' crashed; restarting in %.1fs", name, backoff)
        if time.monotonic() - started > 60:
            backoff = min_backoff  # it ran fine for a while, reset
        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)


class _RetryableHTTP(Exception):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(f"upstream returned {response.status_code}")
        self.response = response


class ResilientClient:
    """httpx client = timeout + bulkhead + circuit breaker + retry (idempotent methods only)."""

    IDEMPOTENT = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}

    def __init__(
        self,
        name: str,
        base_url: str,
        *,
        service: str = "unknown",
        timeout: float = 3.0,
        retries: int = 3,
        max_concurrent: int = 50,
        failure_threshold: int = 5,
        recovery_timeout: float = 10.0,
        token_provider: Any | None = None,
    ) -> None:
        self.name = name
        self.client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
        self.breaker = CircuitBreaker(
            name, service=service, failure_threshold=failure_threshold, recovery_timeout=recovery_timeout
        )
        self.bulkhead = Bulkhead(name, max_concurrent)
        self.retries = retries
        self.token_provider = token_provider

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        headers = {**outgoing_headers(), **(kwargs.pop("headers", None) or {})}
        if self.token_provider is not None and "authorization" not in {k.lower() for k in headers}:
            headers["authorization"] = f"Bearer {await self.token_provider.token()}"

        async def once() -> httpx.Response:
            async def send() -> httpx.Response:
                resp = await self.client.request(method, url, headers=headers, **kwargs)
                if resp.status_code >= 500:
                    raise _RetryableHTTP(resp)
                return resp

            return await self.bulkhead.call(lambda: self.breaker.call(send))

        attempts = self.retries if method.upper() in self.IDEMPOTENT else 1
        try:
            return await retry_async(
                once, attempts=attempts, retry_on=(httpx.TransportError, _RetryableHTTP)
            )
        except _RetryableHTTP as exc:
            return exc.response  # surface the upstream 5xx to the caller
        except httpx.TransportError as exc:
            raise DependencyUnavailable(f"{self.name} unreachable: {exc}") from exc

    async def get(self, url: str, **kw: Any) -> httpx.Response:
        return await self.request("GET", url, **kw)

    async def post(self, url: str, **kw: Any) -> httpx.Response:
        return await self.request("POST", url, **kw)

    async def delete(self, url: str, **kw: Any) -> httpx.Response:
        return await self.request("DELETE", url, **kw)

    async def aclose(self) -> None:
        await self.client.aclose()
