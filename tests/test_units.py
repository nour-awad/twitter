"""Unit tests of the pattern implementations (no network)."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from twitter_common.events import EventTypes, make_event, parse_data
from twitter_common.resilience import (Bulkhead, BulkheadFullError, CircuitBreaker, CircuitOpenError, CircuitState,
                                       retry_async)


# ------------------------------------------------------------ resilience
async def test_circuit_breaker_opens_and_recovers() -> None:
    cb = CircuitBreaker("t1", service="test", failure_threshold=3, recovery_timeout=0.1)

    async def boom() -> None:
        raise ValueError

    for _ in range(3):
        with pytest.raises(ValueError):
            await cb.call(boom)
    assert cb.state == CircuitState.OPEN
    with pytest.raises(CircuitOpenError):  # fail fast, dependency not even called
        await cb.call(boom)
    await asyncio.sleep(0.12)

    async def ok() -> str:
        return "ok"

    assert await cb.call(ok) == "ok"  # half-open probe succeeds
    assert cb.state == CircuitState.CLOSED


async def test_retry_eventually_succeeds() -> None:
    calls = {"n": 0}

    async def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError
        return 42

    assert await retry_async(flaky, attempts=3, base_delay=0.001) == 42
    assert calls["n"] == 3


async def test_bulkhead_rejects_when_full() -> None:
    bh = Bulkhead("b", max_concurrent=1, max_wait=0.05)

    async def slow() -> None:
        await asyncio.sleep(0.2)

    first = asyncio.create_task(bh.call(slow))
    await asyncio.sleep(0.01)
    with pytest.raises(BulkheadFullError):
        await bh.call(slow)
    await first


# ------------------------------------------------------------ contracts
def test_event_contract_validation() -> None:
    ev = make_event(EventTypes.USER_FOLLOWED, {"follower_id": "a", "followee_id": "b"}, source="test")
    assert ev.specversion == "1.0" and ev.source == "/twitter/test"
    assert parse_data(ev).followee_id == "b"  # type: ignore[union-attr]
    with pytest.raises(ValidationError):
        make_event(EventTypes.USER_FOLLOWED, {"follower_id": "a"}, source="test")  # missing field


# ------------------------------------------------------------ event sourcing
def test_event_sourced_aggregate_is_idempotent_and_replayable() -> None:
    from engagement_service.main import LIKED, TweetEngagement

    agg = TweetEngagement("t")
    for event_type, data in agg.decide("like", "u1"):
        agg.apply(event_type, data)
    assert agg.decide("like", "u1") == []  # already liked -> no new event
    assert len(agg.likes) == 1 and agg.version == 1
    replayed = TweetEngagement("t")
    replayed.apply(LIKED, {"user_id": "u1"})
    assert replayed.likes == agg.likes
    snap = TweetEngagement.from_state("t", agg.version, agg.to_state())
    assert snap.likes == {"u1"} and snap.version == 1


# ------------------------------------------------------------ self-adaptation
def test_mape_k_controller_degrades_and_recovers() -> None:
    from api_gateway.adaptation import AdaptiveController, RateLimiter

    ctl = AdaptiveController(RateLimiter(10, 10), slo_p95_ms=100, slo_error_rate=0.1, min_samples=5)
    for _ in range(20):
        ctl.record(0.5, 200)  # 500 ms >> 2 x SLO
    ctl.tick()
    assert ctl.mode == "CRITICAL" and ctl.limiter.factor == 0.25 and ctl.is_shed("/api/v1/search/tweets")
    ctl.samples.clear()
    for _ in range(20):
        ctl.record(0.01, 200)
    for _ in range(6):  # hysteresis: needs consecutive healthy windows per step
        ctl.tick()
    assert ctl.mode == "NORMAL" and len(ctl.history) >= 3


def test_adaptive_fanout_lowers_threshold_when_slow() -> None:
    from timeline_service.main import AdaptiveFanout

    f = AdaptiveFanout()
    f.threshold, f.target_ms = 1000, 10
    f.observe(500, 50)
    assert f.threshold < 1000 and f.history


# ------------------------------------------------------------ AIOps
def test_aiops_detects_spike_and_root_cause() -> None:
    from aiops_service.main import AIOpsEngine, EwmaDetector

    det = EwmaDetector()
    for x in [0.01, 0.012, 0.009, 0.011, 0.01, 0.01]:
        det.update(x)
    assert det.score(0.5) > det.threshold
    engine = AIOpsEngine(targets={})
    causes = engine.root_causes({"api-gateway", "tweet-service", "user-service"})
    assert causes["api-gateway"] == "user-service"
    assert causes["tweet-service"] == "user-service"
    assert causes["user-service"] == "user-service"

