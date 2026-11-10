"""Self-adaptive gateway: a MAPE-K control loop (Week 13).

  Monitor  - sliding window of request latencies and status codes.
  Analyze  - compare p95 latency / error rate with the SLO.
  Plan     - choose an operating mode: NORMAL, DEGRADED, CRITICAL.
  Execute  - re-tune the rate limiter and toggle features:
               DEGRADED -> skip optional enrichment (brownout), 50% rate limit
               CRITICAL -> also shed non-essential routes (search, AI), 25% rate limit
  Knowledge- SLO targets + decision history (exposed at /api/v1/adaptation).

Hysteresis (several healthy windows before stepping down) prevents flapping.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from twitter_common.config import env_float, env_int
from twitter_common.observability import ADAPTATION_MODE

MODES = ["NORMAL", "DEGRADED", "CRITICAL"]


class TokenBucket:
    def __init__(self, rate: float, capacity: float) -> None:
        self.rate, self.capacity = rate, capacity
        self.tokens, self.updated = capacity, time.monotonic()

    def take(self, rate: float, capacity: float) -> bool:
        now = time.monotonic()
        self.rate, self.capacity = rate, capacity
        self.tokens = min(capacity, self.tokens + (now - self.updated) * rate)
        self.updated = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class RateLimiter:
    """Per-client token bucket whose limits are adjusted by the controller."""

    def __init__(self, rate: float, burst: float) -> None:
        self.base_rate, self.base_burst = rate, burst
        self.factor = 1.0
        self.buckets: dict[str, TokenBucket] = {}

    def allow(self, key: str) -> bool:
        rate, burst = self.base_rate * self.factor, max(1.0, self.base_burst * self.factor)
        bucket = self.buckets.setdefault(key, TokenBucket(rate, burst))
        return bucket.take(rate, burst)


@dataclass
class Sample:
    at: float
    latency: float
    error: bool


@dataclass
class AdaptiveController:
    limiter: RateLimiter
    slo_p95_ms: float = field(default_factory=lambda: env_float("SLO_P95_MS", 800))
    slo_error_rate: float = field(default_factory=lambda: env_float("SLO_ERROR_RATE", 0.10))
    window_s: float = field(default_factory=lambda: env_float("ADAPT_WINDOW_SECONDS", 30))
    interval_s: float = field(default_factory=lambda: env_float("ADAPT_INTERVAL_SECONDS", 5))
    min_samples: int = field(default_factory=lambda: env_int("ADAPT_MIN_SAMPLES", 20))
    mode: str = "NORMAL"
    healthy_streak: int = 0
    samples: deque = field(default_factory=lambda: deque(maxlen=5000))
    history: deque = field(default_factory=lambda: deque(maxlen=100))
    last_analysis: dict = field(default_factory=dict)

    # ---- Monitor
    def record(self, latency_s: float, status: int) -> None:
        self.samples.append(Sample(time.monotonic(), latency_s, status >= 500))

    # ---- Analyze
    def analyze(self) -> dict:
        cutoff = time.monotonic() - self.window_s
        window = [s for s in self.samples if s.at >= cutoff]
        if len(window) < self.min_samples:
            return {"samples": len(window), "p95_ms": None, "error_rate": None, "violation": "none"}
        lat = sorted(s.latency for s in window)
        p95 = lat[int(0.95 * (len(lat) - 1))] * 1000
        err = sum(s.error for s in window) / len(window)
        severe = p95 > 2 * self.slo_p95_ms or err > 2 * self.slo_error_rate
        mild = p95 > self.slo_p95_ms or err > self.slo_error_rate
        return {"samples": len(window), "p95_ms": round(p95, 1), "error_rate": round(err, 3),
                "violation": "severe" if severe else "mild" if mild else "none"}

    # ---- Plan
    def plan(self, analysis: dict) -> str:
        idx = MODES.index(self.mode)
        if analysis["violation"] == "severe":
            self.healthy_streak = 0
            return "CRITICAL"
        if analysis["violation"] == "mild":
            self.healthy_streak = 0
            return MODES[max(idx, 1)]
        self.healthy_streak += 1
        if self.healthy_streak >= 3 and idx > 0:  # hysteresis
            self.healthy_streak = 0
            return MODES[idx - 1]
        return self.mode

    # ---- Execute
    def execute(self, new_mode: str, analysis: dict) -> None:
        self.limiter.factor = {"NORMAL": 1.0, "DEGRADED": 0.5, "CRITICAL": 0.25}[new_mode]
        ADAPTATION_MODE.labels("api-gateway").set(MODES.index(new_mode))
        if new_mode != self.mode:
            self.history.append({"at": datetime.now(timezone.utc).isoformat(), "from": self.mode,
                                 "to": new_mode, "analysis": analysis})
            self.mode = new_mode

    def tick(self) -> None:
        analysis = self.analyze()
        self.last_analysis = analysis
        self.execute(self.plan(analysis), analysis)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval_s)
            self.tick()

    # ---- feature switches used by request handlers
    @property
    def enrichment_enabled(self) -> bool:
        return self.mode == "NORMAL"

    def is_shed(self, path: str) -> bool:
        return self.mode == "CRITICAL" and any(p in path for p in ("/search", "/trends", "/ai/", "/aiops"))

    def status(self) -> dict:
        return {"mode": self.mode, "rate_limit_factor": self.limiter.factor,
                "slo": {"p95_ms": self.slo_p95_ms, "error_rate": self.slo_error_rate},
                "last_analysis": self.last_analysis, "history": list(self.history)}
