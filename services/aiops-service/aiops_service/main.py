"""aiops-service: AIOps for the microservice fleet (Week 11).

Observability produces far more signals than humans can watch. This service
closes the loop automatically:

  1. Collect   - scrape every service's Prometheus /metrics + readiness probe.
  2. Detect    - per-service, per-metric online anomaly detection
                 (EWMA mean/variance -> z-score), plus hard availability rules.
  3. Correlate - use the service dependency graph to separate the *probable
                 root cause* from services that are merely victims
                 (an anomalous service whose own dependencies are healthy).
  4. Act       - publish aiops.anomaly_detected events and expose a health map
                 that the AI agent (MCP tool `get_system_health`) can explain.

In production you would point COLLECT at Prometheus/Tempo instead of
scraping directly; the detection/correlation logic stays the same.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
from prometheus_client.parser import text_string_to_metric_families

from twitter_common import config
from twitter_common.bus import create_event_bus
from twitter_common.config import env, env_float
from twitter_common.events import AnomalyDetected, EventTypes, Topics, make_event
from twitter_common.service import create_service_app

log = logging.getLogger(__name__)
SERVICE = "aiops-service"

# Static service topology (in production: derived from distributed traces).
DEPENDS_ON: dict[str, list[str]] = {
    "api-gateway": ["auth-service", "user-service", "tweet-service", "timeline-service", "engagement-service",
                    "notification-service", "search-service", "saga-orchestrator", "ai-agent-service"],
    "tweet-service": ["user-service", "auth-service"],
    "saga-orchestrator": ["auth-service", "user-service", "timeline-service", "notification-service"],
    "ai-agent-service": ["api-gateway"],
    "user-service": ["auth-service"],
    "timeline-service": ["auth-service"],
    "engagement-service": ["auth-service"],
    "notification-service": ["auth-service"],
    "search-service": [],
    "auth-service": [],
}

REMEDIATION = {
    "error_rate": "Check recent deploys / roll back; inspect circuit breakers and dependency health.",
    "latency_ms": "Scale out (HPA), check DB/broker saturation, verify timeouts and bulkheads.",
    "availability": "Pod failing readiness: Kubernetes stops routing traffic; check logs and restart if needed.",
}


def default_targets() -> dict[str, str]:
    raw = env("AIOPS_TARGETS")
    if raw:
        return dict(item.split("=", 1) for item in raw.split(","))
    return {"api-gateway": config.GATEWAY_URL(), "auth-service": config.AUTH_URL(), "user-service": config.USER_URL(),
            "tweet-service": config.TWEET_URL(), "timeline-service": config.TIMELINE_URL(),
            "engagement-service": config.ENGAGEMENT_URL(), "notification-service": config.NOTIFICATION_URL(),
            "search-service": config.SEARCH_URL(), "saga-orchestrator": config.SAGA_URL(),
            "ai-agent-service": config.AI_AGENT_URL()}


@dataclass
class EwmaDetector:
    """Online mean/variance estimate; flags points far outside the learned baseline."""

    alpha: float = 0.3
    warmup: int = 5
    threshold: float = 3.0
    mean: float = 0.0
    var: float = 0.0
    n: int = 0

    def score(self, x: float) -> float:
        if self.n < self.warmup:
            return 0.0
        return (x - self.mean) / math.sqrt(self.var + 1e-9)

    def update(self, x: float) -> None:
        self.n += 1
        if self.n == 1:
            self.mean = x
            return
        diff = x - self.mean
        incr = self.alpha * diff
        self.mean += incr
        self.var = (1 - self.alpha) * (self.var + diff * incr)


@dataclass
class AIOpsEngine:
    targets: dict[str, str]
    floors: dict[str, float] = field(default_factory=lambda: {"error_rate": 0.05, "latency_ms": 250.0})
    detectors: dict[tuple[str, str], EwmaDetector] = field(default_factory=dict)
    previous: dict[tuple[str, str], float] = field(default_factory=dict)
    health: dict[str, dict] = field(default_factory=dict)
    anomalies: deque = field(default_factory=lambda: deque(maxlen=200))
    cycles: int = 0

    async def collect(self) -> tuple[dict[tuple, float], dict[str, bool]]:
        samples: dict[tuple, float] = {}
        ready: dict[str, bool] = {}
        async with httpx.AsyncClient(timeout=3) as client:
            async def scrape(name: str, url: str) -> None:
                try:
                    r = await client.get(f"{url}/health/ready")
                    ready[name] = r.status_code == 200
                    m = await client.get(f"{url}/metrics")
                    for fam in text_string_to_metric_families(m.text):
                        for s in fam.samples:
                            if s.name.startswith("http_request"):
                                samples[(s.name, tuple(sorted(s.labels.items())))] = s.value
                except httpx.HTTPError:
                    ready[name] = False

            await asyncio.gather(*(scrape(n, u) for n, u in self.targets.items()))
        return samples, ready

    def _delta(self, key: tuple[str, str], value: float) -> float:
        prev = self.previous.get(key, value)
        self.previous[key] = value
        return max(0.0, value - prev)

    def compute(self, samples: dict[tuple, float]) -> dict[str, dict[str, float]]:
        totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for (name, labels), value in samples.items():
            lab = dict(labels)
            svc = lab.get("service", "unknown")
            if name == "http_requests_total":
                totals[svc]["requests"] += value
                if lab.get("status", "200").startswith("5"):
                    totals[svc]["errors"] += value
            elif name == "http_request_duration_seconds_sum":
                totals[svc]["lat_sum"] += value
            elif name == "http_request_duration_seconds_count":
                totals[svc]["lat_count"] += value
        out: dict[str, dict[str, float]] = {}
        for svc, t in totals.items():
            req = self._delta((svc, "requests"), t["requests"])
            err = self._delta((svc, "errors"), t["errors"])
            lsum = self._delta((svc, "lat_sum"), t["lat_sum"])
            lcnt = self._delta((svc, "lat_count"), t["lat_count"])
            out[svc] = {"requests": req, "error_rate": err / req if req else 0.0,
                        "latency_ms": (lsum / lcnt * 1000) if lcnt else 0.0}
        return out

    def root_causes(self, anomalous: set[str]) -> dict[str, str]:
        """A service is a root-cause candidate if none of its dependencies is anomalous."""
        result = {}
        for svc in anomalous:
            sick_deps = [d for d in DEPENDS_ON.get(svc, []) if d in anomalous]
            result[svc] = svc if not sick_deps else self._deepest(sick_deps[0], anomalous, {svc})
        return result

    def _deepest(self, svc: str, anomalous: set[str], seen: set[str]) -> str:
        for dep in DEPENDS_ON.get(svc, []):
            if dep in anomalous and dep not in seen:
                return self._deepest(dep, anomalous, seen | {svc})
        return svc

    async def cycle(self) -> list[dict]:
        samples, ready = await self.collect()
        metrics = self.compute(samples)
        self.cycles += 1
        found: list[dict] = []
        for svc in self.targets:
            m = metrics.get(svc, {"requests": 0.0, "error_rate": 0.0, "latency_ms": 0.0})
            self.health[svc] = {"ready": ready.get(svc, False), **{k: round(v, 4) for k, v in m.items()}}
            if not ready.get(svc, False):
                found.append({"service": svc, "metric": "availability", "value": 0.0, "baseline": 1.0, "zscore": 99.0})
                continue
            if m["requests"] == 0:
                continue
            for metric in ("error_rate", "latency_ms"):
                det = self.detectors.setdefault((svc, metric), EwmaDetector())
                z = det.score(m[metric])
                if z > det.threshold and m[metric] > self.floors[metric]:
                    found.append({"service": svc, "metric": metric, "value": round(m[metric], 4),
                                  "baseline": round(det.mean, 4), "zscore": round(z, 2)})
                else:
                    det.update(m[metric])  # only learn from normal behaviour
        causes = self.root_causes({a["service"] for a in found})
        now = datetime.now(timezone.utc).isoformat()
        for a in found:
            a["probable_root_cause"] = causes.get(a["service"])
            a["remediation"] = REMEDIATION[a["metric"]]
            a["detected_at"] = now
            self.anomalies.appendleft(a)
        return found


engine = AIOpsEngine(default_targets())
bus = create_event_bus(SERVICE)


async def loop() -> None:
    interval = env_float("AIOPS_INTERVAL_SECONDS", 10)
    while True:
        await asyncio.sleep(interval)  # first cycle after start-up, once the fleet is up
        for a in await engine.cycle():
            payload = AnomalyDetected(**{k: a[k] for k in ("service", "metric", "value", "baseline", "zscore",
                                                           "probable_root_cause")})
            await bus.publish(Topics.AIOPS, make_event(EventTypes.ANOMALY_DETECTED, payload, source=SERVICE,
                                                       subject=a["service"]), key=a["service"])
            log.warning("anomaly: %s", a)


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await bus.start()
    app.state.runner.spawn("aiops-loop", loop)
    yield
    await app.state.runner.stop()
    await bus.stop()


app = create_service_app(
    name=SERVICE,
    title="AIOps Service",
    description="Anomaly detection and root-cause hints computed from the fleet's own telemetry.",
    lifespan=lifespan,
)


@app.get("/v1/aiops/health", tags=["aiops"])
async def health_map() -> dict:
    return {"cycles": engine.cycles, "services": engine.health}


@app.get("/v1/aiops/anomalies", tags=["aiops"])
async def anomalies(limit: int = 50) -> list[dict]:
    return list(engine.anomalies)[:limit]


@app.get("/v1/aiops/topology", tags=["aiops"])
async def topology() -> dict:
    return DEPENDS_ON


@app.post("/v1/aiops/analyze", tags=["aiops"])
async def analyze_now() -> dict:
    """Run a detection cycle immediately (useful in demos and chaos experiments)."""
    found = await engine.cycle()
    return {"anomalies": found, "services": engine.health}
