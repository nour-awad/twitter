"""Observability (Week 11): the three pillars, wired the same way in every service.

* Metrics  - Prometheus RED metrics (Rate, Errors, Duration) at /metrics.
* Traces   - OpenTelemetry, exported via OTLP to Jaeger/Tempo. Trace context is
             propagated over HTTP (W3C traceparent), gRPC metadata *and* inside
             CloudEvents, so a trace survives asynchronous hops through Kafka.
* Logs     - structured JSON with trace_id / span_id / correlation_id so logs
             can be joined with traces.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import sys
import time
import uuid
from typing import Any, Iterator

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

_correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("correlation_id", default=None)
_service_name: contextvars.ContextVar[str] = contextvars.ContextVar("service_name", default="unknown")

# ------------------------------------------------------------------ metrics
# Defined once per process and labelled by `service`, so several services can
# share a process in the local dev stack without metric-name collisions.
HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests", ["service", "method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
EVENTS_PUBLISHED = Counter("events_published_total", "Events published", ["service", "topic", "type"])
EVENTS_CONSUMED = Counter(
    "events_consumed_total", "Events consumed", ["service", "topic", "type", "outcome"]
)
CIRCUIT_STATE = Gauge(
    "circuit_breaker_state", "0=closed 1=half-open 2=open", ["service", "breaker"]
)
OUTBOX_PENDING = Gauge("outbox_pending_messages", "Unpublished outbox rows", ["service"])
SAGAS = Counter("sagas_total", "Saga executions", ["saga", "status"])
ADAPTATION_MODE = Gauge("adaptation_mode", "0=normal 1=degraded 2=critical", ["service"])


# ------------------------------------------------------------------ logging
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "service": _service_name.get(),
            "correlation_id": _correlation_id.get(),
        }
        trace_id, span_id = _current_trace_ids()
        if trace_id:
            payload["trace_id"] = trace_id
            payload["span_id"] = span_id
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_logging_configured = False


def configure_logging() -> None:
    global _logging_configured
    if _logging_configured:
        return
    handler = logging.StreamHandler(sys.stdout)
    if os.getenv("LOG_FORMAT", "json") == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    for noisy in ("aiokafka", "httpx", "uvicorn.access", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _logging_configured = True


# ------------------------------------------------------------------ tracing
_tracing_ready = False
_tracing_enabled = False


def _current_trace_ids() -> tuple[str | None, str | None]:
    try:
        from opentelemetry import trace

        ctx = trace.get_current_span().get_span_context()
        if ctx and ctx.is_valid:
            return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")
    except Exception:  # pragma: no cover - otel optional at runtime
        pass
    return None, None


def setup_tracing(service_name: str) -> None:
    """Enable OpenTelemetry when OTEL_EXPORTER_OTLP_ENDPOINT is configured."""
    global _tracing_ready, _tracing_enabled
    if _tracing_ready:
        return
    _tracing_ready = True
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorClient, GrpcAioInstrumentorServer
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create(
            {"service.name": os.getenv("OTEL_SERVICE_NAME", service_name), "service.namespace": "twitter"}
        )
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    HTTPXClientInstrumentor().instrument()
    GrpcAioInstrumentorClient().instrument()
    GrpcAioInstrumentorServer().instrument()
    _tracing_enabled = True


def current_traceparent() -> str | None:
    if not _tracing_enabled:
        return None
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    return carrier.get("traceparent")


@contextlib.contextmanager
def consumer_span(service: str, topic: str, event: Any) -> Iterator[None]:
    """Continue the producer's trace when consuming an event from the broker."""
    token = _correlation_id.set(getattr(event, "correlationid", None))
    try:
        if not _tracing_enabled:
            yield
            return
        from opentelemetry import trace
        from opentelemetry.trace import SpanKind
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        ctx = TraceContextTextMapPropagator().extract(
            {"traceparent": event.traceparent} if event.traceparent else {}
        )
        tracer = trace.get_tracer(service)
        with tracer.start_as_current_span(
            f"consume {event.type}",
            context=ctx,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": topic,
                "messaging.message.id": event.id,
                "cloudevents.event_type": event.type,
            },
        ):
            yield
    finally:
        _correlation_id.reset(token)


def current_correlation_id() -> str | None:
    return _correlation_id.get()


# --------------------------------------------------------------- middleware
def setup_observability(app: FastAPI, service_name: str) -> None:
    configure_logging()
    setup_tracing(service_name)
    if _tracing_enabled:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app, excluded_urls="health,metrics")

    @app.middleware("http")
    async def _observe(request: Request, call_next):  # type: ignore[no-untyped-def]
        cid = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        tok_c = _correlation_id.set(cid)
        tok_s = _service_name.set(service_name)
        start = time.perf_counter()
        status = 500
        try:
            response: Response = await call_next(request)
            status = response.status_code
            response.headers["x-correlation-id"] = cid
            response.headers["x-served-by"] = service_name
            return response
        finally:
            route = request.scope.get("route")
            path = getattr(route, "path", None) or "unmatched"
            if path not in ("/metrics", "/health/live", "/health/ready"):
                elapsed = time.perf_counter() - start
                HTTP_REQUESTS.labels(service_name, request.method, path, str(status)).inc()
                HTTP_LATENCY.labels(service_name, request.method, path).observe(elapsed)
            _correlation_id.reset(tok_c)
            _service_name.reset(tok_s)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def outgoing_headers() -> dict[str, str]:
    """Headers to propagate on outgoing service calls."""
    cid = _correlation_id.get()
    return {"x-correlation-id": cid} if cid else {}
