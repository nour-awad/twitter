#!/usr/bin/env python
"""Run the WHOLE microservice system in one process -- no Docker, no Kafka.

Every service still runs as its own FastAPI app on its own port (and the
user-service gRPC server on its own port) and they still talk over real
HTTP/gRPC. Only the broker is replaced by the in-memory event bus and each
service gets a private SQLite database under ./data.

    python scripts/devstack.py            # gateway on http://localhost:8000
    DEVSTACK_BASE_PORT=18000 python scripts/devstack.py

For the production-like topology (Kafka, Postgres, Jaeger, Prometheus,
Grafana, one container per service) use `docker compose up --build`.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (name, module:attr, port offset, env var used by other services to find it)
SERVICES = [
    ("auth-service", "auth_service.main:app", 1, "AUTH"),
    ("user-service", "user_service.main:app", 2, "USER"),
    ("tweet-service", "tweet_service.main:app", 3, "TWEET"),
    ("timeline-service", "timeline_service.main:app", 4, "TIMELINE"),
    ("engagement-service", "engagement_service.main:app", 5, "ENGAGEMENT"),
    ("notification-service", "notification_service.main:app", 6, "NOTIFICATION"),
    ("search-service", "search_service.main:app", 7, "SEARCH"),
    ("saga-orchestrator", "saga_orchestrator.main:app", 8, "SAGA"),
    ("aiops-service", "aiops_service.main:app", 9, "AIOPS"),
    ("ai-agent-service", "ai_agent_service.main:app", 10, "AI_AGENT"),
    ("content-filter", "handler:app", 11, "CONTENT_FILTER"),
    ("api-gateway", "api_gateway.main:app", 0, "GATEWAY"),
]


def configure(base_port: int, data_dir: str | None = None) -> None:
    """Set env vars BEFORE importing services (their settings are read at import time)."""
    sys.path.insert(0, str(ROOT / "libs" / "common"))
    sys.path.insert(0, str(ROOT / "functions" / "content-filter"))
    for svc_dir in (ROOT / "services").iterdir():
        sys.path.insert(0, str(svc_dir))
    os.environ.setdefault("EVENT_BUS", "memory")
    os.environ.setdefault("DATA_DIR", data_dir or str(ROOT / "data"))
    os.environ.setdefault("LOG_FORMAT", "text")
    os.environ.setdefault("AIOPS_INTERVAL_SECONDS", "10")
    for _name, _, offset, key in SERVICES:
        os.environ.setdefault(f"{key}_SERVICE_URL", f"http://127.0.0.1:{base_port + offset}")
    os.environ.setdefault("CONTENT_FILTER_URL", f"http://127.0.0.1:{base_port + 11}")
    os.environ.setdefault("USER_GRPC_PORT", str(base_port + 51))
    os.environ.setdefault("USER_GRPC_TARGET", f"127.0.0.1:{base_port + 51}")


def build_servers(base_port: int) -> list:
    import uvicorn

    servers = []
    for name, target, offset, _ in SERVICES:
        module, attr = target.split(":")
        app = getattr(importlib.import_module(module), attr)
        cfg = uvicorn.Config(app, host="127.0.0.1", port=base_port + offset, log_level="warning", lifespan="on")
        server = uvicorn.Server(cfg)
        server.capture_signals = contextlib.nullcontext  # type: ignore[method-assign]  # we handle signals
        server.install_signal_handlers = lambda: None  # type: ignore[method-assign]  # older uvicorn
        servers.append((name, base_port + offset, server))
    return servers


async def main() -> None:
    base = int(os.getenv("DEVSTACK_BASE_PORT", "8000"))
    configure(base)
    servers = build_servers(base)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: [setattr(s, "should_exit", True) for _, _, s in servers])
    print("\nTwitter microservices dev stack")
    for name, port, _ in servers:
        print(f"  {name:<22} http://localhost:{port}/docs")
    print(f"  {'user-service (gRPC)':<22} localhost:{os.environ['USER_GRPC_PORT']}")
    print(f"\n  REST  -> http://localhost:{base}/docs\n  GraphQL -> http://localhost:{base}/graphql"
          f"\n  MCP   -> http://localhost:{base}/mcp\n")
    await asyncio.gather(*(s.serve() for _, _, s in servers))


if __name__ == "__main__":
    asyncio.run(main())
