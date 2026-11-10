"""Twelve-factor configuration: everything comes from environment variables."""

from __future__ import annotations

import os
from pathlib import Path


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def service_url(name: str, default_port: int) -> str:
    """Base URL of another service, e.g. service_url("USER", 8002)."""
    return os.getenv(f"{name}_SERVICE_URL", f"http://localhost:{default_port}")


def database_url(service: str) -> str:
    """Database-per-service: each service owns its own schema/database.

    `TWEET_DATABASE_URL=postgresql+asyncpg://...` in production,
    a private SQLite file for local development.
    """
    key = service.upper().replace("-", "_").replace("_SERVICE", "") + "_DATABASE_URL"
    url = os.getenv(key) or os.getenv("DATABASE_URL")
    if url:
        return url
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite+aiosqlite:///{data_dir / (service + '.db')}"


# Well-known service locations (overridden by Docker/Kubernetes DNS names).
AUTH_URL = lambda: service_url("AUTH", 8001)  # noqa: E731
USER_URL = lambda: service_url("USER", 8002)  # noqa: E731
TWEET_URL = lambda: service_url("TWEET", 8003)  # noqa: E731
TIMELINE_URL = lambda: service_url("TIMELINE", 8004)  # noqa: E731
ENGAGEMENT_URL = lambda: service_url("ENGAGEMENT", 8005)  # noqa: E731
NOTIFICATION_URL = lambda: service_url("NOTIFICATION", 8006)  # noqa: E731
SEARCH_URL = lambda: service_url("SEARCH", 8007)  # noqa: E731
SAGA_URL = lambda: service_url("SAGA", 8008)  # noqa: E731
AIOPS_URL = lambda: service_url("AIOPS", 8009)  # noqa: E731
AI_AGENT_URL = lambda: service_url("AI_AGENT", 8010)  # noqa: E731
GATEWAY_URL = lambda: service_url("GATEWAY", 8000)  # noqa: E731
USER_GRPC_TARGET = lambda: os.getenv("USER_GRPC_TARGET", "localhost:50051")  # noqa: E731
