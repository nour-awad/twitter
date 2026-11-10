"""Async SQLAlchemy helpers. Each service owns a *private* database."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_engine(url: str) -> AsyncEngine:
    kwargs: dict = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs = {"connect_args": {"timeout": 30}}
    return create_async_engine(url, **kwargs)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_schema(engine: AsyncEngine, base: type[DeclarativeBase]) -> None:
    # Schema migrations (Alembic) are out of scope for the course project;
    # create_all is idempotent and good enough for dev/demo.
    async with engine.begin() as conn:
        if engine.url.get_backend_name() == "sqlite":
            await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.run_sync(base.metadata.create_all)


def db_ready_check(engine: AsyncEngine):
    async def check() -> None:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    return check
