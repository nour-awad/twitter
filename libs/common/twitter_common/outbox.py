"""Transactional Outbox + Idempotent Inbox (Week 5: consistency without 2PC).

Problem ("dual write"): a service must update its database *and* publish an
event. Doing both separately is not atomic -- a crash in between either
loses the event or publishes an event for data that was never committed.

Solution:
  1. Write the business row and the event row (outbox) in the SAME local
     ACID transaction.
  2. A relay process publishes outbox rows to the broker and marks them sent.
     Crash after publish but before marking -> event re-sent (at-least-once).
  3. Consumers record processed event ids in an inbox table, in the same
     transaction as their own state change -> duplicates become no-ops.

Outbox + at-least-once + idempotent inbox = effectively-once processing.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Integer, String, Text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from twitter_common.bus import EventBus
from twitter_common.db import utcnow
from twitter_common.events import CloudEvent
from twitter_common.observability import OUTBOX_PENDING

log = logging.getLogger(__name__)


def make_outbox_model(base: Any) -> Any:
    class OutboxMessage(base):  # type: ignore[valid-type, misc]
        __tablename__ = "outbox"
        id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
        topic: Mapped[str] = mapped_column(String(128))
        key: Mapped[str | None] = mapped_column(String(128), nullable=True)
        payload: Mapped[str] = mapped_column(Text)
        created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
        published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
        attempts: Mapped[int] = mapped_column(Integer, default=0)

    return OutboxMessage


def make_inbox_model(base: Any) -> Any:
    class ProcessedEvent(base):  # type: ignore[valid-type, misc]
        __tablename__ = "processed_events"
        event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
        consumer: Mapped[str] = mapped_column(String(64), primary_key=True)
        processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    return ProcessedEvent


def add_to_outbox(session: AsyncSession, outbox_model: Any, topic: str, event: CloudEvent, key: str | None) -> None:
    """Stage an event inside the caller's transaction."""
    session.add(outbox_model(topic=topic, key=key, payload=event.model_dump_json()))


async def claim_event(session: AsyncSession, inbox_model: Any, event_id: str, consumer: str) -> bool:
    """Return True the first time an event is seen by `consumer` (idempotent consumer)."""
    if await session.get(inbox_model, (event_id, consumer)) is not None:
        return False
    session.add(inbox_model(event_id=event_id, consumer=consumer))
    try:
        await session.flush()
    except IntegrityError:  # concurrent duplicate delivery
        return False
    return True


class OutboxRelay:
    """Polls the outbox and publishes pending messages in creation order."""

    def __init__(
        self,
        service: str,
        session_factory: async_sessionmaker,
        outbox_model: Any,
        bus: EventBus,
        interval: float = 0.5,
        batch_size: int = 100,
    ) -> None:
        self.service = service
        self.session_factory = session_factory
        self.model = outbox_model
        self.bus = bus
        self.interval = interval
        self.batch_size = batch_size
        self._wake = asyncio.Event()

    def notify(self) -> None:
        """Called after a commit so events go out immediately instead of on next poll."""
        self._wake.set()

    async def run(self) -> None:
        while True:
            await self.publish_pending()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def publish_pending(self) -> int:
        sent = 0
        async with self.session_factory() as session:
            rows = (
                await session.execute(
                    select(self.model)
                    .where(self.model.published_at.is_(None))
                    .order_by(self.model.created_at)
                    .limit(self.batch_size)
                )
            ).scalars().all()
            for row in rows:
                try:
                    await self.bus.publish(row.topic, CloudEvent.model_validate_json(row.payload), key=row.key)
                    row.published_at = utcnow()
                    sent += 1
                except Exception:
                    row.attempts += 1
                    log.exception("outbox publish failed; will retry")
                    break  # preserve ordering: stop at the first failure
            await session.commit()
            OUTBOX_PENDING.labels(self.service).set(len(rows) - sent)
        return sent
