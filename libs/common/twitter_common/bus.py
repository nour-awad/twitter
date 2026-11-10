"""Pluggable event bus (Weeks 4 & 9).

Three interchangeable implementations behind one interface:

* InMemoryEventBus - single-process dev stack & tests (no broker needed).
* KafkaEventBus    - production: durable, partitioned, replayable log.
* DaprEventBus     - cloud-native: the Dapr sidecar abstracts the broker
                     (Kafka, RabbitMQ, Azure Service Bus, ...) entirely.

Delivery semantics are *at-least-once* in every implementation, so all
consumers must be idempotent (see twitter_common.outbox.claim_event).
Poison messages are retried with exponential backoff and then parked in a
dead-letter queue instead of blocking the partition (Week 10).
"""

from __future__ import annotations

import abc
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import httpx
from fastapi import FastAPI, Request

from twitter_common.config import env, env_int
from twitter_common.events import CloudEvent
from twitter_common.observability import EVENTS_CONSUMED, EVENTS_PUBLISHED, consumer_span

log = logging.getLogger(__name__)

Handler = Callable[[CloudEvent], Awaitable[None]]

MAX_DELIVERY_ATTEMPTS = env_int("EVENT_MAX_ATTEMPTS", 5)


async def deliver_with_retry(service: str, topic: str, handler: Handler, event: CloudEvent) -> bool:
    """Run a handler with exponential backoff. Returns False if it must be dead-lettered."""
    for attempt in range(1, MAX_DELIVERY_ATTEMPTS + 1):
        try:
            with consumer_span(service, topic, event):
                await handler(event)
            EVENTS_CONSUMED.labels(service, topic, event.type, "ok").inc()
            return True
        except Exception:
            log.exception("handler failed for %s (attempt %d/%d)", event.type, attempt, MAX_DELIVERY_ATTEMPTS)
            EVENTS_CONSUMED.labels(service, topic, event.type, "retry").inc()
            await asyncio.sleep(min(0.05 * 2**attempt, 2.0))
    EVENTS_CONSUMED.labels(service, topic, event.type, "dead_lettered").inc()
    return False


class EventBus(abc.ABC):
    def __init__(self, service: str) -> None:
        self.service = service

    async def start(self) -> None:  # noqa: B027
        pass

    async def stop(self) -> None:  # noqa: B027
        pass

    @abc.abstractmethod
    async def publish(self, topic: str, event: CloudEvent, key: str | None = None) -> None: ...

    @abc.abstractmethod
    async def subscribe(self, topic: str, handler: Handler, *, replay: bool = False) -> None:
        """Subscribe this service's consumer group to `topic`.

        replay=True re-reads the topic from the beginning on every start --
        used by projections that are rebuilt from the log (e.g. search index).
        """


# ======================================================================
# In-memory bus
# ======================================================================
@dataclass
class _Subscription:
    service: str
    topic: str
    handler: Handler
    queue: asyncio.Queue[CloudEvent] = field(default_factory=asyncio.Queue)
    task: asyncio.Task[None] | None = None


class _MemoryBroker:
    """Process-wide broker shared by every service in the dev stack."""

    def __init__(self) -> None:
        self.log: dict[str, list[CloudEvent]] = defaultdict(list)
        self.subs: list[_Subscription] = []
        self.dead_letters: list[tuple[str, CloudEvent]] = []

    async def _worker(self, sub: _Subscription) -> None:
        # One worker per (topic, consumer-group) => ordered processing, like a partition.
        while True:
            event = await sub.queue.get()
            try:
                ok = await deliver_with_retry(sub.service, sub.topic, sub.handler, event)
                if not ok:
                    self.dead_letters.append((sub.topic, event))
            finally:
                sub.queue.task_done()

    async def drain(self, timeout: float = 10.0) -> None:
        """Wait until every queued event has been processed (tests)."""

        async def _all() -> None:
            while any(not s.queue.empty() or s.queue._unfinished_tasks for s in self.subs):  # type: ignore[attr-defined]
                await asyncio.sleep(0.01)

        await asyncio.wait_for(_all(), timeout)

    def reset(self) -> None:
        for s in self.subs:
            if s.task:
                s.task.cancel()
        self.__init__()  # type: ignore[misc]


BROKER = _MemoryBroker()


class InMemoryEventBus(EventBus):
    async def publish(self, topic: str, event: CloudEvent, key: str | None = None) -> None:
        BROKER.log[topic].append(event)
        EVENTS_PUBLISHED.labels(self.service, topic, event.type).inc()
        for sub in BROKER.subs:
            if sub.topic == topic:
                sub.queue.put_nowait(event)

    async def subscribe(self, topic: str, handler: Handler, *, replay: bool = False) -> None:
        sub = _Subscription(self.service, topic, handler)
        if replay:
            for event in BROKER.log[topic]:
                sub.queue.put_nowait(event)
        sub.task = asyncio.create_task(BROKER._worker(sub), name=f"{self.service}:{topic}")
        BROKER.subs.append(sub)

    async def stop(self) -> None:
        for sub in [s for s in BROKER.subs if s.service == self.service]:
            if sub.task:
                sub.task.cancel()
            BROKER.subs.remove(sub)


# ======================================================================
# Kafka bus
# ======================================================================
class KafkaEventBus(EventBus):
    def __init__(self, service: str, bootstrap: str) -> None:
        super().__init__(service)
        self.bootstrap = bootstrap
        self._producer = None
        self._consumers: list[object] = []
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            client_id=self.service,
            enable_idempotence=True,  # no duplicates on producer retries
            acks="all",
        )
        # Brokers may still be booting (docker compose / k8s): retry instead of crashing.
        for attempt in range(30):
            try:
                await self._producer.start()
                return
            except Exception:
                log.warning("kafka not reachable yet (attempt %d)", attempt + 1)
                await asyncio.sleep(2)
        raise RuntimeError("Kafka unavailable")

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for c in self._consumers:
            await c.stop()  # type: ignore[attr-defined]
        if self._producer:
            await self._producer.stop()

    async def publish(self, topic: str, event: CloudEvent, key: str | None = None) -> None:
        assert self._producer is not None, "bus not started"
        await self._producer.send_and_wait(
            topic,
            value=event.model_dump_json().encode(),
            key=key.encode() if key else None,
            headers=[("ce_type", event.type.encode()), ("ce_id", event.id.encode())],
        )
        EVENTS_PUBLISHED.labels(self.service, topic, event.type).inc()

    async def subscribe(self, topic: str, handler: Handler, *, replay: bool = False) -> None:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap,
            # replay -> anonymous group, always read from the beginning of the log
            group_id=None if replay else self.service,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        self._consumers.append(consumer)

        async def loop() -> None:
            async for msg in consumer:
                event = CloudEvent.model_validate_json(msg.value)
                ok = await deliver_with_retry(self.service, topic, handler, event)
                if not ok:
                    await self.publish(f"{topic}.dlq", event, key=msg.key.decode() if msg.key else None)
                if not replay:
                    await consumer.commit()  # commit only after processing => at-least-once

        self._tasks.append(asyncio.create_task(loop(), name=f"kafka:{topic}"))


# ======================================================================
# Dapr bus
# ======================================================================
class DaprEventBus(EventBus):
    """Publish through the Dapr sidecar; Dapr pushes subscribed events to us over HTTP."""

    def __init__(self, service: str) -> None:
        super().__init__(service)
        self.pubsub = env("DAPR_PUBSUB_NAME", "pubsub")
        self.base = f"http://localhost:{env('DAPR_HTTP_PORT', '3500')}"
        self.handlers: dict[str, Handler] = {}
        self._client = httpx.AsyncClient(timeout=5)

    async def publish(self, topic: str, event: CloudEvent, key: str | None = None) -> None:
        resp = await self._client.post(
            f"{self.base}/v1.0/publish/{self.pubsub}/{topic}",
            content=event.model_dump_json(),
            headers={"content-type": "application/cloudevents+json"},
            params={"metadata.partitionKey": key} if key else None,
        )
        resp.raise_for_status()
        EVENTS_PUBLISHED.labels(self.service, topic, event.type).inc()

    async def subscribe(self, topic: str, handler: Handler, *, replay: bool = False) -> None:
        self.handlers[topic] = handler

    def mount(self, app: FastAPI) -> None:
        @app.get("/dapr/subscribe", include_in_schema=False)
        async def dapr_subscriptions() -> list[dict[str, str]]:
            return [
                {"pubsubname": self.pubsub, "topic": t, "route": f"/dapr/events/{t}"}
                for t in self.handlers
            ]

        @app.post("/dapr/events/{topic}", include_in_schema=False)
        async def dapr_event(topic: str, request: Request) -> dict[str, str]:
            event = CloudEvent.model_validate(await request.json())
            handler = self.handlers.get(topic)
            if handler is None:
                return {"status": "DROP"}
            ok = await deliver_with_retry(self.service, topic, handler, event)
            # RETRY lets Dapr's resiliency policy + dead-letter topic take over.
            return {"status": "SUCCESS" if ok else "RETRY"}


def create_event_bus(service: str) -> EventBus:
    kind = (env("EVENT_BUS", "memory") or "memory").lower()
    if kind == "kafka":
        return KafkaEventBus(service, env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "")
    if kind == "dapr":
        return DaprEventBus(service)
    return InMemoryEventBus(service)
