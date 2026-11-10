"""notification-service: a pure event *choreography* participant (Weeks 4 & 7).

Nobody calls this service to say "notify X". It listens to facts that
happened elsewhere (user.followed, engagement.tweet_liked, tweet.created with
@mentions, moderation.tweet_flagged) and decides on its own what to notify.
Adding a new reaction = adding a consumer; producers are unaware of it.

Real-time delivery to browsers uses Server-Sent Events.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

from fastapi import Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import Boolean, DateTime, String, Text, delete, select, update
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.bus import create_event_bus
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.config import database_url
from twitter_common.events import CloudEvent, EventTypes, Topics
from twitter_common.outbox import claim_event, make_inbox_model
from twitter_common.security import AUD_PUBLIC, Principal, get_verifier, require_scope, require_service
from twitter_common.service import create_service_app

SERVICE = "notification-service"


class Base(DeclarativeBase):
    pass


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(String(36), index=True)
    kind: Mapped[str] = mapped_column(String(32))
    actor_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    tweet_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    message: Mapped[str] = mapped_column(Text)
    read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class Username(Base):
    """Projection username -> user_id, needed to resolve @mentions locally."""

    __tablename__ = "usernames"
    username: Mapped[str] = mapped_column(String(32), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(36), index=True)


Inbox = make_inbox_model(Base)
engine = make_engine(database_url("notification"))
Session = make_session_factory(engine)
bus = create_event_bus(SERVICE)
listeners: dict[str, set[asyncio.Queue[dict]]] = defaultdict(set)


class NotificationOut(BaseModel):
    id: str
    kind: str
    actor_id: str | None
    tweet_id: str | None
    message: str
    read: bool
    created_at: datetime


def to_out(n: Notification) -> NotificationOut:
    return NotificationOut(id=n.id, kind=n.kind, actor_id=n.actor_id, tweet_id=n.tweet_id,
                           message=n.message, read=n.read, created_at=n.created_at)


def push_realtime(created: list[Notification]) -> None:
    for n in created:
        for q in listeners.get(n.user_id, ()):
            q.put_nowait(to_out(n).model_dump(mode="json"))


# ============================================================== consumers
async def _consume(event: CloudEvent, build) -> None:  # type: ignore[no-untyped-def]
    async with Session() as s:
        async with s.begin():
            if not await claim_event(s, Inbox, event.id, "notifications"):
                return
            created = await build(s, event.data)
            for n in created:
                s.add(n)
    push_realtime(created)  # only after commit


async def on_user_event(event: CloudEvent) -> None:
    async def build(s, d):  # type: ignore[no-untyped-def]
        if event.type in (EventTypes.USER_CREATED, EventTypes.USER_UPDATED):
            await s.merge(Username(username=d["username"], user_id=d["user_id"]))
        elif event.type == EventTypes.USER_DELETED:
            await s.execute(delete(Username).where(Username.user_id == d["user_id"]))
            await s.execute(delete(Notification).where(Notification.user_id == d["user_id"]))
        elif event.type == EventTypes.USER_FOLLOWED:
            who = d.get("follower_username") or "Someone"
            return [Notification(user_id=d["followee_id"], kind="follow", actor_id=d["follower_id"],
                                 message=f"@{who} followed you")]
        return []

    await _consume(event, build)


async def on_tweet_event(event: CloudEvent) -> None:
    async def build(s, d):  # type: ignore[no-untyped-def]
        if event.type != EventTypes.TWEET_CREATED or not d.get("mentions"):
            return []
        rows = (await s.execute(select(Username).where(Username.username.in_(d["mentions"])))).scalars().all()
        return [Notification(user_id=r.user_id, kind="mention", actor_id=d["author_id"], tweet_id=d["tweet_id"],
                             message=f"@{d['author_username']} mentioned you: {d['text'][:80]}")
                for r in rows if r.user_id != d["author_id"]]

    await _consume(event, build)


async def on_engagement_event(event: CloudEvent) -> None:
    async def build(s, d):  # type: ignore[no-untyped-def]
        kinds = {EventTypes.TWEET_LIKED: ("like", "liked"), EventTypes.TWEET_RETWEETED: ("retweet", "retweeted")}
        if event.type not in kinds or not d.get("tweet_author_id") or d["tweet_author_id"] == d["user_id"]:
            return []
        actor = (await s.execute(select(Username).where(Username.user_id == d["user_id"]))).scalar_one_or_none()
        kind, verb = kinds[event.type]
        name = f"@{actor.username}" if actor else "Someone"
        return [Notification(user_id=d["tweet_author_id"], kind=kind, actor_id=d["user_id"], tweet_id=d["tweet_id"],
                             message=f"{name} {verb} your tweet")]

    await _consume(event, build)


async def on_moderation_event(event: CloudEvent) -> None:
    async def build(s, d):  # type: ignore[no-untyped-def]
        return [Notification(user_id=d["author_id"], kind="moderation", tweet_id=d["tweet_id"],
                             message=f"Your tweet was flagged for review: {d['reason']}")]

    await _consume(event, build)


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await create_schema(engine, Base)
    await bus.start()
    await bus.subscribe(Topics.USERS, on_user_event)
    await bus.subscribe(Topics.TWEETS, on_tweet_event)
    await bus.subscribe(Topics.ENGAGEMENT, on_engagement_event)
    await bus.subscribe(Topics.MODERATION, on_moderation_event)
    yield
    await app.state.runner.stop()
    await bus.stop()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="Notification Service",
    description="Event-choreographed notifications with real-time Server-Sent Events.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


@app.get("/v1/notifications", response_model=list[NotificationOut], tags=["notifications"])
async def list_notifications(unread_only: bool = False, limit: int = Query(50, le=200),
                             me: Principal = Depends(require_scope("notifications:read"))) -> list[NotificationOut]:
    async with Session() as s:
        q = select(Notification).where(Notification.user_id == me.sub)
        if unread_only:
            q = q.where(Notification.read.is_(False))
        rows = (await s.execute(q.order_by(Notification.created_at.desc()).limit(limit))).scalars()
        return [to_out(n) for n in rows]


@app.post("/v1/notifications/read-all", tags=["notifications"])
async def read_all(me: Principal = Depends(require_scope("notifications:read"))) -> dict:
    async with Session() as s, s.begin():
        res = await s.execute(update(Notification).where(Notification.user_id == me.sub, Notification.read.is_(False))
                              .values(read=True))
    return {"marked_read": res.rowcount}


@app.get("/v1/notifications/stream", tags=["notifications"])
async def stream(request: Request, access_token: str = Query(..., description="EventSource cannot send headers")):
    """Server-Sent Events stream of new notifications."""
    try:
        me = await get_verifier().verify(access_token, AUD_PUBLIC)
    except Exception as exc:
        raise HTTPException(401, "invalid token") from exc
    queue: asyncio.Queue[dict] = asyncio.Queue()
    listeners[me.sub].add(queue)

    async def gen():  # type: ignore[no-untyped-def]
        try:
            yield "retry: 3000\n\n"
            while not await request.is_disconnected():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"event: notification\ndata: {json.dumps(item)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            listeners[me.sub].discard(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---- saga participant endpoint --------------------------------------------
class Welcome(BaseModel):
    user_id: str
    username: str


@app.post("/internal/notifications/welcome", status_code=201, tags=["internal"])
async def welcome(body: Welcome, _: Principal = Depends(require_service("notifications:send"))) -> dict:
    n = Notification(id=f"welcome-{body.user_id}", user_id=body.user_id, kind="welcome",
                     message=f"Welcome to Twitter, @{body.username}! 🎉")
    async with Session() as s, s.begin():
        if await s.get(Notification, n.id) is None:  # idempotent
            s.add(n)
    return {"id": n.id}
