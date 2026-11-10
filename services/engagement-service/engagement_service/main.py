"""engagement-service: likes & retweets, implemented with *Event Sourcing* (Week 6).

Instead of storing "current state" (a likes counter) we store the immutable
sequence of domain events per tweet (one stream per tweet):

    stream tweet-42:  v1 TweetLiked(u1)  v2 TweetLiked(u2)  v3 TweetUnliked(u1) ...

Current state is a left-fold of the stream. That gives us:
  * a full audit log (who liked what, when),
  * temporal queries ("engagement as of version N"),
  * optimistic concurrency via the (stream_id, version) primary key,
  * snapshots to keep replay cheap for hot streams.

Domain events stay private; after each append we publish an *integration
event* (engagement.*) through the outbox for the rest of the system.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime

from fastapi import Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import Boolean, DateTime, Integer, String, Text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.bus import create_event_bus
from twitter_common.config import database_url, env_int
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.events import CloudEvent, EngagementChanged, EventTypes, Topics, make_event
from twitter_common.outbox import OutboxRelay, add_to_outbox, claim_event, make_inbox_model, make_outbox_model
from twitter_common.security import Principal, require_scope
from twitter_common.service import create_service_app

SERVICE = "engagement-service"
SNAPSHOT_EVERY = env_int("SNAPSHOT_EVERY", 50)


class Base(DeclarativeBase):
    pass


class StoredEvent(Base):
    """The event store: append-only, one row per domain event."""

    __tablename__ = "event_store"
    stream_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    data: Mapped[str] = mapped_column(Text)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Snapshot(Base):
    __tablename__ = "snapshots"
    stream_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    version: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(Text)


class TweetRef(Base):
    """Local projection of tweets (from tweet events) so we can validate commands
    without a synchronous call to tweet-service."""

    __tablename__ = "tweet_refs"
    tweet_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    author_id: Mapped[str] = mapped_column(String(36))
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)


Outbox = make_outbox_model(Base)
Inbox = make_inbox_model(Base)
engine = make_engine(database_url("engagement"))
Session = make_session_factory(engine)
bus = create_event_bus(SERVICE)
relay = OutboxRelay(SERVICE, Session, Outbox, bus)


# ============================================================== aggregate
LIKED, UNLIKED, RETWEETED, UNRETWEETED = "TweetLiked", "TweetUnliked", "Retweeted", "RetweetUndone"


@dataclass
class TweetEngagement:
    tweet_id: str
    likes: set[str] = field(default_factory=set)
    retweets: set[str] = field(default_factory=set)
    version: int = 0

    # --- event application (pure, no side effects) ---
    def apply(self, event_type: str, data: dict) -> None:
        user = data["user_id"]
        if event_type == LIKED:
            self.likes.add(user)
        elif event_type == UNLIKED:
            self.likes.discard(user)
        elif event_type == RETWEETED:
            self.retweets.add(user)
        elif event_type == UNRETWEETED:
            self.retweets.discard(user)
        self.version += 1

    # --- command handling: decide which events (if any) to emit ---
    def decide(self, command: str, user_id: str) -> list[tuple[str, dict]]:
        ev = {"user_id": user_id, "at": utcnow().isoformat()}
        if command == "like" and user_id not in self.likes:
            return [(LIKED, ev)]
        if command == "unlike" and user_id in self.likes:
            return [(UNLIKED, ev)]
        if command == "retweet" and user_id not in self.retweets:
            return [(RETWEETED, ev)]
        if command == "unretweet" and user_id in self.retweets:
            return [(UNRETWEETED, ev)]
        return []  # idempotent: command already satisfied

    def to_state(self) -> str:
        return json.dumps({"likes": sorted(self.likes), "retweets": sorted(self.retweets)})

    @classmethod
    def from_state(cls, tweet_id: str, version: int, state: str) -> "TweetEngagement":
        raw = json.loads(state)
        return cls(tweet_id, set(raw["likes"]), set(raw["retweets"]), version)


def stream_id(tweet_id: str) -> str:
    return f"tweet-{tweet_id}"


async def load(s: AsyncSession, tweet_id: str, as_of_version: int | None = None) -> TweetEngagement:
    agg = TweetEngagement(tweet_id)
    snap = await s.get(Snapshot, stream_id(tweet_id))
    if snap is not None and (as_of_version is None or snap.version <= as_of_version):
        agg = TweetEngagement.from_state(tweet_id, snap.version, snap.state)
    q = select(StoredEvent).where(StoredEvent.stream_id == stream_id(tweet_id),
                                  StoredEvent.version > agg.version).order_by(StoredEvent.version)
    if as_of_version is not None:
        q = q.where(StoredEvent.version <= as_of_version)
    for e in (await s.execute(q)).scalars():
        agg.apply(e.event_type, json.loads(e.data))
    return agg


INTEGRATION = {LIKED: EventTypes.TWEET_LIKED, UNLIKED: EventTypes.TWEET_UNLIKED,
               RETWEETED: EventTypes.TWEET_RETWEETED, UNRETWEETED: EventTypes.RETWEET_UNDONE}


async def handle_command(tweet_id: str, command: str, user_id: str) -> TweetEngagement:
    for _attempt in range(5):  # optimistic concurrency: reload & retry on version clash
        try:
            async with Session() as s, s.begin():
                ref = await s.get(TweetRef, tweet_id)
                if ref is None or ref.deleted:
                    raise HTTPException(status.HTTP_404_NOT_FOUND,
                                        "tweet not found (new tweets can take a moment to propagate)")
                agg = await load(s, tweet_id)
                for event_type, data in agg.decide(command, user_id):
                    s.add(StoredEvent(stream_id=stream_id(tweet_id), version=agg.version + 1,
                                      event_type=event_type, data=json.dumps(data)))
                    agg.apply(event_type, data)
                    payload = EngagementChanged(tweet_id=tweet_id, user_id=user_id, tweet_author_id=ref.author_id,
                                                like_count=len(agg.likes), retweet_count=len(agg.retweets))
                    add_to_outbox(s, Outbox, Topics.ENGAGEMENT,
                                  make_event(INTEGRATION[event_type], payload, source=SERVICE, subject=tweet_id),
                                  key=tweet_id)
                    if agg.version % SNAPSHOT_EVERY == 0:
                        await s.merge(Snapshot(stream_id=stream_id(tweet_id), version=agg.version,
                                               state=agg.to_state()))
            relay.notify()
            return agg
        except IntegrityError:
            continue  # someone appended version N+1 first
    raise HTTPException(status.HTTP_409_CONFLICT, "too much contention, retry")


# ============================================================== consumers
async def on_tweet_event(event: CloudEvent) -> None:
    async with Session() as s, s.begin():
        if not await claim_event(s, Inbox, event.id, "engagement"):
            return
        if event.type == EventTypes.TWEET_CREATED:
            await s.merge(TweetRef(tweet_id=event.data["tweet_id"], author_id=event.data["author_id"]))
        elif event.type == EventTypes.TWEET_DELETED:
            ref = await s.get(TweetRef, event.data["tweet_id"])
            if ref:
                ref.deleted = True


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await create_schema(engine, Base)
    await bus.start()
    await bus.subscribe(Topics.TWEETS, on_tweet_event)
    app.state.runner.spawn("outbox-relay", relay.run)
    yield
    await app.state.runner.stop()
    await bus.stop()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="Engagement Service",
    description="Likes and retweets stored as an event-sourced, append-only event stream per tweet.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


# ============================================================== API
class EngagementOut(BaseModel):
    tweet_id: str
    like_count: int
    retweet_count: int
    liked_by: list[str]
    retweeted_by: list[str]
    version: int


def out(agg: TweetEngagement) -> EngagementOut:
    return EngagementOut(tweet_id=agg.tweet_id, like_count=len(agg.likes), retweet_count=len(agg.retweets),
                         liked_by=sorted(agg.likes), retweeted_by=sorted(agg.retweets), version=agg.version)


@app.post("/v1/tweets/{tweet_id}/like", response_model=EngagementOut, tags=["engagement"])
async def like(tweet_id: str, me: Principal = Depends(require_scope("engagement:write"))) -> EngagementOut:
    return out(await handle_command(tweet_id, "like", me.sub))


@app.delete("/v1/tweets/{tweet_id}/like", response_model=EngagementOut, tags=["engagement"])
async def unlike(tweet_id: str, me: Principal = Depends(require_scope("engagement:write"))) -> EngagementOut:
    return out(await handle_command(tweet_id, "unlike", me.sub))


@app.post("/v1/tweets/{tweet_id}/retweet", response_model=EngagementOut, tags=["engagement"])
async def retweet(tweet_id: str, me: Principal = Depends(require_scope("engagement:write"))) -> EngagementOut:
    return out(await handle_command(tweet_id, "retweet", me.sub))


@app.delete("/v1/tweets/{tweet_id}/retweet", response_model=EngagementOut, tags=["engagement"])
async def unretweet(tweet_id: str, me: Principal = Depends(require_scope("engagement:write"))) -> EngagementOut:
    return out(await handle_command(tweet_id, "unretweet", me.sub))


@app.get("/v1/tweets/{tweet_id}/engagement", response_model=EngagementOut, tags=["engagement"])
async def engagement(tweet_id: str, as_of_version: int | None = Query(None, ge=0,
                     description="Temporal query: rebuild state as it was at this version")) -> EngagementOut:
    async with Session() as s:
        return out(await load(s, tweet_id, as_of_version))


@app.get("/v1/tweets/{tweet_id}/engagement/events", tags=["engagement"])
async def event_stream(tweet_id: str) -> list[dict]:
    """The raw event stream -- the audit log *is* the source of truth."""
    async with Session() as s:
        rows = (await s.execute(select(StoredEvent).where(StoredEvent.stream_id == stream_id(tweet_id))
                                .order_by(StoredEvent.version))).scalars()
        return [{"version": e.version, "type": e.event_type, "data": json.loads(e.data),
                 "recorded_at": e.recorded_at} for e in rows]
