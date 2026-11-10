"""timeline-service: the CQRS *read model* for home timelines (Weeks 6 & 13).

It owns no source-of-truth data. Everything it stores is a projection built
from events (tweets, follows, engagement), denormalised for one query:
"give me my home timeline, fast". It never calls another service
synchronously to answer a read -> it stays available when writers are down.

Hybrid fan-out (the classic Twitter design):
  * normal authors  -> fan-out-on-write: push the tweet into every
                       follower's home timeline when it is created;
  * celebrities     -> fan-out-on-read: merge their tweets at query time
                       (pushing to millions of inboxes is too expensive).

Self-adaptive (MAPE-K): the celebrity threshold is not a constant; a control
loop watches fan-out latency and moves the threshold to keep it under the
target, trading write cost against read cost at runtime.
"""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy import DateTime, Integer, String, Text, delete, func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.bus import create_event_bus
from twitter_common.config import database_url, env_float, env_int
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.events import CloudEvent, EventTypes, Topics
from twitter_common.outbox import claim_event, make_inbox_model
from twitter_common.security import Principal, require_scope, require_service
from twitter_common.service import create_service_app

SERVICE = "timeline-service"


class Base(DeclarativeBase):
    pass


class FollowEdge(Base):
    __tablename__ = "follow_edges"
    follower_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    followee_id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)


class TweetView(Base):
    __tablename__ = "tweet_views"
    tweet_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    author_id: Mapped[str] = mapped_column(String(36), index=True)
    author_username: Mapped[str] = mapped_column(String(32))
    text: Mapped[str] = mapped_column(Text)
    hashtags: Mapped[str] = mapped_column(Text, default="[]")
    reply_to: Mapped[str | None] = mapped_column(String(36), nullable=True)
    like_count: Mapped[int] = mapped_column(Integer, default=0)
    retweet_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class HomeEntry(Base):
    __tablename__ = "home_entries"
    user_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tweet_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class TimelineOwner(Base):
    __tablename__ = "timeline_owners"
    user_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


Inbox = make_inbox_model(Base)
engine = make_engine(database_url("timeline"))
Session = make_session_factory(engine)
bus = create_event_bus(SERVICE)


# ======================================================== MAPE-K controller
class AdaptiveFanout:
    """Monitor -> Analyze -> Plan -> Execute over shared Knowledge (Week 13)."""

    def __init__(self) -> None:
        self.threshold = env_int("FANOUT_CELEBRITY_THRESHOLD", 1000)
        self.min_threshold = env_int("FANOUT_MIN_THRESHOLD", 2)
        self.max_threshold = env_int("FANOUT_MAX_THRESHOLD", 100_000)
        self.target_ms = env_float("FANOUT_TARGET_MS", 250.0)
        self.ewma_ms = 0.0
        self.samples = 0
        self.history: list[dict] = []  # Knowledge

    def observe(self, fanout_ms: float, recipients: int) -> None:  # Monitor
        self.samples += 1
        self.ewma_ms = fanout_ms if self.samples == 1 else 0.8 * self.ewma_ms + 0.2 * fanout_ms
        self._adapt(recipients)

    def _adapt(self, recipients: int) -> None:
        old = self.threshold
        if self.ewma_ms > self.target_ms:  # Analyze: write path too slow
            self.threshold = max(self.min_threshold, int(self.threshold * 0.7))  # Plan + Execute
        elif self.ewma_ms < self.target_ms / 4 and recipients >= self.threshold * 0.8:
            self.threshold = min(self.max_threshold, int(self.threshold * 1.2) + 1)
        if old != self.threshold:
            self.history.append({"at": utcnow().isoformat(), "ewma_ms": round(self.ewma_ms, 2),
                                 "from": old, "to": self.threshold})
            self.history = self.history[-50:]

    def is_celebrity(self, followers: int) -> bool:
        return followers > self.threshold


fanout = AdaptiveFanout()


# ======================================================== projections
async def follower_count(s: AsyncSession, user_id: str) -> int:
    return int(await s.scalar(select(func.count()).select_from(FollowEdge)
                              .where(FollowEdge.followee_id == user_id)) or 0)


async def push_entries(s: AsyncSession, user_ids: list[str], tweet_id: str, created_at: datetime) -> None:
    if not user_ids:
        return
    rows = [{"user_id": u, "tweet_id": tweet_id, "created_at": created_at} for u in user_ids]
    if engine.url.get_backend_name() == "sqlite":
        await s.execute(sqlite_insert(HomeEntry).values(rows).on_conflict_do_nothing())
    else:
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        await s.execute(pg_insert(HomeEntry).values(rows).on_conflict_do_nothing())


async def on_tweet_event(event: CloudEvent) -> None:
    async with Session() as s, s.begin():
        if not await claim_event(s, Inbox, event.id, "timeline"):
            return
        d = event.data
        if event.type == EventTypes.TWEET_CREATED:
            created = datetime.fromisoformat(d["created_at"])
            s.add(TweetView(tweet_id=d["tweet_id"], author_id=d["author_id"], author_username=d["author_username"],
                            text=d["text"], hashtags=json.dumps(d.get("hashtags", [])), reply_to=d.get("reply_to"),
                            created_at=created))
            recipients = [d["author_id"]]
            n_followers = await follower_count(s, d["author_id"])
            if not fanout.is_celebrity(n_followers):
                start = time.perf_counter()
                followers = (await s.execute(select(FollowEdge.follower_id)
                                             .where(FollowEdge.followee_id == d["author_id"]))).scalars().all()
                recipients += list(followers)
                await push_entries(s, recipients, d["tweet_id"], created)
                fanout.observe((time.perf_counter() - start) * 1000, len(recipients))
            else:
                await push_entries(s, recipients, d["tweet_id"], created)
        elif event.type == EventTypes.TWEET_DELETED:
            await s.execute(delete(HomeEntry).where(HomeEntry.tweet_id == d["tweet_id"]))
            await s.execute(delete(TweetView).where(TweetView.tweet_id == d["tweet_id"]))


async def on_user_event(event: CloudEvent) -> None:
    async with Session() as s, s.begin():
        if not await claim_event(s, Inbox, event.id, "timeline"):
            return
        d = event.data
        if event.type == EventTypes.USER_FOLLOWED:
            if await s.get(FollowEdge, (d["follower_id"], d["followee_id"])) is None:
                s.add(FollowEdge(follower_id=d["follower_id"], followee_id=d["followee_id"]))
            # Backfill: the new followee's recent tweets appear immediately.
            recent = (await s.execute(select(TweetView).where(TweetView.author_id == d["followee_id"])
                                      .order_by(TweetView.created_at.desc()).limit(20))).scalars().all()
            for t in recent:
                await push_entries(s, [d["follower_id"]], t.tweet_id, t.created_at)
        elif event.type == EventTypes.USER_UNFOLLOWED:
            await s.execute(delete(FollowEdge).where(FollowEdge.follower_id == d["follower_id"],
                                                     FollowEdge.followee_id == d["followee_id"]))
            their = select(TweetView.tweet_id).where(TweetView.author_id == d["followee_id"])
            await s.execute(delete(HomeEntry).where(HomeEntry.user_id == d["follower_id"],
                                                    HomeEntry.tweet_id.in_(their)))
        elif event.type == EventTypes.USER_UPDATED:
            await s.execute(update(TweetView).where(TweetView.author_id == d["user_id"])
                            .values(author_username=d["username"]))
        elif event.type == EventTypes.USER_DELETED:
            uid = d["user_id"]
            their = select(TweetView.tweet_id).where(TweetView.author_id == uid)
            await s.execute(delete(HomeEntry).where(HomeEntry.tweet_id.in_(their)))
            await s.execute(delete(HomeEntry).where(HomeEntry.user_id == uid))
            await s.execute(delete(TweetView).where(TweetView.author_id == uid))
            await s.execute(delete(FollowEdge).where((FollowEdge.follower_id == uid) | (FollowEdge.followee_id == uid)))
            await s.execute(delete(TimelineOwner).where(TimelineOwner.user_id == uid))


async def on_engagement_event(event: CloudEvent) -> None:
    async with Session() as s, s.begin():
        if not await claim_event(s, Inbox, event.id, "timeline"):
            return
        view = await s.get(TweetView, event.data["tweet_id"])
        if view is not None:
            view.like_count = event.data["like_count"]
            view.retweet_count = event.data["retweet_count"]


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await create_schema(engine, Base)
    await bus.start()
    await bus.subscribe(Topics.TWEETS, on_tweet_event)
    await bus.subscribe(Topics.USERS, on_user_event)
    await bus.subscribe(Topics.ENGAGEMENT, on_engagement_event)
    yield
    await app.state.runner.stop()
    await bus.stop()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="Timeline Service",
    description="CQRS read model: precomputed home timelines with adaptive hybrid fan-out.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


# ======================================================== queries
class TimelineItem(BaseModel):
    tweet_id: str
    author_id: str
    author_username: str
    text: str
    hashtags: list[str]
    reply_to: str | None
    like_count: int
    retweet_count: int
    created_at: datetime


class TimelinePage(BaseModel):
    items: list[TimelineItem]
    next_cursor: datetime | None
    strategy: dict


def item(t: TweetView) -> TimelineItem:
    return TimelineItem(tweet_id=t.tweet_id, author_id=t.author_id, author_username=t.author_username,
                        text=t.text, hashtags=json.loads(t.hashtags), reply_to=t.reply_to,
                        like_count=t.like_count, retweet_count=t.retweet_count, created_at=t.created_at)


@app.get("/v1/timelines/home", response_model=TimelinePage, tags=["timelines"])
async def home(limit: int = Query(20, le=100), before: datetime | None = None,
               me: Principal = Depends(require_scope("tweets:read"))) -> TimelinePage:
    async with Session() as s:
        pushed = select(TweetView).join(HomeEntry, HomeEntry.tweet_id == TweetView.tweet_id) \
            .where(HomeEntry.user_id == me.sub)
        if before:
            pushed = pushed.where(TweetView.created_at < before)
        rows = list((await s.execute(pushed.order_by(TweetView.created_at.desc()).limit(limit))).scalars())
        n_pushed = len(rows)

        # fan-out-on-read for the celebrities I follow
        followees = (await s.execute(select(FollowEdge.followee_id)
                                     .where(FollowEdge.follower_id == me.sub))).scalars().all()
        celebs = [f for f in followees if fanout.is_celebrity(await follower_count(s, f))]
        if celebs:
            pulled = select(TweetView).where(TweetView.author_id.in_(celebs))
            if before:
                pulled = pulled.where(TweetView.created_at < before)
            rows += list((await s.execute(pulled.order_by(TweetView.created_at.desc()).limit(limit))).scalars())

    seen: dict[str, TweetView] = {t.tweet_id: t for t in rows}
    merged = sorted(seen.values(), key=lambda t: t.created_at, reverse=True)[:limit]
    return TimelinePage(
        items=[item(t) for t in merged],
        next_cursor=merged[-1].created_at if len(merged) == limit else None,
        strategy={"pushed": n_pushed, "pulled_from_celebrities": celebs,
                  "celebrity_threshold": fanout.threshold},
    )


@app.get("/v1/timelines/users/{user_id}", response_model=TimelinePage, tags=["timelines"])
async def user_timeline(user_id: str, limit: int = Query(20, le=100), before: datetime | None = None) -> TimelinePage:
    async with Session() as s:
        q = select(TweetView).where(TweetView.author_id == user_id)
        if before:
            q = q.where(TweetView.created_at < before)
        rows = (await s.execute(q.order_by(TweetView.created_at.desc()).limit(limit))).scalars().all()
    return TimelinePage(items=[item(t) for t in rows],
                        next_cursor=rows[-1].created_at if len(rows) == limit else None,
                        strategy={"source": "projection"})


@app.get("/v1/timelines/adaptation", tags=["self-adaptation"])
async def adaptation() -> dict:
    """Knowledge base of the MAPE-K loop: current threshold and its history."""
    return {"celebrity_threshold": fanout.threshold, "fanout_ewma_ms": round(fanout.ewma_ms, 3),
            "target_ms": fanout.target_ms, "samples": fanout.samples, "history": fanout.history}


# ---- saga participant endpoints ------------------------------------------
class ProvisionTimeline(BaseModel):
    user_id: str


@app.post("/internal/timelines", status_code=201, tags=["internal"])
async def provision(body: ProvisionTimeline, _: Principal = Depends(require_service("timeline:provision"))) -> dict:
    async with Session() as s, s.begin():
        if await s.get(TimelineOwner, body.user_id) is None:
            s.add(TimelineOwner(user_id=body.user_id))
    return {"user_id": body.user_id}


@app.delete("/internal/timelines/{user_id}", status_code=204, tags=["internal"])
async def deprovision(user_id: str, _: Principal = Depends(require_service("timeline:provision"))) -> None:
    async with Session() as s, s.begin():
        await s.execute(delete(TimelineOwner).where(TimelineOwner.user_id == user_id))
