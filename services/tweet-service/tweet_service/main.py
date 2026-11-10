"""tweet-service: the write side for tweets (command model).

Demonstrates:
  * synchronous gRPC call to user-service with deadline, retry, circuit
    breaker and a *graceful-degradation fallback* (Weeks 3 & 10);
  * an optional call to a serverless content-filter function (Week 9);
  * transactional outbox -> tweet.created / tweet.deleted events (Week 5);
  * API versioning: /v1 (deprecated shape) and /v2 (Week 3);
  * choreography: reacts to user.deleted / user.updated (Week 7).
"""

from __future__ import annotations

import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

from fastapi import Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, String, Text, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.bus import create_event_bus
from twitter_common.clients.user_grpc import UserGrpcClient
from twitter_common.config import database_url, env
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.events import CloudEvent, EventTypes, TweetCreated, TweetDeleted, Topics, make_event
from twitter_common.outbox import OutboxRelay, add_to_outbox, claim_event, make_inbox_model, make_outbox_model
from twitter_common.resilience import DependencyUnavailable, ResilientClient
from twitter_common.security import Principal, ServiceTokenProvider, require_scope
from twitter_common.service import create_service_app

log = logging.getLogger(__name__)
SERVICE = "tweet-service"
HASHTAG = re.compile(r"#(\w{1,64})")
MENTION = re.compile(r"@(\w{1,32})")


class Base(DeclarativeBase):
    pass


class Tweet(Base):
    __tablename__ = "tweets"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    author_id: Mapped[str] = mapped_column(String(36), index=True)
    author_username: Mapped[str] = mapped_column(String(32))
    text: Mapped[str] = mapped_column(Text)
    reply_to: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    hashtags: Mapped[str] = mapped_column(Text, default="[]")
    mentions: Mapped[str] = mapped_column(Text, default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


Outbox = make_outbox_model(Base)
Inbox = make_inbox_model(Base)
engine = make_engine(database_url("tweet"))
Session = make_session_factory(engine)
bus = create_event_bus(SERVICE)
relay = OutboxRelay(SERVICE, Session, Outbox, bus)
tokens = ServiceTokenProvider(SERVICE)
users: UserGrpcClient | None = None
content_filter: ResilientClient | None = None


# ---------------------------------------------------------------- consumers
async def on_user_event(event: CloudEvent) -> None:
    if event.type not in (EventTypes.USER_DELETED, EventTypes.USER_UPDATED):
        return
    async with Session() as s, s.begin():
        if not await claim_event(s, Inbox, event.id, "tweets"):
            return
        uid = event.data["user_id"]
        if event.type == EventTypes.USER_DELETED:
            await s.execute(update(Tweet).where(Tweet.author_id == uid, Tweet.deleted_at.is_(None))
                            .values(deleted_at=utcnow()))
        else:
            await s.execute(update(Tweet).where(Tweet.author_id == uid)
                            .values(author_username=event.data["username"]))


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    global users, content_filter
    await create_schema(engine, Base)
    await bus.start()
    users = UserGrpcClient(SERVICE, tokens)
    if env("CONTENT_FILTER_URL"):
        content_filter = ResilientClient("content-filter", env("CONTENT_FILTER_URL") or "", service=SERVICE,
                                         timeout=1.0, retries=1, failure_threshold=3, recovery_timeout=15)
    await bus.subscribe(Topics.USERS, on_user_event)
    app.state.runner.spawn("outbox-relay", relay.run)
    yield
    await app.state.runner.stop()
    await users.close()
    await bus.stop()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="Tweet Service",
    description="Create/read/delete tweets. Publishes tweet events through a transactional outbox.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


# ---------------------------------------------------------------- schemas
class CreateTweet(BaseModel):
    text: str = Field(min_length=1, max_length=280)
    reply_to: str | None = None


class TweetV1(BaseModel):
    id: str
    author_id: str
    author_username: str
    text: str
    reply_to: str | None
    hashtags: list[str]
    mentions: list[str]
    created_at: datetime


class AuthorV2(BaseModel):
    id: str
    username: str


class EntitiesV2(BaseModel):
    hashtags: list[str]
    mentions: list[str]


class TweetV2(BaseModel):
    """v2 groups related fields and adds hypermedia links (non-breaking evolution would
    have been additive; this reshaping is breaking, hence a new major version)."""

    id: str
    text: str
    author: AuthorV2
    entities: EntitiesV2
    in_reply_to: str | None
    created_at: datetime
    links: dict[str, str]


def v1(t: Tweet) -> TweetV1:
    return TweetV1(id=t.id, author_id=t.author_id, author_username=t.author_username, text=t.text,
                   reply_to=t.reply_to, hashtags=json.loads(t.hashtags), mentions=json.loads(t.mentions),
                   created_at=t.created_at)


def v2(t: Tweet) -> TweetV2:
    return TweetV2(
        id=t.id, text=t.text, author=AuthorV2(id=t.author_id, username=t.author_username),
        entities=EntitiesV2(hashtags=json.loads(t.hashtags), mentions=json.loads(t.mentions)),
        in_reply_to=t.reply_to, created_at=t.created_at,
        links={"self": f"/v2/tweets/{t.id}", "engagement": f"/v1/tweets/{t.id}/engagement",
               "author": f"/v1/users/{t.author_id}"},
    )


async def _live_tweet(s: AsyncSession, tweet_id: str) -> Tweet:
    t = await s.get(Tweet, tweet_id)
    if t is None or t.deleted_at is not None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "tweet not found")
    return t


async def _filter_text(text: str) -> str:
    """Call the serverless content-filter function; degrade gracefully if it is cold/down."""
    if content_filter is None:
        return text
    try:
        resp = await content_filter.post("/", json={"text": text})
        if resp.status_code == 200:
            return resp.json().get("clean_text", text)
    except DependencyUnavailable as exc:
        log.warning("content filter unavailable, publishing unfiltered: %s", exc)
    return text


# ---------------------------------------------------------------- endpoints
@app.post("/v1/tweets", response_model=TweetV1, status_code=201, tags=["tweets"])
async def create_tweet(body: CreateTweet, response: Response,
                       me: Principal = Depends(require_scope("tweets:write"))) -> TweetV1:
    assert users is not None
    # 1) Synchronous validation over gRPC, with a fallback when user-service is down:
    #    the JWT already carries the username, so we can keep accepting tweets.
    try:
        author = await users.get_user(me.sub)
        if author is None:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "author account no longer exists")
        username = author["username"]
    except DependencyUnavailable:
        username = me.username or "unknown"
        response.headers["x-degraded"] = "user-service-unavailable"

    text = await _filter_text(body.text)
    async with Session() as s:
        async with s.begin():
            if body.reply_to:
                await _live_tweet(s, body.reply_to)
            tweet = Tweet(id=str(uuid4()), author_id=me.sub, author_username=username, text=text,
                          reply_to=body.reply_to,
                          hashtags=json.dumps(sorted({h.lower() for h in HASHTAG.findall(text)})),
                          mentions=json.dumps(sorted(set(MENTION.findall(text)))), created_at=utcnow())
            s.add(tweet)
            # 2) Same ACID transaction as the tweet row => no dual-write problem.
            event = make_event(EventTypes.TWEET_CREATED, TweetCreated(
                tweet_id=tweet.id, author_id=tweet.author_id, author_username=username, text=text,
                hashtags=json.loads(tweet.hashtags), mentions=json.loads(tweet.mentions),
                reply_to=tweet.reply_to, created_at=tweet.created_at), source=SERVICE, subject=tweet.id)
            add_to_outbox(s, Outbox, Topics.TWEETS, event, key=tweet.author_id)
    relay.notify()
    return v1(tweet)


@app.get("/v1/tweets/{tweet_id}", response_model=TweetV1, tags=["tweets"])
async def get_tweet_v1(tweet_id: str, response: Response) -> TweetV1:
    # RFC 8594 / RFC 9745 deprecation signalling: clients learn about v2 at runtime.
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 31 Dec 2027 23:59:59 GMT"
    response.headers["Link"] = f'</v2/tweets/{tweet_id}>; rel="successor-version"'
    async with Session() as s:
        return v1(await _live_tweet(s, tweet_id))


@app.get("/v2/tweets/{tweet_id}", response_model=TweetV2, tags=["tweets v2"])
async def get_tweet_v2(tweet_id: str) -> TweetV2:
    async with Session() as s:
        return v2(await _live_tweet(s, tweet_id))


@app.get("/v1/tweets", response_model=list[TweetV1], tags=["tweets"])
async def batch_get(ids: str = Query(..., description="comma separated ids")) -> list[TweetV1]:
    wanted = [i for i in ids.split(",") if i][:100]
    async with Session() as s:
        rows = (await s.execute(select(Tweet).where(Tweet.id.in_(wanted), Tweet.deleted_at.is_(None)))).scalars()
        return [v1(t) for t in rows]


@app.get("/v1/tweets/{tweet_id}/replies", response_model=list[TweetV1], tags=["tweets"])
async def replies(tweet_id: str, limit: int = Query(50, le=200)) -> list[TweetV1]:
    async with Session() as s:
        q = (select(Tweet).where(Tweet.reply_to == tweet_id, Tweet.deleted_at.is_(None))
             .order_by(Tweet.created_at).limit(limit))
        return [v1(t) for t in (await s.execute(q)).scalars()]


@app.get("/v1/users/{user_id}/tweets", response_model=list[TweetV1], tags=["tweets"])
async def user_tweets(user_id: str, limit: int = Query(20, le=100), before: datetime | None = None) -> list[TweetV1]:
    async with Session() as s:
        q = select(Tweet).where(Tweet.author_id == user_id, Tweet.deleted_at.is_(None))
        if before:
            q = q.where(Tweet.created_at < before)
        q = q.order_by(Tweet.created_at.desc()).limit(limit)
        return [v1(t) for t in (await s.execute(q)).scalars()]


@app.delete("/v1/tweets/{tweet_id}", status_code=204, tags=["tweets"])
async def delete_tweet(tweet_id: str, me: Principal = Depends(require_scope("tweets:write"))) -> None:
    async with Session() as s, s.begin():
        t = await _live_tweet(s, tweet_id)
        if t.author_id != me.sub and "admin" not in me.roles:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "not your tweet")
        t.deleted_at = utcnow()
        add_to_outbox(s, Outbox, Topics.TWEETS,
                      make_event(EventTypes.TWEET_DELETED, TweetDeleted(tweet_id=t.id, author_id=t.author_id),
                                 source=SERVICE, subject=t.id), key=t.author_id)
    relay.notify()
