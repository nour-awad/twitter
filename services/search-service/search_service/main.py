"""search-service: full-text search & trending hashtags.

A *disposable* projection: the index lives in memory and is rebuilt on every
start by replaying the event log from the beginning (subscribe(replay=True)).
This is the event-sourcing superpower applied at system level: new read
models can be added at any time and back-filled from history (Week 6).
It also scales horizontally with zero coordination -- every replica builds
its own copy of the index.
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import Query
from pydantic import BaseModel

from twitter_common.bus import create_event_bus
from twitter_common.events import CloudEvent, EventTypes, Topics
from twitter_common.service import create_service_app

SERVICE = "search-service"
TOKEN = re.compile(r"[#@]?\w+")


def tokenize(text: str) -> list[str]:
    return [t.lower().lstrip("#@") for t in TOKEN.findall(text)]


class SearchIndex:
    def __init__(self) -> None:
        self.tweets: dict[str, dict] = {}
        self.inverted: dict[str, set[str]] = defaultdict(set)
        self.users: dict[str, dict] = {}
        self.events_applied = 0

    def add_tweet(self, d: dict) -> None:
        self.tweets[d["tweet_id"]] = {**d, "likes": 0, "retweets": 0}
        for tok in set(tokenize(d["text"])) | {h.lower() for h in d.get("hashtags", [])}:
            self.inverted[tok].add(d["tweet_id"])

    def remove_tweet(self, tweet_id: str) -> None:
        doc = self.tweets.pop(tweet_id, None)
        if doc:
            for tok in set(tokenize(doc["text"])) | set(doc.get("hashtags", [])):
                self.inverted[tok].discard(tweet_id)

    def search(self, q: str, limit: int) -> list[dict]:
        terms = tokenize(q)
        if not terms:
            return []
        ids = set.intersection(*(self.inverted.get(t, set()) for t in terms))
        now = datetime.now(timezone.utc)

        def score(doc: dict) -> float:
            age_h = (now - datetime.fromisoformat(doc["created_at"])).total_seconds() / 3600
            tf = sum(tokenize(doc["text"]).count(t) for t in terms)
            return tf + math.log1p(doc["likes"] + 2 * doc["retweets"]) - 0.05 * age_h

        return sorted((self.tweets[i] for i in ids), key=score, reverse=True)[:limit]

    def trends(self, window: timedelta, limit: int) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - window
        counts: Counter[str] = Counter()
        for doc in self.tweets.values():
            if datetime.fromisoformat(doc["created_at"]) >= cutoff:
                counts.update(h.lower() for h in doc.get("hashtags", []))
        return [{"hashtag": f"#{h}", "tweets": n} for h, n in counts.most_common(limit)]


index = SearchIndex()
bus = create_event_bus(SERVICE)


async def on_tweet_event(event: CloudEvent) -> None:
    # Naturally idempotent: re-adding/removing the same doc yields the same index.
    if event.type == EventTypes.TWEET_CREATED:
        index.add_tweet(event.data)
    elif event.type == EventTypes.TWEET_DELETED:
        index.remove_tweet(event.data["tweet_id"])
    index.events_applied += 1


async def on_user_event(event: CloudEvent) -> None:
    d = event.data
    if event.type in (EventTypes.USER_CREATED, EventTypes.USER_UPDATED):
        index.users[d["user_id"]] = {"user_id": d["user_id"], "username": d["username"],
                                     "display_name": d.get("display_name", ""), "bio": d.get("bio", "")}
    elif event.type == EventTypes.USER_DELETED:
        index.users.pop(d["user_id"], None)
        for tid in [t for t, doc in index.tweets.items() if doc["author_id"] == d["user_id"]]:
            index.remove_tweet(tid)
    index.events_applied += 1


async def on_engagement_event(event: CloudEvent) -> None:
    doc = index.tweets.get(event.data["tweet_id"])
    if doc:
        doc["likes"], doc["retweets"] = event.data["like_count"], event.data["retweet_count"]


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await bus.start()
    await bus.subscribe(Topics.USERS, on_user_event, replay=True)
    await bus.subscribe(Topics.TWEETS, on_tweet_event, replay=True)
    await bus.subscribe(Topics.ENGAGEMENT, on_engagement_event, replay=True)
    yield
    await app.state.runner.stop()
    await bus.stop()


app = create_service_app(
    name=SERVICE,
    title="Search Service",
    description="In-memory inverted index rebuilt by replaying the event log; trending hashtags.",
    lifespan=lifespan,
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


class TweetHit(BaseModel):
    tweet_id: str
    author_id: str
    author_username: str
    text: str
    hashtags: list[str]
    likes: int
    retweets: int
    created_at: datetime


@app.get("/v1/search/tweets", response_model=list[TweetHit], tags=["search"])
async def search_tweets(q: str = Query(..., min_length=1), limit: int = Query(20, le=100)) -> list[dict]:
    return index.search(q, limit)


@app.get("/v1/search/users", tags=["search"])
async def search_users(q: str = Query(..., min_length=1), limit: int = Query(20, le=100)) -> list[dict]:
    ql = q.lower().lstrip("@")
    hits = [u for u in index.users.values() if ql in u["username"].lower() or ql in u["display_name"].lower()]
    return sorted(hits, key=lambda u: (not u["username"].lower().startswith(ql), u["username"]))[:limit]


@app.get("/v1/trends", tags=["search"])
async def trends(window_minutes: int = Query(24 * 60, ge=1), limit: int = Query(10, le=50)) -> list[dict]:
    return index.trends(timedelta(minutes=window_minutes), limit)


@app.get("/v1/search/stats", tags=["search"])
async def stats() -> dict:
    return {"tweets_indexed": len(index.tweets), "users_indexed": len(index.users),
            "terms": len(index.inverted), "events_applied": index.events_applied}
