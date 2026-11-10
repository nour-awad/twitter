#!/usr/bin/env python
"""End-to-end walkthrough of every course topic against a running stack.

    python scripts/devstack.py   # terminal 1   (or: docker compose up --build)
    python scripts/demo.py       # terminal 2

GATEWAY=http://localhost:8000 by default.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import httpx

GATEWAY = os.getenv("GATEWAY", "http://localhost:8000")


def show(title: str, data: object) -> None:
    print(f"\n=== {title}")
    print(json.dumps(data, indent=2, default=str)[:1500])


async def eventually(fn, timeout: float = 10.0):  # type: ignore[no-untyped-def]
    """Read models are eventually consistent: poll until the condition holds."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = await fn()
        if result or asyncio.get_running_loop().time() > deadline:
            return result
        await asyncio.sleep(0.2)


async def run(gateway: str = GATEWAY, verbose: bool = True) -> dict:
    out = show if verbose else (lambda *_: None)
    suffix = uuid.uuid4().hex[:6]
    alice, bob = f"alice_{suffix}", f"bob_{suffix}"
    async with httpx.AsyncClient(base_url=gateway, timeout=30) as c:
        # Week 5-7: orchestrated registration saga
        reg = {}
        for name in (alice, bob):
            r = await c.post("/api/v1/register", json={"username": name, "email": f"{name}@example.com",
                                                       "password": "s3cret-pass", "display_name": name.title()})
            r.raise_for_status()
            reg[name] = r.json()
        out("Registration saga (orchestration)", reg[alice]["saga"])

        dup = await c.post("/api/v1/register", json={"username": alice, "email": "x@example.com",
                                                     "password": "s3cret-pass"})
        out(f"Duplicate username -> HTTP {dup.status_code}, saga compensated", dup.json()["saga"]["status"])

        # Week 12: authentication -> JWT
        tokens = {}
        for name in (alice, bob):
            r = await c.post("/api/v1/auth/login", json={"username": name, "password": "s3cret-pass"})
            r.raise_for_status()
            tokens[name] = {"authorization": f"Bearer {r.json()['access_token']}"}
        a_id, b_id = reg[alice]["user_id"], reg[bob]["user_id"]

        # social graph -> user.followed event
        r = await c.post(f"/api/v1/users/{a_id}/follow", headers=tokens[bob])
        out("Bob follows Alice", r.json())

        # Week 3/5: tweet-service validates author via gRPC, writes tweet + outbox atomically
        r = await c.post("/api/v1/tweets", headers=tokens[alice],
                         json={"text": f"Hello #microservices world, @{bob}! darn this is fun #sose"})
        r.raise_for_status()
        tweet = r.json()
        out("Alice tweets (content-filter function masked a word)", tweet)

        # Week 4/6: CQRS timeline projection built from events
        async def bob_timeline():  # type: ignore[no-untyped-def]
            t = (await c.get("/api/v1/timelines/home", headers=tokens[bob])).json()
            return t if t.get("items") else None

        out("Bob's home timeline (CQRS read model, fan-out-on-write)", await eventually(bob_timeline))

        # Week 6: event-sourced likes
        async def like():  # type: ignore[no-untyped-def]
            r = await c.post(f"/api/v1/tweets/{tweet['id']}/like", headers=tokens[bob])
            return r.json() if r.status_code == 200 else None

        out("Bob likes the tweet (event sourced)", await eventually(like))
        await c.delete(f"/api/v1/tweets/{tweet['id']}/like", headers=tokens[bob])
        await c.post(f"/api/v1/tweets/{tweet['id']}/like", headers=tokens[bob])
        out("Event stream for the tweet", (await c.get(f"/api/v1/tweets/{tweet['id']}/engagement/events")).json())
        out("Temporal query: as_of_version=1",
            (await c.get(f"/api/v1/tweets/{tweet['id']}/engagement", params={"as_of_version": 1})).json())

        # Week 4/7: choreography -> notifications
        async def alice_notifs():  # type: ignore[no-untyped-def]
            n = (await c.get("/api/v1/notifications", headers=tokens[alice])).json()
            return n if len(n) >= 3 else None

        out("Alice's notifications (welcome, follow, like - choreographed)", await eventually(alice_notifs))

        # search projection rebuilt from the log
        async def search():  # type: ignore[no-untyped-def]
            return (await c.get("/api/v1/search/tweets", params={"q": "#microservices"})).json() or None

        out("Search #microservices", await eventually(search))
        out("Trending", (await c.get("/api/v1/trends")).json())

        # Week 3: API versioning
        v1 = await c.get(f"/api/v1/tweets/{tweet['id']}")
        out("v1 tweet deprecation headers", {k: v1.headers.get(k) for k in ("deprecation", "sunset", "link")})
        out("v2 tweet", (await c.get(f"/api/v2/tweets/{tweet['id']}")).json())

        # Week 3: GraphQL (+ DataLoader -> one gRPC BatchGetUsers)
        q = """query { homeTimeline(limit: 5) { id text likeCount author { username followersCount } } }"""
        r = await c.post("/graphql", json={"query": q}, headers=tokens[bob])
        out("GraphQL homeTimeline", r.json())

        # BFF aggregation
        out("BFF tweet details", (await c.get(f"/api/v1/bff/tweets/{tweet['id']}")).json())

        # Week 12: zero trust - a forged/absent token is rejected everywhere
        r = await c.post("/api/v1/tweets", json={"text": "no token"})
        out("Tweet without token", {"status": r.status_code})

        # Week 13: self-adaptation status
        out("Gateway MAPE-K controller", (await c.get("/api/v1/adaptation")).json())
        out("Timeline adaptive fan-out", (await c.get("/api/v1/timelines/adaptation")).json())

        # Week 14: AI / MCP
        out("AI tool catalogue (also served over MCP at /mcp)", (await c.get("/api/v1/ai/tools")).json()["tools"][:2])
        out("AI timeline summary", (await c.post("/api/v1/ai/timeline-summary", headers=tokens[bob])).json())

        # Week 11: AIOps
        out("AIOps analysis", (await c.post("/api/v1/aiops/analyze")).json())

        return {"alice": a_id, "bob": b_id, "tweet": tweet, "tokens": tokens}


if __name__ == "__main__":
    asyncio.run(run())
