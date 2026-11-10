"""GraphQL API (Week 3) - one endpoint, client-shaped queries, composed from many services.

The gateway acts as a *GraphQL federation-lite* layer: each field resolver
calls the owning microservice. Authors are resolved through a DataLoader that
batches all author ids of a request into ONE gRPC BatchGetUsers call
(avoids the N+1 problem that naive GraphQL-over-microservices suffers from).

Example:
    query {
      homeTimeline(limit: 10) {
        text likeCount
        author { username displayName followersCount }
      }
    }
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import strawberry
from fastapi import Request
from strawberry.dataloader import DataLoader
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from api_gateway.upstreams import Upstream, UpstreamError
from twitter_common.clients.user_grpc import UserGrpcClient


@strawberry.type
class User:
    id: strawberry.ID
    username: str
    display_name: str
    bio: str
    followers_count: int
    following_count: int

    @strawberry.field
    async def tweets(self, info: Info, limit: int = 10) -> list["Tweet"]:
        data = await ctx_up(info, "tweet").json("GET", f"/v1/users/{self.id}/tweets", params={"limit": limit})
        return [Tweet.from_rest(t) for t in data]


@strawberry.type
class Engagement:
    like_count: int
    retweet_count: int
    liked_by: list[str]


@strawberry.type
class Tweet:
    id: strawberry.ID
    text: str
    author_id: strawberry.Private[str]
    author_username: str
    hashtags: list[str]
    reply_to: str | None
    created_at: datetime
    like_count: int = 0
    retweet_count: int = 0

    @classmethod
    def from_rest(cls, d: dict[str, Any]) -> "Tweet":
        return cls(id=d.get("id") or d["tweet_id"], text=d["text"], author_id=d["author_id"],
                   author_username=d["author_username"], hashtags=d.get("hashtags", []),
                   reply_to=d.get("reply_to"), created_at=d["created_at"],
                   like_count=d.get("like_count", d.get("likes", 0)),
                   retweet_count=d.get("retweet_count", d.get("retweets", 0)))

    @strawberry.field
    async def author(self, info: Info) -> User | None:
        return await info.context["user_loader"].load(self.author_id)

    @strawberry.field
    async def engagement(self, info: Info) -> Engagement | None:
        try:
            e = await ctx_up(info, "engagement").json("GET", f"/v1/tweets/{self.id}/engagement")
        except UpstreamError:
            return None  # partial result instead of failing the whole query
        return Engagement(like_count=e["like_count"], retweet_count=e["retweet_count"], liked_by=e["liked_by"])


@strawberry.type
class Trend:
    hashtag: str
    tweets: int


def ctx_up(info: Info, name: str) -> Upstream:
    return info.context["upstreams"][name]


def token(info: Info) -> str:
    t = info.context["token"]
    if not t:
        raise PermissionError("authentication required: send 'Authorization: Bearer <token>'")
    return t


def to_user(u: dict[str, Any]) -> User:
    return User(id=u["id"], username=u["username"], display_name=u.get("display_name", ""), bio=u.get("bio", ""),
                followers_count=int(u.get("followers_count", 0)), following_count=int(u.get("following_count", 0)))


@strawberry.type
class Query:
    @strawberry.field
    async def me(self, info: Info) -> User | None:
        me = await ctx_up(info, "auth").json("GET", "/v1/auth/me", token(info))
        return await info.context["user_loader"].load(me["user_id"])

    @strawberry.field
    async def user(self, info: Info, username: str) -> User | None:
        try:
            return to_user(await ctx_up(info, "user").json("GET", f"/v1/users/by-username/{username}"))
        except UpstreamError as e:
            if e.status == 404:
                return None
            raise

    @strawberry.field
    async def tweet(self, info: Info, id: strawberry.ID) -> Tweet | None:
        try:
            return Tweet.from_rest(await ctx_up(info, "tweet").json("GET", f"/v1/tweets/{id}"))
        except UpstreamError as e:
            if e.status == 404:
                return None
            raise

    @strawberry.field
    async def home_timeline(self, info: Info, limit: int = 20) -> list[Tweet]:
        page = await ctx_up(info, "timeline").json("GET", "/v1/timelines/home", token(info), params={"limit": limit})
        return [Tweet.from_rest(t) for t in page["items"]]

    @strawberry.field
    async def search_tweets(self, info: Info, q: str, limit: int = 20) -> list[Tweet]:
        hits = await ctx_up(info, "search").json("GET", "/v1/search/tweets", params={"q": q, "limit": limit})
        return [Tweet.from_rest(t) for t in hits]

    @strawberry.field
    async def trends(self, info: Info, limit: int = 10) -> list[Trend]:
        return [Trend(**t) for t in await ctx_up(info, "search").json("GET", "/v1/trends", params={"limit": limit})]


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def post_tweet(self, info: Info, text: str, reply_to: str | None = None) -> Tweet:
        t = await ctx_up(info, "tweet").json("POST", "/v1/tweets", token(info),
                                             json={"text": text, "reply_to": reply_to})
        return Tweet.from_rest(t)

    @strawberry.mutation
    async def like_tweet(self, info: Info, tweet_id: strawberry.ID) -> Engagement:
        e = await ctx_up(info, "engagement").json("POST", f"/v1/tweets/{tweet_id}/like", token(info))
        return Engagement(like_count=e["like_count"], retweet_count=e["retweet_count"], liked_by=e["liked_by"])

    @strawberry.mutation
    async def follow(self, info: Info, user_id: strawberry.ID) -> bool:
        r = await ctx_up(info, "user").json("POST", f"/v1/users/{user_id}/follow", token(info))
        return bool(r["following"])

    @strawberry.mutation
    async def delete_tweet(self, info: Info, tweet_id: strawberry.ID) -> bool:
        await ctx_up(info, "tweet").json("DELETE", f"/v1/tweets/{tweet_id}", token(info))
        return True


schema = strawberry.Schema(query=Query, mutation=Mutation)


def build_graphql_router(upstreams: dict[str, Upstream], users_grpc: Callable[[], UserGrpcClient]) -> GraphQLRouter:
    async def get_context(request: Request) -> dict[str, Any]:
        async def load_users(ids: list[str]) -> list[User | None]:
            found = await users_grpc().batch_get_users(list(ids))  # one gRPC call per request
            return [to_user(found[i]) if i in found else None for i in ids]

        auth = request.headers.get("authorization", "")
        return {
            "request": request,
            "upstreams": upstreams,
            "token": auth[7:] if auth.lower().startswith("bearer ") else None,
            "user_loader": DataLoader(load_fn=load_users),
        }

    return GraphQLRouter(schema, context_getter=get_context)
