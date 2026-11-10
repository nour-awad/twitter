"""Twitter capabilities exposed as *tools* (Week 14: AI-native SOA).

The same tool functions are published two ways:
  * over MCP (Model Context Protocol) -> any MCP-capable agent (Claude
    Desktop, Claude Code, IDEs, other agents) can discover & call them;
  * to our own Claude agent loop (POST /v1/ai/agent).

A "service" here is no longer an endpoint a developer hard-codes against;
it is a *described capability* an agent discovers and composes at runtime.
Tools call the public API gateway with the end user's own token, so the
agent can never do more than the user could (no privilege escalation).
"""

from __future__ import annotations

import contextvars
from typing import Any, Awaitable, Callable

import httpx

from twitter_common.config import GATEWAY_URL, env

current_token: contextvars.ContextVar[str | None] = contextvars.ContextVar("current_token", default=None)


async def _api(method: str, path: str, **kw: Any) -> Any:
    token = current_token.get() or env("AGENT_ACCESS_TOKEN")
    headers = {"authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(base_url=GATEWAY_URL(), timeout=15) as client:
        resp = await client.request(method, f"/api{path}", headers=headers, **kw)
    if resp.status_code >= 400:
        return {"error": resp.status_code, "detail": resp.text[:500]}
    return resp.json() if resp.content else {"ok": True}


async def search_tweets(query: str, limit: int = 10) -> Any:
    """Full-text search over all tweets. Supports words and #hashtags."""
    return await _api("GET", "/v1/search/tweets", params={"q": query, "limit": limit})


async def get_home_timeline(limit: int = 20) -> Any:
    """Return the signed-in user's home timeline (tweets from accounts they follow)."""
    page = await _api("GET", "/v1/timelines/home", params={"limit": limit})
    return page.get("items", page) if isinstance(page, dict) else page


async def get_user_profile(username: str) -> Any:
    """Look up a user's public profile by @username (without the @)."""
    return await _api("GET", f"/v1/users/by-username/{username.lstrip('@')}")


async def get_trending_hashtags(limit: int = 10) -> Any:
    """Return currently trending hashtags with tweet counts."""
    return await _api("GET", "/v1/trends", params={"limit": limit})


async def post_tweet(text: str) -> Any:
    """Publish a tweet (max 280 characters) as the signed-in user."""
    return await _api("POST", "/v1/tweets", json={"text": text[:280]})


async def like_tweet(tweet_id: str) -> Any:
    """Like a tweet by id as the signed-in user."""
    return await _api("POST", f"/v1/tweets/{tweet_id}/like")


async def follow_user(username: str) -> Any:
    """Follow another user by @username."""
    profile = await get_user_profile(username)
    if "id" not in profile:
        return profile
    return await _api("POST", f"/v1/users/{profile['id']}/follow")


async def get_system_health() -> Any:
    """Operational health of every microservice plus recent anomalies detected by AIOps."""
    return {"health": await _api("GET", "/v1/aiops/health"),
            "recent_anomalies": await _api("GET", "/v1/aiops/anomalies", params={"limit": 10})}


ToolFn = Callable[..., Awaitable[Any]]

# name -> (function, JSON schema of its input, has side effects?)
TOOLS: dict[str, tuple[ToolFn, dict[str, Any], bool]] = {
    "search_tweets": (search_tweets, {"type": "object", "properties": {
        "query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 50}},
        "required": ["query"], "additionalProperties": False}, False),
    "get_home_timeline": (get_home_timeline, {"type": "object", "properties": {
        "limit": {"type": "integer", "minimum": 1, "maximum": 100}}, "additionalProperties": False}, False),
    "get_user_profile": (get_user_profile, {"type": "object", "properties": {
        "username": {"type": "string"}}, "required": ["username"], "additionalProperties": False}, False),
    "get_trending_hashtags": (get_trending_hashtags, {"type": "object", "properties": {
        "limit": {"type": "integer", "minimum": 1, "maximum": 50}}, "additionalProperties": False}, False),
    "post_tweet": (post_tweet, {"type": "object", "properties": {
        "text": {"type": "string", "maxLength": 280}}, "required": ["text"], "additionalProperties": False}, True),
    "like_tweet": (like_tweet, {"type": "object", "properties": {
        "tweet_id": {"type": "string"}}, "required": ["tweet_id"], "additionalProperties": False}, True),
    "follow_user": (follow_user, {"type": "object", "properties": {
        "username": {"type": "string"}}, "required": ["username"], "additionalProperties": False}, True),
    "get_system_health": (get_system_health, {"type": "object", "properties": {},
                                              "additionalProperties": False}, False),
}


def claude_tool_definitions(allow_side_effects: bool) -> list[dict[str, Any]]:
    return [
        {"name": name, "description": (fn.__doc__ or "").strip(), "input_schema": schema}
        for name, (fn, schema, side_effect) in TOOLS.items()
        if allow_side_effects or not side_effect
    ]
