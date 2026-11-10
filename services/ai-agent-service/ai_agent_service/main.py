"""ai-agent-service: AI-native service-oriented architecture (Week 14).

Three ways AI changes the notion of a "service":

1. Services as *tools*  - Twitter's capabilities are published through an
   MCP server at /mcp. Any MCP client (Claude Desktop/Code, IDEs, other
   agents) discovers them at runtime from their descriptions & schemas.
2. Agents as *orchestrators* - POST /v1/ai/agent takes a natural-language
   goal; Claude plans and calls the tools (dynamic, model-driven
   orchestration instead of a hard-coded workflow).
3. AI as an *event consumer* - every tweet.created event is moderated and,
   if needed, a moderation.tweet_flagged event is published (choreography).

Runs without an API key: deterministic fallbacks keep the system working
(graceful degradation applies to AI dependencies too).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import typing
from collections import Counter
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, HTTPException
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, Field

from ai_agent_service.tools import TOOLS, claude_tool_definitions, current_token, get_home_timeline
from twitter_common.bus import create_event_bus
from twitter_common.config import env, env_bool
from twitter_common.events import CloudEvent, EventTypes, Topics, TweetFlagged, make_event
from twitter_common.security import Principal, require_scope
from twitter_common.service import create_service_app

log = logging.getLogger(__name__)
SERVICE = "ai-agent-service"
MODEL = env("CLAUDE_MODEL", "claude-opus-5-5")
LLM_ENABLED = bool(env("ANTHROPIC_API_KEY") or env("ANTHROPIC_AUTH_TOKEN")) or env_bool("AI_LLM_ENABLED")
FALLBACK_BETA = "server-side-fallback-2026-07-01"

bus = create_event_bus(SERVICE)
_client = None


def claude():  # type: ignore[no-untyped-def]
    global _client
    if not LLM_ENABLED:
        raise HTTPException(503, "LLM not configured: set ANTHROPIC_API_KEY to enable Claude-powered features")
    if _client is None:
        import anthropic

        _client = anthropic.AsyncAnthropic()
    return _client


async def ask_claude(**params: Any) -> Any:
    """One Messages API call with the house defaults (adaptive thinking, refusal fallback)."""
    resp = await claude().beta.messages.create(
        model=MODEL,
        thinking={"type": "adaptive"},
        betas=[FALLBACK_BETA],
        fallbacks="default",  # on a policy refusal the API re-runs the request on a fallback model
        **params,
    )
    if resp.stop_reason == "refusal":
        raise HTTPException(422, "the model declined this request")
    return resp


def text_of(resp: Any) -> str:
    return "".join(b.text for b in resp.content if b.type == "text").strip()


# ============================================================ moderation
BLOCKLIST = {"scam", "giveaway-click", "hate", "kill", "spamlink"}


def heuristic_moderation(text: str) -> tuple[bool, str, float]:
    words = set(re.findall(r"\w+", text.lower()))
    hits = sorted(words & BLOCKLIST)
    shouting = sum(c.isupper() for c in text) / max(1, sum(c.isalpha() for c in text))
    score = min(1.0, 0.45 * len(hits) + (0.3 if shouting > 0.8 and len(text) > 20 else 0.0))
    return score >= 0.45, (f"matched terms: {', '.join(hits)}" if hits else "excessive capitals"), score


MODERATION_SCHEMA = {
    "type": "object",
    "properties": {"flagged": {"type": "boolean"}, "reason": {"type": "string"}, "score": {"type": "number"}},
    "required": ["flagged", "reason", "score"],
    "additionalProperties": False,
}


async def llm_moderation(text: str) -> tuple[bool, str, float]:
    resp = await ask_claude(
        max_tokens=1024,
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": MODERATION_SCHEMA}},
        system="You are a content moderator for a social network. Flag harassment, hate, threats, scams "
               "and spam. Ordinary opinions, jokes and criticism are fine. score is 0..1 severity.",
        messages=[{"role": "user", "content": f"Tweet:\n{text}"}],
    )
    data = json.loads(text_of(resp))
    return bool(data["flagged"]), str(data["reason"]), float(data["score"])


async def on_tweet_created(event: CloudEvent) -> None:
    if event.type != EventTypes.TWEET_CREATED:
        return
    text = event.data["text"]
    use_llm = LLM_ENABLED and env_bool("AI_MODERATION_LLM", False)
    if use_llm:
        flagged, reason, score = await llm_moderation(text)
    else:
        flagged, reason, score = heuristic_moderation(text)
    if flagged:
        payload = TweetFlagged(tweet_id=event.data["tweet_id"], author_id=event.data["author_id"],
                               reason=reason, score=score, model=MODEL if use_llm else "heuristic-v1")
        await bus.publish(Topics.MODERATION, make_event(EventTypes.TWEET_FLAGGED, payload, source=SERVICE,
                                                        subject=payload.tweet_id), key=payload.tweet_id)


# ============================================================ MCP server
mcp = MCPServer(
    "twitter",
    instructions="Tools for a Twitter-like social network: search, read timelines, profiles, trends, "
                 "post, like, follow, and inspect system health. Send the user's bearer token in the "
                 "Authorization header; tools act with exactly that user's permissions.",
)


def _mcp_tool(name: str, fn: Any) -> Any:
    """Adapt a plain tool function to MCP: inject the caller's token from the request headers."""
    hints = typing.get_type_hints(fn)
    params = [inspect.Parameter(p.name, inspect.Parameter.KEYWORD_ONLY, default=p.default,
                                annotation=hints.get(p.name, Any))
              for p in inspect.signature(fn).parameters.values()]
    params.append(inspect.Parameter("ctx", inspect.Parameter.KEYWORD_ONLY, annotation=Context))

    async def wrapper(**kwargs: Any) -> Any:
        ctx = kwargs.pop("ctx")
        auth = (ctx.headers or {}).get("authorization", "")
        tok = current_token.set(auth[7:] if auth.lower().startswith("bearer ") else None)
        try:
            return await fn(**kwargs)
        finally:
            current_token.reset(tok)

    wrapper.__name__ = name
    wrapper.__doc__ = fn.__doc__
    wrapper.__signature__ = inspect.Signature(params, return_annotation=Any)  # type: ignore[attr-defined]
    wrapper.__annotations__ = {p.name: p.annotation for p in params} | {"return": Any}
    return wrapper


for _name, (_fn, _schema, _side_effect) in TOOLS.items():
    mcp.tool(name=_name, description=_fn.__doc__, structured_output=False)(_mcp_tool(_name, _fn))

mcp_app = mcp.streamable_http_app(
    streamable_http_path="/mcp",
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await bus.start()
    await bus.subscribe(Topics.TWEETS, on_tweet_created)
    async with mcp.session_manager.run():
        yield
    await app.state.runner.stop()
    await bus.stop()


app = create_service_app(
    name=SERVICE,
    title="AI Agent Service",
    description="MCP tool server (/mcp), Claude-powered agent, timeline summaries and AI moderation.",
    lifespan=lifespan,
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


# ============================================================ REST endpoints
class AgentRequest(BaseModel):
    instruction: str = Field(min_length=1, max_length=4000)
    allow_actions: bool = Field(False, description="Allow tools with side effects (post, like, follow)")
    max_steps: int = Field(8, ge=1, le=20)


SYSTEM_PROMPT = (
    "You are an assistant embedded in a Twitter-like social network. Use the tools to look things up and, "
    "only when the user explicitly asks, to act on their behalf. Keep answers concise. Tweets you write must "
    "be under 280 characters."
)


@app.post("/v1/ai/agent", tags=["ai"])
async def run_agent(body: AgentRequest, me: Principal = Depends(require_scope("ai:use"))) -> dict:
    """Natural-language goal -> Claude plans and calls Twitter tools -> answer + tool trace."""
    tok = current_token.set(me.token)
    try:
        tools = claude_tool_definitions(body.allow_actions)
        allowed = {t["name"] for t in tools}
        messages: list[dict[str, Any]] = [{"role": "user", "content": body.instruction}]
        trace: list[dict[str, Any]] = []
        resp = None
        for _ in range(body.max_steps):
            resp = await ask_claude(max_tokens=16000, output_config={"effort": "medium"},
                                    system=SYSTEM_PROMPT, tools=tools, messages=messages)
            messages.append({"role": "assistant", "content": resp.content})
            if resp.stop_reason != "tool_use":
                break
            calls = [b for b in resp.content if b.type == "tool_use"]

            async def execute(call: Any) -> dict[str, Any]:
                if call.name not in allowed:
                    return {"type": "tool_result", "tool_use_id": call.id, "is_error": True,
                            "content": "tool not permitted for this request"}
                try:
                    result = await TOOLS[call.name][0](**call.input)
                    trace.append({"tool": call.name, "input": call.input})
                    return {"type": "tool_result", "tool_use_id": call.id,
                            "content": json.dumps(result, default=str)[:20000]}
                except Exception as exc:  # noqa: BLE001
                    return {"type": "tool_result", "tool_use_id": call.id, "is_error": True, "content": str(exc)}

            # parallel tool calls -> all results go back in ONE user message
            messages.append({"role": "user", "content": list(await asyncio.gather(*(execute(c) for c in calls)))})
        return {"answer": text_of(resp) if resp else "", "tool_calls": trace, "model": MODEL,
                "stop_reason": resp.stop_reason if resp else None}
    finally:
        current_token.reset(tok)


@app.post("/v1/ai/timeline-summary", tags=["ai"])
async def timeline_summary(me: Principal = Depends(require_scope("ai:use"))) -> dict:
    tok = current_token.set(me.token)
    try:
        items = await get_home_timeline(50)
    finally:
        current_token.reset(tok)
    if not isinstance(items, list) or not items:
        return {"summary": "Your timeline is empty -- follow some people!", "source": "none"}
    if not LLM_ENABLED:
        tags = Counter(h for t in items for h in t.get("hashtags", []))
        top = sorted(items, key=lambda t: t.get("like_count", 0), reverse=True)[:3]
        return {"source": "extractive-fallback",
                "summary": f"{len(items)} tweets. Top topics: "
                           f"{', '.join('#' + h for h, _ in tags.most_common(3)) or 'none'}. "
                           "Most liked: " + " | ".join(f"@{t['author_username']}: {t['text'][:60]}" for t in top)}
    lines = "\n".join(f"@{t['author_username']} ({t.get('like_count', 0)} likes): {t['text']}" for t in items)
    resp = await ask_claude(max_tokens=2048, output_config={"effort": "low"},
                            messages=[{"role": "user", "content": "Summarize what is happening in this timeline in "
                                       f"3-5 bullet points, citing @authors:\n\n{lines}"}])
    return {"summary": text_of(resp), "source": MODEL}


class ComposeRequest(BaseModel):
    topic: str = Field(min_length=1, max_length=500)
    tone: str = "friendly"


@app.post("/v1/ai/compose", tags=["ai"])
async def compose(body: ComposeRequest, _: Principal = Depends(require_scope("ai:use"))) -> dict:
    if not LLM_ENABLED:
        tag = "#" + re.sub(r"\W", "", body.topic.title())[:30]
        return {"draft": f"Thinking about {body.topic} today. What's your take? {tag}"[:280],
                "source": "template-fallback"}
    resp = await ask_claude(max_tokens=1024, output_config={"effort": "low"},
                            messages=[{"role": "user", "content": f"Write one tweet (<=280 chars, {body.tone} tone) "
                                       f"about: {body.topic}. Reply with the tweet text only."}])
    return {"draft": text_of(resp)[:280], "source": MODEL}


@app.get("/v1/ai/tools", tags=["ai"])
async def list_tools() -> dict:
    """The tool catalogue (same one MCP clients discover via tools/list)."""
    return {"llm_enabled": LLM_ENABLED, "model": MODEL, "mcp_endpoint": "/mcp",
            "tools": claude_tool_definitions(allow_side_effects=True)}


# MCP (Streamable HTTP) is served at /mcp. Mounted last so FastAPI routes take precedence.
app.mount("/", mcp_app)
