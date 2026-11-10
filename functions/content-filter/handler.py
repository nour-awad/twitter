"""content-filter: a serverless function (Week 9).

Stateless, single-purpose, scale-to-zero. Deployed as a Knative Service
(deploy/knative/content-filter.yaml) it costs nothing while idle and scales
out automatically under load. tweet-service calls it through a circuit
breaker and simply skips filtering if the function is cold or failing.

It also accepts CloudEvents (Knative Eventing / Dapr bindings) so the same
function could be triggered asynchronously by tweet.created events.
"""

from __future__ import annotations

import os
import re

from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI(title="content-filter (serverless function)", version="1.0.0")

WORDS = [w.strip() for w in os.getenv("FILTER_WORDS", "darn,heck,spamlink").split(",") if w.strip()]
PATTERN = re.compile(r"\b(" + "|".join(map(re.escape, WORDS)) + r")\b", re.IGNORECASE) if WORDS else None


class FilterRequest(BaseModel):
    text: str


class FilterResponse(BaseModel):
    clean_text: str
    flagged: bool
    matches: list[str]


def apply(text: str) -> FilterResponse:
    if PATTERN is None:
        return FilterResponse(clean_text=text, flagged=False, matches=[])
    matches = [m.group(0) for m in PATTERN.finditer(text)]
    clean = PATTERN.sub(lambda m: m.group(0)[0] + "*" * (len(m.group(0)) - 1), text)
    return FilterResponse(clean_text=clean, flagged=bool(matches), matches=matches)


@app.post("/", response_model=FilterResponse)
async def invoke(request: Request) -> FilterResponse:
    body = await request.json()
    # CloudEvent (structured or binary mode) or plain JSON
    data = body.get("data", body) if isinstance(body, dict) else {}
    return apply(FilterRequest.model_validate(data).text)


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}
