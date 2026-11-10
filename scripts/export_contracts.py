#!/usr/bin/env python
"""Export every API contract into contracts/ (Week 3: contract-first & API governance).

  contracts/openapi/<service>.json   REST contracts (OpenAPI 3)
  contracts/asyncapi.yaml            event contracts (AsyncAPI 3) built from the pydantic event models
  proto/                             gRPC contracts (hand-written source of truth)

CI fails if these files are out of date (`git diff --exit-code contracts/`), so
any interface change is visible -- and reviewable -- in the pull request.
"""

from __future__ import annotations

import importlib
import json
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import devstack  # noqa: E402

devstack.configure(8000, data_dir=tempfile.mkdtemp())

from twitter_common.events import EVENT_SCHEMAS, EventTypes, Topics  # noqa: E402

TOPIC_OF = {
    "user.": Topics.USERS, "tweet.": Topics.TWEETS, "engagement.": Topics.ENGAGEMENT,
    "moderation.": Topics.MODERATION, "aiops.": Topics.AIOPS,
}


def export_openapi() -> None:
    out = ROOT / "contracts" / "openapi"
    out.mkdir(parents=True, exist_ok=True)
    for name, target, _, _ in devstack.SERVICES:
        module, attr = target.split(":")
        app = getattr(importlib.import_module(module), attr)
        (out / f"{name}.json").write_text(json.dumps(app.openapi(), indent=2, default=str) + "\n")


def export_asyncapi() -> None:
    channels, operations, messages, schemas = {}, {}, {}, {}
    for event_type, model in EVENT_SCHEMAS.items():
        topic = next(t for prefix, t in TOPIC_OF.items() if event_type.startswith(prefix))
        cid = topic.replace(".", "_")
        mid = event_type.replace(".", "_")
        schema = model.model_json_schema(ref_template="#/components/schemas/{model}")
        for k, v in schema.pop("$defs", {}).items():
            schemas[k] = v
        schemas[model.__name__] = schema
        messages[mid] = {
            "name": event_type,
            "contentType": "application/cloudevents+json",
            "headers": {"type": "object", "properties": {"ce_type": {"const": event_type}}},
            "payload": {"$ref": f"#/components/schemas/{model.__name__}"},
        }
        channels.setdefault(cid, {"address": topic, "messages": {}})["messages"][mid] = {
            "$ref": f"#/components/messages/{mid}"}
    for cid in channels:
        operations[f"publish_{cid}"] = {"action": "send", "channel": {"$ref": f"#/channels/{cid}"}}
    doc = {
        "asyncapi": "3.0.0",
        "info": {"title": "Twitter integration events", "version": "1.0.0",
                 "description": "CloudEvents 1.0 envelopes on Kafka; one topic per aggregate, key = aggregate id."},
        "servers": {"kafka": {"host": "redpanda:9092", "protocol": "kafka"}},
        "channels": channels, "operations": operations,
        "components": {"messages": messages, "schemas": schemas},
    }
    (ROOT / "contracts" / "asyncapi.yaml").write_text(yaml.safe_dump(doc, sort_keys=False))


if __name__ == "__main__":
    _ = EventTypes
    export_openapi()
    export_asyncapi()
    print("contracts exported to", ROOT / "contracts")
