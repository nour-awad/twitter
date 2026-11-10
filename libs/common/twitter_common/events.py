"""Event contracts (Week 4: Event-Driven Architecture).

Every integration event is wrapped in a CNCF CloudEvents 1.0 envelope so that
it can travel over Kafka, Dapr pub/sub or Knative Eventing unchanged.
The `data` payloads are versioned pydantic models: they *are* the
contract between producers and consumers (see contracts/asyncapi.yaml).

Topics are per aggregate (users, tweets, engagement, ...) and every message
is keyed by the aggregate id -> Kafka guarantees per-aggregate ordering.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class CloudEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    specversion: str = "1.0"
    id: str = Field(default_factory=lambda: str(uuid4()))
    source: str
    type: str
    subject: str | None = None
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    datacontenttype: str = "application/json"
    dataschema: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    # CloudEvents extensions (distributed tracing + correlation, Week 11)
    traceparent: str | None = None
    correlationid: str | None = None


# --------------------------------------------------------------------- topics
class Topics:
    USERS = "twitter.users"
    TWEETS = "twitter.tweets"
    ENGAGEMENT = "twitter.engagement"
    MODERATION = "twitter.moderation"
    AIOPS = "twitter.aiops"


# --------------------------------------------------------------- event types
class EventTypes:
    USER_CREATED = "user.created.v1"
    USER_UPDATED = "user.updated.v1"
    USER_DELETED = "user.deleted.v1"
    USER_FOLLOWED = "user.followed.v1"
    USER_UNFOLLOWED = "user.unfollowed.v1"

    TWEET_CREATED = "tweet.created.v1"
    TWEET_DELETED = "tweet.deleted.v1"

    TWEET_LIKED = "engagement.tweet_liked.v1"
    TWEET_UNLIKED = "engagement.tweet_unliked.v1"
    TWEET_RETWEETED = "engagement.tweet_retweeted.v1"
    RETWEET_UNDONE = "engagement.retweet_undone.v1"

    TWEET_FLAGGED = "moderation.tweet_flagged.v1"
    ANOMALY_DETECTED = "aiops.anomaly_detected.v1"


# ------------------------------------------------------------- data payloads
class UserCreated(BaseModel):
    user_id: str
    username: str
    display_name: str
    bio: str = ""


class UserUpdated(BaseModel):
    user_id: str
    username: str
    display_name: str
    bio: str = ""


class UserDeleted(BaseModel):
    user_id: str
    username: str | None = None


class UserFollowed(BaseModel):
    follower_id: str
    followee_id: str
    follower_username: str | None = None


class UserUnfollowed(BaseModel):
    follower_id: str
    followee_id: str


class TweetCreated(BaseModel):
    tweet_id: str
    author_id: str
    author_username: str
    text: str
    hashtags: list[str] = []
    mentions: list[str] = []
    reply_to: str | None = None
    created_at: datetime


class TweetDeleted(BaseModel):
    tweet_id: str
    author_id: str


class EngagementChanged(BaseModel):
    tweet_id: str
    user_id: str
    tweet_author_id: str | None = None
    like_count: int
    retweet_count: int


class TweetFlagged(BaseModel):
    tweet_id: str
    author_id: str
    reason: str
    score: float
    model: str


class AnomalyDetected(BaseModel):
    service: str
    metric: str
    value: float
    baseline: float
    zscore: float
    probable_root_cause: str | None = None


EVENT_SCHEMAS: dict[str, type[BaseModel]] = {
    EventTypes.USER_CREATED: UserCreated,
    EventTypes.USER_UPDATED: UserUpdated,
    EventTypes.USER_DELETED: UserDeleted,
    EventTypes.USER_FOLLOWED: UserFollowed,
    EventTypes.USER_UNFOLLOWED: UserUnfollowed,
    EventTypes.TWEET_CREATED: TweetCreated,
    EventTypes.TWEET_DELETED: TweetDeleted,
    EventTypes.TWEET_LIKED: EngagementChanged,
    EventTypes.TWEET_UNLIKED: EngagementChanged,
    EventTypes.TWEET_RETWEETED: EngagementChanged,
    EventTypes.RETWEET_UNDONE: EngagementChanged,
    EventTypes.TWEET_FLAGGED: TweetFlagged,
    EventTypes.ANOMALY_DETECTED: AnomalyDetected,
}


def make_event(
    event_type: str,
    data: BaseModel | dict[str, Any],
    *,
    source: str,
    subject: str | None = None,
) -> CloudEvent:
    """Build a CloudEvent, validating `data` against the registered contract."""
    schema = EVENT_SCHEMAS.get(event_type)
    if isinstance(data, BaseModel):
        payload = data.model_dump(mode="json")
    elif schema is not None:
        payload = schema.model_validate(data).model_dump(mode="json")
    else:
        payload = dict(data)

    from twitter_common.observability import current_correlation_id, current_traceparent

    return CloudEvent(
        source=f"/twitter/{source}",
        type=event_type,
        subject=subject,
        data=payload,
        dataschema=f"https://twitter.local/schemas/{event_type}.json" if schema else None,
        traceparent=current_traceparent(),
        correlationid=current_correlation_id(),
    )


def parse_data(event: CloudEvent) -> BaseModel | dict[str, Any]:
    """Validate an incoming event against its contract (tolerant reader)."""
    schema = EVENT_SCHEMAS.get(event.type)
    return schema.model_validate(event.data) if schema else event.data


def json_schemas() -> dict[str, Any]:
    """Export JSON Schemas for every event (used to generate AsyncAPI docs)."""
    return {t: m.model_json_schema() for t, m in EVENT_SCHEMAS.items()}
