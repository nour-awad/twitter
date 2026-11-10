"""user-service: profiles and the social graph.

Exposes two API styles over the same domain model (Week 3):
  * REST (FastAPI)  - public, resource-oriented API for clients.
  * gRPC (port 50051) - internal, contract-first, binary, low-latency API for
    other services (unary, batch and server-streaming RPCs).

Every state change is published as an integration event through the
transactional outbox (users topic).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime

import grpc
from fastapi import Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, DateTime, String, Text, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.bus import create_event_bus
from twitter_common.config import database_url, env_int
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.events import (EventTypes, Topics, UserCreated, UserDeleted, UserFollowed,
                                   UserUnfollowed, UserUpdated, make_event)
from twitter_common.outbox import OutboxRelay, add_to_outbox, make_inbox_model, make_outbox_model
from twitter_common.security import Principal, authorize_grpc, require_scope, require_service
from twitter_common.service import create_service_app
from twitter_protos.user.v1 import user_pb2, user_pb2_grpc

SERVICE = "user-service"
GRPC_PORT = env_int("USER_GRPC_PORT", 50051)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    username: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(64))
    bio: Mapped[str] = mapped_column(Text, default="")
    verified: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Follow(Base):
    __tablename__ = "follows"
    follower_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    followee_id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


Outbox = make_outbox_model(Base)
Inbox = make_inbox_model(Base)
engine = make_engine(database_url("user"))
Session = make_session_factory(engine)
bus = create_event_bus(SERVICE)
relay = OutboxRelay(SERVICE, Session, Outbox, bus)


def publish(s: AsyncSession, event_type: str, data: BaseModel, key: str) -> None:
    add_to_outbox(s, Outbox, Topics.USERS, make_event(event_type, data, source=SERVICE, subject=key), key)


async def counts(s: AsyncSession, user_id: str) -> tuple[int, int]:
    followers = await s.scalar(select(func.count()).select_from(Follow).where(Follow.followee_id == user_id))
    following = await s.scalar(select(func.count()).select_from(Follow).where(Follow.follower_id == user_id))
    return int(followers or 0), int(following or 0)


class UserOut(BaseModel):
    id: str
    username: str
    display_name: str
    bio: str
    verified: bool
    created_at: datetime
    followers_count: int = 0
    following_count: int = 0


async def to_out(s: AsyncSession, u: User) -> UserOut:
    followers, following = await counts(s, u.id)
    return UserOut(id=u.id, username=u.username, display_name=u.display_name, bio=u.bio or "",
                   verified=u.verified, created_at=u.created_at,
                   followers_count=followers, following_count=following)


# =============================================================== gRPC server
async def to_proto(s: AsyncSession, u: User) -> user_pb2.User:
    followers, following = await counts(s, u.id)
    return user_pb2.User(id=u.id, username=u.username, display_name=u.display_name, bio=u.bio or "",
                         created_at_unix=int(u.created_at.timestamp()),
                         followers_count=followers, following_count=following)


class UserServicer(user_pb2_grpc.UserServiceServicer):
    async def GetUser(self, request, context):  # type: ignore[no-untyped-def]
        await authorize_grpc(context, "users:read")
        async with Session() as s:
            u = await s.get(User, request.user_id)
            if u is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, "user not found")
            return await to_proto(s, u)

    async def BatchGetUsers(self, request, context):  # type: ignore[no-untyped-def]
        await authorize_grpc(context, "users:read")
        async with Session() as s:
            rows = (await s.execute(select(User).where(User.id.in_(list(request.user_ids))))).scalars().all()
            return user_pb2.BatchGetUsersResponse(users={u.id: await to_proto(s, u) for u in rows})

    async def ListFollowers(self, request, context):  # type: ignore[no-untyped-def]
        await authorize_grpc(context, "users:read")
        async with Session() as s:
            q = select(User).join(Follow, Follow.follower_id == User.id).where(Follow.followee_id == request.user_id)
            if request.limit:
                q = q.limit(request.limit)
            for u in (await s.execute(q)).scalars():
                yield await to_proto(s, u)

    async def IsFollowing(self, request, context):  # type: ignore[no-untyped-def]
        await authorize_grpc(context, "users:read")
        async with Session() as s:
            f = await s.get(Follow, (request.follower_id, request.followee_id))
            return user_pb2.IsFollowingResponse(following=f is not None)


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await create_schema(engine, Base)
    await bus.start()
    app.state.runner.spawn("outbox-relay", relay.run)
    server = grpc.aio.server()
    user_pb2_grpc.add_UserServiceServicer_to_server(UserServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await server.start()
    yield
    await server.stop(grace=2)
    await app.state.runner.stop()
    await bus.stop()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="User Service",
    description="Profiles and follow graph. REST for clients, gRPC (port 50051) for services.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)
if hasattr(bus, "mount"):
    bus.mount(app)  # type: ignore[attr-defined]


# =============================================================== REST
class UpdateProfile(BaseModel):
    display_name: str | None = Field(default=None, max_length=64)
    bio: str | None = Field(default=None, max_length=280)


class ProvisionUser(BaseModel):
    user_id: str
    username: str
    display_name: str


async def _get_or_404(s: AsyncSession, user_id: str) -> User:
    u = await s.get(User, user_id)
    if u is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found")
    return u


@app.get("/v1/users/by-username/{username}", response_model=UserOut, tags=["users"])
async def get_by_username(username: str) -> UserOut:
    async with Session() as s:
        u = (await s.execute(select(User).where(User.username == username))).scalar_one_or_none()
        if u is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found")
        return await to_out(s, u)


@app.get("/v1/users", response_model=list[UserOut], tags=["users"])
async def list_users(q: str | None = None, limit: int = Query(20, le=100)) -> list[UserOut]:
    async with Session() as s:
        stmt = select(User).order_by(User.created_at.desc()).limit(limit)
        if q:
            stmt = stmt.where(or_(User.username.contains(q), User.display_name.contains(q)))
        return [await to_out(s, u) for u in (await s.execute(stmt)).scalars()]


@app.get("/v1/users/{user_id}", response_model=UserOut, tags=["users"])
async def get_user(user_id: str) -> UserOut:
    async with Session() as s:
        return await to_out(s, await _get_or_404(s, user_id))


@app.patch("/v1/users/me", response_model=UserOut, tags=["users"])
async def update_me(body: UpdateProfile, me: Principal = Depends(require_scope("profile:write"))) -> UserOut:
    async with Session() as s:
        async with s.begin():
            u = await _get_or_404(s, me.sub)
            if body.display_name is not None:
                u.display_name = body.display_name
            if body.bio is not None:
                u.bio = body.bio
            publish(s, EventTypes.USER_UPDATED,
                    UserUpdated(user_id=u.id, username=u.username, display_name=u.display_name, bio=u.bio), u.id)
        relay.notify()
        return await to_out(s, u)


@app.delete("/v1/users/me", status_code=204, tags=["users"])
async def delete_me(me: Principal = Depends(require_scope("profile:write"))) -> None:
    """Account deletion is *choreographed*: we publish user.deleted and every
    service that holds data about the user cleans up on its own."""
    async with Session() as s, s.begin():
        u = await _get_or_404(s, me.sub)
        await s.execute(delete(Follow).where(or_(Follow.follower_id == u.id, Follow.followee_id == u.id)))
        await s.delete(u)
        publish(s, EventTypes.USER_DELETED, UserDeleted(user_id=u.id, username=u.username), u.id)
    relay.notify()


@app.post("/v1/users/{user_id}/follow", status_code=201, tags=["graph"])
async def follow(user_id: str, me: Principal = Depends(require_scope("profile:write"))) -> dict:
    if user_id == me.sub:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "cannot follow yourself")
    async with Session() as s, s.begin():
        await _get_or_404(s, user_id)
        if await s.get(Follow, (me.sub, user_id)) is None:  # idempotent
            s.add(Follow(follower_id=me.sub, followee_id=user_id))
            publish(s, EventTypes.USER_FOLLOWED,
                    UserFollowed(follower_id=me.sub, followee_id=user_id, follower_username=me.username), user_id)
    relay.notify()
    return {"follower_id": me.sub, "followee_id": user_id, "following": True}


@app.delete("/v1/users/{user_id}/follow", tags=["graph"])
async def unfollow(user_id: str, me: Principal = Depends(require_scope("profile:write"))) -> dict:
    async with Session() as s, s.begin():
        f = await s.get(Follow, (me.sub, user_id))
        if f is not None:
            await s.delete(f)
            publish(s, EventTypes.USER_UNFOLLOWED, UserUnfollowed(follower_id=me.sub, followee_id=user_id), user_id)
    relay.notify()
    return {"follower_id": me.sub, "followee_id": user_id, "following": False}


@app.get("/v1/users/{user_id}/followers", response_model=list[UserOut], tags=["graph"])
async def followers(user_id: str, limit: int = Query(50, le=200)) -> list[UserOut]:
    async with Session() as s:
        q = (select(User).join(Follow, Follow.follower_id == User.id)
             .where(Follow.followee_id == user_id).limit(limit))
        return [await to_out(s, u) for u in (await s.execute(q)).scalars()]


@app.get("/v1/users/{user_id}/following", response_model=list[UserOut], tags=["graph"])
async def following(user_id: str, limit: int = Query(50, le=200)) -> list[UserOut]:
    async with Session() as s:
        q = (select(User).join(Follow, Follow.followee_id == User.id)
             .where(Follow.follower_id == user_id).limit(limit))
        return [await to_out(s, u) for u in (await s.execute(q)).scalars()]


# ---- saga participant endpoints ------------------------------------------
@app.post("/internal/users", status_code=201, tags=["internal"])
async def provision_user(body: ProvisionUser, _: Principal = Depends(require_service("users:provision"))) -> dict:
    async with Session() as s, s.begin():
        if await s.get(User, body.user_id) is not None:
            return {"user_id": body.user_id}  # idempotent
        clash = (await s.execute(select(User).where(User.username == body.username))).scalar_one_or_none()
        if clash:
            raise HTTPException(status.HTTP_409_CONFLICT, "username already taken")
        s.add(User(id=body.user_id, username=body.username, display_name=body.display_name))
        publish(s, EventTypes.USER_CREATED,
                UserCreated(user_id=body.user_id, username=body.username, display_name=body.display_name),
                body.user_id)
    relay.notify()
    return {"user_id": body.user_id}


@app.delete("/internal/users/{user_id}", status_code=204, tags=["internal"])
async def deprovision_user(user_id: str, _: Principal = Depends(require_service("users:provision"))) -> None:
    """Compensation: undo profile creation and tell everyone who saw user.created."""
    async with Session() as s, s.begin():
        u = await s.get(User, user_id)
        if u is None:
            return
        await s.delete(u)
        publish(s, EventTypes.USER_DELETED, UserDeleted(user_id=user_id, username=u.username), user_id)
    relay.notify()


