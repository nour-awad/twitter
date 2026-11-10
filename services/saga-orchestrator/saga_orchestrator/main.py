"""saga-orchestrator: *orchestrated* Saga for user registration (Weeks 5-7).

Registering a user touches four services, each with its own database, so a
single ACID transaction is impossible. The Saga pattern replaces it with a
sequence of local transactions + compensating actions:

    step                 action                         compensation
    -------------------  -----------------------------  ------------------------
    1 create_credentials auth  POST /internal/credentials  DELETE credentials
    2 create_profile     user  POST /internal/users        DELETE user (emits user.deleted)
    3 init_timeline      timeline POST /internal/timelines DELETE timeline
    4 send_welcome       notification POST welcome      (retriable, after pivot)

If step N fails, steps N-1..1 are compensated in reverse order. The saga
log is persisted, so after a crash the orchestrator *recovers* on start-up
by compensating half-finished sagas (self-healing, Week 10).

Contrast with account deletion, which is choreographed via user.deleted
events (no central coordinator) -- see docs/07-orchestration-vs-choreography.md.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable
from uuid import uuid4

from fastapi import HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Integer, String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from twitter_common.config import AUTH_URL, NOTIFICATION_URL, TIMELINE_URL, USER_URL, database_url
from twitter_common.db import create_schema, db_ready_check, make_engine, make_session_factory, utcnow
from twitter_common.observability import SAGAS
from twitter_common.resilience import DependencyUnavailable, ResilientClient
from twitter_common.security import ServiceTokenProvider
from twitter_common.service import create_service_app

log = logging.getLogger(__name__)
SERVICE = "saga-orchestrator"


class Base(DeclarativeBase):
    pass


class SagaInstance(Base):
    __tablename__ = "saga_instances"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    saga_type: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), index=True)  # RUNNING|COMPLETED|COMPENSATING|COMPENSATED|FAILED
    context: Mapped[str] = mapped_column(Text)  # never contains secrets
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SagaStep(Base):
    __tablename__ = "saga_steps"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    saga_id: Mapped[str] = mapped_column(String(36), index=True)
    step: Mapped[str] = mapped_column(String(64))
    phase: Mapped[str] = mapped_column(String(16))  # execute|compensate
    status: Mapped[str] = mapped_column(String(16))  # ok|failed
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


engine = make_engine(database_url("saga"))
Session = make_session_factory(engine)
tokens = ServiceTokenProvider(SERVICE)
clients: dict[str, ResilientClient] = {}


class StepFailed(Exception):
    def __init__(self, step: str, status_code: int, detail: str) -> None:
        super().__init__(f"{step} failed ({status_code}): {detail}")
        self.step, self.status_code, self.detail = step, status_code, detail


async def _call(client: str, method: str, path: str, step: str, body: dict | None = None) -> dict:
    try:
        resp = await clients[client].request(method, path, json=body)
    except DependencyUnavailable as exc:
        raise StepFailed(step, 503, str(exc)) from exc
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except ValueError:
            detail = resp.text
        raise StepFailed(step, resp.status_code, str(detail))
    return resp.json() if resp.content else {}


# ============================================================ saga definition
@dataclass
class Step:
    name: str
    action: Callable[[dict], Awaitable[None]]
    compensation: Callable[[dict], Awaitable[None]] | None
    retriable: bool = False  # steps after the pivot must eventually succeed, never compensate


async def create_credentials(ctx: dict) -> None:
    await _call("auth", "POST", "/internal/credentials", "create_credentials",
                {"user_id": ctx["user_id"], "username": ctx["username"], "email": ctx["email"],
                 "password": ctx["password"]})


async def delete_credentials(ctx: dict) -> None:
    await _call("auth", "DELETE", f"/internal/credentials/{ctx['user_id']}", "delete_credentials")


async def create_profile(ctx: dict) -> None:
    await _call("user", "POST", "/internal/users", "create_profile",
                {"user_id": ctx["user_id"], "username": ctx["username"], "display_name": ctx["display_name"]})


async def delete_profile(ctx: dict) -> None:
    await _call("user", "DELETE", f"/internal/users/{ctx['user_id']}", "delete_profile")


async def init_timeline(ctx: dict) -> None:
    await _call("timeline", "POST", "/internal/timelines", "init_timeline", {"user_id": ctx["user_id"]})


async def drop_timeline(ctx: dict) -> None:
    await _call("timeline", "DELETE", f"/internal/timelines/{ctx['user_id']}", "drop_timeline")


async def send_welcome(ctx: dict) -> None:
    await _call("notification", "POST", "/internal/notifications/welcome", "send_welcome",
                {"user_id": ctx["user_id"], "username": ctx["username"]})


REGISTRATION_SAGA = [
    Step("create_credentials", create_credentials, delete_credentials),
    Step("create_profile", create_profile, delete_profile),
    Step("init_timeline", init_timeline, drop_timeline),
    Step("send_welcome", send_welcome, None, retriable=True),
]
STEPS_BY_NAME = {s.name: s for s in REGISTRATION_SAGA}


async def _log(saga_id: str, step: str, phase: str, status_: str, detail: str | None = None) -> None:
    async with Session() as s, s.begin():
        s.add(SagaStep(saga_id=saga_id, step=step, phase=phase, status=status_, detail=detail))


async def _set_status(saga_id: str, status_: str, error: str | None = None) -> None:
    async with Session() as s, s.begin():
        saga = await s.get(SagaInstance, saga_id)
        saga.status = status_
        if error:
            saga.error = error


async def compensate(saga_id: str, ctx: dict, completed: list[str]) -> None:
    await _set_status(saga_id, "COMPENSATING")
    for name in reversed(completed):
        step = STEPS_BY_NAME[name]
        if step.compensation is None:
            continue
        try:
            await step.compensation(ctx)
            await _log(saga_id, name, "compensate", "ok")
        except Exception as exc:  # noqa: BLE001
            # A failed compensation needs a human (or a retry job): mark FAILED.
            await _log(saga_id, name, "compensate", "failed", str(exc))
            await _set_status(saga_id, "FAILED", f"compensation of {name} failed: {exc}")
            SAGAS.labels("registration", "failed").inc()
            return
    await _set_status(saga_id, "COMPENSATED")
    SAGAS.labels("registration", "compensated").inc()


async def run_registration(ctx: dict) -> tuple[SagaInstance, StepFailed | None]:
    saga_id = str(uuid4())
    public_ctx = {k: v for k, v in ctx.items() if k != "password"}  # never persist secrets
    async with Session() as s, s.begin():
        s.add(SagaInstance(id=saga_id, saga_type="registration", status="RUNNING", context=json.dumps(public_ctx)))
    completed: list[str] = []
    failure: StepFailed | None = None
    for step in REGISTRATION_SAGA:
        attempts = 3 if step.retriable else 1
        for attempt in range(attempts):
            try:
                await step.action(ctx)
                await _log(saga_id, step.name, "execute", "ok")
                completed.append(step.name)
                break
            except StepFailed as exc:
                await _log(saga_id, step.name, "execute", "failed", str(exc))
                if attempt == attempts - 1:
                    if step.retriable:
                        log.warning("retriable step %s still failing; leaving for later", step.name)
                        break
                    failure = exc
        if failure:
            break
    if failure:
        await compensate(saga_id, ctx, completed)
    else:
        await _set_status(saga_id, "COMPLETED")
        SAGAS.labels("registration", "completed").inc()
    async with Session() as s:
        return await s.get(SagaInstance, saga_id), failure


async def recover_incomplete_sagas() -> int:
    """On start-up: anything still RUNNING/COMPENSATING was interrupted by a crash."""
    async with Session() as s:
        stuck = (await s.execute(select(SagaInstance).where(
            SagaInstance.status.in_(["RUNNING", "COMPENSATING"])))).scalars().all()
    for saga in stuck:
        async with Session() as s:
            steps = (await s.execute(select(SagaStep).where(SagaStep.saga_id == saga.id)
                                     .order_by(SagaStep.id))).scalars().all()
        done = [st.step for st in steps if st.phase == "execute" and st.status == "ok"]
        undone = {st.step for st in steps if st.phase == "compensate" and st.status == "ok"}
        log.warning("recovering saga %s (%s); compensating %s", saga.id, saga.status, done)
        await compensate(saga.id, json.loads(saga.context), [d for d in done if d not in undone])
    return len(stuck)


@asynccontextmanager
async def lifespan(app):  # type: ignore[no-untyped-def]
    await create_schema(engine, Base)
    for name, url in {"auth": AUTH_URL(), "user": USER_URL(), "timeline": TIMELINE_URL(),
                      "notification": NOTIFICATION_URL()}.items():
        clients[name] = ResilientClient(name, url, service=SERVICE, timeout=5.0, token_provider=tokens)
    app.state.recovered = 0

    async def recover() -> None:
        app.state.recovered = await recover_incomplete_sagas()

    app.state.runner.spawn("saga-recovery", recover)
    yield
    await app.state.runner.stop()
    for c in clients.values():
        await c.aclose()
    await engine.dispose()


app = create_service_app(
    name=SERVICE,
    title="Saga Orchestrator",
    description="Orchestrated sagas with persisted saga log, compensation and crash recovery.",
    lifespan=lifespan,
    ready_checks={"database": db_ready_check(engine)},
)


class RegistrationRequest(BaseModel):
    username: str = Field(min_length=3, max_length=32, pattern=r"^[A-Za-z0-9_]+$")
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    password: str = Field(min_length=8)
    display_name: str | None = Field(default=None, max_length=64)


class SagaOut(BaseModel):
    saga_id: str
    saga_type: str
    status: str
    context: dict
    error: str | None
    steps: list[dict] = []
    created_at: datetime


async def describe(saga_id: str) -> SagaOut:
    async with Session() as s:
        saga = await s.get(SagaInstance, saga_id)
        if saga is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "saga not found")
        steps = (await s.execute(select(SagaStep).where(SagaStep.saga_id == saga_id)
                                 .order_by(SagaStep.id))).scalars().all()
    return SagaOut(saga_id=saga.id, saga_type=saga.saga_type, status=saga.status, context=json.loads(saga.context),
                   error=saga.error, created_at=saga.created_at,
                   steps=[{"step": st.step, "phase": st.phase, "status": st.status, "detail": st.detail,
                           "at": st.at.isoformat()} for st in steps])


@app.post("/v1/sagas/registration", status_code=201, tags=["sagas"])
async def register(body: RegistrationRequest):  # type: ignore[no-untyped-def]
    ctx = {"user_id": str(uuid4()), "username": body.username, "email": str(body.email),
           "password": body.password, "display_name": body.display_name or body.username}
    saga, failure = await run_registration(ctx)
    result = (await describe(saga.id)).model_dump(mode="json")
    if failure is None:
        return {"user_id": ctx["user_id"], "username": body.username, "saga": result}
    code = failure.status_code if failure.status_code in (409, 422) else 503
    return JSONResponse({"error": failure.detail, "detail": failure.detail, "saga": result}, status_code=code)


@app.get("/v1/sagas/{saga_id}", response_model=SagaOut, tags=["sagas"])
async def get_saga(saga_id: str) -> SagaOut:
    return await describe(saga_id)


@app.get("/v1/sagas", response_model=list[SagaOut], tags=["sagas"])
async def list_sagas(status_filter: str | None = Query(None, alias="status"),
                     limit: int = Query(20, le=100)) -> list[SagaOut]:
    async with Session() as s:
        q = select(SagaInstance.id).order_by(SagaInstance.created_at.desc()).limit(limit)
        if status_filter:
            q = q.where(SagaInstance.status == status_filter.upper())
        ids = (await s.execute(q)).scalars().all()
    return [await describe(i) for i in ids]
