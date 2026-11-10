"""Resilient gRPC client for the user-service (Week 3 gRPC + Week 10 resilience).

Every call has a deadline, is retried on transient status codes, goes through
a circuit breaker and carries this service's own identity token (zero trust).
"""

from __future__ import annotations

from typing import Any, AsyncIterator

import grpc
import httpx
from google.protobuf.json_format import MessageToDict

from twitter_common.config import USER_GRPC_TARGET
from twitter_common.observability import outgoing_headers
from twitter_common.resilience import CircuitBreaker, DependencyUnavailable, retry_async
from twitter_common.security import ServiceTokenProvider
from twitter_protos.user.v1 import user_pb2, user_pb2_grpc

TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED}


class _Transient(Exception):
    pass


def user_to_dict(user: user_pb2.User) -> dict[str, Any]:
    return MessageToDict(user, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)


class UserGrpcClient:
    def __init__(self, service: str, tokens: ServiceTokenProvider, target: str | None = None,
                 timeout: float = 1.5) -> None:
        self.channel = grpc.aio.insecure_channel(  # mTLS is terminated by the mesh sidecar
            target or USER_GRPC_TARGET(),
            options=[("grpc.keepalive_time_ms", 30000), ("grpc.enable_retries", 0)],
        )
        self.stub = user_pb2_grpc.UserServiceStub(self.channel)
        self.tokens = tokens
        self.timeout = timeout
        self.breaker = CircuitBreaker("user-grpc", service=service, failure_threshold=5, recovery_timeout=5)

    async def _metadata(self) -> list[tuple[str, str]]:
        try:
            token = await self.tokens.token()
        except httpx.HTTPError as exc:  # cannot obtain an identity => treat as transient
            raise _Transient(f"auth-service unavailable: {exc}") from exc
        md = [("authorization", f"Bearer {token}")]
        md += list(outgoing_headers().items())
        return md

    async def _unary(self, method: Any, request: Any) -> Any:
        async def once() -> Any:
            async def call() -> Any:
                try:
                    return await method(request, timeout=self.timeout, metadata=await self._metadata())
                except grpc.aio.AioRpcError as exc:
                    if exc.code() in TRANSIENT:
                        raise _Transient(exc.details()) from exc
                    raise

            return await self.breaker.call(call)

        try:
            return await retry_async(once, attempts=3, retry_on=(_Transient,))
        except _Transient as exc:
            raise DependencyUnavailable(f"user-service unavailable: {exc}") from exc

    async def get_user(self, user_id: str) -> dict[str, Any] | None:
        try:
            user = await self._unary(self.stub.GetUser, user_pb2.GetUserRequest(user_id=user_id))
        except grpc.aio.AioRpcError as exc:
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
        return user_to_dict(user)

    async def batch_get_users(self, user_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not user_ids:
            return {}
        resp = await self._unary(self.stub.BatchGetUsers, user_pb2.BatchGetUsersRequest(user_ids=list(set(user_ids))))
        return {uid: user_to_dict(u) for uid, u in resp.users.items()}

    async def iter_followers(self, user_id: str, limit: int = 0) -> AsyncIterator[dict[str, Any]]:
        call = self.stub.ListFollowers(
            user_pb2.ListFollowersRequest(user_id=user_id, limit=limit),
            timeout=10, metadata=await self._metadata(),
        )
        async for user in call:
            yield user_to_dict(user)

    async def close(self) -> None:
        await self.channel.close()
