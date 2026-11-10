"""End-to-end tests against the full in-process stack (real HTTP and gRPC between services)."""

from __future__ import annotations

import uuid

import grpc
import httpx
import pytest

import demo
from conftest import port

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_full_user_journey(stack: str) -> None:
    result = await demo.run(stack, verbose=False)
    assert result["tweet"]["text"].count("*") >= 3  # serverless content filter applied
    async with httpx.AsyncClient(base_url=stack) as c:
        bob = result["tokens"]
        timeline = (await c.get("/api/v1/timelines/home", headers=list(bob.values())[1])).json()
        assert timeline["items"][0]["tweet_id"] == result["tweet"]["id"]
        assert timeline["items"][0]["like_count"] == 1  # engagement projected into the read model
        sagas = (await c.get("/api/v1/sagas", params={"status": "COMPENSATED"})).json()
        # duplicate username failed at step 1 -> nothing to undo, saga still ends COMPENSATED
        assert sagas and sagas[0]["steps"][-1] == {**sagas[0]["steps"][-1], "step": "create_credentials",
                                                   "status": "failed"}


async def test_saga_compensates_when_a_later_step_fails(stack: str) -> None:
    name = "saga_" + uuid.uuid4().hex[:6]
    async with httpx.AsyncClient(base_url=stack) as c:
        await c.put(f"{port(4)}/internal/chaos", json={"error_rate": 1.0})  # timeline-service down
        try:
            r = await c.post("/api/v1/register", json={"username": name, "email": f"{name}@x.io",
                                                       "password": "s3cret-pass"})
        finally:
            await c.put(f"{port(4)}/internal/chaos", json={"error_rate": 0.0})
        assert r.status_code == 503
        saga = r.json()["saga"]
        assert saga["status"] == "COMPENSATED"
        assert [(s["step"], s["phase"]) for s in saga["steps"]] == [
            ("create_credentials", "execute"), ("create_profile", "execute"), ("init_timeline", "execute"),
            ("create_profile", "compensate"), ("create_credentials", "compensate")]
        # the local transactions really were undone
        login = await c.post("/api/v1/auth/login", json={"username": name, "password": "s3cret-pass"})
        assert login.status_code == 401
        assert (await c.get(f"/api/v1/users/by-username/{name}")).status_code == 404
        # ... and the username is free again
        assert (await register(c, name))[0]


async def register(c: httpx.AsyncClient, name: str | None = None) -> tuple[str, dict]:
    name = name or "u_" + uuid.uuid4().hex[:8]
    r = await c.post("/api/v1/register", json={"username": name, "email": f"{name}@x.io", "password": "s3cret-pass"})
    assert r.status_code == 201, r.text
    tok = (await c.post("/api/v1/auth/login", json={"username": name, "password": "s3cret-pass"})).json()
    return r.json()["user_id"], {"authorization": f"Bearer {tok['access_token']}"}


async def test_zero_trust_internal_endpoints_reject_user_tokens(stack: str) -> None:
    async with httpx.AsyncClient(base_url=stack) as c:
        _, user_headers = await register(c)
        # A *user* token must not be able to call a service-only endpoint, even directly.
        r = await c.post(f"{port(2)}/internal/users", headers=user_headers,
                         json={"user_id": "x", "username": "hacker", "display_name": "h"})
        assert r.status_code == 401
        r = await c.get("/api/v1/timelines/home", headers={"authorization": "Bearer forged.token.value"})
        assert r.status_code == 401


async def test_grpc_requires_service_identity(stack: str) -> None:
    from twitter_protos.user.v1 import user_pb2, user_pb2_grpc

    async with grpc.aio.insecure_channel(f"127.0.0.1:{int(port(51).rsplit(':', 1)[1])}") as ch:
        stub = user_pb2_grpc.UserServiceStub(ch)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await stub.GetUser(user_pb2.GetUserRequest(user_id="nobody"))
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED


async def test_grpc_streaming_followers(stack: str) -> None:
    from twitter_common.clients.user_grpc import UserGrpcClient
    from twitter_common.security import ServiceTokenProvider

    async with httpx.AsyncClient(base_url=stack) as c:
        star_id, _ = await register(c)
        for _ in range(3):
            _, h = await register(c)
            assert (await c.post(f"/api/v1/users/{star_id}/follow", headers=h)).status_code == 201
    client = UserGrpcClient("tests", ServiceTokenProvider("timeline-service"))
    followers = [u async for u in client.iter_followers(star_id)]
    await client.close()
    assert len(followers) == 3


async def test_choreographed_account_deletion(stack: str) -> None:
    async with httpx.AsyncClient(base_url=stack) as c:
        uid, h = await register(c)
        tweet = (await c.post("/api/v1/tweets", headers=h, json={"text": "soon gone #bye"})).json()
        assert (await c.delete("/api/v1/users/me", headers=h)).status_code == 204

        async def tweet_gone():  # type: ignore[no-untyped-def]
            return (await c.get(f"/api/v1/tweets/{tweet['id']}")).status_code == 404

        async def login_revoked():  # type: ignore[no-untyped-def]
            name = tweet["author_username"]
            r = await c.post("/api/v1/auth/login", json={"username": name, "password": "s3cret-pass"})
            return r.status_code == 401

        assert await demo.eventually(tweet_gone)  # tweet-service reacted to user.deleted
        assert await demo.eventually(login_revoked)  # auth-service reacted to user.deleted


async def test_circuit_breaker_fails_fast_under_chaos(stack: str) -> None:
    import api_gateway.main as gw

    async with httpx.AsyncClient(base_url=stack) as c:
        await c.put(f"{port(7)}/internal/chaos", json={"error_rate": 1.0, "latency_ms": 0})
        try:
            statuses = [(await c.get("/api/v1/search/tweets", params={"q": "x"})).json().get("error")
                        for _ in range(7)]
            assert "circuit_open" in statuses  # gateway stopped calling the failing service
            # graceful degradation elsewhere: timelines still work while search is down
            _, h = await register(c)
            assert (await c.get("/api/v1/timelines/home", headers=h)).status_code == 200
        finally:
            await c.put(f"{port(7)}/internal/chaos", json={"error_rate": 0.0, "latency_ms": 0})
            gw.upstreams["search"].breaker.record_success()


async def test_mcp_tools_over_gateway(stack: str) -> None:
    import httpx2
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    async with httpx.AsyncClient(base_url=stack) as c:
        _, h = await register(c)
    async with httpx2.AsyncClient(headers=h) as hc:
        async with streamable_http_client(f"{stack}/mcp", http_client=hc) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                names = {t.name for t in (await s.list_tools()).tools}
                assert {"post_tweet", "search_tweets", "get_system_health"} <= names
                res = await s.call_tool("post_tweet", {"text": "hello from an agent #mcp"})
                assert "hello from an agent" in res.content[0].text
