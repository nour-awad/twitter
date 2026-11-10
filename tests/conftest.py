"""Test harness: boots the full system in-process (real HTTP + gRPC, in-memory broker)."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest_asyncio

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

BASE_PORT = int(os.getenv("TEST_BASE_PORT", "19000"))
os.environ.setdefault("RATE_LIMIT_BURST", "10000")
os.environ.setdefault("RATE_LIMIT_RPS", "10000")
os.environ.setdefault("AIOPS_INTERVAL_SECONDS", "3600")
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.pop("ANTHROPIC_API_KEY", None)  # tests must not spend money; AI uses fallbacks

import devstack  # noqa: E402

devstack.configure(BASE_PORT, data_dir=tempfile.mkdtemp(prefix="twitter-test-"))

GATEWAY = f"http://127.0.0.1:{BASE_PORT}"


def port(offset: int) -> str:
    return f"http://127.0.0.1:{BASE_PORT + offset}"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def stack():
    servers = devstack.build_servers(BASE_PORT)
    tasks = [asyncio.create_task(s.serve()) for _, _, s in servers]
    for _ in range(200):
        if all(s.started for _, _, s in servers):
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("stack did not start")
    yield GATEWAY
    for _, _, s in servers:
        s.should_exit = True
    await asyncio.gather(*tasks, return_exceptions=True)
