"""Redis fixtures for External Workflow Streams tests (X2).

One Redis server is shared by the whole suite, so every test gets its own key
prefix rather than its own server. The prefix embeds the ``pytest-xdist``
worker id, so parallel workers cannot collide even when two of them run the
same test module.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest
import pytest_asyncio

#: Asking for either of these is what gives a case a running server. A case
#: that asks for neither never observes a clock, so no environment can fail it.
_SERVER_FIXTURES = frozenset({"client", "env"})


@pytest.fixture(autouse=True)
def skip_under_time_skipping(request: pytest.FixtureRequest) -> None:
    """Hold the server-backed cases to a clock the tests can reason about.

    Those cases measure real-server timing: how long a Workflow Task was held
    open, the interval a wake sweep runs on, the deadline a shutdown waits out.
    The time-skipping server advances the clock whenever workers go idle, which
    removes exactly the quantities being measured, so the failures it produces
    say nothing about the feature. Which of them fail drifts run to run, so the
    server-backed cases are held as a group rather than by name. Everything
    else here is offline and keeps running on both environments.
    """
    if _SERVER_FIXTURES.isdisjoint(request.fixturenames):
        return
    if request.getfixturevalue("env").supports_time_skipping:
        pytest.skip("this case measures real-server timing; see conftest")


DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"

#: Every key this suite creates starts with this, so a leaked key is
#: attributable and a global cleanup is possible without guessing.
KEY_NAMESPACE = "temporal-extstream-test"


def redis_url() -> str:
    return os.getenv("TEMPORAL_TEST_REDIS_URL", DEFAULT_REDIS_URL)


@dataclass
class RedisKeyspace:
    """An isolated slice of the shared Redis server.

    ``client`` is a live ``redis.asyncio.Redis``. Every key a test touches must
    be built with :meth:`key`, which is what makes the teardown complete.
    """

    client: "object"  # redis.asyncio.Redis, untyped to keep redis an optional import
    prefix: str

    def key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def cleanup(self) -> int:
        """Delete every key under this keyspace. Returns the number removed."""
        removed = 0
        batch: list[str] = []
        async for found in self.client.scan_iter(match=f"{self.prefix}*", count=500):  # type: ignore[attr-defined]
            batch.append(found)
            if len(batch) >= 500:
                removed += await self.client.delete(*batch)  # type: ignore[attr-defined]
                batch = []
        if batch:
            removed += await self.client.delete(*batch)  # type: ignore[attr-defined]
        return removed


@pytest.fixture(scope="session")
def redis_worker_id(request: pytest.FixtureRequest) -> str:
    """The ``pytest-xdist`` worker id, or ``master`` when running serially."""
    return getattr(request.config, "workerinput", {}).get("workerid", "master")


@pytest_asyncio.fixture(scope="session")  # type: ignore[reportUntypedFunctionDecorator]
async def redis_client() -> AsyncGenerator[object, None]:
    """A session-wide connection, skipping the suite if no server is reachable.

    Reachability is checked once per session rather than per test so a missing
    Redis produces one clear skip reason instead of one per case.
    """
    redis = pytest.importorskip("redis.asyncio", reason="redis is not installed")

    client = redis.from_url(redis_url(), decode_responses=True)
    try:
        await client.ping()
    except Exception as err:
        await client.aclose()
        pytest.skip(f"Redis is not reachable at {redis_url()}: {err}")

    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def redis_keyspace(
    redis_client: object, redis_worker_id: str
) -> AsyncGenerator[RedisKeyspace, None]:
    """An isolated key prefix, cleaned up whether or not the test passed."""
    prefix = f"{KEY_NAMESPACE}:{redis_worker_id}:{uuid.uuid4().hex}:"
    keyspace = RedisKeyspace(client=redis_client, prefix=prefix)
    try:
        yield keyspace
    finally:
        await keyspace.cleanup()
