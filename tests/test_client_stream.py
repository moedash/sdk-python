"""End-to-end tests for the server-side stream client.

These need a Temporal server built from the AI-198 branch, because the stream
service does not exist on a released server. Point them at one:

    TEMPORAL_STREAM_TARGET=localhost:7233 uv run pytest tests/test_client_stream.py

Skipped otherwise, rather than silently passing against a server that has no
idea what a stream is.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import AsyncIterator

import pytest

from temporalio.client_stream import StreamClient, StreamHandle

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream service",
)


@pytest.fixture
async def stream() -> AsyncIterator[StreamHandle]:
    client = StreamClient.connect(TARGET or "", os.environ.get("TEMPORAL_NAMESPACE", "default"))
    try:
        yield await client.create("py-test-" + uuid.uuid4().hex[:8], max_items=1000)
    finally:
        await client.close()


async def test_append_returns_the_offset_it_landed_at(stream: StreamHandle) -> None:
    assert await stream.append(b"alpha", b"beta") == 0
    assert await stream.append(b"gamma") == 2


async def test_read_from_an_offset(stream: StreamHandle) -> None:
    await stream.append(b"alpha", b"beta", b"gamma")

    messages, next_offset = await stream.read()
    assert [m.data for m in messages] == [b"alpha", b"beta", b"gamma"]
    assert next_offset == 3

    messages, next_offset = await stream.read(from_offset=1)
    assert [m.data for m in messages] == [b"beta", b"gamma"]
    assert next_offset == 3


async def test_read_past_the_end_returns_nothing(stream: StreamHandle) -> None:
    await stream.append(b"alpha")

    messages, next_offset = await stream.read(from_offset=1)
    assert messages == []
    assert next_offset == 1, "a caught-up reader keeps its cursor"


# Without an identity the append is at-least-once, so this is the only way a
# producer that retries can avoid writing the same message twice.
async def test_append_is_idempotent_for_a_named_producer(stream: StreamHandle) -> None:
    first = await stream.append(b"once", producer_id="p1", sequence=1)
    retry = await stream.append(b"once", producer_id="p1", sequence=1)
    assert first == retry == 0

    messages, _ = await stream.read()
    assert [m.data for m in messages] == [b"once"]


async def test_topics_filter_a_read(stream: StreamHandle) -> None:
    await stream.append(b"tok", topic="tokens")
    await stream.append(b"prog", topic="progress")
    await stream.append(b"tok2", topic="tokens")

    messages, next_offset = await stream.read(topics=["tokens"])
    assert [m.data for m in messages] == [b"tok", b"tok2"]
    assert next_offset == 3, "the cursor advances past filtered-out offsets too"


# A closed stream stays readable, which is what removes the shutdown handshake
# between producer and consumer.
async def test_follow_drains_then_ends_when_the_stream_closes(
    stream: StreamHandle,
) -> None:
    received: list[bytes] = []

    async def follower() -> None:
        async for message in stream.follow():
            received.append(message.data)

    task = asyncio.create_task(follower())
    await stream.append(b"one")
    await asyncio.sleep(0.2)
    await stream.append(b"two")
    await asyncio.sleep(0.2)
    await stream.close()

    await asyncio.wait_for(task, timeout=30)
    assert received == [b"one", b"two"]


async def test_follow_started_late_still_sees_everything(
    stream: StreamHandle,
) -> None:
    await stream.append(b"one", b"two")
    await stream.close()

    received = [m.data async for m in stream.follow()]
    assert received == [b"one", b"two"]


async def test_describe_reports_the_frontier(stream: StreamHandle) -> None:
    await stream.append(b"one", b"two")

    state = await stream.describe()
    assert state.head_offset == 2
    assert not state.closed

    await stream.close()
    assert (await stream.describe()).closed
