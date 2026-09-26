"""End-to-end tests for the server-side stream client.

These need a Temporal server built from the AI-198 branch, because the stream
service does not exist on a released server. Point them at one:

    TEMPORAL_STREAM_TARGET=127.0.0.1:7333 uv run pytest tests/test_client_stream.py

Skipped otherwise, rather than silently passing against a server that has no
idea what a stream is. ``TEMPORAL_STREAM_SERVER_PREDATES_KIND_DEFAULT=1`` skips
the one case that needs the server to store an unset kind as ``DATA``, for a
branch build older than that rule.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import StreamRecord, StreamRecordKind
from temporalio.client_stream import StreamClient, StreamHandle
from temporalio.service import RPCError
from temporalio.streams import StreamNotFoundError, StreamProducerError

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream service",
)


def rec(body: bytes, topic: str = "") -> StreamRecord:
    return StreamRecord(
        body=Payload(data=body, metadata={"encoding": b"binary/plain"}), topic=topic
    )


def data(entries: list) -> list[bytes]:
    return [entry.record.body.data for entry in entries]


@pytest.fixture
async def streams() -> AsyncIterator[StreamClient]:
    client = StreamClient.connect(
        TARGET or "", os.environ.get("TEMPORAL_NAMESPACE", "default")
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def stream(streams: StreamClient) -> StreamHandle:
    return await streams.create("py-test-" + uuid.uuid4().hex[:8], max_items=1000)


async def test_append_returns_the_offset_it_landed_at(stream: StreamHandle) -> None:
    first = await stream.append(rec(b"alpha"), rec(b"beta"))
    assert (first.first_offset, first.next_offset, first.count) == (0, 2, 2)
    assert not first.deduplicated
    assert (await stream.append(rec(b"gamma"))).first_offset == 2


async def test_read_from_an_offset(stream: StreamHandle) -> None:
    await stream.append(rec(b"alpha"), rec(b"beta"), rec(b"gamma"))

    entries, next_offset = await stream.read()
    assert data(entries) == [b"alpha", b"beta", b"gamma"]
    assert [entry.offset for entry in entries] == [0, 1, 2]
    assert next_offset == 3

    entries, next_offset = await stream.read(from_offset=1)
    assert data(entries) == [b"beta", b"gamma"]
    assert next_offset == 3


async def test_read_past_the_end_returns_nothing(stream: StreamHandle) -> None:
    await stream.append(rec(b"alpha"))

    entries, next_offset = await stream.read(from_offset=1)
    assert entries == []
    assert next_offset == 1, "a caught-up reader keeps its cursor"


# Without an identity the append is at-least-once, so this is the only way a
# producer that retries can avoid writing the same record twice.
async def test_append_is_idempotent_for_a_named_producer(stream: StreamHandle) -> None:
    first = await stream.append(rec(b"once"), producer_id="p1", sequence=1)
    retry = await stream.append(rec(b"once"), producer_id="p1", sequence=1)
    assert first.first_offset == retry.first_offset == 0
    assert not first.deduplicated
    # The retry is told it wrote nothing and where the original landed.
    assert retry.deduplicated
    assert retry.next_offset == first.next_offset

    entries, _ = await stream.read()
    assert data(entries) == [b"once"]


async def test_topics_filter_a_read(stream: StreamHandle) -> None:
    await stream.append(rec(b"tok", topic="tokens"))
    await stream.append(rec(b"prog", topic="progress"))
    await stream.append(rec(b"tok2", topic="tokens"))

    entries, next_offset = await stream.read(topics=["tokens"])
    assert data(entries) == [b"tok", b"tok2"]
    assert [entry.offset for entry in entries] == [0, 2]
    assert next_offset == 3, "the cursor advances past filtered-out offsets too"


# The record comes back as it went in: kind, producer identity and sequence
# are the server's to store, not to interpret.
async def test_the_record_roundtrips_field_for_field(stream: StreamHandle) -> None:
    sent = rec(b"x", topic="t")
    sent.producer_id = "model"
    sent.attempt = 2
    sent.sequence = 7
    sent.kind = StreamRecordKind.STREAM_RECORD_KIND_DATA
    sent.metadata["trace"].CopyFrom(Payload(data=b"abc"))
    finish = StreamRecord(
        topic="t", kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH, producer_id="model"
    )
    await stream.append(sent, finish)

    entries, _ = await stream.read()
    got = entries[0].record
    assert (got.topic, got.producer_id, got.attempt, got.sequence) == (
        "t",
        "model",
        2,
        7,
    )
    assert got.kind == StreamRecordKind.STREAM_RECORD_KIND_DATA
    assert got.metadata["trace"].data == b"abc"
    assert entries[1].record.kind == StreamRecordKind.STREAM_RECORD_KIND_FINISH
    assert not entries[1].record.HasField("body")


@pytest.mark.skipif(
    bool(os.environ.get("TEMPORAL_STREAM_SERVER_PREDATES_KIND_DEFAULT")),
    reason="the target server stores an unset kind as sent",
)
async def test_an_unset_kind_reads_back_as_data(stream: StreamHandle) -> None:
    await stream.append(rec(b"x"))
    entries, _ = await stream.read()
    assert entries[0].record.kind == StreamRecordKind.STREAM_RECORD_KIND_DATA


# A closed stream stays readable, which is what removes the shutdown handshake
# between producer and consumer.
async def test_follow_drains_then_ends_when_the_stream_closes(
    stream: StreamHandle,
) -> None:
    received: list[bytes] = []

    async def follower() -> None:
        async for entry in stream.follow():
            received.append(entry.record.body.data)

    task = asyncio.create_task(follower())
    await stream.append(rec(b"one"))
    await asyncio.sleep(0.2)
    await stream.append(rec(b"two"))
    await asyncio.sleep(0.2)
    await stream.close()

    await asyncio.wait_for(task, timeout=30)
    assert received == [b"one", b"two"]


async def test_follow_started_late_still_sees_everything(
    stream: StreamHandle,
) -> None:
    await stream.append(rec(b"one"), rec(b"two"))
    await stream.close()

    received = [entry.record.body.data async for entry in stream.follow()]
    assert received == [b"one", b"two"]


async def test_describe_reports_the_frontier(stream: StreamHandle) -> None:
    await stream.append(rec(b"one"), rec(b"two"))

    state = await stream.describe()
    assert state.head_offset == 2
    assert not state.closed

    await stream.close()
    assert (await stream.describe()).closed


# The transport's own exception never escapes: a missing stream is the SDK's
# not-found error and anything else is its RPC error.
async def test_failures_surface_as_sdk_errors(streams: StreamClient) -> None:
    missing = streams.get("py-test-missing-" + uuid.uuid4().hex[:8])
    with pytest.raises(StreamNotFoundError):
        await missing.describe()
    with pytest.raises((StreamNotFoundError, RPCError)):
        await missing.append(rec(b"x"))


# A producer that asked to be deduplicated and could not be is a condition of
# its own, not a bare argument error: the store already holds that sequence.
async def test_a_producer_conflict_is_a_stream_producer_error(
    stream: StreamHandle,
) -> None:
    await stream.append(rec(b"one"), producer_id="p1", sequence=0)
    # Same producer and sequence, different content. The server cannot know
    # which of the two the reader was meant to see.
    with pytest.raises(StreamProducerError, match="different content"):
        await stream.append(rec(b"other"), producer_id="p1", sequence=0)
    # And a sequence behind the one it accepted last.
    await stream.append(rec(b"two"), producer_id="p1", sequence=1)
    with pytest.raises(StreamProducerError, match="stale producer sequence"):
        await stream.append(rec(b"three"), producer_id="p1", sequence=0)

    entries, _ = await stream.read()
    assert data(entries) == [b"one", b"two"]
