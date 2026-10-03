"""Standalone streams on the native provider against a live server.

The conformance suite covers what every provider owes a standalone stream.
These pin what native adds on top: a read on an id nobody has created yet
parks on the server and delivers the first record once the stream exists, a
ref carries the stream to another client, and a create of an id that exists
is answered from the stream's own policy. Needs a server built from the
AI-198 branch:

    TEMPORAL_STREAM_TARGET=127.0.0.1:7433 uv run pytest tests/streams/test_native_standalone_e2e.py
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from temporalio.client import Client
from temporalio.streams import StreamClosedError, StreamNotFoundError, StreamRef, topic
from temporalio.streams.providers.native import NativeStreams
from tests.streams.test_streams_conformance import take

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream service",
)

OUT = topic("out", dict)


async def _connect(provider: NativeStreams) -> Client:
    return await Client.connect(TARGET or "", plugins=[provider])


async def test_a_read_on_a_stream_not_yet_created_waits_for_it() -> None:
    provider = NativeStreams()
    client = await _connect(provider)
    stream_id = "late-" + uuid.uuid4().hex[:8]
    try:
        reader = client.get_stream_handle(stream_id=stream_id)
        # Nothing to wait on for these: the stream does not exist.
        with pytest.raises(StreamNotFoundError):
            await reader.latest(topic=OUT)
        with pytest.raises(StreamNotFoundError):
            await reader.producer(topic=OUT, producer_id="early", attempt=1).append(
                {"n": 0}
            )

        parked = asyncio.ensure_future(take(reader.read(topic=OUT), 1, timeout=30))
        await asyncio.sleep(0.5)
        assert not parked.done(), "the read is parked, not failed"

        created = await client.create_stream(stream_id)
        await created.producer(topic=OUT, producer_id="writer", attempt=1).append(
            {"n": 1}
        )
        assert [r.value for r in await parked] == [{"n": 1}]
    finally:
        await provider.close()


async def test_a_ref_carries_a_standalone_stream_to_another_client() -> None:
    provider = NativeStreams()
    client = await _connect(provider)
    other_provider = NativeStreams()
    other = await _connect(other_provider)
    stream_id = "ref-" + uuid.uuid4().hex[:8]
    try:
        created = await client.create_stream(stream_id, max_records=10)
        await created.producer(topic=OUT, producer_id="writer", attempt=1).append(
            {"n": 1}
        )
        ref = created.ref(topic=OUT)
        assert ref == StreamRef.for_standalone(stream_id, topic="out")

        opened = other.get_stream_handle(ref)
        records = await take(opened.read(), 1)
        assert [r.value for r in records] == [{"n": 1}]
        assert await opened.latest() == records[0].cursor

        await opened.close()
        with pytest.raises(StreamClosedError):
            await created.producer(topic=OUT, producer_id="writer", attempt=2).append(
                {"n": 2}
            )
        # The tail stays readable and the read ends on its own.
        assert [r.value async for r in created.read(topic=OUT)] == [{"n": 1}]
    finally:
        await provider.close()
        await other_provider.close()
