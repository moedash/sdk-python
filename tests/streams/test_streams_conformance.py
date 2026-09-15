"""Conformance tests for the stream contract.

Written against the public surface plus the in-memory reference provider, so
they run with no server and no store. A storage provider reuses the same
expectations through its own fixtures; what this file pins down is the
contract: framing, producer identity, retry deduplication, supersession,
topic filtering, and cursor resumption.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from temporalio import streams
from temporalio.streams import _frame
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import Cursor, RecordKind
from temporalio.streams.providers import memory


@pytest.fixture(autouse=True)
def _fresh_memory_provider():
    memory.reset()
    streams.configure(provider="memory")
    yield
    memory.reset()


async def take(records: Any, count: int, timeout: float = 5.0) -> list:
    out: list = []

    async def _collect() -> None:
        async for record in records:
            out.append(record)
            if len(out) >= count:
                return

    await asyncio.wait_for(_collect(), timeout)
    return out


def test_frame_roundtrip():
    frame = _frame.encode(
        topic="decisions",
        kind=RecordKind.DATA,
        producer="model",
        attempt=3,
        sequence=7,
        body=b"payload-bytes",
    )
    kind, topic, producer, attempt, sequence, body = _frame.decode(frame)
    assert (kind, topic, producer, attempt, sequence, body) == (
        RecordKind.DATA,
        "decisions",
        "model",
        3,
        7,
        b"payload-bytes",
    )


def test_supersession_is_synthesized_from_observations():
    attempts = AttemptTracker()
    assert attempts.note("model", 1, Cursor("0")) is None
    superseded = attempts.note("model", 2, Cursor("1"))
    assert superseded is not None
    assert superseded.kind is RecordKind.SUPERSEDED
    assert superseded.value.previous_attempt == 1
    # The same attempt again is not a new generation.
    assert attempts.note("model", 2, Cursor("2")) is None


async def test_append_read_roundtrip():
    producer = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await producer.append({"id": "r1"}, {"id": "r2"})
    await producer.finish()

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict), 3)
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert [r.value for r in records[:2]] == [{"id": "r1"}, {"id": "r2"}]
    assert all(r.producer == "model" and r.attempt == 1 for r in records)
    assert [r.sequence for r in records] == [0, 1, 2]


async def test_retried_append_is_deduplicated():
    first = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await first.append({"id": "r1"})
    # The retry of the same attempt starts its sequence over and appends the
    # same record. The provider must not store it twice.
    retry = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await retry.append({"id": "r1"})

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict), 1)
    assert records[0].value == {"id": "r1"}
    with pytest.raises(asyncio.TimeoutError):
        await take(consumer.read(type=dict), 2, timeout=0.2)


async def test_new_attempt_supersedes_the_old_one():
    first = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await first.append({"text": "The capital of"})
    second = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=2
    )
    await second.append({"text": "Paris is the capital"})

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict), 3)
    assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
    assert records[1].kind is RecordKind.SUPERSEDED
    assert records[1].value.previous_attempt == 1
    assert records[2].kind is RecordKind.DATA and records[2].attempt == 2


async def test_topic_filter():
    producer = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await producer.append({"n": 1})
    other = await streams.producer(
        None, workflow_id="wf", stream="other", producer_id="model", attempt=1
    )
    await other.append({"n": 2})

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict, topic="inputs"), 1)
    assert records[0].value == {"n": 1}


async def test_cursor_resumes_where_it_points():
    producer = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict), 3)
    checkpoint = records[1].cursor

    resumed = await streams.consumer(None, workflow_id="wf", stream="inputs")
    again = await take(resumed.read(type=dict, start=checkpoint), 2)
    assert [r.value for r in again] == [{"n": 2}, {"n": 3}]


def test_unknown_provider_is_a_clear_error():
    with pytest.raises(RuntimeError, match="no stream provider 'nope'"):
        streams.configure(provider="nope")
