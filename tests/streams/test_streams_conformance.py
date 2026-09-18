"""Conformance tests for the stream contract's outside surface.

Written against the public surface plus the in-memory reference provider, so
they run with no server and no store. A storage provider reuses the same
expectations through its own fixtures; what this file pins down is the
contract: framing, producer identity, retry deduplication, supersession,
topic filtering, and cursor resumption. The workflow-side handles and the
two rules about workflow tasks live in ``test_streams_workflow``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from temporalio import streams
from temporalio.streams import _frame, _provider
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import Cursor, RecordKind
from temporalio.streams.providers import memory


@pytest.fixture(autouse=True)
def _fresh_memory_provider():  # pyright: ignore[reportUnusedFunction]
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
    assert isinstance(superseded.value, streams.Supersession)
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
    # An inbound stream has no topics; its name is the whole address.
    assert all(r.topic == "" for r in records)


async def test_retried_append_is_deduplicated():
    first = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await first.append({"id": "r1"})
    # The retry of the same attempt starts its sequence over and appends the
    # same record. The provider must not store it twice, and says so by
    # returning no position for what it dropped.
    retry = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    assert await retry.append({"id": "r1"}) is None

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict), 1)
    assert records[0].value == {"id": "r1"}
    # The store holds exactly the one record: the newest position is its cursor.
    assert await consumer.latest() == records[0].cursor


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
    assert isinstance(records[1].value, streams.Supersession)
    assert records[1].value.previous_attempt == 1
    assert records[2].kind is RecordKind.DATA and records[2].attempt == 2


async def test_topic_filter_on_the_owners_stream():
    # Two producers on two topics of the stream the workflow publishes. The
    # filter is only meaningful on a store that mixes topics, which is this
    # one; an inbound stream carries none.
    on_a = await streams.producer(
        None, workflow_id="wf", topic="a", producer_id="tool-a", attempt=1
    )
    await on_a.append({"n": 1})
    on_b = await streams.producer(
        None, workflow_id="wf", topic="b", producer_id="tool-b", attempt=1
    )
    await on_b.append({"n": 2})

    consumer = await streams.consumer(None, workflow_id="wf")
    only_a = await take(consumer.read(type=dict, topic="a"), 1)
    assert [r.value for r in only_a] == [{"n": 1}]

    both = await take(consumer.read(type=dict), 2)
    assert [(r.topic, r.value) for r in both] == [("a", {"n": 1}), ("b", {"n": 2})]


async def test_inbound_streams_have_no_topics():
    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    with pytest.raises(ValueError, match="inbound stream 'inputs' has no topics"):
        await take(consumer.read(type=dict, topic="inputs"), 1)


async def test_cursor_resumes_where_it_points():
    producer = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict), 3)
    checkpoint = records[0].cursor

    # Resuming after a record hands back everything past it and nothing
    # twice, without the reader ever advancing a cursor itself.
    resumed = await streams.consumer(None, workflow_id="wf", stream="inputs")
    again = await take(resumed.read(type=dict, after=checkpoint), 2)
    assert [r.value for r in again] == [{"n": 2}, {"n": 3}]


async def test_append_cursor_names_the_last_record_of_the_batch():
    producer = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    appended = await producer.append({"n": 1}, {"n": 2}, {"n": 3})
    assert appended is not None
    await producer.append({"n": 4})

    # A producer that resumes a reader after its own append must see only
    # what came later, not the tail of the batch it just wrote.
    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    records = await take(consumer.read(type=dict, after=appended), 1)
    assert [r.value for r in records] == [{"n": 4}]
    assert await producer.append() is None


async def test_latest_positions_a_reader_at_the_end():
    producer = await streams.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    consumer = await streams.consumer(None, workflow_id="wf", stream="inputs")
    assert await consumer.latest() == streams.BEGINNING

    await producer.append({"n": 1}, {"n": 2})
    since = await consumer.latest()
    await producer.append({"n": 3})

    # A reader that positioned itself before the last append sees only what
    # came after, which is how a client follows a turn it is about to start.
    records = await take(consumer.read(type=dict, after=since), 1)
    assert [r.value for r in records] == [{"n": 3}]


async def test_producer_appends_onto_the_owners_topic():
    # An activity's live output lands on the same topic the workflow
    # publishes, so one outside reader follows both.
    producer = await streams.producer(
        None, workflow_id="wf", topic="events", producer_id="model", attempt=1
    )
    await producer.append({"delta": "hel"}, {"delta": "lo"})

    consumer = await streams.consumer(None, workflow_id="wf")
    records = await take(consumer.read(type=dict, topic="events"), 2)
    assert [r.value["delta"] for r in records] == ["hel", "lo"]
    assert {r.producer for r in records} == {"model"}

    with pytest.raises(ValueError):
        await streams.producer(None, workflow_id="wf", producer_id="model", attempt=1)
    # Outside an activity there is no identity to fall back on.
    with pytest.raises(ValueError, match="producer_id is required"):
        await streams.producer(None, workflow_id="wf", stream="inputs")


def test_unknown_provider_is_a_clear_error():
    with pytest.raises(RuntimeError, match="no stream provider 'nope'"):
        streams.configure(provider="nope")


async def test_opening_a_stream_needs_a_configured_provider():
    # Building a provider is process setup, so the first workflow's thread
    # is not allowed to do it as a side effect of opening a stream.
    _provider._active = None
    with pytest.raises(RuntimeError, match="streams.configure"):
        await streams.consumer(None, workflow_id="wf")
    streams.configure(provider="memory")
    assert await streams.consumer(None, workflow_id="wf") is not None
