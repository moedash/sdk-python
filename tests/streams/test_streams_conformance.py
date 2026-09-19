"""Conformance tests for the stream contract's outside surface.

Written against the public surface, parametrised over the providers this
tree can stand up. The memory provider always runs, with no server and no
store. A storage provider adds itself to ``SETUPS`` behind its own
``STREAMS_LIVE`` gate: its setup configures the provider, hands back the
client the cases should use and a workflow that exists for the case to
address, and says which capabilities it lacks, so the cases marked
``inbound_stream`` or ``unfiltered_read`` are skipped with a reason on a
provider that cannot do them. Every other case reads with a topic, which is
what a store that keys by topic needs.

What this file pins down is the contract: framing, producer identity, retry
deduplication, supersession, topic filtering, cursor resumption, and store
keys that cannot collide. The workflow-side handles and the two rules about
workflow tasks live in ``test_streams_workflow``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import pytest

from temporalio import streams
from temporalio.client import Client
from temporalio.streams import _frame, _ids, _provider
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import Cursor, RecordKind
from temporalio.streams.providers import memory


@dataclass
class ProviderCase:
    """One provider under test, and what the cases may ask of it."""

    name: str
    client: Any = None
    workflow_id: str = ""
    """The workflow whose streams the case addresses.

    A store keeps a workflow's stream with the workflow, so the setup makes
    the execution exist before the case opens a producer on its account.
    """
    inbound_streams: bool = True
    """Outside producers may write a workflow's inbound stream."""
    unfiltered_reads: bool = True
    """A consumer may read the owner's stream without naming a topic."""
    reports_dropped_repeats: bool = True
    """``append`` returns ``None`` for a repeat it dropped.

    A store that answers a repeat with the original position instead still
    holds the record once; it just cannot say at append time that this call
    wrote nothing.
    """


async def _memory_case() -> AsyncIterator[ProviderCase]:
    memory.reset()
    streams.configure(provider="memory")
    yield ProviderCase("memory", workflow_id=new_workflow_id())
    memory.reset()


async def _native_case() -> AsyncIterator[ProviderCase]:
    streams.configure(provider="native")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    # The execution only has to exist for the server to attach a stream to
    # it; no worker ever picks the task up, and the case terminates it.
    owner = await client.start_workflow(
        "StreamOwner",
        id=new_workflow_id(),
        task_queue="streams-conformance-unserved",
    )
    try:
        yield ProviderCase("native", client, workflow_id=owner.id)
    finally:
        await owner.terminate("conformance case finished")
        await streams.close()


SETUPS: dict[str, Callable[[], AsyncIterator[ProviderCase]]] = {"memory": _memory_case}
if os.environ.get("STREAMS_LIVE") == "native":
    # Needs a server built from the stream-carrying branch at TEMPORAL_ADDRESS.
    SETUPS["native"] = _native_case

_CAPABILITIES = {
    "inbound_stream": lambda case: case.inbound_streams,
    "unfiltered_read": lambda case: case.unfiltered_reads,
}


@pytest.fixture(params=sorted(SETUPS))
async def provider(request: pytest.FixtureRequest) -> AsyncIterator[ProviderCase]:
    async for case in SETUPS[request.param]():
        for marker, supported in _CAPABILITIES.items():
            if request.node.get_closest_marker(marker) and not supported(case):
                pytest.skip(f"the {case.name} provider does not support {marker}")
        yield case


def new_workflow_id() -> str:
    # Unique per case, because a storage provider keeps what earlier cases
    # wrote and the memory provider only happens to forget.
    return f"wf-{uuid.uuid4().hex}"


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


def test_inbound_stream_ids_cannot_collide():
    # A colon in a workflow id must not make two addresses one key.
    assert _ids.inbound_stream_id("a:b", "c") != _ids.inbound_stream_id("a", "b:c")
    assert _ids.inbound_stream_id("a:b", "") != _ids.inbound_stream_id("a", "b")
    assert _ids.inbound_stream_id("a%3Ab", "c") != _ids.inbound_stream_id("a:b", "c")
    assert _ids.inbound_stream_id("wf", "inputs") == "wf:inputs"


async def test_append_read_roundtrip(provider: ProviderCase):
    workflow_id = provider.workflow_id
    producer = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    await producer.append({"id": "r1"}, {"id": "r2"})
    await producer.finish()

    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    records = await take(consumer.read(type=dict, topic="out"), 3)
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert [r.value for r in records[:2]] == [{"id": "r1"}, {"id": "r2"}]
    assert all(r.producer == "model" and r.attempt == 1 for r in records)
    assert [r.sequence for r in records] == [0, 1, 2]
    assert all(r.topic == "out" for r in records)


@pytest.mark.inbound_stream
async def test_inbound_records_carry_no_topic(provider: ProviderCase):
    workflow_id = provider.workflow_id
    producer = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        stream="inputs",
        producer_id="model",
        attempt=1,
    )
    await producer.append({"id": "r1"})
    await producer.finish()

    consumer = await streams.consumer(
        provider.client, workflow_id=workflow_id, stream="inputs"
    )
    records = await take(consumer.read(type=dict), 2)
    assert [r.kind for r in records] == [RecordKind.DATA, RecordKind.FINISH]
    assert records[0].value == {"id": "r1"}
    # An inbound stream has no topics; its name is the whole address.
    assert all(r.topic == "" for r in records)
    with pytest.raises(ValueError, match="inbound stream 'inputs' has no topics"):
        await take(consumer.read(type=dict, topic="inputs"), 1)


async def test_retried_append_is_deduplicated(provider: ProviderCase):
    workflow_id = provider.workflow_id
    first = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    await first.append({"id": "r1"})
    # The retry of the same attempt starts its sequence over and appends the
    # same record. The provider must not store it twice, and says so by
    # returning no position for what it dropped, or the original one when
    # its store cannot tell the two apart at append time.
    retry = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    dropped = await retry.append({"id": "r1"})

    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    records = await take(consumer.read(type=dict, topic="out"), 1)
    assert records[0].value == {"id": "r1"}
    if provider.reports_dropped_repeats:
        assert dropped is None
    else:
        assert dropped == records[0].cursor
    # The store holds exactly the one record: the newest position is its cursor.
    assert await consumer.latest(topic="out") == records[0].cursor


async def test_new_attempt_supersedes_the_old_one(provider: ProviderCase):
    workflow_id = provider.workflow_id
    first = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    await first.append({"text": "The capital of"})
    second = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=2,
    )
    await second.append({"text": "Paris is the capital"})

    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    records = await take(consumer.read(type=dict, topic="out"), 3)
    assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
    assert records[1].kind is RecordKind.SUPERSEDED
    assert isinstance(records[1].value, streams.Supersession)
    assert records[1].value.previous_attempt == 1
    assert records[2].kind is RecordKind.DATA and records[2].attempt == 2


async def test_topic_filter_on_the_owners_stream(provider: ProviderCase):
    # Two producers on two topics of the stream the workflow publishes. The
    # filter is only meaningful on a store that mixes topics, which is this
    # one; an inbound stream carries none.
    workflow_id = provider.workflow_id
    on_a = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="a",
        producer_id="tool-a",
        attempt=1,
    )
    await on_a.append({"n": 1})
    on_b = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="b",
        producer_id="tool-b",
        attempt=1,
    )
    await on_b.append({"n": 2})

    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    only_a = await take(consumer.read(type=dict, topic="a"), 1)
    assert [(r.topic, r.value) for r in only_a] == [("a", {"n": 1})]
    only_b = await take(consumer.read(type=dict, topic="b"), 1)
    assert [(r.topic, r.value) for r in only_b] == [("b", {"n": 2})]


@pytest.mark.unfiltered_read
async def test_a_read_without_a_topic_sees_every_topic(provider: ProviderCase):
    workflow_id = provider.workflow_id
    on_a = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="a",
        producer_id="tool-a",
        attempt=1,
    )
    await on_a.append({"n": 1})
    on_b = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="b",
        producer_id="tool-b",
        attempt=1,
    )
    await on_b.append({"n": 2})

    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    both = await take(consumer.read(type=dict), 2)
    assert [(r.topic, r.value) for r in both] == [("a", {"n": 1}), ("b", {"n": 2})]


async def test_cursor_resumes_where_it_points(provider: ProviderCase):
    workflow_id = provider.workflow_id
    producer = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    records = await take(consumer.read(type=dict, topic="out"), 3)
    checkpoint = records[0].cursor

    # Resuming after a record hands back everything past it and nothing
    # twice, without the reader ever advancing a cursor itself.
    resumed = await streams.consumer(provider.client, workflow_id=workflow_id)
    again = await take(resumed.read(type=dict, topic="out", after=checkpoint), 2)
    assert [r.value for r in again] == [{"n": 2}, {"n": 3}]


async def test_append_cursor_names_the_last_record_of_the_batch(
    provider: ProviderCase,
):
    workflow_id = provider.workflow_id
    producer = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    appended = await producer.append({"n": 1}, {"n": 2}, {"n": 3})
    if appended is None:
        pytest.skip(f"the {provider.name} provider learns positions at read time")
    await producer.append({"n": 4})

    # A producer that resumes a reader after its own append must see only
    # what came later, not the tail of the batch it just wrote.
    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    records = await take(consumer.read(type=dict, topic="out", after=appended), 1)
    assert [r.value for r in records] == [{"n": 4}]
    assert await producer.append() is None


async def test_latest_positions_a_reader_at_the_end(provider: ProviderCase):
    workflow_id = provider.workflow_id
    producer = await streams.producer(
        provider.client,
        workflow_id=workflow_id,
        topic="out",
        producer_id="model",
        attempt=1,
    )
    consumer = await streams.consumer(provider.client, workflow_id=workflow_id)
    assert await consumer.latest(topic="out") == streams.BEGINNING

    await producer.append({"n": 1}, {"n": 2})
    since = await consumer.latest(topic="out")
    await producer.append({"n": 3})

    # A reader that positioned itself before the last append sees only what
    # came after, which is how a client follows a turn it is about to start.
    records = await take(consumer.read(type=dict, topic="out", after=since), 1)
    assert [r.value for r in records] == [{"n": 3}]


@pytest.mark.inbound_stream
async def test_stream_addresses_with_colons_do_not_share_a_store(
    provider: ProviderCase,
):
    # ("wf:x", "y") and ("wf", "x:y") differ only in where the colon sits.
    base = provider.workflow_id
    left = await streams.producer(
        provider.client, workflow_id=f"{base}:x", stream="y", producer_id="l", attempt=1
    )
    right = await streams.producer(
        provider.client, workflow_id=base, stream="x:y", producer_id="r", attempt=1
    )
    await left.append({"side": "left"})
    await right.append({"side": "right"})

    seen_left = await streams.consumer(
        provider.client, workflow_id=f"{base}:x", stream="y"
    )
    only_left = await take(seen_left.read(type=dict), 1)
    assert [r.value for r in only_left] == [{"side": "left"}]
    assert await seen_left.latest() == only_left[0].cursor
    seen_right = await streams.consumer(provider.client, workflow_id=base, stream="x:y")
    only_right = await take(seen_right.read(type=dict), 1)
    assert [r.value for r in only_right] == [{"side": "right"}]
    assert await seen_right.latest() == only_right[0].cursor


async def test_producer_needs_exactly_one_address_and_an_identity(
    provider: ProviderCase,
):
    workflow_id = provider.workflow_id
    with pytest.raises(ValueError):
        await streams.producer(
            provider.client, workflow_id=workflow_id, producer_id="model", attempt=1
        )
    with pytest.raises(ValueError):
        await streams.producer(
            provider.client,
            workflow_id=workflow_id,
            stream="inputs",
            topic="out",
            producer_id="model",
            attempt=1,
        )
    # Outside an activity there is no identity to fall back on.
    with pytest.raises(ValueError, match="producer_id is required"):
        await streams.producer(provider.client, workflow_id=workflow_id, topic="out")


def test_unknown_provider_is_a_clear_error():
    with pytest.raises(RuntimeError, match="no stream provider 'nope'"):
        streams.configure(provider="nope")


async def test_opening_a_stream_needs_a_configured_provider():
    # Building a provider is process setup, so the first workflow's thread
    # is not allowed to do it as a side effect of opening a stream.
    _provider._active = None
    with pytest.raises(RuntimeError, match="streams.configure"):
        await streams.consumer(None, workflow_id="wf")
    # Closing with nothing configured is allowed, so a shutdown path can
    # call it unconditionally.
    await streams.close()
    streams.configure(provider="memory")
    assert await streams.consumer(None, workflow_id="wf") is not None
    await streams.close()
