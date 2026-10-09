"""Conformance tests for a stream provider's outside surface.

Written against the public surface and parametrized over ``SETUPS``. The
memory provider always runs. A storage provider adds a setup that yields a
:class:`ProviderCase`, behind its own gate when it needs a store the test
environment does not start. Every case goes through the public surface, so a
new provider proves the contract by passing this file.

A store keyed by the owner's run chain needs the owner to exist before a
stream can be opened, so a setup can give a ``host`` that starts one; cases
open streams through :meth:`ProviderCase.open`, which calls it. A setup
whose provider cannot read yet sets ``reads=False``, and the cases marked
``reads`` are skipped for it.

The scope is the publish path this release ships: append, read and latest,
resuming after a cursor, ``BEGINNING`` and ``END``, batch order, retry
deduplication by producer, attempt and sequence with the content hash,
``SUPERSEDED`` on a new attempt, ``FINISH``, and refusing a cursor from
another provider or another stream.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    BEGINNING,
    DEFAULT_TOPIC,
    END,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamExpiredError,
    StreamHandle,
    StreamProducerError,
    StreamProvider,
    StreamRef,
    Supersession,
    topic,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter, PayloadCodec
from tests.helpers import new_worker

OUT = topic("out", dict)
OTHER = topic("other", dict)


reads = pytest.mark.reads


@workflow.defn
class OwnerHost:
    """Owns a stream until it is told to finish."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@dataclass
class ProviderCase:
    """One provider under test, and what the cases may ask of it."""

    name: str
    provider: StreamProvider
    client: Client
    truncate: Callable[[str, str, int], Awaitable[None]] | None = None
    """Drops all but the newest records of a Workflow's topic, standing in
    for retention, or ``None`` when the provider offers no way to."""
    host: Callable[[str], Awaitable[str]] | None = None
    """Starts the Workflow that owns ``workflow_id``'s stream and returns its
    run id, when the store needs the owner to exist."""
    reads: bool = True
    """The provider can read, so the cases marked ``reads`` run."""
    live_gaps: bool = True
    """A read in progress notices records dropped from under it, so the
    cases marked ``live_gaps`` run."""
    hosted: dict[str, str] = field(default_factory=dict)
    """The run id of each owner ``host`` started, by Workflow id."""

    async def open(
        self,
        workflow_id: str,
        *,
        client: Client | None = None,
        topic: str | None = None,
        pin_run: bool = False,
    ) -> StreamHandle:
        run_id = self.hosted.get(workflow_id, "a-run")
        if self.host is not None and workflow_id not in self.hosted:
            run_id = self.hosted[workflow_id] = await self.host(workflow_id)
        ref = StreamRef.for_workflow(
            workflow_id, run_id=run_id if pin_run else None, topic=topic
        )
        return self.provider.get_stream_handle(client or self.client, ref)


@asynccontextmanager
async def _memory_case(client: Client) -> AsyncIterator[ProviderCase]:
    provider = MemoryStreams()

    async def truncate(workflow_id: str, name: str, keep: int) -> None:
        provider.truncate(workflow_id, name, keep=keep, namespace=client.namespace)

    yield ProviderCase("memory", provider, client, truncate=truncate)
    await provider.close()


@asynccontextmanager
async def _redis_case(client: Client) -> AsyncIterator[ProviderCase]:
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the Redis provider cases")
    from temporalio.contrib.streams.redis import RedisStreams

    # A prefix per case keeps cases apart in one Redis.
    provider = RedisStreams(url, key_prefix=f"conformance-{uuid.uuid4().hex}")
    owners: list[WorkflowHandle] = []
    async with new_worker(client, OwnerHost) as worker:

        async def host(workflow_id: str) -> str:
            handle = await client.start_workflow(
                OwnerHost.run, id=workflow_id, task_queue=worker.task_queue
            )
            owners.append(handle)
            assert handle.result_run_id is not None
            return handle.result_run_id

        case = ProviderCase("redis", provider, client, host=host, live_gaps=False)

        async def truncate(workflow_id: str, name: str, keep: int) -> None:
            # Trims as the append script does, watermark included.
            keys = provider._chain_keys(
                client.namespace, workflow_id, case.hosted[workflow_id]
            )
            entries = await provider._redis.xrange(keys.log(name))
            doomed = entries[: len(entries) - keep]
            if doomed:
                await provider._redis.xtrim(
                    keys.log(name), maxlen=keep, approximate=False
                )
                redis_client: Any = provider._redis
                await redis_client.hset(keys.meta(name), "trimmed", doomed[-1][0])

        case.truncate = truncate
        yield case
        for owner in owners:
            await owner.terminate()
    await provider.close()


SETUPS: dict[str, Callable[[Client], Any]] = {
    "memory": _memory_case,
    "redis": _redis_case,
}


@pytest.fixture(params=sorted(SETUPS))
async def case(request: pytest.FixtureRequest, client: Client):
    async with SETUPS[request.param](client) as found:
        if request.node.get_closest_marker("reads") and not found.reads:
            pytest.skip(f"the {found.name} provider cannot read")
        if request.node.get_closest_marker("live_gaps") and not found.live_gaps:
            pytest.skip(f"the {found.name} provider misses a gap during a read")
        yield found


def new_workflow_id() -> str:
    return f"streams-conf-{uuid.uuid4().hex}"


async def take(records: Any, count: int, timeout: float = 5.0) -> list:
    out = []

    async def pull() -> None:
        async for record in records:
            out.append(record)
            if len(out) == count:
                return

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    return out


async def nothing_arrives(records: Any, wait: float = 0.3) -> bool:
    try:
        await asyncio.wait_for(records.__anext__(), wait)
        return False
    except asyncio.TimeoutError:
        return True
    finally:
        await records.aclose()


@reads
async def test_append_read_roundtrip(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    await producer.append({"n": 1})
    await producer.append({"n": 2})
    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"n": 1}, {"n": 2}]
    assert all(r.kind is RecordKind.DATA for r in records)
    assert [(r.producer_id, r.attempt, r.sequence) for r in records] == [
        ("p", 1, 1),
        ("p", 1, 2),
    ]
    assert all(r.topic == "out" for r in records)


@reads
async def test_raw_values_pass_through_untouched(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    raw = Payload(metadata={"encoding": b"binary/custom"}, data=b"\x00\xff")
    await stream.producer(topic="raw", producer_id="p", attempt=1).append(RawValue(raw))
    (record,) = await take(stream.read(topic="raw", result_type=RawValue), 1)
    assert record.value.payload == raw


@reads
async def test_a_batch_lands_in_order_and_its_cursor_names_the_last_record(
    case: ProviderCase,
):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    cursor = await producer.append({"n": 1}, {"n": 2}, {"n": 3})
    records = await take(stream.read(topic=OUT), 3)
    assert [r.value for r in records] == [{"n": 1}, {"n": 2}, {"n": 3}]
    assert [r.sequence for r in records] == [1, 2, 3]
    assert records[-1].cursor == cursor
    assert await stream.latest(topic=OUT) == cursor


async def test_an_empty_append_writes_nothing(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    assert await producer.append() == BEGINNING
    cursor = await producer.append({"n": 1})
    assert await producer.append() == cursor
    assert await stream.latest(topic=OUT) == cursor


async def test_latest_is_beginning_on_an_empty_topic(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    assert await stream.latest(topic=OUT) == BEGINNING


@reads
async def test_a_cursor_resumes_strictly_after_its_record(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    first = await producer.append({"n": 1})
    await producer.append({"n": 2}, {"n": 3})
    records = await take(stream.read(topic=OUT, after=first), 2)
    assert [r.value for r in records] == [{"n": 2}, {"n": 3}]
    # A cursor a reader saw resumes the same way as one an append returned.
    again = await take(stream.read(topic=OUT, after=records[0].cursor), 1)
    assert [r.value for r in again] == [{"n": 3}]


@reads
async def test_latest_positions_a_reader_at_the_end(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    await producer.append({"n": 1})
    records = stream.read(topic=OUT, after=await stream.latest(topic=OUT))
    await producer.append({"n": 2})
    assert [r.value for r in await take(records, 1)] == [{"n": 2}]


@reads
async def test_end_reads_only_what_arrives_after_the_read_starts(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    await producer.append({"n": 1})
    records = stream.read(topic=OUT, after=END)
    # A store that keeps positions remotely resolves END when the read
    # starts iterating, so the read is started before the append.
    first = asyncio.ensure_future(records.__anext__())
    await asyncio.sleep(0.3)
    await producer.append({"n": 2})
    assert (await asyncio.wait_for(first, 5.0)).value == {"n": 2}
    await records.aclose()
    assert await nothing_arrives(stream.read(topic=OUT, after=END))


@reads
async def test_beginning_starts_at_the_oldest_record_still_held(case: ProviderCase):
    if case.truncate is None:
        pytest.skip(f"{case.name} offers no way to drop records")
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    old = await producer.append({"n": 1})
    await producer.append({"n": 2}, {"n": 3})
    await case.truncate(workflow_id, OUT.name, 2)
    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"n": 2}, {"n": 3}]
    # The dropped record's cursor still resumes, because nothing after it
    # was dropped.
    resumed = await take(stream.read(topic=OUT, after=old), 2)
    assert [r.value for r in resumed] == [{"n": 2}, {"n": 3}]
    # Once a record after it is gone too, the cursor is expired, which a
    # reader can tell apart from a cursor that was never valid here.
    await case.truncate(workflow_id, OUT.name, 1)
    with pytest.raises(StreamExpiredError):
        await stream.read(topic=OUT, after=old).__anext__()


@reads
@pytest.mark.live_gaps
async def test_a_reader_that_falls_behind_retention_is_told(case: ProviderCase):
    if case.truncate is None:
        pytest.skip(f"{case.name} offers no way to drop records")
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})
    records = stream.read(topic=OUT)
    assert (await records.__anext__()).value == {"n": 1}
    await case.truncate(workflow_id, OUT.name, 1)
    with pytest.raises(StreamExpiredError):
        await records.__anext__()


@reads
async def test_a_retried_append_returns_the_original_position(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    first = await stream.producer(topic=OUT, producer_id="p", attempt=1).append(
        {"n": 1}, {"n": 2}
    )
    # A producer that lost the answer starts over with the same identity, as
    # a restarted process does.
    retry = stream.producer(topic=OUT, producer_id="p", attempt=1)
    assert await retry.append({"n": 1}, {"n": 2}) == first
    assert await stream.latest(topic=OUT) == first
    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"n": 1}, {"n": 2}]
    assert await nothing_arrives(stream.read(topic=OUT, after=first))


async def test_a_divergent_retry_is_refused(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    first = await stream.producer(topic=OUT, producer_id="p", attempt=1).append(
        {"n": 1}
    )
    retry = stream.producer(topic=OUT, producer_id="p", attempt=1)
    with pytest.raises(StreamProducerError):
        await retry.append({"n": "different"})
    assert await stream.latest(topic=OUT) == first


async def test_a_sequence_below_the_newest_is_refused(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    await producer.append({"n": 1})
    newest = await producer.append({"n": 2})
    stale = stream.producer(topic=OUT, producer_id="p", attempt=1)
    # Even the same content is refused: only the newest batch is a retry.
    with pytest.raises(StreamProducerError):
        await stale.append({"n": 1})
    assert await stream.latest(topic=OUT) == newest


@reads
async def test_producers_dedupe_apart(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    await stream.producer(topic=OUT, producer_id="a", attempt=1).append({"n": 1})
    await stream.producer(topic=OUT, producer_id="b", attempt=1).append({"n": 1})
    await stream.producer(topic=OUT, producer_id="a", attempt=2).append({"n": 1})
    records = await take(stream.read(topic=OUT), 4)
    data = [r for r in records if r.kind is RecordKind.DATA]
    assert [(r.producer_id, r.attempt) for r in data] == [
        ("a", 1),
        ("b", 1),
        ("a", 2),
    ]


@reads
async def test_a_new_attempt_supersedes_the_old_one(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    await stream.producer(topic=OUT, producer_id="model", attempt=1).append({"n": 1})
    await stream.producer(topic=OUT, producer_id="model", attempt=2).append({"n": 2})
    records = await take(stream.read(topic=OUT), 3)
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
    ]
    assert records[1].supersession == Supersession("model", 1, 2)
    assert records[1].value is None
    # A consumer that checkpoints the supersession and resumes after it gets
    # the new attempt's first record next.
    assert records[1].cursor == records[0].cursor
    resumed = await take(stream.read(topic=OUT, after=records[1].cursor), 1)
    assert resumed[0].value == {"n": 2}


@reads
async def test_finish_is_a_record_of_its_own(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    await producer.append({"n": 1})
    finished = await producer.finish()
    records = await take(stream.read(topic=OUT), 2)
    assert [r.kind for r in records] == [RecordKind.DATA, RecordKind.FINISH]
    assert records[1].producer_id == "p"
    assert records[1].value is None
    assert records[1].cursor == finished


@reads
async def test_topics_are_addressed_by_name(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    await stream.producer(topic=OUT, producer_id="p", attempt=1).append({"n": 1})
    await stream.producer(topic=OTHER, producer_id="p", attempt=1).append({"n": 2})
    assert [r.value for r in await take(stream.read(topic=OTHER), 1)] == [{"n": 2}]
    assert [r.value for r in await take(stream.read(topic=OUT), 1)] == [{"n": 1}]


@reads
async def test_naming_no_topic_addresses_the_default_topic(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    await stream.producer(producer_id="p", attempt=1).append("hello")
    (record,) = await take(stream.read(topic=DEFAULT_TOPIC, result_type=str), 1)
    assert record.value == "hello"
    assert record.topic == DEFAULT_TOPIC
    assert stream.ref.topic == DEFAULT_TOPIC


@reads
async def test_a_ref_topic_is_the_handle_default(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id, topic=OUT.name)
    await stream.producer(producer_id="p", attempt=1).append({"n": 1})
    plain = await case.open(workflow_id)
    assert [r.value for r in await take(plain.read(topic=OUT), 1)] == [{"n": 1}]


@reads
async def test_a_cursor_from_another_stream_is_refused_at_the_call(
    case: ProviderCase,
):
    one = await case.open(new_workflow_id())
    other = await case.open(new_workflow_id())
    cursor = await one.producer(topic=OUT, producer_id="p", attempt=1).append({"n": 1})
    await other.producer(topic=OUT, producer_id="p", attempt=1).append({"n": 1})
    # Another Workflow's stream, and another topic of the same stream, both
    # hold a record at that position; neither may resume from it.
    with pytest.raises(StreamCursorError, match="another stream"):
        other.read(topic=OUT, after=cursor)
    await one.producer(topic=OTHER, producer_id="p", attempt=1).append({"n": 1})
    with pytest.raises(StreamCursorError, match="another stream"):
        one.read(topic=OTHER, after=cursor)


@reads
async def test_a_cursor_survives_pinning_to_a_run(case: ProviderCase):
    # A stream follows its owner's run chain, so a cursor read through a
    # handle pinned to one run resumes on a handle that follows the chain.
    workflow_id = new_workflow_id()
    pinned = await case.open(workflow_id, pin_run=True)
    producer = pinned.producer(topic=OUT, producer_id="p", attempt=1)
    first = await producer.append({"n": 1})
    await producer.append({"n": 2})
    follower = await case.open(workflow_id)
    assert [r.value for r in await take(follower.read(topic=OUT, after=first), 1)] == [
        {"n": 2}
    ]


@reads
async def test_a_cursor_from_another_provider_is_refused_at_the_call(
    case: ProviderCase,
):
    stream = await case.open(new_workflow_id())
    with pytest.raises(StreamCursorError):
        stream.read(topic=OUT, after=Cursor("elsewhere:0000abcd:1"))
    with pytest.raises(StreamCursorError):
        stream.read(topic=OUT, after=Cursor("not a cursor"))


@reads
async def test_argument_mistakes_are_value_errors(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    with pytest.raises(ValueError):
        stream.producer(topic=OUT, producer_id="", attempt=1)
    with pytest.raises(ValueError):
        stream.producer(topic=OUT, producer_id="p", attempt=0)
    with pytest.raises(ValueError):
        stream.read(topic=OUT, result_type=dict)  # type: ignore[call-overload]
    with pytest.raises(ValueError):
        stream.read(topic="")


@reads
async def test_closing_a_read_early_releases_it(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="p", attempt=1)
    records = stream.read(topic=OUT)
    pending = asyncio.ensure_future(records.__anext__())
    await asyncio.sleep(0.1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await asyncio.wait_for(records.aclose(), 5.0)
    await producer.append({"n": 1})
    assert [r.value for r in await take(stream.read(topic=OUT), 1)] == [{"n": 1}]


class NonceCodec(PayloadCodec):
    """Encrypts with a fresh nonce per call, so two encodings never match."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=uuid.uuid4().bytes + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[16:]) for p in payloads]


def _client_with(client: Client, converter: DataConverter) -> Client:
    config = client.config()
    config["data_converter"] = converter
    config["plugins"] = []
    return Client(**config)


@reads
async def test_a_retry_through_a_nondeterministic_codec_still_deduplicates(
    case: ProviderCase,
):
    coded = _client_with(case.client, DataConverter(payload_codec=NonceCodec()))
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id, client=coded)
    first = await stream.producer(topic=OUT, producer_id="p", attempt=1).append(
        {"n": 1}
    )
    retry = stream.producer(topic=OUT, producer_id="p", attempt=1)
    assert await retry.append({"n": 1}) == first
    (record,) = await take(stream.read(topic=OUT), 1)
    assert record.value == {"n": 1}


@reads
async def test_a_read_ends_when_the_owner_closes(case: ProviderCase):
    workflow_id = new_workflow_id()
    async with new_worker(case.client, OwnerHost) as worker:
        owner = await case.client.start_workflow(
            OwnerHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = case.provider.get_stream_handle(
            case.client, StreamRef.for_workflow(workflow_id)
        )
        await stream.producer(topic=OUT, producer_id="p", attempt=1).append({"n": 1})
        await owner.signal(OwnerHost.finish)
        await owner.result()
    records = [r async for r in _bounded(stream.read(topic=OUT), 10.0)]
    assert [r.value for r in records] == [{"n": 1}]


async def _bounded(records: Any, timeout: float) -> AsyncIterator[Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                yield await asyncio.wait_for(records.__anext__(), remaining)
            except StopAsyncIteration:
                return
    finally:
        await records.aclose()
