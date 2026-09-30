"""Conformance tests for the stream contract's outside surface.

Written against the public surface, parametrised over the providers this
tree can stand up. The memory provider always runs, with no server and no
store. A storage provider adds itself to ``SETUPS``, behind its own
``STREAMS_LIVE`` gate when it needs a store the test environment does not
start: its setup receives the environment's client and hands back a provider
instance, the client the cases should use, a ``host`` that starts the
workflow owning a stream when the store lives inside a running workflow, and
which capabilities it lacks, so the cases marked ``reports_positions`` are
skipped with a reason on a provider whose ``append()`` learns positions at
read time.

What this file pins down is what a provider owes: producer identity, retry
deduplication, positions, supersession, topic addressing, cursor resumption,
cursor ownership, releasing a read the caller stopped early, naming a stream
as a ``StreamRef``, and running bodies through the client's data converter so
external storage applies and a retry through a nondeterministic codec still
matches its original. Every case here goes through the public surface, so a
new provider answers this file and nothing else. The shared pieces no provider
implements are unit-tested in ``test_streams_internals``; the workflow-side
handles and the two rules about Workflow Tasks live in
``test_streams_workflow``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.converter import (
    DataConverter,
    ExternalStorage,
    PayloadCodec,
    StorageDriver,
    StorageDriverClaim,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
)
from temporalio.streams import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamClosedError,
    StreamCursorError,
    StreamHandle,
    StreamNotFoundError,
    StreamProducerError,
    StreamProvider,
    StreamRef,
    Supersession,
    topic,
)
from temporalio.streams._ref import RefHandle, open_ref
from temporalio.streams.providers.memory import MemoryStreams

# Defined once and shared by every case, the way an application shares them
# between its workflow, its activities and its backend.
OUT = topic("out", dict)
A = topic("a", dict)
B = topic("b", dict)
Y = topic("y", dict)
XY = topic("x:y", dict)


@dataclass
class ProviderCase:
    """One provider under test, and what the cases may ask of it."""

    name: str
    provider: StreamProvider
    client: Client | None = None
    reports_positions: bool = True
    """``append()`` returns where the records landed."""
    detects_divergent_retries: bool = True
    """``append()`` compares a repeat's content with what it already holds."""
    host: Callable[[str], Awaitable[None]] | None = None
    """Starts the workflow that owns ``workflow_id``'s stream, when a store needs one."""
    truncate: Callable[[str, str, int], Awaitable[None]] | None = None
    """Drops all but the newest records of a workflow's topic, standing in
    for retention, or ``None`` when the provider offers no way to."""
    hosts_standalone_streams: bool = True
    """The store holds a stream with an id of its own and no owner."""
    waits_for_standalone_creation: bool = False
    """A read on a standalone stream id that does not exist yet parks until
    the first write instead of raising ``StreamNotFoundError``."""
    bounds_standalone_bytes: bool = True
    """A standalone stream's policy can bound the bytes it keeps."""
    trims_open_stream_by_age: bool = True
    """A standalone stream drops records older than ``retention`` while it is
    open, rather than keeping them that long after it closes."""

    async def open(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        client: Client | None = None,
    ) -> StreamHandle:
        if self.host is not None:
            await self.host(workflow_id)
        if client is not None:
            # The explicit form, for a case that needs the handle to encode
            # bodies through this client's data converter.
            return self.provider.get_stream_handle(client, workflow_id, run_id=run_id)
        if self.client is not None:
            # A storage provider's setup registers the provider on the client,
            # so the cases go through the accessor an application uses.
            return self.client.get_stream_handle(workflow_id, run_id=run_id)
        # Only the memory provider gets here, and it takes no client.
        return self.provider.get_stream_handle(
            None,  # type: ignore[arg-type]
            workflow_id,
            run_id=run_id,
        )

    async def open_ref(self, ref: StreamRef) -> RefHandle:
        if self.client is not None:
            return self.client.get_stream_handle(ref)
        return open_ref(self.provider, None, ref)  # type: ignore[arg-type]

    async def create_stream(
        self,
        stream_id: str,
        *,
        retention: timedelta | None = None,
        max_records: int | None = None,
        max_bytes: int | None = None,
    ) -> StreamHandle:
        if self.client is not None:
            return await self.client.create_stream(
                stream_id,
                retention=retention,
                max_records=max_records,
                max_bytes=max_bytes,
            )
        return await self.provider.create_standalone_stream(
            None,  # type: ignore[arg-type]
            stream_id,
            retention=retention,
            max_records=max_records,
            max_bytes=max_bytes,
        )

    async def open_standalone(self, stream_id: str) -> StreamHandle:
        if self.client is not None:
            return self.client.get_stream_handle(stream_id=stream_id)
        return self.provider.get_standalone_stream_handle(
            None,  # type: ignore[arg-type]
            stream_id,
        )


class RecordingDriver(StorageDriver):
    """An in-memory external storage driver that counts what it was asked to hold."""

    def __init__(self) -> None:
        self.held: dict[str, bytes] = {}
        self.stored = 0
        self.retrieved = 0

    def name(self) -> str:
        return "recording"

    async def store(
        self, context: StorageDriverStoreContext, payloads: Sequence[Payload]
    ) -> list[StorageDriverClaim]:
        claims: list[StorageDriverClaim] = []
        for payload in payloads:
            key = f"payload-{len(self.held)}"
            self.held[key] = payload.SerializeToString()
            self.stored += 1
            claims.append(StorageDriverClaim(claim_data={"key": key}))
        return claims

    async def retrieve(
        self,
        context: StorageDriverRetrieveContext,
        claims: Sequence[StorageDriverClaim],
    ) -> list[Payload]:
        self.retrieved += len(claims)
        return [Payload.FromString(self.held[c.claim_data["key"]]) for c in claims]


class NonceCodec(PayloadCodec):
    """A codec whose output differs on every call, as one that encrypts with a fresh nonce does."""

    def __init__(self) -> None:
        self.encoded = 0

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.encoded += len(payloads)
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=os.urandom(16) + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[16:]) for p in payloads]


def _client_with(client: Client, converter: DataConverter) -> Client:
    # The same connection, carrying the converter the case wants bodies to
    # pass through.
    config = client.config()
    config["data_converter"] = converter
    return Client(**config)


async def _memory_case(_client: Client) -> AsyncIterator[ProviderCase]:
    provider = MemoryStreams()

    async def truncate(workflow_id: str, topic: str, keep: int) -> None:
        provider.truncate(workflow_id, topic, keep=keep)

    yield ProviderCase(
        "memory",
        provider,
        truncate=truncate,
        bounds_standalone_bytes=True,
        trims_open_stream_by_age=True,
    )
    provider.reset()


SETUPS: dict[str, Callable[[Client], AsyncIterator[ProviderCase]]] = {
    "memory": _memory_case
}

_CAPABILITIES = {
    "reports_positions": lambda case: case.reports_positions,
    "detects_divergent_retries": lambda case: case.detects_divergent_retries,
    "truncates": lambda case: case.truncate is not None,
    "hosts_standalone_streams": lambda case: case.hosts_standalone_streams,
}


@pytest.fixture(params=sorted(SETUPS))
async def case(
    request: pytest.FixtureRequest, client: Client
) -> AsyncIterator[ProviderCase]:
    async for provider_case in SETUPS[request.param](client):
        for marker, supported in _CAPABILITIES.items():
            if request.node.get_closest_marker(marker) and not supported(provider_case):
                pytest.skip(f"the {provider_case.name} provider does not {marker}")
        yield provider_case


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


async def drain(records: Any, timeout: float = 5.0) -> list:
    """Every record until the read ends on its own."""

    async def _collect() -> list:
        return [record async for record in records]

    return await asyncio.wait_for(_collect(), timeout)


def new_stream_id() -> str:
    return f"stream-{uuid.uuid4().hex}"


async def test_append_read_roundtrip(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    assert (producer.producer_id, producer.attempt) == ("model", 1)
    await producer.append({"id": "r1"}, {"id": "r2"})
    await producer.finish()

    records = await take(stream.read(topic=OUT), 3)
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert [r.value for r in records[:2]] == [{"id": "r1"}, {"id": "r2"}]
    assert records[2].value is None
    assert all(r.producer_id == "model" and r.attempt == 1 for r in records)
    assert [r.sequence for r in records] == [0, 1, 2]
    assert all(r.topic == OUT.name for r in records)


async def test_raw_values_pass_through_untouched(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    payload = Payload(metadata={"encoding": b"binary/plain"}, data=b"\x00\x01raw")
    producer = stream.producer(topic=OUT.name, producer_id="model", attempt=1)
    await producer.append(RawValue(payload))

    records = await take(stream.read(topic="out", result_type=RawValue), 1)
    assert isinstance(records[0].value, RawValue)
    assert records[0].value.payload == payload


@pytest.mark.reports_positions
async def test_retried_append_returns_the_original_position(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    first = stream.producer(topic=OUT, producer_id="model", attempt=1)
    landed = await first.append({"id": "r1"})
    assert landed is not None
    # The retry of the same attempt starts its sequence over and appends the
    # same record. The provider stores it once and answers with where the
    # original landed, so the retry can checkpoint the same position.
    retry = stream.producer(topic=OUT, producer_id="model", attempt=1)
    assert await retry.append({"id": "r1"}) == landed
    # An empty call writes nothing and answers the same way.
    assert await retry.append() == landed

    records = await take(stream.read(topic=OUT), 1)
    assert records[0].value == {"id": "r1"}
    assert records[0].cursor == landed
    # The store holds exactly the one record: the newest position is its cursor.
    assert await stream.latest(topic=OUT) == landed


async def test_retried_append_is_stored_once(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    first = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await first.append({"id": "r1"})
    retry = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await retry.append({"id": "r1"})
    await retry.append({"id": "r2"})

    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"id": "r1"}, {"id": "r2"}]


async def test_new_attempt_supersedes_the_old_one(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    first = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await first.append({"text": "The capital of"})
    second = stream.producer(topic=OUT, producer_id="model", attempt=2)
    await second.append({"text": "Paris is the capital"})

    records = await take(stream.read(topic=OUT), 3)
    assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
    assert records[1].kind is RecordKind.SUPERSEDED
    assert records[1].supersession == Supersession("model", 1, 2)
    assert records[1].value is None
    assert records[2].kind is RecordKind.DATA and records[2].attempt == 2


async def test_a_superseded_record_resumes_to_the_triggering_record(
    case: ProviderCase,
):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    first = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await first.append({"n": 1})
    second = stream.producer(topic=OUT, producer_id="model", attempt=2)
    await second.append({"n": 2})

    records = await take(stream.read(topic=OUT), 3)
    superseded = records[1]
    assert superseded.kind is RecordKind.SUPERSEDED
    # The synthesized record sits at the position before the new attempt's
    # first record, so a consumer that checkpoints it and restarts is handed
    # that record rather than skipping it.
    assert superseded.cursor == records[0].cursor
    resumed = await take(stream.read(topic=OUT, after=superseded.cursor), 1)
    assert resumed[0].kind is RecordKind.DATA
    assert resumed[0].value == {"n": 2}


async def test_topics_are_addressed_by_name(case: ProviderCase):
    # Two producers on two topics of the same workflow's stream: each read
    # names its topic and sees only that topic's records.
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    on_a = stream.producer(topic=A, producer_id="tool-a", attempt=1)
    await on_a.append({"n": 1})
    on_b = stream.producer(topic=B, producer_id="tool-b", attempt=1)
    await on_b.append({"n": 2})

    only_a = await take(stream.read(topic=A), 1)
    assert [(r.topic, r.value) for r in only_a] == [("a", {"n": 1})]
    only_b = await take(stream.read(topic=B), 1)
    assert [(r.topic, r.value) for r in only_b] == [("b", {"n": 2})]


async def test_cursor_resumes_where_it_points(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    records = await take(stream.read(topic=OUT), 3)
    checkpoint = records[0].cursor

    # Resuming after a record hands back everything past it and nothing
    # twice, without the reader ever advancing a cursor itself.
    again = await take(stream.read(topic=OUT, after=checkpoint), 2)
    assert [r.value for r in again] == [{"n": 2}, {"n": 3}]


@pytest.mark.reports_positions
async def test_append_cursor_names_the_last_record_of_the_batch(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    appended = await producer.append({"n": 1}, {"n": 2}, {"n": 3})
    assert appended is not None
    then = await producer.append({"n": 4})

    # A producer that resumes a reader after its own append must see only
    # what came later, not the tail of the batch it just wrote.
    records = await take(stream.read(topic=OUT, after=appended), 1)
    assert [r.value for r in records] == [{"n": 4}]
    assert records[0].cursor == then
    assert await producer.append() == then


async def test_latest_positions_a_reader_at_the_end(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    assert await stream.latest(topic=OUT) == BEGINNING

    await producer.append({"n": 1}, {"n": 2})
    since = await stream.latest(topic=OUT)
    await producer.append({"n": 3})

    # A reader that positioned itself before the last append sees only what
    # came after, which is how a client follows a turn it is about to start.
    records = await take(stream.read(topic=OUT, after=since), 1)
    assert [r.value for r in records] == [{"n": 3}]


async def test_topic_addresses_with_colons_do_not_share_a_store(case: ProviderCase):
    # ("wf:x", "y") and ("wf", "x:y") differ only in where the colon sits.
    base = new_workflow_id()
    left = await case.open(f"{base}:x")
    right = await case.open(base)
    await left.producer(topic=Y, producer_id="l", attempt=1).append({"side": "left"})
    await right.producer(topic=XY, producer_id="r", attempt=1).append({"side": "right"})

    only_left = await take(left.read(topic=Y), 1)
    assert [r.value for r in only_left] == [{"side": "left"}]
    assert await left.latest(topic=Y) == only_left[0].cursor
    only_right = await take(right.read(topic=XY), 1)
    assert [r.value for r in only_right] == [{"side": "right"}]
    assert await right.latest(topic=XY) == only_right[0].cursor


async def test_a_foreign_cursor_is_refused_at_the_call(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    # Refused by read() itself, not by the first iteration of its generator,
    # so the caller's except clause is where the mistake surfaces.
    with pytest.raises(StreamCursorError):
        stream.read(topic=OUT, after=Cursor("elsewhere:42"))


async def test_argument_mistakes_are_value_errors(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    with pytest.raises(ValueError):
        stream.read(topic="")
    with pytest.raises(ValueError):
        stream.producer(topic="", producer_id="model", attempt=1)
    # Outside an activity there is no identity to fall back on.
    with pytest.raises(ValueError, match="producer_id is required"):
        stream.producer(topic=OUT)


async def test_a_definition_carries_its_type_once(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    with pytest.raises(ValueError, match="already carries its type"):
        stream.read(topic=OUT, result_type=dict)  # type: ignore[call-overload]
    with pytest.raises(ValueError):
        topic("", dict)
    # A string names a topic decided at runtime, and the hint rides the call.
    assert await stream.latest(topic=OUT.name) == BEGINNING


async def test_last_n_starts_at_the_newest_records(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3}, {"n": 4})

    newest = await take(stream.read(topic=OUT, last=2), 2)
    assert [r.value for r in newest] == [{"n": 3}, {"n": 4}]
    # Fewer records than asked for is all of them, not an error.
    everything = await take(stream.read(topic=OUT, last=100), 4)
    assert [r.value for r in everything] == [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]
    # The cursors it yields are ordinary cursors, so a resume after one works.
    again = await take(stream.read(topic=OUT, after=newest[0].cursor), 1)
    assert [r.value for r in again] == [{"n": 4}]


async def test_last_n_counts_finish_records(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2})
    await producer.finish()

    records = await take(stream.read(topic=OUT, last=2), 2)
    assert [(r.kind, r.value) for r in records] == [
        (RecordKind.DATA, {"n": 2}),
        (RecordKind.FINISH, None),
    ]


async def test_end_reads_only_what_arrives_after_the_read_starts(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": "old"}, {"n": "old"})

    records = stream.read(topic=OUT, after=END)
    first = asyncio.ensure_future(records.__anext__())
    # END resolves when the read starts, and nothing says when that was, so
    # appends keep coming until the reader takes one.
    try:
        for _ in range(100):
            await producer.append({"n": "new"})
            done, _ = await asyncio.wait({first}, timeout=0.1)
            if done:
                break
        record = await asyncio.wait_for(first, 5)
    finally:
        await records.aclose()
    assert record.value == {"n": "new"}


@pytest.mark.truncates
async def test_beginning_starts_at_the_oldest_record_still_held(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3}, {"n": 4})
    before = await take(stream.read(topic=OUT), 1)
    assert case.truncate is not None
    await case.truncate(workflow_id, OUT.name, 2)

    # BEGINNING is the oldest record retained, not offset zero, which a
    # truncated stream no longer holds.
    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"n": 3}, {"n": 4}]
    newest = await take(stream.read(topic=OUT, last=3), 2)
    assert [r.value for r in newest] == [{"n": 3}, {"n": 4}]
    with pytest.raises(StreamCursorError):
        stream.read(topic=OUT, after=before[0].cursor)


async def test_a_read_start_names_one_place(case: ProviderCase):
    stream = await case.open(new_workflow_id())
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    appended = await producer.append({"n": 1})
    for last in (0, -1, True):
        with pytest.raises(ValueError, match="positive"):
            stream.read(topic=OUT, last=last)
    if appended is not None:
        with pytest.raises(ValueError, match="either after= or last="):
            stream.read(topic=OUT, after=appended, last=1)
    with pytest.raises(ValueError, match="either after= or last="):
        stream.read(topic=OUT, after=END, last=1)


async def test_a_ref_names_the_stream_and_round_trips_as_data(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    ref = stream.ref(topic=OUT)
    assert ref == StreamRef.for_workflow(workflow_id, topic="out")
    assert (ref.kind, ref.run_id, ref.activity_id, ref.stream_id) == (
        "workflow",
        None,
        None,
        None,
    )
    # Without a topic the ref names the owner alone; the reader names one.
    assert stream.ref().topic is None
    assert stream.ref().with_topic(A) == stream.ref(topic=A)
    # A pinned handle hands out a pinned ref.
    pinned = await case.open(workflow_id, run_id="run-1")
    assert pinned.ref(topic=OUT).run_id == "run-1"

    # Plain data through the default converter, so it can be a workflow
    # argument, an activity result or a Nexus operation input or result.
    converter = DataConverter.default
    [carried] = await converter.decode(await converter.encode([ref]), [StreamRef])
    assert carried == ref


async def test_an_owned_stream_cannot_be_closed_by_a_handle(case: ProviderCase):
    # A workflow's stream ends with the workflow; close() is for a stream
    # that stands alone.
    stream = await case.open(new_workflow_id())
    with pytest.raises(ValueError, match="standalone"):
        await stream.close()


async def test_a_body_above_the_threshold_is_offloaded_and_read_back(
    case: ProviderCase, client: Client
):
    driver = RecordingDriver()
    converter = dataclasses.replace(
        DataConverter.default,
        external_storage=ExternalStorage(drivers=[driver], payload_size_threshold=256),
    )
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id, client=_client_with(client, converter))
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    small = {"n": 1}
    large = {"blob": "x" * 1024}
    await producer.append(small)
    await producer.append(large)
    # Only the body over the threshold left the record; the small one stayed
    # inline, as it would on any other payload the SDK sends.
    assert driver.stored == 1

    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [small, large]
    assert driver.retrieved == 1


@pytest.mark.detects_divergent_retries
async def test_a_retry_through_a_nondeterministic_codec_still_deduplicates(
    case: ProviderCase, client: Client
):
    codec = NonceCodec()
    converter = dataclasses.replace(DataConverter.default, payload_codec=codec)
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id, client=_client_with(client, converter))
    first = stream.producer(topic=OUT, producer_id="model", attempt=1)
    landed = await first.append({"id": "r1"})
    assert codec.encoded == 1

    # The codec produced different bytes for the retry. The provider matched
    # it by the plaintext it converted, so it is the same append: stored
    # once, answered with the original position.
    retry = stream.producer(topic=OUT, producer_id="model", attempt=1)
    again = await retry.append({"id": "r1"})
    if landed is not None:
        assert again == landed
    # And a retry that really does differ is still told apart.
    divergent = stream.producer(topic=OUT, producer_id="model", attempt=1)
    with pytest.raises(StreamProducerError):
        await divergent.append({"id": "other"})

    await first.append({"id": "r2"})
    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"id": "r1"}, {"id": "r2"}]


async def test_a_ref_opens_the_stream_it_names(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    await stream.producer(topic=A, producer_id="model", attempt=1).append({"n": 1})
    ref = stream.ref(topic=A)

    # The receiver names no topic: the ref carried it, so every call on the
    # handle it opened addresses topic ``a`` of that workflow.
    opened = await case.open_ref(ref)
    records = await take(opened.read(result_type=dict), 1)
    assert [(r.topic, r.value) for r in records] == [("a", {"n": 1})]
    assert await opened.latest() == records[0].cursor
    assert opened.ref() == ref
    await opened.producer(producer_id="tool", attempt=1).append({"n": 2})
    assert [r.value for r in await take(stream.read(topic=A), 2)] == [
        {"n": 1},
        {"n": 2},
    ]
    # Naming a topic on the opened handle addresses that topic instead.
    assert await opened.latest(topic=B) == BEGINNING
    assert opened.ref(topic=B) == stream.ref(topic=B)


@pytest.mark.hosts_standalone_streams
async def test_a_standalone_stream_is_read_from_another_handle(case: ProviderCase):
    stream_id = new_stream_id()
    created = await case.create_stream(stream_id)
    producer = created.producer(topic=OUT, producer_id="writer", attempt=1)
    await producer.append({"n": 1}, {"n": 2})

    # Any process reaches the stream by its id, or by a ref the creator
    # handed out; nothing about the stream depends on who created it.
    other = await case.open_standalone(stream_id)
    records = await take(other.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"n": 1}, {"n": 2}]
    assert await other.latest(topic=OUT) == records[1].cursor
    assert other.ref(topic=OUT) == StreamRef.for_standalone(stream_id, topic="out")
    via_ref = await case.open_ref(created.ref(topic=OUT))
    assert [r.value for r in await take(via_ref.read(), 2)] == [{"n": 1}, {"n": 2}]


@pytest.mark.hosts_standalone_streams
async def test_a_missing_standalone_stream_is_not_found(case: ProviderCase):
    if case.waits_for_standalone_creation:
        pytest.skip(
            f"the {case.name} provider waits for a standalone stream to be created"
        )
    # get_stream_handle(stream_id=) creates nothing: the stream has to have
    # been created on purpose, and a use before that says so.
    stream = await case.open_standalone(new_stream_id())
    with pytest.raises(StreamNotFoundError):
        await stream.latest(topic=OUT)
    with pytest.raises(StreamNotFoundError):
        await take(stream.read(topic=OUT), 1)
    with pytest.raises(StreamNotFoundError):
        await stream.producer(topic=OUT, producer_id="writer", attempt=1).append(
            {"n": 1}
        )


@pytest.mark.hosts_standalone_streams
async def test_creating_a_standalone_stream_is_idempotent_for_one_policy(
    case: ProviderCase,
):
    stream_id = new_stream_id()
    first = await case.create_stream(stream_id, max_records=10)
    # The same id and policy again is the same stream, not an error, so a
    # retried create is harmless.
    again = await case.create_stream(stream_id, max_records=10)
    await first.producer(topic=OUT, producer_id="writer", attempt=1).append({"n": 1})
    assert [r.value for r in await take(again.read(topic=OUT), 1)] == [{"n": 1}]
    # A different policy on an existing id is a mistake, not a change.
    with pytest.raises(ValueError):
        await case.create_stream(stream_id, max_records=5)
    for bad in (dict(max_records=0), dict(max_bytes=-1), dict(retention=timedelta(0))):
        with pytest.raises(ValueError):
            await case.create_stream(new_stream_id(), **bad)  # type: ignore[arg-type]


@pytest.mark.hosts_standalone_streams
async def test_closing_a_standalone_stream_ends_reads_and_refuses_appends(
    case: ProviderCase,
):
    stream_id = new_stream_id()
    stream = await case.create_stream(stream_id)
    producer = stream.producer(topic=OUT, producer_id="writer", attempt=1)
    await producer.append({"n": 1})
    # A reader parked on the tail before the close has to learn of it.
    other = await case.open_standalone(stream_id)
    parked = asyncio.ensure_future(drain(other.read(topic=OUT), timeout=10))
    await asyncio.sleep(0.2)
    await producer.append({"n": 2})

    await stream.close()
    assert [r.value for r in await parked] == [{"n": 1}, {"n": 2}]
    # Sealed: the tail stays readable, and a read opened now ends by itself.
    assert [r.value for r in await drain(stream.read(topic=OUT))] == [
        {"n": 1},
        {"n": 2},
    ]
    with pytest.raises(StreamClosedError):
        await producer.append({"n": 3})
    with pytest.raises(StreamClosedError):
        await other.producer(topic=A, producer_id="late", attempt=1).append({"n": 3})
    # Closing again is not an error.
    await stream.close()


@pytest.mark.hosts_standalone_streams
async def test_a_standalone_stream_honors_its_retention_policy(case: ProviderCase):
    by_count = await case.create_stream(new_stream_id(), max_records=2)
    producer = by_count.producer(topic=OUT, producer_id="writer", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3}, {"n": 4})
    # BEGINNING is the oldest record still held, which the policy decided.
    kept = await take(by_count.read(topic=OUT), 2)
    assert [r.value for r in kept] == [{"n": 3}, {"n": 4}]

    by_bytes = await case.create_stream(new_stream_id(), max_bytes=700)
    producer = by_bytes.producer(topic=OUT, producer_id="writer", attempt=1)
    for n in range(3):
        await producer.append({"n": n, "blob": "x" * 500})
    kept = await take(by_bytes.read(topic=OUT), 1)
    assert kept[0].value is not None and kept[0].value["n"] == 2

    by_age = await case.create_stream(
        new_stream_id(), retention=timedelta(milliseconds=200)
    )
    producer = by_age.producer(topic=OUT, producer_id="writer", attempt=1)
    await producer.append({"n": "old"})
    await asyncio.sleep(0.3)
    await producer.append({"n": "new"})
    kept = await take(by_age.read(topic=OUT), 1)
    assert [r.value for r in kept] == [{"n": "new"}]
