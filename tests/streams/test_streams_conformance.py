"""Conformance tests for the stream contract's outside surface.

Written against the public surface, parametrised over the providers this
tree can stand up. The memory provider always runs, with no server and no
store. A storage provider adds itself to ``SETUPS`` behind its own
``STREAMS_LIVE`` gate: its setup hands back a provider instance, the client
the cases should use, and says which capabilities it lacks, so the cases
marked ``reports_positions`` are skipped with a reason on a provider whose
``append()`` learns positions at read time.

What this file pins down is the contract: the record on the wire, producer
identity, retry deduplication, positions, supersession, topic addressing,
cursor resumption, cursor ownership, and store keys that cannot collide. The
workflow-side handles and the two rules about Workflow Tasks live in
``test_streams_workflow``.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, cast

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.converter import DataConverter
from temporalio.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamHandle,
    StreamProvider,
    Supersession,
    _ids,
    _wire,
)
from temporalio.streams._policy import AttemptTracker
from temporalio.streams.providers.memory import MemoryStreams


@dataclass
class ProviderCase:
    """One provider under test, and what the cases may ask of it."""

    name: str
    provider: StreamProvider
    client: Client | None = None
    reports_positions: bool = True
    """``append()`` returns where the records landed."""

    def handle(self, workflow_id: str, *, run_id: str | None = None) -> StreamHandle:
        # The memory provider takes no client; every storage provider's setup
        # supplies one, so the cast only ever lies for the provider that
        # does not read it.
        return self.provider.get_stream_handle(
            cast(Client, self.client), workflow_id, run_id=run_id
        )


async def _memory_case() -> AsyncIterator[ProviderCase]:
    provider = MemoryStreams()
    yield ProviderCase("memory", provider)
    provider.reset()


SETUPS: dict[str, Callable[[], AsyncIterator[ProviderCase]]] = {"memory": _memory_case}

_CAPABILITIES = {
    "reports_positions": lambda case: case.reports_positions,
}


@pytest.fixture(params=sorted(SETUPS))
async def case(request: pytest.FixtureRequest) -> AsyncIterator[ProviderCase]:
    async for provider_case in SETUPS[request.param]():
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


def test_record_roundtrips_through_the_wire():
    converter = DataConverter.default.payload_converter
    wire = _wire.to_wire(
        converter,
        topic="decisions",
        kind=RecordKind.DATA,
        value={"n": 1},
        producer_id="model",
        attempt=3,
        sequence=7,
    )
    parsed = _wire.WireRecord.FromString(wire.SerializeToString())
    record = _wire.from_wire(converter, Cursor("memory:0"), parsed, dict)
    assert (
        record.kind,
        record.topic,
        record.producer_id,
        record.attempt,
        record.sequence,
        record.value,
    ) == (RecordKind.DATA, "decisions", "model", 3, 7, {"n": 1})
    assert record.supersession is None
    finish = _wire.to_wire(converter, topic="decisions", kind=RecordKind.FINISH)
    assert not finish.HasField("body")
    assert _wire.from_wire(converter, Cursor("memory:1"), finish, dict).value is None


def test_a_stored_supersession_is_not_a_record():
    converter = DataConverter.default.payload_converter
    wire = _wire.WireRecord(topic="t", kind=int(RecordKind.SUPERSEDED))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="synthesized"):
        _wire.from_wire(converter, Cursor("memory:0"), wire, None)


def test_an_unset_kind_is_read_as_data():
    converter = DataConverter.default.payload_converter
    wire = _wire.WireRecord(topic="t", body=converter.to_payloads([{"n": 1}])[0])
    record = _wire.from_wire(converter, Cursor("memory:0"), wire, dict)
    assert record.kind is RecordKind.DATA
    assert record.value == {"n": 1}


def test_supersession_is_synthesized_from_observations():
    attempts = AttemptTracker()
    assert attempts.note("model", 1, topic="t", previous=BEGINNING) is None
    superseded = attempts.note("model", 2, topic="t", previous=Cursor("memory:0"))
    assert superseded is not None
    assert superseded.kind is RecordKind.SUPERSEDED
    assert superseded.supersession == Supersession("model", 1, 2)
    assert superseded.value is None
    # Positioned before the triggering record, so a resume after it delivers
    # that record next.
    assert superseded.cursor == Cursor("memory:0")
    # The same attempt again is not a new generation.
    assert attempts.note("model", 2, topic="t", previous=Cursor("memory:1")) is None


def test_topic_keys_cannot_collide():
    # A colon in a workflow id must not make two addresses one key.
    assert _ids.topic_key("a:b", "c") != _ids.topic_key("a", "b:c")
    assert _ids.topic_key("a%3Ab", "c") != _ids.topic_key("a:b", "c")
    assert _ids.topic_key("wf", "inputs") == "wf:inputs"


def test_cursors_name_their_provider():
    assert _wire.cursor_position(BEGINNING, provider="memory") is None
    assert _wire.cursor_position(Cursor("memory:42"), provider="memory") == "42"
    with pytest.raises(StreamCursorError):
        _wire.cursor_position(Cursor("redis:1700000000000-0"), provider="memory")


async def test_append_read_roundtrip(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    producer = stream.producer(topic="out", producer_id="model", attempt=1)
    assert (producer.producer_id, producer.attempt) == ("model", 1)
    await producer.append({"id": "r1"}, {"id": "r2"})
    await producer.finish()

    records = await take(stream.read(topic="out", result_type=dict), 3)
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert [r.value for r in records[:2]] == [{"id": "r1"}, {"id": "r2"}]
    assert records[2].value is None
    assert all(r.producer_id == "model" and r.attempt == 1 for r in records)
    assert [r.sequence for r in records] == [0, 1, 2]
    assert all(r.topic == "out" for r in records)


async def test_raw_values_pass_through_untouched(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    payload = Payload(metadata={"encoding": b"binary/plain"}, data=b"\x00\x01raw")
    producer = stream.producer(topic="out", producer_id="model", attempt=1)
    await producer.append(RawValue(payload))

    records = await take(stream.read(topic="out", result_type=RawValue), 1)
    assert isinstance(records[0].value, RawValue)
    assert records[0].value.payload == payload


@pytest.mark.reports_positions
async def test_retried_append_returns_the_original_position(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    first = stream.producer(topic="out", producer_id="model", attempt=1)
    landed = await first.append({"id": "r1"})
    assert landed is not None
    # The retry of the same attempt starts its sequence over and appends the
    # same record. The provider stores it once and answers with where the
    # original landed, so the retry can checkpoint the same position.
    retry = stream.producer(topic="out", producer_id="model", attempt=1)
    assert await retry.append({"id": "r1"}) == landed
    # An empty call writes nothing and answers the same way.
    assert await retry.append() == landed

    records = await take(stream.read(topic="out", result_type=dict), 1)
    assert records[0].value == {"id": "r1"}
    assert records[0].cursor == landed
    # The store holds exactly the one record: the newest position is its cursor.
    assert await stream.latest(topic="out") == landed


async def test_retried_append_is_stored_once(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    first = stream.producer(topic="out", producer_id="model", attempt=1)
    await first.append({"id": "r1"})
    retry = stream.producer(topic="out", producer_id="model", attempt=1)
    await retry.append({"id": "r1"})
    await retry.append({"id": "r2"})

    records = await take(stream.read(topic="out", result_type=dict), 2)
    assert [r.value for r in records] == [{"id": "r1"}, {"id": "r2"}]


async def test_new_attempt_supersedes_the_old_one(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    first = stream.producer(topic="out", producer_id="model", attempt=1)
    await first.append({"text": "The capital of"})
    second = stream.producer(topic="out", producer_id="model", attempt=2)
    await second.append({"text": "Paris is the capital"})

    records = await take(stream.read(topic="out", result_type=dict), 3)
    assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
    assert records[1].kind is RecordKind.SUPERSEDED
    assert records[1].supersession == Supersession("model", 1, 2)
    assert records[1].value is None
    assert records[2].kind is RecordKind.DATA and records[2].attempt == 2


async def test_a_superseded_record_resumes_to_the_triggering_record(
    case: ProviderCase,
):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    first = stream.producer(topic="out", producer_id="model", attempt=1)
    await first.append({"n": 1})
    second = stream.producer(topic="out", producer_id="model", attempt=2)
    await second.append({"n": 2})

    records = await take(stream.read(topic="out", result_type=dict), 3)
    superseded = records[1]
    assert superseded.kind is RecordKind.SUPERSEDED
    # The synthesized record sits at the position before the new attempt's
    # first record, so a consumer that checkpoints it and restarts is handed
    # that record rather than skipping it.
    assert superseded.cursor == records[0].cursor
    resumed = await take(
        stream.read(topic="out", result_type=dict, after=superseded.cursor), 1
    )
    assert resumed[0].kind is RecordKind.DATA
    assert resumed[0].value == {"n": 2}


async def test_topics_are_addressed_by_name(case: ProviderCase):
    # Two producers on two topics of the same workflow's stream: each read
    # names its topic and sees only that topic's records.
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    on_a = stream.producer(topic="a", producer_id="tool-a", attempt=1)
    await on_a.append({"n": 1})
    on_b = stream.producer(topic="b", producer_id="tool-b", attempt=1)
    await on_b.append({"n": 2})

    only_a = await take(stream.read(topic="a", result_type=dict), 1)
    assert [(r.topic, r.value) for r in only_a] == [("a", {"n": 1})]
    only_b = await take(stream.read(topic="b", result_type=dict), 1)
    assert [(r.topic, r.value) for r in only_b] == [("b", {"n": 2})]


async def test_cursor_resumes_where_it_points(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    producer = stream.producer(topic="out", producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    records = await take(stream.read(topic="out", result_type=dict), 3)
    checkpoint = records[0].cursor

    # Resuming after a record hands back everything past it and nothing
    # twice, without the reader ever advancing a cursor itself.
    again = await take(stream.read(topic="out", result_type=dict, after=checkpoint), 2)
    assert [r.value for r in again] == [{"n": 2}, {"n": 3}]


@pytest.mark.reports_positions
async def test_append_cursor_names_the_last_record_of_the_batch(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    producer = stream.producer(topic="out", producer_id="model", attempt=1)
    appended = await producer.append({"n": 1}, {"n": 2}, {"n": 3})
    assert appended is not None
    then = await producer.append({"n": 4})

    # A producer that resumes a reader after its own append must see only
    # what came later, not the tail of the batch it just wrote.
    records = await take(stream.read(topic="out", result_type=dict, after=appended), 1)
    assert [r.value for r in records] == [{"n": 4}]
    assert records[0].cursor == then
    assert await producer.append() == then


async def test_latest_positions_a_reader_at_the_end(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = case.handle(workflow_id)
    producer = stream.producer(topic="out", producer_id="model", attempt=1)
    assert await stream.latest(topic="out") == BEGINNING

    await producer.append({"n": 1}, {"n": 2})
    since = await stream.latest(topic="out")
    await producer.append({"n": 3})

    # A reader that positioned itself before the last append sees only what
    # came after, which is how a client follows a turn it is about to start.
    records = await take(stream.read(topic="out", result_type=dict, after=since), 1)
    assert [r.value for r in records] == [{"n": 3}]


async def test_topic_addresses_with_colons_do_not_share_a_store(case: ProviderCase):
    # ("wf:x", "y") and ("wf", "x:y") differ only in where the colon sits.
    base = new_workflow_id()
    left = case.handle(f"{base}:x")
    right = case.handle(base)
    await left.producer(topic="y", producer_id="l", attempt=1).append({"side": "left"})
    await right.producer(topic="x:y", producer_id="r", attempt=1).append(
        {"side": "right"}
    )

    only_left = await take(left.read(topic="y", result_type=dict), 1)
    assert [r.value for r in only_left] == [{"side": "left"}]
    assert await left.latest(topic="y") == only_left[0].cursor
    only_right = await take(right.read(topic="x:y", result_type=dict), 1)
    assert [r.value for r in only_right] == [{"side": "right"}]
    assert await right.latest(topic="x:y") == only_right[0].cursor


async def test_a_foreign_cursor_is_refused_at_the_call(case: ProviderCase):
    stream = case.handle(new_workflow_id())
    # Refused by read() itself, not by the first iteration of its generator,
    # so the caller's except clause is where the mistake surfaces.
    with pytest.raises(StreamCursorError):
        stream.read(topic="out", after=Cursor("elsewhere:42"))


async def test_argument_mistakes_are_value_errors(case: ProviderCase):
    stream = case.handle(new_workflow_id())
    with pytest.raises(ValueError):
        stream.read(topic="")
    with pytest.raises(ValueError):
        stream.producer(topic="", producer_id="model", attempt=1)
    # Outside an activity there is no identity to fall back on.
    with pytest.raises(ValueError, match="producer_id is required"):
        stream.producer(topic="out")
