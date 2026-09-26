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

``NexusStreams`` is deliberately absent from ``SETUPS``. It is a front with no
workflow half (its ``workflow_provider()`` raises), so there is no store for a
host workflow to own; ``test_nexus_provider`` covers it through an endpoint
that fronts a storage provider.

What this file pins down is what a provider owes: producer identity, retry
deduplication, positions, supersession, topic addressing, cursor resumption,
cursor ownership, and releasing a read the caller stopped early. Every case
here goes through the public surface, so a new provider answers this file and
nothing else. The shared pieces no provider implements are unit-tested in
``test_streams_internals``; the workflow-side handles and the two rules about
Workflow Tasks live in ``test_streams_workflow``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RawValue
from temporalio.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamHandle,
    StreamProducerError,
    StreamProvider,
    Supersession,
    topic,
)
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.streams.providers.native import NativeStreams
from temporalio.streams.providers.redis import RedisStreams
from temporalio.streams.providers.workflow_streams import WorkflowStreamsProvider
from tests.helpers import new_worker

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

    async def open(
        self, workflow_id: str, *, run_id: str | None = None
    ) -> StreamHandle:
        if self.host is not None:
            await self.host(workflow_id)
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


async def _memory_case(_client: Client) -> AsyncIterator[ProviderCase]:
    provider = MemoryStreams()
    yield ProviderCase("memory", provider)
    provider.reset()


@workflow.defn
class StreamHost:
    """Owns a stream and lingers, so outside code has a running workflow to address."""

    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._released)


async def _workflow_streams_case(client: Client) -> AsyncIterator[ProviderCase]:
    # No STREAMS_LIVE gate: the store is the workflow's own History, which the
    # test environment's server provides.
    provider = WorkflowStreamsProvider(poll_cooldown=timedelta(milliseconds=20))
    # Registered once, on the client: the host's worker inherits it and the
    # cases open handles through client.get_stream_handle.
    config = client.config()
    config["plugins"] = [provider]
    client = Client(**config)
    hosts: dict[str, WorkflowHandle[Any, Any]] = {}
    async with new_worker(client, StreamHost) as worker:

        async def host(workflow_id: str) -> None:
            if workflow_id not in hosts:
                hosts[workflow_id] = await client.start_workflow(
                    StreamHost.run, id=workflow_id, task_queue=worker.task_queue
                )

        yield ProviderCase(
            "workflow_streams",
            provider,
            client,
            reports_positions=False,
            # A publish is a Signal, so the dedupe decision is taken in the
            # workflow with nowhere to report it. See the module docstring of
            # the provider.
            detects_divergent_retries=False,
            host=host,
        )
        for handle in hosts.values():
            await handle.terminate()


async def _native_case(client: Client) -> AsyncIterator[ProviderCase]:
    # The store is a server built from the stream-carrying branch, which the
    # test environment's own server is not; TEMPORAL_ADDRESS names it.
    # Without it the cases would reach a server that has no stream service and
    # fail on the wire, which says nothing about the provider.
    address = os.environ.get("TEMPORAL_ADDRESS")
    if not address:
        pytest.skip(
            "STREAMS_LIVE=native needs TEMPORAL_ADDRESS naming a server with the "
            "stream service"
        )
    client = await Client.connect(
        address, namespace=os.environ.get("TEMPORAL_NAMESPACE", "default")
    )
    provider = NativeStreams()
    # Registered once, on the client: the host's worker inherits it and the
    # cases open handles through client.get_stream_handle.
    config = client.config()
    config["plugins"] = [provider]
    client = Client(**config)
    hosts: dict[str, WorkflowHandle[Any, Any]] = {}
    async with new_worker(client, StreamHost) as worker:

        async def host(workflow_id: str) -> None:
            if workflow_id not in hosts:
                hosts[workflow_id] = await client.start_workflow(
                    StreamHost.run, id=workflow_id, task_queue=worker.task_queue
                )

        yield ProviderCase("native", provider, client, host=host)
        for handle in hosts.values():
            await handle.terminate()
    await provider.close()


async def _redis_case(client: Client) -> AsyncIterator[ProviderCase]:
    # The store is a Redis the test environment does not start; the server
    # is the environment's own unless TEMPORAL_ADDRESS names another.
    address = os.environ.get("TEMPORAL_ADDRESS")
    if address:
        client = await Client.connect(
            address, namespace=os.environ.get("TEMPORAL_NAMESPACE", "default")
        )
    provider = RedisStreams(
        url=os.environ.get("TEMPORAL_TEST_REDIS_URL")
        or os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6379"),
        # A prefix per setup, because the store keeps what earlier runs wrote.
        key_prefix=f"streams-conformance-{uuid.uuid4().hex}",
    )
    # Registered once, on the client: the host's worker inherits it and the
    # cases open handles through client.get_stream_handle.
    config = client.config()
    config["plugins"] = [provider]
    client = Client(**config)
    hosts: dict[str, WorkflowHandle[Any, Any]] = {}
    async with new_worker(client, StreamHost) as worker:

        async def host(workflow_id: str) -> None:
            if workflow_id not in hosts:
                hosts[workflow_id] = await client.start_workflow(
                    StreamHost.run, id=workflow_id, task_queue=worker.task_queue
                )

        yield ProviderCase("redis", provider, client, host=host)
        for handle in hosts.values():
            await handle.terminate()
    await provider.close()


SETUPS: dict[str, Callable[[Client], AsyncIterator[ProviderCase]]] = {
    "memory": _memory_case,
    "workflow_streams": _workflow_streams_case,
}
if os.environ.get("STREAMS_LIVE") == "native":
    SETUPS["native"] = _native_case
if os.environ.get("STREAMS_LIVE") == "redis":
    SETUPS["redis"] = _redis_case

_CAPABILITIES = {
    "reports_positions": lambda case: case.reports_positions,
    "detects_divergent_retries": lambda case: case.detects_divergent_retries,
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


async def test_the_native_setup_skips_without_a_server_address(
    monkeypatch: pytest.MonkeyPatch, client: Client
):
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    setup = _native_case(client)
    with pytest.raises(pytest.skip.Exception, match="TEMPORAL_ADDRESS"):
        await setup.__anext__()


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
    assert [r.sequence for r in records] == [1, 2, 3]
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


@pytest.mark.detects_divergent_retries
async def test_a_divergent_retry_is_refused(case: ProviderCase):
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    first = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await first.append({"id": "r1"})

    # Same producer, attempt and sequence, different content. The store has no
    # way to know which of the two the reader was meant to see, so it says so
    # rather than answering with the position of the one it kept.
    retry = stream.producer(topic=OUT, producer_id="model", attempt=1)
    with pytest.raises(StreamProducerError):
        await retry.append({"id": "other"})

    # And it wrote nothing: the producer that owns the sequence carries on
    # past the original, with no second record wedged in front of it.
    await first.append({"id": "r2"})
    records = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in records] == [{"id": "r1"}, {"id": "r2"}]


async def test_closing_a_read_early_releases_it(case: ProviderCase):
    # A read with nothing left to hand over waits against the store. Closing
    # the generator is how a caller that stops early says so, and it has to
    # let go of whatever it parked instead of hanging on it.
    workflow_id = new_workflow_id()
    stream = await case.open(workflow_id)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1})

    records = stream.read(topic=OUT)
    assert (await asyncio.wait_for(records.__anext__(), 5.0)).value == {"n": 1}
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(records.__anext__(), 0.5)
    await asyncio.wait_for(records.aclose(), 5.0)

    # The topic is untouched by the close: a new read still sees everything.
    await producer.append({"n": 2})
    again = await take(stream.read(topic=OUT), 2)
    assert [r.value for r in again] == [{"n": 1}, {"n": 2}]


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
