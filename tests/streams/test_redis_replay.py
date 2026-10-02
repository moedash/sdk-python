"""Live checks for the client-side (Redis) provider inside a workflow.

The conformance suite covers the outside surface when ``STREAMS_LIVE=redis``.
This module runs the interface loop inside a workflow over the staged commit,
lets a read end with the workflow, shares a topic between an outside producer
and the workflow, queries a completed run, which replays it, trims by
retention and shows what a replay and a read past the trim do, and seeds a
workflow reader from a cursor. All need a dev server (``TEMPORAL_ADDRESS``)
and a Redis (``TEMPORAL_TEST_REDIS_URL`` or ``AI198_REDIS_URL``). The worker
keeps a warm cache because the transport holds the task open between records.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams import (
    RecordKind as TransportRecordKind,
)
from temporalio.contrib.external_workflow_streams import StreamDirection
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputStageManifest,
    OutputStageStatus,
    StagedOutputRecord,
)
from temporalio.converter import WorkflowSerializationContext
from temporalio.streams import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamProducerError,
)
from temporalio.streams._wire import to_wire
from temporalio.streams.providers import redis as redis_provider
from temporalio.streams.providers.redis import RedisStreams, _chain, _TopicLogBackend
from temporalio.worker import Replayer, Worker
from tests.streams.test_streams_conformance import StreamHost, take
from tests.streams.test_streams_workflow import (
    DECISIONS,
    INPUTS,
    ContractLoop,
    OneLine,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "redis",
    reason="needs a live server and redis; run with STREAMS_LIVE=redis",
)


def redis_url() -> str:
    return os.environ.get("TEMPORAL_TEST_REDIS_URL") or os.environ.get(
        "AI198_REDIS_URL", "redis://127.0.0.1:6379"
    )


@pytest.fixture
async def provider() -> AsyncIterator[RedisStreams]:
    # A prefix per case, because the store keeps what earlier cases wrote.
    streams = RedisStreams(
        url=redis_url(), key_prefix=f"streams-redis-{uuid.uuid4().hex}"
    )
    try:
        yield streams
    finally:
        await streams.close()


@pytest.fixture
async def live_client(client: Client) -> Client:
    # The test environment's own server, unless TEMPORAL_ADDRESS names another.
    address = os.environ.get("TEMPORAL_ADDRESS")
    return await Client.connect(address) if address else client


async def test_interface_loop_over_redis(live_client: Client, provider: RedisStreams):
    workflow_id = f"streams-redis-live-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[ContractLoop],
        plugins=[provider],
        max_cached_workflows=100,
    ):
        handle = await live_client.start_workflow(
            ContractLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1}, {"n": 2})
        await producer.append({"n": 3})
        await producer.finish()

        records = await take(stream.read(topic=DECISIONS), 4, 60)
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        assert [r.value["decided"] for r in records[:3]] == [1, 2, 3]
        assert all(r.producer_id == "" and r.topic == DECISIONS.name for r in records)
        assert await handle.result() == [
            {"kind": "decision", "n": 1, "attempt": 1},
            {"kind": "decision", "n": 2, "attempt": 1},
            {"kind": "decision", "n": 3, "attempt": 1},
            {"kind": "finish", "producer": "model"},
        ]

        # The read ends by itself once the workflow is closed and every
        # promoted record has been handed over.
        async def read_everything() -> list[Any]:
            return [r.value async for r in stream.read(topic=DECISIONS)]

        assert await asyncio.wait_for(read_everything(), 60) == [
            {"decided": 1},
            {"decided": 2},
            {"decided": 3},
            None,
        ]
        # The producer's own records are readable from outside as well, on
        # the topic it wrote.
        inputs = await take(stream.read(topic=INPUTS), 4, 60)
        assert [r.value for r in inputs[:3]] == [{"n": 1}, {"n": 2}, {"n": 3}]
        assert inputs[3].kind is RecordKind.FINISH
        assert all(r.producer_id == "model" and r.attempt == 1 for r in inputs)


async def _serves_wakes(client: Client) -> bool:
    """Whether the server implements the wake call, probed with a wake to nobody."""
    import temporalio.api.common.v1
    import temporalio.api.workflow.v1
    import temporalio.api.workflowservice.v1
    from temporalio.service import RPCError, RPCStatusCode

    try:
        await client.workflow_service.wake_workflow_execution(
            temporalio.api.workflowservice.v1.WakeWorkflowExecutionRequest(
                namespace=client.namespace,
                workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=f"streams-redis-probe-{uuid.uuid4().hex}"
                ),
                wake=temporalio.api.workflow.v1.Wake(source="probe", counter=1),
            )
        )
    except RPCError as error:
        return error.status != RPCStatusCode.UNIMPLEMENTED
    return True


async def test_an_outside_producer_wakes_the_reader_without_a_signal(
    live_client: Client,
):
    if not await _serves_wakes(live_client):
        pytest.skip("the server does not implement WakeWorkflowExecution")
    # The wake transport rather than "auto", so a fallback to the Signal fails
    # the History check below instead of passing quietly.
    provider = RedisStreams(
        url=redis_url(),
        key_prefix=f"streams-redis-{uuid.uuid4().hex}",
        wake_transport="wake",
    )
    workflow_id = f"streams-redis-wake-{uuid.uuid4().hex}"
    try:
        async with Worker(
            live_client,
            task_queue=f"tq-{workflow_id}",
            workflows=[ContractLoop],
            plugins=[provider],
            max_cached_workflows=100,
        ):
            handle = await live_client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
            )
            stream = provider.get_stream_handle(live_client, workflow_id)
            producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            # Spaced past the reader's idle timeout, so the reader parks between
            # appends and only a wake from outside can move it.
            for n in (1, 2, 3):
                await producer.append({"n": n})
                await asyncio.sleep(2)
            await producer.finish()

            result = await asyncio.wait_for(handle.result(), 60)
            assert [entry.get("n") for entry in result[:3]] == [1, 2, 3]
            assert result[3] == {"kind": "finish", "producer": "model"}

            signalled = [
                event
                async for event in handle.fetch_history_events()
                if event.HasField("workflow_execution_signaled_event_attributes")
            ]
            assert signalled == []
    finally:
        await provider.close()


async def test_an_outside_producer_and_the_workflow_share_a_topic(
    live_client: Client, provider: RedisStreams
):
    workflow_id = f"streams-redis-shared-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[OneLine],
        plugins=[provider],
    ):
        handle = await live_client.start_workflow(
            OneLine.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        await stream.producer(topic=DECISIONS, producer_id="tool", attempt=1).append(
            {"from": "producer"}
        )
        await handle.result()

        async def read_everything() -> list[Any]:
            return [r async for r in stream.read(topic=DECISIONS)]

        records = await asyncio.wait_for(read_everything(), 60)
    # Both writers land on one topic, each under its own identity. The order
    # between them is whatever the store took first.
    assert sorted(
        ((r.producer_id, r.kind, r.value) for r in records), key=str
    ) == sorted(
        [
            ("tool", RecordKind.DATA, {"from": "producer"}),
            ("", RecordKind.DATA, {"from": "workflow"}),
            ("", RecordKind.FINISH, None),
        ],
        key=str,
    )


@workflow.defn
class SignalWokenPublish:
    """Publishes in the task a signal wakes, then completes in that same task."""

    def __init__(self) -> None:
        self._closed = False

    @workflow.signal
    def close(self) -> None:
        self._closed = True

    @workflow.run
    async def run(self) -> int:
        out = workflow.stream_writer("out")
        out.publish({"n": 0})
        await workflow.wait_condition(lambda: self._closed)
        for n in range(1, 4):
            out.publish({"n": n})
        return 4

    @workflow.query
    def probe(self) -> int:
        return 1


async def test_query_after_completion_replays_the_final_task(
    live_client: Client, provider: RedisStreams
):
    # A query against a completed run replays it. The final task's publishes
    # were woken by a signal, which the activation applies before the replay
    # marker, so the marker's expectations have to be installed before that
    # task's code runs or the replay records nothing against a manifest of
    # three.
    workflow_id = f"streams-redis-replay-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[SignalWokenPublish],
        plugins=[provider],
    ):
        handle = await live_client.start_workflow(
            SignalWokenPublish.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        await take(stream.read(topic="out", result_type=dict), 1, timeout=60)
        await handle.signal(SignalWokenPublish.close)
        assert await handle.result() == 4
        assert await handle.query(SignalWokenPublish.probe) == 1
        values = [r.value async for r in stream.read(topic="out", result_type=dict)]
        assert values == [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}]


async def _log_length(
    streams: RedisStreams, client: Client, workflow_id: str, topic: str
) -> int:
    """How many entries the topic's log holds.

    Both of the transport's keys for the topic render onto the log, which is
    what makes it one; asked through both so a test would notice if they came
    apart.
    """
    import redis.asyncio

    backend = streams._require_backend()
    chain = await _chain(client, workflow_id)
    store = redis.asyncio.from_url(redis_url())
    try:
        by_input = backend.stream_key(
            chain.stream_key(topic, direction=StreamDirection.INPUT)
        )
        by_output = backend.stream_key(
            chain.stream_key(topic, direction=StreamDirection.OUTPUT)
        )
        assert by_input == by_output
        return await store.xlen(by_input)
    finally:
        await store.aclose()


async def _trim_everything(
    streams: RedisStreams, client: Client, workflow_id: str, topic: str
) -> None:
    """What retention elsewhere, or an operator, does to a topic's output key."""
    import redis.asyncio

    backend = streams._require_backend()
    chain = await _chain(client, workflow_id)
    store = redis.asyncio.from_url(redis_url())
    try:
        await store.xtrim(
            backend.stream_key(
                chain.stream_key(topic, direction=StreamDirection.OUTPUT)
            ),
            maxlen=0,
            approximate=False,
        )
    finally:
        await store.aclose()


async def test_a_replay_past_the_retention_window_fails_loudly(live_client: Client):
    # Six entries per key: the loop's four input records stay while it runs,
    # and six more appends afterwards push them out.
    streams = RedisStreams(
        url=redis_url(), key_prefix=f"streams-redis-{uuid.uuid4().hex}", max_len=6
    )
    workflow_id = f"streams-redis-retention-{uuid.uuid4().hex}"
    try:
        async with Worker(
            live_client,
            task_queue=f"tq-{workflow_id}",
            workflows=[ContractLoop],
            plugins=[streams],
            max_cached_workflows=100,
        ):
            handle = await live_client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
            )
            stream = streams.get_stream_handle(live_client, workflow_id)
            producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            await producer.append({"n": 1}, {"n": 2}, {"n": 3})
            await producer.finish()
            assert len(await handle.result()) == 4
            before = await take(stream.read(topic=INPUTS), 4, 60)
        history = await handle.fetch_history()
        assert await _log_length(streams, live_client, workflow_id, INPUTS.name) == 4

        # Inside the window the recorded ranges read back and the replay passes.
        await Replayer(workflows=[ContractLoop], plugins=[streams]).replay_workflow(
            history
        )

        late = stream.producer(topic=INPUTS, producer_id="late", attempt=1)
        cursors = [await late.append({"n": n}) for n in range(10, 16)]
        assert await _log_length(streams, live_client, workflow_id, INPUTS.name) == 6

        # The recorded input ranges are gone, and the replay says so rather
        # than delivering fewer records.
        with pytest.raises(Exception) as failure:
            await Replayer(workflows=[ContractLoop], plugins=[streams]).replay_workflow(
                history
            )
        # The task fails under the transport's integrity row: its type, the
        # external storage cause, and a message that names the window.
        message = str(failure.value)
        assert "StreamIntegrityError" in message and "ExternalStorageFailure" in message
        assert "past the redis provider's retention (" in message
        assert "max_len=6)" in message

        # An outside cursor below the trim is refused, not resumed from the
        # first retained record.
        with pytest.raises(StreamCursorError, match="retention has trimmed"):
            await take(stream.read(topic=INPUTS, after=before[0].cursor), 1, 10)

        # Inside the window a read still works and lands where append said.
        records = [r async for r in stream.read(topic=INPUTS, after=cursors[1])]
        assert [r.value for r in records] == [{"n": n} for n in range(12, 16)]
        assert [r.cursor for r in records] == cursors[2:]
    finally:
        await streams.close()


async def test_retention_by_age_trims_older_entries(live_client: Client):
    streams = RedisStreams(
        url=redis_url(),
        key_prefix=f"streams-redis-{uuid.uuid4().hex}",
        retention=timedelta(milliseconds=300),
    )
    workflow_id = f"streams-redis-age-{uuid.uuid4().hex}"
    try:
        async with Worker(
            live_client,
            task_queue=f"tq-{workflow_id}",
            workflows=[StreamHost],
            plugins=[streams],
        ):
            handle = await live_client.start_workflow(
                StreamHost.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
            )
            stream = streams.get_stream_handle(live_client, workflow_id)
            producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            first = await producer.append({"n": 1})
            await producer.append({"n": 2})
            await asyncio.sleep(0.6)
            # The append that crosses the window is what trims the two before it.
            third = await producer.append({"n": 3})
            assert (
                await _log_length(streams, live_client, workflow_id, INPUTS.name) == 1
            )
            assert await stream.latest(topic=INPUTS) == third
            with pytest.raises(StreamCursorError, match="retention has trimmed"):
                await take(stream.read(topic=INPUTS, after=first), 1, 10)

            # A fully trimmed topic answers the way an empty one does.
            await _trim_everything(streams, live_client, workflow_id, INPUTS.name)
            assert await stream.latest(topic=INPUTS) == BEGINNING
            await handle.signal(StreamHost.release)
            await handle.result()
            assert [r async for r in stream.read(topic=INPUTS)] == []
    finally:
        await streams.close()


@workflow.defn
class ResumeAfter:
    """Reads ``inputs``; the first run hands its first record's cursor to the next."""

    def __init__(self) -> None:
        self._seen: list[int] = []

    @workflow.query
    def seen(self) -> list[int]:
        return self._seen

    @workflow.run
    async def run(self, after: str | None) -> list[int]:
        reader = workflow.stream_reader(
            INPUTS, after=Cursor(after) if after else BEGINNING
        )
        first: str | None = None
        async for record in reader:
            if record.kind is RecordKind.FINISH:
                break
            assert record.value is not None
            self._seen.append(record.value["n"])
            first = first or record.cursor.token
            if after is None and len(self._seen) == 2:
                workflow.continue_as_new(first)
        return self._seen


async def test_a_workflow_reader_started_from_a_cursor_skips_the_earlier_records(
    live_client: Client, provider: RedisStreams
):
    # The first run reads two records and continues as new with the cursor of
    # the first. Without the cursor the successor would resume after the
    # second, which is where the chain left off; with it, it reads the second
    # again and then the rest.
    workflow_id = f"streams-redis-resume-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[ResumeAfter],
        plugins=[provider],
        max_cached_workflows=100,
    ):
        handle = await live_client.start_workflow(
            ResumeAfter.run, None, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1}, {"n": 2}, {"n": 3})
        await producer.finish()
        assert await handle.result() == [2, 3]
        # The completed run is evicted, so the query replays it cold with the
        # seeded position and the recorded ranges.
        assert await handle.query(ResumeAfter.seen) == [2, 3]

    # Both runs replay offline against the store, the seeded one included.
    replayer = Replayer(workflows=[ResumeAfter], plugins=[provider])
    await replayer.replay_workflow(
        await live_client.get_workflow_handle(
            workflow_id, run_id=handle.first_execution_run_id
        ).fetch_history()
    )
    await replayer.replay_workflow(await handle.fetch_history())


@workflow.defn
class BatchConsumer:
    """Applies one batch of ``inputs`` per run, handing over at the sender's FINISH."""

    @workflow.run
    async def run(self, applied: list[str]) -> list[str]:
        stop = False
        async for record in workflow.stream_reader(INPUTS):
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            assert record.value is not None
            if record.value["op"] == "stop":
                stop = True
                continue
            applied.append(record.value["op"])
        if stop:
            return applied
        workflow.continue_as_new(applied)


async def _current_open_run(
    client: Client, workflow_id: str, previous: str | None
) -> str:
    """Wait until the chain's newest run is a new one and still open."""
    while True:
        description = await client.get_workflow_handle(workflow_id).describe()
        assert description.run_id is not None
        if description.run_id != previous and description.close_time is None:
            return description.run_id
        await asyncio.sleep(0.1)


async def test_a_wake_refused_by_a_run_continuing_as_new_reaches_its_successor(
    live_client: Client, provider: RedisStreams
):
    # The consumer reads the sender's FINISH straight from the store while it
    # holds its task open and continues as new on it, so the wake Signal for
    # that record resolves to a run that is closing and the server refuses it.
    # The records are keyed by the chain and already where the successor reads
    # them; the wake has to follow, and finish() must not fail the sender.
    workflow_id = f"streams-redis-batches-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[BatchConsumer],
        plugins=[provider],
        max_cached_workflows=100,
    ):
        handle = await live_client.start_workflow(
            BatchConsumer.run, [], id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        run_id: str | None = None
        for number, batch in enumerate([["a", "b"], ["c"], ["d", "stop"]], start=1):
            run_id = await asyncio.wait_for(
                _current_open_run(live_client, workflow_id, run_id), 30
            )
            # A sender per batch: the stream spans the chain, so one identity
            # numbering its records from the start again would collide with
            # the batch before.
            sender = stream.producer(
                topic=INPUTS, producer_id=f"console-{number}", attempt=1
            )
            for op in batch:
                await sender.append({"op": op})
            await sender.finish()
        assert await asyncio.wait_for(handle.result(), 30) == ["a", "b", "c", "d"]


async def _stage_one(
    streams: RedisStreams,
    client: Client,
    workflow_id: str,
    run_id: str,
    topic: str,
    values: list[Any],
) -> tuple[Any, Any]:
    """Stage ``values`` on ``topic``'s output key without committing them.

    Staged the way the worker stages a task's publishes, so the records read back as
    records rather than as bytes the reader has to skip.
    """
    backend = streams._require_backend()
    chain = await _chain(client, workflow_id)
    key = chain.stream_key(topic, direction=StreamDirection.OUTPUT)
    codec: StreamPayloadCodec[bytes] = StreamPayloadCodec(
        client.data_converter.with_context(
            WorkflowSerializationContext(
                namespace=client.namespace, workflow_id=workflow_id
            )
        ),
        bytes,
    )
    records = []
    for index, value in enumerate(values):
        wire = to_wire(
            client.data_converter.payload_converter,
            topic=topic,
            kind=RecordKind.DATA,
            value=value,
        )
        records.append(
            StagedOutputRecord(
                kind=TransportRecordKind.DATA,
                payload=await codec.encode(wire.SerializeToString()),
                publish_index=index,
            )
        )
    manifest = OutputStageManifest(
        stream_key=key,
        provider_id=backend.provider_id,
        provider_format_version=backend.provider_format_version,
        stage_token=uuid.uuid4().hex,
        run_id=run_id,
        history_floor_event_id=3,
        sub_batch_id=0,
        fingerprint_version=1,
        fingerprint=b"\x01" * 32,
        record_count=len(records),
        logical_byte_count=sum(len(r.payload) for r in records),
    )
    await backend.stage_output(manifest, records)
    return backend, manifest


async def test_a_pending_stage_is_settled_by_the_reader_rather_than_wedging_it(
    live_client: Client, provider: RedisStreams
):
    # The commit protocol's own failure modes, which nothing else here reaches: a
    # stage whose task never landed in History is a barrier until the reader
    # reconciles it, the reconcile aborts it, and the records it held are never
    # handed over while everything behind it flows.
    workflow_id = f"streams-redis-stage-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[StreamHost],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            StreamHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1})

        backend, manifest = await _stage_one(
            provider,
            live_client,
            workflow_id,
            handle.first_execution_run_id or "",
            INPUTS.name,
            [{"n": 10}, {"n": 11}],
        )
        assert (
            await backend.output_stage(manifest)
        ).status is OutputStageStatus.PENDING

        # Behind the stage so the read has to get past it to reach this.
        await producer.append({"n": 2})

        assert [r.value for r in await take(stream.read(topic=INPUTS), 2, 30)] == [
            {"n": 1},
            {"n": 2},
        ]
        # The task never reached History, so the reader settled the stage as
        # aborted and its records are gone rather than pending forever.
        assert (
            await backend.output_stage(manifest)
        ).status is OutputStageStatus.ABORTED

        await handle.signal(StreamHost.release)
        await handle.result()


async def test_an_aborted_stage_yields_nothing_and_stops_being_a_barrier(
    live_client: Client, provider: RedisStreams
):
    workflow_id = f"streams-redis-abort-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[StreamHost],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            StreamHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1})
        backend, manifest = await _stage_one(
            provider,
            live_client,
            workflow_id,
            handle.first_execution_run_id or "",
            INPUTS.name,
            [{"n": 10}],
        )
        await backend.abort_output(manifest)
        await producer.append({"n": 2})

        await handle.signal(StreamHost.release)
        await handle.result()
        assert [r.value async for r in stream.read(topic=INPUTS)] == [
            {"n": 1},
            {"n": 2},
        ]


async def test_a_committed_stage_is_released_in_the_order_it_was_staged(
    live_client: Client, provider: RedisStreams
):
    workflow_id = f"streams-redis-commit-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[StreamHost],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            StreamHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        backend, manifest = await _stage_one(
            provider,
            live_client,
            workflow_id,
            handle.first_execution_run_id or "",
            INPUTS.name,
            [{"n": 10}, {"n": 11}],
        )
        await backend.commit_output(manifest)

        assert await stream.latest(topic=INPUTS) != BEGINNING
        await handle.signal(StreamHost.release)
        await handle.result()
        assert [r.value async for r in stream.read(topic=INPUTS)] == [
            {"n": 10},
            {"n": 11},
        ]


async def test_a_batch_the_window_cannot_hold_is_refused_at_the_stage(
    live_client: Client,
):
    # max_len at or below a task's batch trims the stage before its commit, and
    # the commit then fails on a missing record for as long as the task retries.
    streams = RedisStreams(
        url=redis_url(), key_prefix=f"streams-redis-{uuid.uuid4().hex}", max_len=2
    )
    workflow_id = f"streams-redis-window-{uuid.uuid4().hex}"
    try:
        async with Worker(
            live_client,
            task_queue=f"tq-{workflow_id}",
            workflows=[StreamHost],
            plugins=[streams],
        ) as worker:
            handle = await live_client.start_workflow(
                StreamHost.run, id=workflow_id, task_queue=worker.task_queue
            )
            run_id = handle.first_execution_run_id or ""
            with pytest.raises(ValueError, match="has to exceed the largest batch"):
                await _stage_one(
                    streams,
                    live_client,
                    workflow_id,
                    run_id,
                    INPUTS.name,
                    [{"n": 1}, {"n": 2}],
                )
            # One below the window still stages.
            await _stage_one(
                streams, live_client, workflow_id, run_id, INPUTS.name, [{"n": 1}]
            )
            await handle.signal(StreamHost.release)
            await handle.result()
    finally:
        await streams.close()


async def test_a_producer_record_lands_once_however_often_it_is_sent(
    live_client: Client, provider: RedisStreams
):
    # One log per topic: an outside record is written once, where the
    # workflow's subscription and outside readers both find it.
    workflow_id = f"streams-redis-once-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[StreamHost],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            StreamHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        producer = provider.get_stream_handle(live_client, workflow_id).producer(
            topic=INPUTS, producer_id="model", attempt=1
        )
        await producer.append({"n": 1}, {"n": 2}, {"n": 3})
        assert await _log_length(provider, live_client, workflow_id, INPUTS.name) == 3

        # A producer that comes back with a fresh sequence re-appends the same
        # identities, which the script reuses rather than doubling.
        again = provider.get_stream_handle(live_client, workflow_id).producer(
            topic=INPUTS, producer_id="model", attempt=1
        )
        await again.append({"n": 1}, {"n": 2}, {"n": 3})
        assert await _log_length(provider, live_client, workflow_id, INPUTS.name) == 3

        await handle.signal(StreamHost.release)
        await handle.result()


async def test_a_repeat_under_one_identity_with_other_bytes_writes_nothing(
    live_client: Client, provider: RedisStreams
):
    workflow_id = f"streams-redis-conflict-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[StreamHost],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            StreamHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        await stream.producer(topic=INPUTS, producer_id="model", attempt=1).append(
            {"n": 1}
        )
        other = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        with pytest.raises(StreamProducerError, match="sequence 1"):
            await other.append({"n": 99})
        # The refusal wrote nothing.
        assert await _log_length(provider, live_client, workflow_id, INPUTS.name) == 1
        await handle.signal(StreamHost.release)
        await handle.result()


async def test_an_identity_claimed_with_other_bytes_refuses_the_first_append(
    live_client: Client, provider: RedisStreams
):
    # The idempotency hash is read before the log is touched, so a claim that
    # disagrees with the record refuses it without writing.
    workflow_id = f"streams-redis-claimed-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[StreamHost],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            StreamHost.run, id=workflow_id, task_queue=worker.task_queue
        )
        backend = provider._require_backend()
        chain = await _chain(live_client, workflow_id)
        key = chain.stream_key(INPUTS.name, direction=StreamDirection.OUTPUT)
        await backend._client.hset(
            backend._idempotency_key(key), "model#1/1", "1-0|" + "0" * 64
        )

        producer = provider.get_stream_handle(live_client, workflow_id).producer(
            topic=INPUTS, producer_id="model", attempt=1
        )
        with pytest.raises(StreamProducerError, match="sequence 1"):
            await producer.append({"n": 1})

        assert await _log_length(provider, live_client, workflow_id, INPUTS.name) == 0
        await handle.signal(StreamHost.release)
        await handle.result()


@workflow.defn
class NudgedLoop:
    """The contract loop with a signal that does nothing but complete a task."""

    def __init__(self) -> None:
        self._nudges = 0

    @workflow.signal
    def nudge(self) -> None:
        self._nudges += 1

    @workflow.run
    async def run(self) -> list[int]:
        decisions = workflow.stream_writer(DECISIONS)
        seen: list[int] = []
        async for record in workflow.stream_reader(INPUTS):
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            assert record.value is not None
            decisions.publish({"decided": record.value["n"]})
            seen.append(record.value["n"])
        decisions.finish()
        return seen


async def _completed_tasks(client: Client, workflow_id: str, run_id: str) -> list[int]:
    return [
        event.event_id
        async for event in client.get_workflow_handle(
            workflow_id, run_id=run_id
        ).fetch_history_events()
        if event.HasField("workflow_task_completed_event_attributes")
    ]


_MARKER_DETAILS_KEY = "external_stream"


async def _consuming_tasks(client: Client, workflow_id: str, run_id: str) -> list[int]:
    """The completed Workflow Tasks whose marker delivered a record, in order.

    Read from the markers rather than counted by position: which task opens
    the reader, and whether that task can stay open for the first record,
    depends on the server, so a task index names a different task from one
    server to the next while the marker says what each task consumed.
    """
    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData
    from temporalio.contrib.external_workflow_streams._annotation import (
        decode_annotation,
    )

    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    completed: int | None = None
    consuming: list[int] = []
    async for event in handle.fetch_history_events():
        if event.HasField("workflow_task_completed_event_attributes"):
            completed = event.event_id
            continue
        if not event.HasField("marker_recorded_event_attributes"):
            continue
        details = event.marker_recorded_event_attributes.details
        if _MARKER_DETAILS_KEY not in details or completed is None:
            continue
        data = ExternalStreamMarkerData()
        data.ParseFromString(details[_MARKER_DETAILS_KEY].payloads[0].data)
        annotation = decode_annotation(data.replay_annotation)
        if any(segment.runs for segment in annotation.segments):
            consuming.append(completed)
    return consuming


async def _reset_at(
    client: Client, workflow_id: str, run_id: str, *, finish_event_id: int
) -> str:
    """Reset ``run_id`` at the Workflow Task completed by ``finish_event_id``.

    The server keeps History up to that task's completion and runs the task
    again, so the tasks before it are inherited and the task itself is not.
    Returns the new run id.
    """
    from temporalio.api.common.v1 import WorkflowExecution
    from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest

    response = await client.workflow_service.reset_workflow_execution(
        ResetWorkflowExecutionRequest(
            namespace=client.namespace,
            workflow_execution=WorkflowExecution(
                workflow_id=workflow_id, run_id=run_id
            ),
            reason="streams: reset mid-stream",
            workflow_task_finish_event_id=finish_event_id,
            request_id=uuid.uuid4().hex,
        )
    )
    return response.run_id


async def _consume_two_then_nudge(
    live_client: Client, provider: RedisStreams, workflow_id: str, task_queue: str
) -> tuple[str, Any, Any]:
    """Two records, each consumed by a task of its own, then a nudged task; the base run."""
    handle = await live_client.start_workflow(
        NudgedLoop.run, id=workflow_id, task_queue=task_queue
    )
    base_run = handle.result_run_id
    assert base_run is not None
    stream = provider.get_stream_handle(live_client, workflow_id)
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    await producer.append({"n": 1})
    await take(stream.read(topic=DECISIONS), 1, 60)
    await producer.append({"n": 2})
    await take(stream.read(topic=DECISIONS), 2, 60)
    before = len(await _completed_tasks(live_client, workflow_id, base_run))
    await handle.signal(NudgedLoop.nudge)
    for _ in range(300):
        if len(await _completed_tasks(live_client, workflow_id, base_run)) > before:
            break
        await asyncio.sleep(0.1)
    return base_run, stream, producer


async def test_a_reset_run_replays_the_inherited_ranges_and_continues(
    live_client: Client, provider: RedisStreams
):
    # Reset at the completion of the task the nudge woke, which consumed and
    # published nothing: both consuming tasks are inherited. Their ranges are
    # re-read from the log and replayed against the inherited markers, so
    # their decisions are not published again, and reading continues from the
    # last inherited boundary.
    workflow_id = f"streams-redis-reset-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[NudgedLoop],
        plugins=[provider],
        max_cached_workflows=100,
    ):
        base_run, stream, producer = await _consume_two_then_nudge(
            live_client, provider, workflow_id, f"tq-{workflow_id}"
        )
        last_task = (await _completed_tasks(live_client, workflow_id, base_run))[-1]
        reset_run = await _reset_at(
            live_client, workflow_id, base_run, finish_event_id=last_task
        )
        assert reset_run != base_run

        await producer.append({"n": 3})
        await producer.finish()
        continued = live_client.get_workflow_handle(workflow_id, run_id=reset_run)
        assert await asyncio.wait_for(continued.result(), 90) == [1, 2, 3]
        decisions = [r async for r in stream.read(topic=DECISIONS)]
        assert [(r.kind, r.value) for r in decisions] == [
            (RecordKind.DATA, {"decided": 1}),
            (RecordKind.DATA, {"decided": 2}),
            (RecordKind.DATA, {"decided": 3}),
            (RecordKind.FINISH, None),
        ]

    # Offline, the reset run's History reads the inherited ranges from the log.
    await Replayer(workflows=[NudgedLoop], plugins=[provider]).replay_workflow(
        await continued.fetch_history()
    )


async def test_a_reset_point_task_is_run_again_from_the_log(
    live_client: Client, provider: RedisStreams
):
    # Reset at the completion of the task that consumed the second record: the
    # tasks before it are inherited, the one that consumed the first record
    # among them, and this one is run again. The record it consumed is still
    # in the log, so the reset run reads it again and publishes again, and an
    # outside reader sees that decision from both runs.
    workflow_id = f"streams-redis-reset-rerun-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[NudgedLoop],
        plugins=[provider],
        max_cached_workflows=100,
    ):
        base_run, stream, producer = await _consume_two_then_nudge(
            live_client, provider, workflow_id, f"tq-{workflow_id}"
        )
        second = (await _consuming_tasks(live_client, workflow_id, base_run))[1]
        reset_run = await _reset_at(
            live_client, workflow_id, base_run, finish_event_id=second
        )

        await producer.append({"n": 3})
        await producer.finish()
        continued = live_client.get_workflow_handle(workflow_id, run_id=reset_run)
        assert await asyncio.wait_for(continued.result(), 90) == [1, 2, 3]
        decisions = [r.value async for r in stream.read(topic=DECISIONS)]
        assert decisions == [
            {"decided": 1},
            {"decided": 2},
            {"decided": 2},
            {"decided": 3},
            None,
        ]

    await Replayer(workflows=[NudgedLoop], plugins=[provider]).replay_workflow(
        await continued.fetch_history()
    )


@workflow.defn
class EchoOnOneTopic:
    """Reads ``inputs`` and answers each value on the same topic.

    Its own records land in the log it reads, so what it returns says whether
    it read them back.
    """

    @workflow.run
    async def run(self) -> list[Any]:
        seen: list[Any] = []
        out = workflow.stream_writer(INPUTS)
        async for record in workflow.stream_reader(INPUTS):
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            seen.append(record.value)
            out.publish({"echo": record.value})
        out.finish()
        return seen

    @workflow.query
    def probe(self) -> int:
        return 1


async def test_a_workflow_does_not_read_its_own_records_from_the_shared_log(
    live_client: Client, provider: RedisStreams
):
    workflow_id = f"streams-redis-echo-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[EchoOnOneTopic],
        plugins=[provider],
    ) as worker:
        handle = await live_client.start_workflow(
            EchoOnOneTopic.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1}, {"n": 2})
        # The echoes are promoted into the same log the producer wrote to.
        first_four = await take(stream.read(topic=INPUTS), 4, 60)
        assert sorted(
            ((r.producer_id, r.value) for r in first_four), key=str
        ) == sorted(
            [
                ("model", {"n": 1}),
                ("model", {"n": 2}),
                ("", {"echo": {"n": 1}}),
                ("", {"echo": {"n": 2}}),
            ],
            key=str,
        )
        await producer.finish()
        # The workflow saw the producer's records and none of its echoes.
        assert await asyncio.wait_for(handle.result(), 60) == [{"n": 1}, {"n": 2}]
        assert await _log_length(provider, live_client, workflow_id, INPUTS.name) == 6

        # A cold query replays the run against the log with its own entries
        # in every recorded range; the read filters them the way the live one did.
        assert await handle.query(EchoOnOneTopic.probe) == 1
        everything = [r async for r in stream.read(topic=INPUTS)]
        assert [(r.producer_id, r.kind) for r in everything if r.producer_id == ""] == [
            ("", RecordKind.DATA),
            ("", RecordKind.DATA),
            ("", RecordKind.FINISH),
        ]

    await Replayer(workflows=[EchoOnOneTopic], plugins=[provider]).replay_workflow(
        await handle.fetch_history()
    )


@workflow.defn
class NewestTwoOnGo:
    """Opens ``inputs`` at its newest two records once told to, and returns them."""

    def __init__(self) -> None:
        self._go = False

    @workflow.signal
    def go(self) -> None:
        self._go = True

    @workflow.run
    async def run(self) -> list[Any]:
        await workflow.wait_condition(lambda: self._go)
        reader = workflow.stream_reader(INPUTS, last=2)
        values: list[Any] = []
        async for value in reader.values():
            values.append(value["n"])
            if len(values) == 2:
                reader.close()
        return values


@workflow.defn
class FromNowOnGo:
    """Opens ``inputs`` at its end once told to, and returns the first value."""

    def __init__(self) -> None:
        self._go = False

    @workflow.signal
    def go(self) -> None:
        self._go = True

    @workflow.run
    async def run(self) -> Any:
        await workflow.wait_condition(lambda: self._go)
        reader = workflow.stream_reader(INPUTS, after=END)
        async for value in reader.values():
            reader.close()
            return value["n"]
        return None


async def test_a_workflow_reader_starts_at_the_last_n_records_and_replays_there(
    live_client: Client, provider: RedisStreams
):
    # The worker positions the subscription against the log after the task that
    # opened it and records the entry with it; replay takes the entry from
    # History, so what lands in the log later does not move the start. The
    # producer needs the run to exist, so the reader opens on a signal.
    workflow_id = f"streams-redis-last-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[NewestTwoOnGo],
        plugins=[provider],
    ):
        handle = await live_client.start_workflow(
            NewestTwoOnGo.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
        await producer.append({"n": 1}, {"n": 2}, {"n": 3}, {"n": 4})
        await handle.signal(NewestTwoOnGo.go)
        assert await asyncio.wait_for(handle.result(), 60) == [3, 4]
        history = await handle.fetch_history()
        await producer.append({"n": 5}, {"n": 6})
    # Offline, with two more records in the log than the run ever saw.
    await Replayer(workflows=[NewestTwoOnGo], plugins=[provider]).replay_workflow(
        history
    )


async def test_a_workflow_reader_at_end_skips_what_was_there_and_replays(
    live_client: Client, provider: RedisStreams
):
    workflow_id = f"streams-redis-end-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[FromNowOnGo],
        plugins=[provider],
    ):
        handle = await live_client.start_workflow(
            FromNowOnGo.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        stream = provider.get_stream_handle(live_client, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
        await producer.append({"n": "old"})
        await handle.signal(FromNowOnGo.go)
        result = asyncio.ensure_future(handle.result())
        # The subscription is positioned when the worker gets to it, which the
        # test does not observe, so appends keep coming until the run takes one.
        for _ in range(150):
            await producer.append({"n": "new"})
            done, _ = await asyncio.wait({result}, timeout=0.2)
            if done:
                break
        assert await asyncio.wait_for(result, 60) == "new"
        history = await handle.fetch_history()
    await Replayer(workflows=[FromNowOnGo], plugins=[provider]).replay_workflow(history)


async def test_a_batch_staged_for_a_rejected_task_does_not_stay_in_the_log(
    live_client: Client, monkeypatch: pytest.MonkeyPatch
):
    # The first stage takes longer than the Workflow Task timeout, so the server
    # times the task out and the worker's completion is rejected; the run is
    # evicted and run again, and the second attempt stages the same batch under
    # a new token. The first stage can never be named by a marker. It is
    # settled against History when the run is evicted and its entries leave the
    # log, so the log holds the batch once and nothing counts the dead one.
    class SlowFirstStage(_TopicLogBackend):
        delayed = False

        async def stage_output(self, manifest: Any, records: Any) -> Any:
            if not SlowFirstStage.delayed:
                SlowFirstStage.delayed = True
                await asyncio.sleep(3)
            return await super().stage_output(manifest, records)

    monkeypatch.setattr(redis_provider, "_TopicLogBackend", SlowFirstStage)
    provider = RedisStreams(
        url=redis_url(), key_prefix=f"streams-redis-{uuid.uuid4().hex}"
    )
    workflow_id = f"streams-redis-rejected-{uuid.uuid4().hex}"
    try:
        async with Worker(
            live_client,
            task_queue=f"tq-{workflow_id}",
            workflows=[OneLine],
            plugins=[provider],
        ):
            handle = await live_client.start_workflow(
                OneLine.run,
                id=workflow_id,
                task_queue=f"tq-{workflow_id}",
                task_timeout=timedelta(seconds=1),
            )
            await asyncio.wait_for(handle.result(), 60)
            assert SlowFirstStage.delayed
            # Counted before any reader could settle a barrier: the worker did.
            assert (
                await _log_length(provider, live_client, workflow_id, DECISIONS.name)
                == 2
            )
            stream = provider.get_stream_handle(live_client, workflow_id)
            records = [r async for r in stream.read(topic=DECISIONS)]
            assert [(r.kind, r.value) for r in records] == [
                (RecordKind.DATA, {"from": "workflow"}),
                (RecordKind.FINISH, None),
            ]
    finally:
        await provider.close()


async def _serves_channels(client: Client) -> bool:
    """Whether the server implements notification channels, asked of a probe channel."""
    from temporalio.contrib.external_workflow_streams._wake import server_has_channels

    return bool(await server_has_channels(client))


@pytest.mark.needs_channel_server
async def test_an_outside_producer_wakes_the_reader_through_the_channel(
    live_client: Client,
):
    if not await _serves_channels(live_client):
        pytest.skip("the server does not implement notification channels")
    from temporalio.contrib.external_workflow_streams._record import Offset

    # The channel outright rather than "auto", so a step down to the wake call
    # or the Signal fails the History checks below instead of passing quietly.
    provider = RedisStreams(
        url=redis_url(),
        key_prefix=f"streams-redis-{uuid.uuid4().hex}",
        wake_transport="channel",
    )
    workflow_id = f"streams-redis-channel-{uuid.uuid4().hex}"
    try:
        async with Worker(
            live_client,
            task_queue=f"tq-{workflow_id}",
            workflows=[ContractLoop],
            plugins=[provider],
            max_cached_workflows=100,
        ):
            handle = await live_client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
            )
            stream = provider.get_stream_handle(live_client, workflow_id)
            producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            # Spaced past the reader's idle timeout, so the reader parks between
            # appends and only a notification from outside can move it.
            for n in (1, 2, 3):
                await producer.append({"n": n})
                await asyncio.sleep(2)
            await producer.finish()

            result = await asyncio.wait_for(handle.result(), 60)
            assert [entry.get("n") for entry in result[:3]] == [1, 2, 3]
            assert result[3] == {"kind": "finish", "producer": "model"}

            events = [e async for e in handle.fetch_history_events()]
            signalled = [
                e
                for e in events
                if e.HasField("workflow_execution_signaled_event_attributes")
            ]
            assert signalled == [], "a Signal woke the reader"
            subscribed = [
                e.workflow_notification_channel_subscribed_event_attributes.channel
                for e in events
                if e.HasField(
                    "workflow_notification_channel_subscribed_event_attributes"
                )
            ]
            assert len(subscribed) == 1, "the run subscribes once per channel"
            notified = [
                n
                for e in events
                if e.HasField("workflow_task_scheduled_event_attributes")
                for n in e.workflow_task_scheduled_event_attributes.notifications
            ]
            assert notified, "no Workflow Task was scheduled with a notification"
            assert {n.channel for n in notified} == set(subscribed)
            # The position is the appended entry id and the counter derives
            # from it, so producers and workers order the same wakes alike.
            positioned = [note for note in notified if note.position]
            assert positioned, "no notification carried the store's position"
            for note in positioned:
                offset = Offset(note.position.decode())
                assert note.counter == redis_provider._wake_counter(offset)
    finally:
        await provider.close()
