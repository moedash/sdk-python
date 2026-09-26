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
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamProducerError,
)
from temporalio.streams._wire import to_wire
from temporalio.streams.providers.redis import RedisStreams, _chain
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


async def _key_lengths(
    streams: RedisStreams, client: Client, workflow_id: str, topic: str
) -> tuple[int, int]:
    """How many entries the topic's input and output keys hold."""
    import redis.asyncio

    backend = streams._require_backend()
    chain = await _chain(client, workflow_id)
    store = redis.asyncio.from_url(redis_url())
    try:
        return (
            await store.xlen(
                backend.stream_key(
                    chain.stream_key(topic, direction=StreamDirection.INPUT)
                )
            ),
            await store.xlen(
                backend.stream_key(
                    chain.stream_key(topic, direction=StreamDirection.OUTPUT)
                )
            ),
        )
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
        assert await _key_lengths(streams, live_client, workflow_id, INPUTS.name) == (
            4,
            4,
        )

        # Inside the window the recorded ranges read back and the replay passes.
        await Replayer(workflows=[ContractLoop], plugins=[streams]).replay_workflow(
            history
        )

        late = stream.producer(topic=INPUTS, producer_id="late", attempt=1)
        cursors = [await late.append({"n": n}) for n in range(10, 16)]
        assert await _key_lengths(streams, live_client, workflow_id, INPUTS.name) == (
            6,
            6,
        )

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
        assert "past the redis provider's retention (max_len=6)" in message

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
            assert await _key_lengths(
                streams, live_client, workflow_id, INPUTS.name
            ) == (
                1,
                1,
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


async def test_a_producer_record_lands_on_both_keys_or_on_neither(
    live_client: Client, provider: RedisStreams
):
    # The workflow reads the input key and outside readers read the output key.
    # Written one at a time, a crash between them leaves a record the workflow
    # acts on that no outside reader can ever see.
    workflow_id = f"streams-redis-pair-{uuid.uuid4().hex}"
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
        assert await _key_lengths(provider, live_client, workflow_id, INPUTS.name) == (
            3,
            3,
        )

        # A producer that comes back with a fresh sequence re-appends the same
        # identities, which the script reuses rather than doubling.
        again = provider.get_stream_handle(live_client, workflow_id).producer(
            topic=INPUTS, producer_id="model", attempt=1
        )
        await again.append({"n": 1}, {"n": 2}, {"n": 3})
        assert await _key_lengths(provider, live_client, workflow_id, INPUTS.name) == (
            3,
            3,
        )

        await handle.signal(StreamHost.release)
        await handle.result()


async def test_a_repeat_under_one_identity_with_other_bytes_writes_neither_key(
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
        # The refusal wrote nothing on either key.
        assert await _key_lengths(provider, live_client, workflow_id, INPUTS.name) == (
            1,
            1,
        )
        await handle.signal(StreamHost.release)
        await handle.result()


async def test_a_refused_output_half_leaves_the_input_key_untouched(
    live_client: Client, provider: RedisStreams
):
    # The two keys are one write. Written one at a time, the input half lands
    # before the output half is refused, and the workflow then consumes a record
    # no outside reader can ever see.
    workflow_id = f"streams-redis-atomic-{uuid.uuid4().hex}"
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
        output_key = chain.stream_key(INPUTS.name, direction=StreamDirection.OUTPUT)
        # Claim the output half's first identity with other bytes, which is what a
        # refusal of that half looks like from the producer's side.
        await backend._client.hset(
            backend._idempotency_key(output_key), "model#1/1", "1-0|" + "0" * 64
        )

        producer = provider.get_stream_handle(live_client, workflow_id).producer(
            topic=INPUTS, producer_id="model", attempt=1
        )
        with pytest.raises(StreamProducerError, match="sequence 1"):
            await producer.append({"n": 1})

        assert await _key_lengths(provider, live_client, workflow_id, INPUTS.name) == (
            0,
            0,
        )
        await handle.signal(StreamHost.release)
        await handle.result()
