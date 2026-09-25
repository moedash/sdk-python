"""The native provider inside a real workflow, against a server that has streams.

The two rules the memory provider cannot keep are the measurement here: a
publish commits with its Workflow Task and never lands if the task fails, and
a read is a recorded observation the server re-supplies on replay. Needs a
Temporal server built from the AI-198 branch, because neither the stream
service nor the commands exist on a released one:

    TEMPORAL_STREAM_TARGET=127.0.0.1:7333 uv run pytest tests/worker/test_workflow_stream_e2e.py

Skipped otherwise, rather than passing against a server that has no idea what a
stream is.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload, WorkflowExecution
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.stream.v1 import StreamRange, StreamRecord
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHistory
from temporalio.client_stream import StreamClient
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.streams import RecordKind, StreamNotFoundError
from temporalio.streams.providers.native import NativeStreams
from temporalio.worker import Replayer, Worker
from temporalio.workflow import NondeterminismError
from tests.streams.test_streams_conformance import take

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream commands",
)

INPUTS = "inputs"
DECISIONS = "decisions"

EVENT_STREAM_SUBSCRIBED = EventType.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED
EVENT_STREAM_RECORDS_APPENDED = EventType.EVENT_TYPE_WORKFLOW_STREAM_RECORDS_APPENDED


async def _connect(provider: NativeStreams | None = None) -> Client:
    # Registered once, on the client: the worker inherits it and the tests
    # open handles through client.get_stream_handle. The contrib tests below
    # need no provider.
    return await Client.connect(TARGET or "", plugins=[provider] if provider else [])


async def _event_counts(client: Client, workflow_id: str) -> dict[Any, int]:
    counts = {
        EVENT_STREAM_RECORDS_APPENDED: 0,
        EVENT_STREAM_SUBSCRIBED: 0,
        EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED: 0,
    }
    async for event in client.get_workflow_handle(workflow_id).fetch_history_events():
        if event.event_type in counts:
            counts[event.event_type] += 1
    return counts


@workflow.defn
class ContractLoop:
    """Reads ``inputs``, publishes a decision per value, reports control records."""

    def __init__(self) -> None:
        self._trace: list[dict[str, Any]] = []

    @workflow.query
    def trace(self) -> list[dict[str, Any]]:
        """What the loop has decided so far, for a query against replayed state."""
        return self._trace

    @workflow.run
    async def run(self) -> list[dict[str, Any]]:
        inputs = workflow.stream_reader(INPUTS, result_type=dict)
        decisions = workflow.stream_writer(DECISIONS)
        trace = self._trace
        async for record in inputs:
            if record.kind is RecordKind.SUPERSEDED:
                assert record.supersession is not None
                trace.append(
                    {
                        "kind": "superseded",
                        "replaced": record.supersession.previous_attempt,
                    }
                )
                decisions.publish(
                    {"retracting_attempt": record.supersession.previous_attempt}
                )
                continue
            if record.kind is RecordKind.FINISH:
                trace.append({"kind": "finish", "producer": record.producer_id})
                break
            assert record.value is not None
            decisions.publish({"decided": record.value["n"]})
            trace.append({"kind": "decision", "n": record.value["n"]})
        decisions.finish()
        return trace


async def test_the_interface_loop_runs_on_the_server_with_a_cold_cache() -> None:
    """Rule 2 on the native provider: every task replays from the server.

    With the cache off, each Workflow Task rebuilds the workflow from History
    and the server re-supplies the ranges earlier tasks consumed, so the loop
    completing at all means the same records came back in the same order.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "loop-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "loop-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ContractLoop],
            max_cached_workflows=0,
        ):
            handle = await client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            first = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            await first.append({"n": 1}, {"n": 2})
            second = stream.producer(topic=INPUTS, producer_id="model", attempt=2)
            await second.append({"n": 3})
            await second.finish()
            trace = await asyncio.wait_for(handle.result(), 60)

            assert trace == [
                {"kind": "decision", "n": 1},
                {"kind": "decision", "n": 2},
                {"kind": "superseded", "replaced": 1},
                {"kind": "decision", "n": 3},
                {"kind": "finish", "producer": "model"},
            ]

            # The read ends by itself: the workflow is closed and the tail
            # delivered, with the workflow's own records carrying no producer.
            async def read_everything() -> list[Any]:
                return [
                    (r.producer_id, r.kind, r.value)
                    async for r in stream.read(topic=DECISIONS, result_type=dict)
                ]

            assert await asyncio.wait_for(read_everything(), 60) == [
                ("", RecordKind.DATA, {"decided": 1}),
                ("", RecordKind.DATA, {"decided": 2}),
                ("", RecordKind.DATA, {"retracting_attempt": 1}),
                ("", RecordKind.DATA, {"decided": 3}),
                ("", RecordKind.FINISH, None),
            ]
        counts = await _event_counts(client, workflow_id)
        assert counts[EVENT_STREAM_SUBSCRIBED] == 1
        # Several tasks published, one event each; the loop spanned more than
        # one task or the cold cache proved nothing.
        assert counts[EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED] >= 2
        assert (
            1
            <= counts[EVENT_STREAM_RECORDS_APPENDED]
            <= counts[EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED]
        )
    finally:
        await provider.close()


# Run ids whose first workflow task already failed, shared with the workflow
# thread so the retry can tell it is the retry. Outside the sandbox on
# purpose: the sandbox re-imports this module per run and would hide the set.
_failed_once: set[str] = set()


@workflow.defn(sandboxed=False)
class PublishThenFail:
    """Publishes, then fails its first workflow task; the retry publishes again."""

    @workflow.run
    async def run(self) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        run_id = workflow.info().run_id
        committed = run_id in _failed_once
        decisions.publish({"committed": committed})
        if not committed:
            _failed_once.add(run_id)
            raise RuntimeError("the first task fails after publishing")
        decisions.finish()


async def test_a_failed_task_publishes_nothing() -> None:
    """Rule 1 on the native provider: the server applies the command with the task."""
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "fail-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "fail-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[PublishThenFail],
        ):
            handle = await client.start_workflow(
                PublishThenFail.run, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            records = await take(
                stream.read(topic=DECISIONS, result_type=dict), 2, timeout=60
            )
            await handle.result()
        assert [(r.kind, r.value) for r in records] == [
            (RecordKind.DATA, {"committed": True}),
            (RecordKind.FINISH, None),
        ]
    finally:
        await provider.close()


@workflow.defn
class Relay:
    """Publishes one record per run and continues as new once."""

    @workflow.run
    async def run(self, run: int) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        decisions.publish({"run": run})
        if run == 0:
            workflow.continue_as_new(run + 1)
        decisions.finish()


async def test_a_handle_without_a_run_id_reads_across_continue_as_new() -> None:
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "relay-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "relay-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Relay]):
            handle = await client.start_workflow(
                Relay.run, 0, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)

            async def read_everything() -> list[Any]:
                return [
                    (r.kind, r.value)
                    async for r in stream.read(topic=DECISIONS, result_type=dict)
                ]

            records = await asyncio.wait_for(read_everything(), 60)
            await handle.result()
        # The chain is followed: the successor's record arrives on the same
        # read, each cursor names its run, and the read ends with the chain.
        assert records == [
            (RecordKind.DATA, {"run": 0}),
            (RecordKind.DATA, {"run": 1}),
            (RecordKind.FINISH, None),
        ]
        # Pinned to the last run, a handle sees that run's stream alone.
        last_run = (await client.get_workflow_handle(workflow_id).describe()).run_id
        pinned = client.get_stream_handle(workflow_id, run_id=last_run)
        only_last = [
            r.value async for r in pinned.read(topic=DECISIONS, result_type=dict)
        ]
        assert only_last == [{"run": 1}, None]
    finally:
        await provider.close()


@workflow.defn
class PublishAndRead:
    """Publishes through the low-level surface and reads its own records back."""

    @workflow.run
    async def run(self) -> list[str]:
        workflow.append_stream_records(
            [_record(b"alpha", "progress"), _record(b"beta", "progress")],
            stream_id="output",
        )
        workflow.append_stream_records([_record(b"gamma")], stream_id="output")
        # A name this workflow has not written yet still names a stream it
        # owns, so subscribing creates the one the later publish lands in.
        workflow.subscribe_stream("output", start_offset=0)

        received: list[str] = []
        while len(received) < 3:
            for item in await workflow.read_stream_records("output"):
                received.append(item.record.body.data.decode())
        return received


def _record(body: bytes, topic: str = "") -> StreamRecord:
    return StreamRecord(
        body=Payload(data=body, metadata={"encoding": b"binary/plain"}), topic=topic
    )


async def test_a_tasks_publishes_become_one_event() -> None:
    client = await _connect()
    task_queue = "publish-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "publish-wf-" + uuid.uuid4().hex[:8]

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishAndRead],
        # Every task after the first is a replay, so completing at all means the
        # reissued publish matched the event the first run wrote.
        max_cached_workflows=0,
    ):
        result = await asyncio.wait_for(
            client.execute_workflow(
                PublishAndRead.run, id=workflow_id, task_queue=task_queue
            ),
            timeout=60,
        )
    assert result == ["alpha", "beta", "gamma"]

    counts = await _event_counts(client, workflow_id)
    # Two calls in one task carrying three records, so one event: the event is
    # per task and stream, which is what makes publishing often free.
    assert counts[EVENT_STREAM_RECORDS_APPENDED] == 1
    assert counts[EVENT_STREAM_SUBSCRIBED] == 1


@workflow.defn
class ConsumeAcrossTasks:
    """Reads a standalone stream over several Workflow Tasks, then reports what it saw.

    The turn structure is the point: each read that finds nothing blocks,
    which ends a Workflow Task, so the run spans several. With the cache on,
    every task after the first is sticky.
    """

    @workflow.run
    async def run(self, stream_id: str, expected: int) -> list[str]:
        workflow.subscribe_stream(stream_id, start_offset=0)
        seen: list[str] = []
        while len(seen) < expected:
            for item in await workflow.read_stream_records(stream_id):
                seen.append(item.record.body.data.decode())
        return seen


async def test_a_cached_workflow_consumes_across_sticky_tasks() -> None:
    """The workflow cache stays on, which is what a real worker does.

    The server sends no replay slice for a sticky task, while the sticky
    history still carries the previous task's consumed range, and the two
    together have to agree on every task after the first consumed range.
    """
    client = await _connect()
    streams = StreamClient.connect(TARGET or "")
    task_queue = "sticky-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "sticky-wf-" + uuid.uuid4().hex[:8]
    stream_id = "sticky-src-" + uuid.uuid4().hex[:8]

    batches = [["a1", "a2"], ["b1"], ["c1", "c2", "c3"]]
    expected = [tok for batch in batches for tok in batch]

    try:
        await streams.create(stream_id)
        async with Worker(
            client, task_queue=task_queue, workflows=[ConsumeAcrossTasks]
        ):
            handle = await client.start_workflow(
                ConsumeAcrossTasks.run,
                args=[stream_id, len(expected)],
                id=workflow_id,
                task_queue=task_queue,
            )

            # Spaced out so the workflow drains, blocks and ends a task between
            # them. Without the gap the appends coalesce into one task and the
            # sticky path is never taken.
            for batch in batches:
                await streams.get(stream_id).append(
                    *[_record(t.encode()) for t in batch]
                )
                await asyncio.sleep(0.4)

            assert await asyncio.wait_for(handle.result(), timeout=60) == expected

        counts = await _event_counts(client, workflow_id)
        assert counts[EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED] >= 3, (
            "sticky path not exercised"
        )
    finally:
        await streams.close()


class _EncryptingCodec(PayloadCodec):
    """A codec whose output never contains its input.

    The SDK's own test codec marks payloads as encrypted but leaves the bytes
    as they are, so it cannot show that a stored body is not plaintext. This
    one keeps its metadata convention and runs a repeating-key XOR over the
    serialized payload, which is enough for the plaintext markers the tests
    look for to be absent from what the server stores.
    """

    _KEY = b"ai198"

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/encrypted"},
                data=self._xor(p.SerializeToString()),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for p in payloads:
            if p.metadata.get("encoding", b"") != b"binary/encrypted":
                out.append(p)
                continue
            out.append(Payload.FromString(self._xor(p.data)))
        return out

    def _xor(self, data: bytes) -> bytes:
        key = self._KEY
        return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


async def _drive_contract_loop(client: Client) -> WorkflowHistory:
    """Run ``ContractLoop`` to completion on ``client`` and return its history."""
    task_queue = "codec-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "codec-wf-" + uuid.uuid4().hex[:8]
    async with Worker(client, task_queue=task_queue, workflows=[ContractLoop]):
        handle = await client.start_workflow(
            ContractLoop.run, id=workflow_id, task_queue=task_queue
        )
        stream = client.get_stream_handle(workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1}, {"n": 2})
        await producer.finish()
        trace = await asyncio.wait_for(handle.result(), 60)
        # The workflow decoded what the outside producer appended.
        assert trace == [
            {"kind": "decision", "n": 1},
            {"kind": "decision", "n": 2},
            {"kind": "finish", "producer": "model"},
        ]
        # The outside read decodes what the workflow published.
        decisions = [
            (r.kind, r.value)
            async for r in stream.read(topic=DECISIONS, result_type=dict)
        ]
        assert decisions == [
            (RecordKind.DATA, {"decided": 1}),
            (RecordKind.DATA, {"decided": 2}),
            (RecordKind.FINISH, None),
        ]
    return await handle.fetch_history()


async def test_a_codec_encodes_records_on_both_halves() -> None:
    """A payload codec on the client covers the workflow's records and an outside producer's.

    The worker's payload visitor runs the codec over the bodies a workflow
    publishes and receives; the outside half applies the client's codec to each
    body it sends and reads. Read raw, without the codec, every stored body is
    ciphertext, and each side still reads the other's records in the clear.
    """
    plain = await _connect()
    config = plain.config()
    config["data_converter"] = DataConverter(payload_codec=_EncryptingCodec())
    provider = NativeStreams()
    config["plugins"] = [provider]
    client = Client(**config)
    raw = StreamClient.connect(TARGET or "")
    try:
        history = await _drive_contract_loop(client)
        run_id = history.run_id

        # What the server holds, read through the stream service with no codec.
        inputs = await raw.workflow_stream(
            history.workflow_id, INPUTS, owner_run_id=run_id
        ).poll(from_offset=0, wait=False)
        decisions = await raw.workflow_stream(
            history.workflow_id, DECISIONS, owner_run_id=run_id
        ).poll(from_offset=0, wait=False)

        # The outside producer's two records, stored encoded.
        stored_inputs = [e.record for e in inputs.entries if e.record.HasField("body")]
        assert len(stored_inputs) == 2
        for record in stored_inputs:
            assert record.body.metadata["encoding"] == b"binary/encrypted"
            assert b'"n"' not in record.body.data
        # The workflow's two decisions, stored encoded by the worker's visitor.
        stored_decisions = [
            e.record for e in decisions.entries if e.record.HasField("body")
        ]
        assert len(stored_decisions) == 2
        for record in stored_decisions:
            assert record.body.metadata["encoding"] == b"binary/encrypted"
            assert b"decided" not in record.body.data
    finally:
        await raw.close()
        await provider.close()


def _consumed_ranges(history: WorkflowHistory) -> list[StreamRange]:
    return [
        consumed
        for event in history.events
        if event.HasField("workflow_task_completed_event_attributes")
        for consumed in event.workflow_task_completed_event_attributes.consumed_stream_ranges
    ]


def _copy_of(
    history: WorkflowHistory, workflow_id: str | None = None
) -> WorkflowHistory:
    events: list[HistoryEvent] = []
    for event in history.events:
        copied = HistoryEvent()
        copied.CopyFrom(event)
        events.append(copied)
    return WorkflowHistory(workflow_id or history.workflow_id, events)


async def test_a_replayer_with_a_client_replays_a_consuming_workflow() -> None:
    """Rule 2 through the ``Replayer``: the records come back from the stream service.

    History holds the offsets each task consumed and never the records, so the
    replayer is given a client to the server that still holds the streams and
    fetches every recorded range before pushing the history. The same delivery
    path as a live cache miss then hands each range to the task that consumed
    it, and the reissued publishes match their events.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    try:
        history = await _drive_contract_loop(client)
        # Something was consumed, or the replay would prove nothing.
        assert any(r.to_offset > r.from_offset for r in _consumed_ranges(history))

        replayer = Replayer(
            workflows=[ContractLoop], plugins=[provider], stream_client=client
        )
        result = await replayer.replay_workflow(history)
        assert result.replay_failure is None
    finally:
        await provider.close()


async def test_a_replayer_fails_a_tampered_range_as_nondeterministic() -> None:
    """A history whose recorded ranges were changed replays on other input and fails.

    Every recorded range is emptied. The task that read two records and
    published two decisions is replayed with nothing to read, so it issues no
    publish, and Core finds the recorded publish event with no command for it.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    try:
        history = await _drive_contract_loop(client)
        tampered = _copy_of(history)
        for consumed in _consumed_ranges(tampered):
            consumed.to_offset = consumed.from_offset

        replayer = Replayer(
            workflows=[ContractLoop], plugins=[provider], stream_client=client
        )
        with pytest.raises(NondeterminismError):
            await replayer.replay_workflow(tampered)
    finally:
        await provider.close()


async def test_a_replayer_fails_loudly_when_the_stream_is_gone() -> None:
    """A range the stream service no longer serves is a ``StreamNotFoundError``, not a replay."""
    provider = NativeStreams()
    client = await _connect(provider)
    try:
        history = await _drive_contract_loop(client)
        # The same history under a workflow id that owns no stream: the
        # records it consumed are nowhere to be fetched from.
        gone = _copy_of(history, workflow_id="gone-wf-" + uuid.uuid4().hex[:8])

        replayer = Replayer(
            workflows=[ContractLoop], plugins=[provider], stream_client=client
        )
        with pytest.raises(StreamNotFoundError, match="cannot be replayed"):
            await replayer.replay_workflow(gone)
    finally:
        await provider.close()


async def test_a_replayer_without_a_client_says_what_it_needs() -> None:
    """Without a stream client a consuming workflow's history is refused, with the remedy."""
    provider = NativeStreams()
    client = await _connect(provider)
    try:
        history = await _drive_contract_loop(client)
        replayer = Replayer(workflows=[ContractLoop], plugins=[provider])

        with pytest.raises(RuntimeError, match="stream_client=") as raised:
            await replayer.replay_workflow(history)
        assert f"'{INPUTS}'" in str(raised.value)

        # The aggregating call reports it per run rather than aborting.
        async def histories():
            yield history

        results = await replayer.replay_workflows(
            histories(), raise_on_replay_failure=False
        )
        assert isinstance(results.replay_failures[history.run_id], RuntimeError)
    finally:
        await provider.close()


async def test_a_replayer_fetches_a_standalone_stream_by_its_id() -> None:
    """A subscribed name that is no stream of the workflow's is a standalone stream's id.

    The server resolves a subscription the same way, an owned stream by that
    name first, so the replayer has to look in both places to hand the replay
    the records the workflow actually read.
    """
    client = await _connect()
    streams = StreamClient.connect(TARGET or "")
    task_queue = "replay-src-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "replay-src-wf-" + uuid.uuid4().hex[:8]
    stream_id = "replay-src-" + uuid.uuid4().hex[:8]
    tokens = ["a1", "a2", "b1"]
    try:
        await streams.create(stream_id)
        async with Worker(
            client, task_queue=task_queue, workflows=[ConsumeAcrossTasks]
        ):
            handle = await client.start_workflow(
                ConsumeAcrossTasks.run,
                args=[stream_id, len(tokens)],
                id=workflow_id,
                task_queue=task_queue,
            )
            await streams.get(stream_id).append(*[_record(t.encode()) for t in tokens])
            assert await asyncio.wait_for(handle.result(), timeout=60) == tokens
        history = await handle.fetch_history()

        result = await Replayer(
            workflows=[ConsumeAcrossTasks], stream_client=client
        ).replay_workflow(history)
        assert result.replay_failure is None
    finally:
        await streams.close()


TWO_DECISIONS = [{"kind": "decision", "n": 1}, {"kind": "decision", "n": 2}]


async def _feed_two(client: Client, workflow_id: str) -> Any:
    """Append two inputs and wait until both decisions are out, so their tasks have completed."""
    stream = client.get_stream_handle(workflow_id)
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2})
    await take(stream.read(topic=DECISIONS, result_type=dict), 2, timeout=60)
    return producer


async def test_a_query_against_a_cold_worker_answers_from_replayed_state() -> None:
    """A query task built through matching carries the recorded ranges.

    With the cache off every task is a full replay. A query dispatched to a
    worker that holds nothing has to rebuild the run from History, and the
    records the run read are not in it, so the server attaches them to the
    query task the way it does to a task after a cache miss. Without them the
    replay would have nothing to read and the query would fail.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "query-cold-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "query-cold-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ContractLoop],
            max_cached_workflows=0,
        ):
            handle = await client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=task_queue
            )
            producer = await _feed_two(client, workflow_id)
            assert await handle.query(ContractLoop.trace) == TWO_DECISIONS
            await producer.finish()
            await asyncio.wait_for(handle.result(), 60)
    finally:
        await provider.close()


async def test_a_sticky_query_after_an_eviction_still_answers() -> None:
    """A query sent to the sticky queue of a run the worker evicted still answers.

    The sticky task carries the history since the last task and no records,
    which a worker that lost the run cannot use. Core sees that the history it
    fetched itself records a consumed range it was sent no records for, and
    lets the query go unanswered rather than answer it from the wrong state.
    The server's sticky attempt then times out, stickiness is reset, and the
    query is dispatched again on the normal queue, which carries the records.
    The sticky timeout is shortened so the test does not wait the default out.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "query-sticky-tq-" + uuid.uuid4().hex[:8]
    first_id = "query-sticky-a-" + uuid.uuid4().hex[:8]
    second_id = "query-sticky-b-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ContractLoop],
            max_cached_workflows=1,
            sticky_queue_schedule_to_start_timeout=timedelta(seconds=2),
        ):
            first = await client.start_workflow(
                ContractLoop.run, id=first_id, task_queue=task_queue
            )
            first_producer = await _feed_two(client, first_id)
            # A second run on a one-slot cache pushes the first out of it.
            second = await client.start_workflow(
                ContractLoop.run, id=second_id, task_queue=task_queue
            )
            second_producer = await _feed_two(client, second_id)

            answer = await first.query(
                ContractLoop.trace, rpc_timeout=timedelta(seconds=60)
            )
            assert answer == TWO_DECISIONS

            await first_producer.finish()
            await second_producer.finish()
            await asyncio.wait_for(first.result(), 60)
            await asyncio.wait_for(second.result(), 60)
    finally:
        await provider.close()


async def _collect(
    client: Client, workflow_id: str, topic: str, run_id: str | None
) -> list[tuple[Any, str, int]]:
    """Every record on ``topic`` as ``(value, run, offset)``, the run and offset from the cursor."""
    handle = client.get_stream_handle(workflow_id, run_id=run_id)
    out: list[tuple[Any, str, int]] = []
    async for record in handle.read(topic=topic, result_type=dict):
        # native:<run>:<offset>
        _, run, offset = record.cursor.token.split(":")
        out.append((record.value, run, int(offset)))
    return out


async def test_a_reset_run_is_followed_and_replayed() -> None:
    """A run reset from a consuming one carries its subscriptions on, and readers follow.

    The reset re-runs the task named by the reset point, so the base run's
    history is copied up to that task and the ranges recorded in the copy are
    what the reset run replays, from the base run's streams. The inherited
    ``inputs`` stream is the reset run's own from the inherited cursor on: it
    starts empty at that offset, and the input the re-run task had consumed is
    not delivered again, so the next input takes that offset. The
    ``decisions`` stream, which the base run only published to, starts at zero.
    A handle without a run id follows the base run into the reset run and starts
    each stream at the floor it reports; a handle pinned to the base run ends
    with it; the ``Replayer`` fetches each era of the reset run's history from
    the run whose stream holds it.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "reset-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "reset-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[ContractLoop]):
            base = await client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=task_queue
            )
            base_run = base.result_run_id
            assert base_run
            producer = await _feed_two(client, workflow_id)
            # A third input in a task of its own, which is the task the reset
            # re-runs: its consumption is dropped with it.
            await producer.append({"n": 3})
            base_stream = client.get_stream_handle(workflow_id, run_id=base_run)
            await take(
                base_stream.read(topic=DECISIONS, result_type=dict), 3, timeout=60
            )

            # Reset to the completion of the last consuming task, while the base
            # run waits for more input.
            completion_id = 0
            async for event in client.get_workflow_handle(
                workflow_id, run_id=base_run
            ).fetch_history_events():
                if event.HasField("workflow_task_completed_event_attributes"):
                    completed = event.workflow_task_completed_event_attributes
                    if any(
                        r.to_offset > r.from_offset
                        for r in completed.consumed_stream_ranges
                    ):
                        completion_id = event.event_id
            assert completion_id
            reset = await client.workflow_service.reset_workflow_execution(
                ResetWorkflowExecutionRequest(
                    namespace=client.namespace,
                    workflow_execution=WorkflowExecution(
                        workflow_id=workflow_id, run_id=base_run
                    ),
                    reason="re-run the last consuming task",
                    workflow_task_finish_event_id=completion_id,
                    request_id=uuid.uuid4().hex,
                )
            )
            reset_run = reset.run_id
            assert reset_run and reset_run != base_run

            # A fresh producer pins to the current run, the reset run, whose
            # inherited inputs stream continues at the inherited offset.
            continued = client.get_stream_handle(workflow_id).producer(
                topic=INPUTS, producer_id="model2", attempt=1
            )
            await continued.append({"n": 4})
            await continued.finish()
            trace = await asyncio.wait_for(
                client.get_workflow_handle(workflow_id, run_id=reset_run).result(),
                60,
            )
            # The first two decisions were replayed from the base run's stream;
            # the third input went with the task the reset re-ran.
            assert trace == TWO_DECISIONS + [
                {"kind": "decision", "n": 4},
                {"kind": "finish", "producer": "model2"},
            ]

            # The base run was terminated by the reset, and describe is the one
            # place that names the run it was reset into.
            described = await client.get_workflow_handle(
                workflow_id, run_id=base_run
            ).describe()
            assert described.status == WorkflowExecutionStatus.TERMINATED
            extended = described.raw_description.workflow_extended_info
            assert extended.reset_run_id == reset_run

            # A reader pinned to the base run ends with the base run.
            pinned = await asyncio.wait_for(
                _collect(client, workflow_id, DECISIONS, base_run), 30
            )
            assert pinned == [
                ({"decided": 1}, base_run, 0),
                ({"decided": 2}, base_run, 1),
                ({"decided": 3}, base_run, 2),
            ]

            # A chain-following reader crosses from the base run into the reset
            # run on both topics, with no gap and no refusal: the published one
            # restarts at zero, the inherited one continues at the floor.
            decisions = await asyncio.wait_for(
                _collect(client, workflow_id, DECISIONS, None), 30
            )
            assert decisions == [
                ({"decided": 1}, base_run, 0),
                ({"decided": 2}, base_run, 1),
                ({"decided": 3}, base_run, 2),
                ({"decided": 4}, reset_run, 0),
                (None, reset_run, 1),
            ]
            inputs = await asyncio.wait_for(
                _collect(client, workflow_id, INPUTS, None), 30
            )
            assert inputs == [
                ({"n": 1}, base_run, 0),
                ({"n": 2}, base_run, 1),
                ({"n": 3}, base_run, 2),
                ({"n": 4}, reset_run, 2),
                (None, reset_run, 3),
            ]
            latest = await client.get_stream_handle(workflow_id).latest(topic=INPUTS)
            assert latest.token == f"native:{reset_run}:3"

        # The reset run's history: the base run's events, the reset marker
        # naming both runs, then its own. The replayer fetches the first era
        # from the base run's stream and the rest from the reset run's.
        history = await client.get_workflow_handle(
            workflow_id, run_id=reset_run
        ).fetch_history()
        reset_cause = WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW
        markers = [
            (failed.base_run_id, failed.new_run_id)
            for event in history.events
            if event.HasField("workflow_task_failed_event_attributes")
            for failed in [event.workflow_task_failed_event_attributes]
            if failed.cause == reset_cause
        ]
        assert markers == [(base_run, reset_run)]
        result = await Replayer(
            workflows=[ContractLoop], plugins=[provider], stream_client=client
        ).replay_workflow(history)
        assert result.replay_failure is None

        # Exported with its records, the reset run's history replays with no
        # server: the slices come from both runs' streams.
        bundle = await Replayer.fetch_stream_slices(client, history)
        assert {s.run_id for s in bundle.stream_slices if s.records} == {
            base_run,
            reset_run,
        }
        restored = WorkflowHistory.from_json(workflow_id, bundle.to_json())
        offline = await Replayer(
            workflows=[ContractLoop], plugins=[provider]
        ).replay_workflow(restored)
        assert offline.replay_failure is None
    finally:
        await provider.close()


async def test_an_exported_history_replays_offline_with_its_records() -> None:
    """A history exported with its stream records is the whole replay input.

    ``fetch_stream_slices`` captures the records while the stream is retained,
    ``to_json`` writes them beside the events, ``from_json`` reads them back,
    and a replayer with no client replays the result. A plain export carries
    none and is refused with both remedies named.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    try:
        history = await _drive_contract_loop(client)
        assert "streamSlices" not in history.to_json()

        bundle = await Replayer.fetch_stream_slices(client, history)
        assert list(bundle.events) == list(history.events)
        assert any(s.records for s in bundle.stream_slices)
        text = bundle.to_json()
        assert "streamSlices" in text
        restored = WorkflowHistory.from_json(history.workflow_id, text)
        assert list(restored.stream_slices) == list(bundle.stream_slices)
        assert list(restored.events) == list(history.events)

        offline = Replayer(workflows=[ContractLoop], plugins=[provider])
        result = await offline.replay_workflow(restored)
        assert result.replay_failure is None

        # With the records stripped it is a plain export again.
        stripped = WorkflowHistory(restored.workflow_id, restored.events)
        with pytest.raises(RuntimeError, match="stream_client=") as raised:
            await offline.replay_workflow(stripped)
        assert "fetch_stream_slices" in str(raised.value)
    finally:
        await provider.close()
