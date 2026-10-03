"""Conformance for the workflow_streams provider on the test environment's server.

Runs the interface loop over the shipped Option 0 transport: an outside
producer appends through the publish Update, or the shipped publish Signal
where a workflow has no Update, the workflow reads and republishes through
its own state, and an outside reader follows the poll Update while the run is
open and the tail Query once it has closed. The outside-surface cases shared
by every provider run from ``test_streams_conformance``; this file covers
what the transport adds.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload, WorkflowExecution
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.contrib.workflow_streams import PublishInput, WorkflowStream
from temporalio.converter import DataConverter, ExternalStorage
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import (
    BEGINNING,
    CONTENT_HASH_KEY,
    DEFAULT_TOPIC,
    END,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
    StreamRef,
    StreamUnsupportedError,
    Supersession,
    content_hash,
)
from temporalio.streams._wire import WireRecord
from temporalio.streams.providers import workflow_streams
from temporalio.streams.providers.workflow_streams import (
    WorkflowStreamsActivityHandle,
    WorkflowStreamsHandle,
    WorkflowStreamsProducer,
    WorkflowStreamsProvider,
    _InstanceStream,
    _PublishResult,
)
from temporalio.worker._workflow_instance import QUERY_HANDLER_NOT_FOUND
from tests.helpers import new_worker

INPUTS = "inputs"
DECISIONS = "decisions"


@pytest.fixture
def provider() -> WorkflowStreamsProvider:
    return WorkflowStreamsProvider(poll_cooldown=timedelta(milliseconds=20))


@workflow.defn
class EchoLoop:
    """Reads ``inputs``, echoes each value onto ``decisions``, ends on FINISH.

    Lingers until released so a reader can follow it while it runs; whoever
    arrives after it closed is served by the tail Query instead.
    """

    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> int:
        inputs = workflow.stream_reader(INPUTS, result_type=dict)
        decisions = workflow.stream_writer(DECISIONS)
        seen = 0
        async for record in inputs:
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            assert record.value is not None
            seen += 1
            decisions.publish({"echo": record.value["n"]})
        decisions.finish()
        await workflow.wait_condition(lambda: self._released)
        return seen


async def take(records: Any, count: int, timeout: float = 30.0) -> list:
    out: list = []

    async def _collect() -> None:
        async for record in records:
            out.append(record)
            if len(out) >= count:
                return

    await asyncio.wait_for(_collect(), timeout)
    return out


async def _feed(
    provider: WorkflowStreamsProvider, client: Client, workflow_id: str
) -> None:
    stream = provider.get_stream_handle(client, workflow_id)
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2})
    await producer.append({"n": 3})
    await producer.finish()


ECHOED = [RecordKind.DATA, RecordKind.DATA, RecordKind.DATA, RecordKind.FINISH]


async def test_interface_loop_over_workflow_streams(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await _feed(provider, client, workflow_id)

        stream = provider.get_stream_handle(client, workflow_id)
        records = await take(stream.read(topic=DECISIONS, result_type=dict), 4)
        assert [r.kind for r in records] == ECHOED
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]
        assert all(r.topic == DECISIONS and r.producer_id == "" for r in records)

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3


async def test_retried_producer_dedupes_and_new_attempt_supersedes(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)

        first = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        # The publish Update answers with where the batch landed.
        landed = await first.append({"n": 1})
        assert landed is not None
        # The retry of the same attempt re-sends its first batch and is
        # answered with the same position.
        retry = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        assert await retry.append({"n": 1}) == landed
        second = stream.producer(topic=INPUTS, producer_id="model", attempt=2)
        await second.append({"n": 2})

        records = await take(stream.read(topic=INPUTS, result_type=dict), 3)
        assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
        assert records[0].cursor == landed
        assert records[1].kind is RecordKind.SUPERSEDED
        assert records[1].supersession == Supersession("model", 1, 2)
        assert records[2].kind is RecordKind.DATA and records[2].attempt == 2
        assert all(r.topic == INPUTS for r in records)

        # A sequence behind the producer's most recent one is stale, and a
        # refusal is typed rather than a silent drop.
        await second.append({"n": 3})
        stale = stream.producer(topic=INPUTS, producer_id="model", attempt=2)
        with pytest.raises(StreamProducerError, match="most recent"):
            await stale.append({"n": "other"})
        assert stale.transport == "update"

        await second.finish()
        await handle.signal(EchoLoop.release)
        await handle.result()


async def test_cold_cache_serves_each_record_once(
    client: Client, provider: WorkflowStreamsProvider
):
    # Every task rebuilds the workflow from history, so the stream object
    # has to belong to the instance that is running: a stale one would carry
    # the previous instance's log and hand out every record twice.
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(
        client, EchoLoop, plugins=[provider], max_cached_workflows=0
    ) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await _feed(provider, client, workflow_id)

        stream = provider.get_stream_handle(client, workflow_id)
        records = await take(stream.read(topic=DECISIONS, result_type=dict), 4)
        assert [r.kind for r in records] == ECHOED
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]
        arrived = await take(stream.read(topic=INPUTS, result_type=dict), 4)
        assert [r.kind for r in arrived] == ECHOED
        assert [r.value["n"] for r in arrived[:3]] == [1, 2, 3]

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3


async def test_a_closed_run_serves_its_tail_by_query_and_the_read_ends(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await _feed(provider, client, workflow_id)
        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3

        # Nothing polled while the run was open. The poll Update is gone with
        # the run, so everything below arrives through the tail Query, and
        # the read ends by itself once the tail is delivered.
        stream = provider.get_stream_handle(client, workflow_id)

        async def read_everything() -> list[Any]:
            return [r async for r in stream.read(topic=DECISIONS, result_type=dict)]

        records = await asyncio.wait_for(read_everything(), 30)
        assert [r.kind for r in records] == ECHOED
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

        checkpoint = records[1].cursor
        again = await take(
            stream.read(topic=DECISIONS, result_type=dict, after=checkpoint), 2
        )
        assert [r.value["echo"] for r in again[:1]] == [3]
        assert again[1].kind is RecordKind.FINISH


async def test_a_bounded_read_is_cancelled_within_its_timeout(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)

        async def read_forever() -> None:
            async for _ in stream.read(topic=DECISIONS, result_type=dict):
                pass

        # The run is open and nothing is published, so the read parks on the
        # poll Update. The bound has to come out as a timeout rather than
        # vanish into a resubscribe.
        started = asyncio.get_running_loop().time()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(read_forever(), timeout=1)
        assert asyncio.get_running_loop().time() - started < 10
        # A reader task cancelled outright ends the same way.
        task = asyncio.create_task(read_forever())
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        await stream.producer(topic=INPUTS, producer_id="model", attempt=1).finish()
        await handle.signal(EchoLoop.release)
        assert await handle.result() == 0


@workflow.defn
class Truncating:
    """Publishes on two topics and truncates its log when told to."""

    def __init__(self) -> None:
        # Constructed here so the shipped signal handler is registered before
        # the provider looks for it, the way a migrating application holds it.
        self._stream = WorkflowStream()
        self._released = False

    @workflow.signal
    def truncate_to(self, offset: int) -> None:
        self._stream.truncate(offset)

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        other = workflow.stream_writer(INPUTS)
        decisions.publish({"n": 0})
        other.publish({"side": "inputs"})
        decisions.publish({"n": 1})
        await workflow.wait_condition(lambda: self._released)


async def test_a_truncated_position_is_refused_rather_than_restarted(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, Truncating, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            Truncating.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)
        first = await take(stream.read(topic=DECISIONS, result_type=dict), 1)
        assert first[0].value == {"n": 0}

        # Everything the reader's cursor names is dropped from the log.
        await handle.signal(Truncating.truncate_to, 3)
        resumed = stream.read(topic=DECISIONS, result_type=dict, after=first[0].cursor)
        # Starting over would hand back records the caller already handled,
        # and only the caller can decide to do that.
        with pytest.raises(StreamCursorError):
            await take(resumed, 1, timeout=30)

        await handle.signal(Truncating.release)
        await handle.result()


async def test_latest_names_the_newest_record_on_the_topic_asked_for(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, Truncating, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            Truncating.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)
        decisions = await take(stream.read(topic=DECISIONS, result_type=dict), 2)

        # One log orders both topics and the newest item on it belongs to
        # `inputs`, so a log-global answer would be the wrong cursor here.
        assert await stream.latest(topic=DECISIONS) == decisions[-1].cursor
        inputs = await take(stream.read(topic=INPUTS, result_type=dict), 1)
        assert await stream.latest(topic=INPUTS) == inputs[0].cursor
        assert await stream.latest(topic="never-written") == BEGINNING

        await handle.signal(Truncating.release)
        await handle.result()


async def test_the_tail_query_pages_instead_of_answering_in_one_blob():
    # The workflow-side half of the tail, driven directly: a Query response
    # has to fit the server's blob limit, so a log larger than the cap comes
    # back a page at a time with a position to resume from.
    big = Payload(metadata={"encoding": b"binary/plain"}, data=b"x" * 400_000)
    items = [(offset, DECISIONS, big) for offset in range(6)]

    class _Log:
        next_offset = len(items)

        def items_from(self, offset: int) -> list[Any]:
            return [item for item in items if item[0] >= offset]

    instance = _InstanceStream.__new__(_InstanceStream)
    instance._stream = _Log()  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage]

    seen: list[int] = []
    offset, more = 0, True
    while more:
        page = instance._tail(offset, DECISIONS)
        assert page["items"], "a page that fits nothing would never finish"
        seen.extend(item["offset"] for item in page["items"])
        offset, more = page["next_offset"], page["more_ready"]
    assert seen == list(range(6))


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


async def test_a_handle_without_a_run_id_reads_across_continue_as_new(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, Relay, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            Relay.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
        first_run = handle.first_execution_run_id
        assert first_run is not None

        chain = provider.get_stream_handle(client, workflow_id)

        async def read_everything(stream: Any) -> list[Any]:
            return [
                (r.kind, r.value)
                async for r in stream.read(topic=DECISIONS, result_type=dict)
            ]

        # Each run keeps its own log, so following the chain means reading
        # the first run to its close and then the successor from its start.
        assert await asyncio.wait_for(read_everything(chain), 30) == [
            (RecordKind.DATA, {"run": 0}),
            (RecordKind.DATA, {"run": 1}),
            (RecordKind.FINISH, None),
        ]
        pinned = provider.get_stream_handle(client, workflow_id, run_id=first_run)
        assert await asyncio.wait_for(read_everything(pinned), 30) == [
            (RecordKind.DATA, {"run": 0}),
        ]
        # A cursor from the first run resumes into the successor.
        records = await take(chain.read(topic=DECISIONS, result_type=dict), 1)
        resumed = await asyncio.wait_for(
            asyncio.ensure_future(
                _values(
                    chain.read(
                        topic=DECISIONS, result_type=dict, after=records[0].cursor
                    )
                )
            ),
            30,
        )
        assert resumed == [{"run": 1}, None]


@workflow.defn
class Rolling:
    """Publishes what it is sent, continues as new onto ``successor_queue`` once."""

    def __init__(self) -> None:
        self._sent: list[int] = []
        self._roll_to = ""
        self._released = False

    @workflow.signal
    def emit(self, n: int) -> None:
        self._sent.append(n)

    @workflow.signal
    def roll(self, successor_queue: str) -> None:
        self._roll_to = successor_queue

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self, run: int) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        while True:
            await workflow.wait_condition(
                lambda: bool(self._sent or self._roll_to or self._released)
            )
            while self._sent:
                decisions.publish({"run": run, "n": self._sent.pop(0)})
            if self._roll_to:
                workflow.continue_as_new(run + 1, task_queue=self._roll_to)
            if self._released:
                decisions.finish()
                return


@workflow.defn
class Idle:
    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._released)


async def _collect_all(records: Any, into: list[Any]) -> None:
    async for record in records:
        into.append((record.kind, record.value))


async def test_a_live_read_without_a_run_id_follows_continue_as_new(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    successor_queue = f"streams-ws-successor-{uuid.uuid4().hex}"
    async with new_worker(client, Rolling, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            Rolling.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
        seen: list[Any] = []
        reader = asyncio.create_task(
            _collect_all(
                provider.get_stream_handle(client, workflow_id).read(
                    topic=DECISIONS, result_type=dict
                ),
                seen,
            )
        )
        await handle.signal(Rolling.emit, 1)
        await handle.signal(Rolling.emit, 2)
        await assert_eventually_len(seen, 2, reader)

        # The successor runs on a queue nobody polls yet, so the reader's
        # first poll on it waits for the successor's first task and lands in
        # it, ahead of the hook that registers the handler.
        await handle.signal(Rolling.roll, successor_queue)
        successor = client.get_workflow_handle(workflow_id)
        while (await successor.describe()).run_id == handle.result_run_id:
            await asyncio.sleep(0.05)
        await asyncio.sleep(1)
        assert not reader.done()
        async with new_worker(
            client, Rolling, plugins=[provider], task_queue=successor_queue
        ):
            await successor.signal(Rolling.emit, 3)
            await successor.signal(Rolling.emit, 4)
            await assert_eventually_len(seen, 4, reader)
            await successor.signal(Rolling.release)
            await asyncio.wait_for(reader, 30)
            await successor.result()

    assert seen == [
        (RecordKind.DATA, {"run": 0, "n": 1}),
        (RecordKind.DATA, {"run": 0, "n": 2}),
        (RecordKind.DATA, {"run": 1, "n": 3}),
        (RecordKind.DATA, {"run": 1, "n": 4}),
        (RecordKind.FINISH, None),
    ]


async def test_a_poll_that_arrives_before_the_first_task_is_retried(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    task_queue = f"streams-ws-{uuid.uuid4().hex}"
    handle = await client.start_workflow(
        Rolling.run, 0, id=workflow_id, task_queue=task_queue
    )
    seen: list[Any] = []
    reader = asyncio.create_task(
        _collect_all(
            provider.get_stream_handle(client, workflow_id).read(
                topic=DECISIONS, result_type=dict
            ),
            seen,
        )
    )
    # No worker yet, so the poll is delivered in the run's first task.
    await asyncio.sleep(1)
    async with new_worker(client, Rolling, plugins=[provider], task_queue=task_queue):
        await handle.signal(Rolling.emit, 1)
        await assert_eventually_len(seen, 1, reader)
        await handle.signal(Rolling.release)
        await asyncio.wait_for(reader, 30)
        await handle.result()
    assert seen == [(RecordKind.DATA, {"run": 0, "n": 1}), (RecordKind.FINISH, None)]


async def _reset_at_last_completed_task(
    client: Client, workflow_id: str, run_id: str
) -> str:
    """Reset ``run_id`` at its last completed task; the id of the run reset into."""
    completion_id = 0
    events = client.get_workflow_handle(
        workflow_id, run_id=run_id
    ).fetch_history_events()
    async for event in events:
        if event.HasField("workflow_task_completed_event_attributes"):
            completion_id = event.event_id
    assert completion_id
    try:
        answer = await client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=client.namespace,
                workflow_execution=WorkflowExecution(
                    workflow_id=workflow_id, run_id=run_id
                ),
                reason="re-run from the last completed task",
                workflow_task_finish_event_id=completion_id,
                request_id=uuid.uuid4().hex,
            )
        )
    except RPCError as error:
        if error.status != RPCStatusCode.UNIMPLEMENTED:
            raise
        # The Java time-skipping test server has no reset; the real server
        # does, and this never skips there.
        pytest.skip("this test server does not implement ResetWorkflowExecution")
    return answer.run_id


async def _collect_records(records: Any, into: list[Any]) -> None:
    async for record in records:
        into.append(record)


def _run_and_offset(record: Any) -> tuple[str, int]:
    _, run_id, offset = record.cursor.token.split(":")
    return run_id, int(offset)


async def test_a_live_read_without_a_run_id_follows_a_reset(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, Rolling, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            Rolling.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
        base_run = handle.result_run_id
        assert base_run is not None
        seen: list[Any] = []
        reader = asyncio.create_task(
            _collect_records(
                provider.get_stream_handle(client, workflow_id).read(
                    topic=DECISIONS, result_type=dict
                ),
                seen,
            )
        )
        await handle.signal(Rolling.emit, 1)
        await handle.signal(Rolling.emit, 2)
        await assert_eventually_len(seen, 2, reader)

        # The base run is closed by the reset with nothing in its own History
        # to say so; the reader learns where it went from describe. The reset
        # run replays the base run's History up to the last completed task,
        # so its log holds the same two records at the same offsets, and the
        # read carries on from the position it had reached.
        reset_run = await _reset_at_last_completed_task(client, workflow_id, base_run)
        assert reset_run != base_run
        current = client.get_workflow_handle(workflow_id)
        assert (await current.describe()).run_id == reset_run
        await current.signal(Rolling.emit, 3)
        await current.signal(Rolling.emit, 4)
        await assert_eventually_len(seen, 4, reader)
        await current.signal(Rolling.release)
        await asyncio.wait_for(reader, 30)
        await current.result()

        assert [(r.kind, r.value) for r in seen] == [
            (RecordKind.DATA, {"run": 0, "n": 1}),
            (RecordKind.DATA, {"run": 0, "n": 2}),
            (RecordKind.DATA, {"run": 0, "n": 3}),
            (RecordKind.DATA, {"run": 0, "n": 4}),
            (RecordKind.FINISH, None),
        ]
        # Two records from the base run, then the reset run's, whose offsets
        # continue where the base run's log stood at the reset point.
        assert [_run_and_offset(r) for r in seen] == [
            (base_run, 0),
            (base_run, 1),
            (reset_run, 2),
            (reset_run, 3),
            (reset_run, 4),
        ]
        # A handle pinned to the base run ends with it. Both closed runs are
        # served by the tail Query, which needs the worker still up.
        pinned = provider.get_stream_handle(client, workflow_id, run_id=base_run)
        pinned_records: list[Any] = []
        await asyncio.wait_for(
            _collect_records(pinned.read(topic=DECISIONS), pinned_records), 30
        )
        assert [_run_and_offset(r) for r in pinned_records] == [
            (base_run, 0),
            (base_run, 1),
        ]
        # BEGINNING on the chain starts at the base run, whose start event the
        # reset run copied, and a resume from a base run cursor crosses over.
        chain = provider.get_stream_handle(client, workflow_id)
        resumed = await take(chain.read(topic=DECISIONS, after=seen[1].cursor), 3)
        assert [_run_and_offset(r) for r in resumed] == [
            (reset_run, 2),
            (reset_run, 3),
            (reset_run, 4),
        ]


@workflow.defn
class ShippedOnly:
    """Holds the shipped stream object alone, as a workflow on a worker without the provider."""

    def __init__(self) -> None:
        self._stream = WorkflowStream()
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._released)


async def test_a_workflow_without_the_publish_update_is_appended_to_by_signal(
    client: Client, provider: WorkflowStreamsProvider
):
    # The worker has no provider, so the workflow serves the shipped Signal
    # and nothing else. The producer learns that from two rejections and
    # falls back, and the batches land in the shipped log all the same.
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, ShippedOnly) as worker:
        handle = await client.start_workflow(
            ShippedOnly.run, id=workflow_id, task_queue=worker.task_queue
        )
        producer = provider.get_stream_handle(client, workflow_id).producer(
            topic=INPUTS, producer_id="model", attempt=1
        )
        assert await producer.append({"n": 1}) is None
        assert producer.transport == "signal"
        await producer.append({"n": 2}, {"n": 3})
        assert (
            await handle.query("__temporal_workflow_stream_offset", result_type=int)
            == 3
        )
        await handle.signal(ShippedOnly.release)
        await handle.result()


async def test_a_running_workflow_without_the_provider_fails_the_read_clearly(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, Idle) as worker:
        handle = await client.start_workflow(
            Idle.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)
        with pytest.raises(StreamError, match="provider is not installed"):
            await take(stream.read(topic=DECISIONS), 1)
        await handle.signal(Idle.release)
        await handle.result()


async def assert_eventually_len(
    items: list[Any], count: int, reader: asyncio.Task[None]
) -> None:
    async def _wait() -> None:
        while len(items) < count:
            if reader.done():
                # A read that failed surfaces its error instead of a timeout.
                reader.result()
                raise AssertionError(f"the read ended early with {items}")
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_wait(), 30)


async def _values(records: Any) -> list[Any]:
    return [r.value async for r in records]


class _FlakyHandle:
    """A workflow handle whose first Signal is accepted and then reported failed."""

    id = "flaky"

    def __init__(self) -> None:
        self.sent: list[PublishInput] = []
        self._fail_next = True

    async def signal(self, name: str, arg: PublishInput) -> None:
        del name
        self.sent.append(arg)
        if self._fail_next:
            self._fail_next = False
            raise ConnectionResetError(
                "the server accepted the signal, the reply was lost"
            )


def _wires(publish: PublishInput) -> list[WireRecord]:
    out = []
    for entry in publish.items:
        payload = Payload.FromString(base64.b64decode(entry.data))
        out.append(WireRecord.FromString(payload.data))
    return out


def _sequences(sent: list[PublishInput]) -> list[tuple[int, list[int]]]:
    return [
        (publish.sequence, [wire.sequence for wire in _wires(publish)])
        for publish in sent
    ]


def _producer(handle: Any, transport: Any = "signal") -> WorkflowStreamsProducer:
    return WorkflowStreamsProducer(
        handle,
        DataConverter.default.payload_converter,
        INPUTS,
        "model",
        1,
        transport=transport,
        retry_cooldown=timedelta(0),
    )


async def test_a_retried_append_after_an_ambiguous_failure_writes_once():
    # The Signal transport: what a producer that fell back to it does.
    handle = _FlakyHandle()
    producer = _producer(handle)
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    assert await producer.append({"n": 1}) is None
    await producer.append({"n": 2})
    # The retry carries the same signal sequence and the same record
    # sequence as the failed send, so the shipped dedupe drops the copy; the
    # batch after it continues the numbering.
    assert _sequences(handle.sent) == [(2, [1]), (2, [1]), (3, [2])]
    assert all(publish.publisher_id == "model#1" for publish in handle.sent)


async def test_a_batch_whose_signal_failed_goes_out_before_the_next_one():
    handle = _FlakyHandle()
    producer = _producer(handle)
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    await producer.append({"n": 2}, {"n": 3})
    await producer.finish()
    assert _sequences(handle.sent) == [(2, [1]), (2, [1]), (4, [2, 3]), (5, [4])]
    assert _wires(handle.sent[-1])[0].kind == int(RecordKind.FINISH)


class _UpdateHandle:
    """A workflow handle serving the publish Update as the server and workflow would.

    Answers a repeated update id from the first outcome, the way the server
    does, positions each new batch at the head of a log, the way the
    workflow does, and loses the reply of the first call when told to.
    Without a handler it rejects every Update the way a workflow whose
    worker predates it does, and takes Signals.
    """

    id = "update"

    def __init__(self, *, lose_first_reply: bool = False, handler: bool = True):
        self.sent: list[tuple[str, PublishInput]] = []
        self.signalled: list[PublishInput] = []
        self._outcomes: dict[str, _PublishResult] = {}
        self._head = 0
        self._lose = lose_first_reply
        self._handler = handler

    async def execute_update(
        self, name: str, arg: PublishInput, *, id: str, result_type: Any
    ) -> _PublishResult:
        del name, result_type
        self.sent.append((id, arg))
        if not self._handler:
            raise WorkflowUpdateFailedError(
                ApplicationError(
                    f"Update handler for 'x' {QUERY_HANDLER_NOT_FOUND}, known updates: []"
                )
            )
        answer = self._outcomes.get(id)
        if answer is None:
            answer = _PublishResult("run", self._head + len(arg.items) - 1)
            self._head += len(arg.items)
            self._outcomes[id] = answer
        if self._lose:
            self._lose = False
            raise ConnectionResetError("the server took the update, the reply was lost")
        return answer

    async def signal(self, name: str, arg: PublishInput) -> None:
        del name
        self.signalled.append(arg)


def _at(offset: int) -> Cursor:
    return Cursor(f"workflow_streams:run:{offset}")


async def test_a_retried_update_after_a_lost_reply_is_answered_from_its_id():
    handle = _UpdateHandle(lose_first_reply=True)
    producer = _producer(handle, "update")
    assert await producer.append() is None
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    # The retry is the same Update: same producer, sequence and content make
    # the same id, so the server answers with the outcome it already holds
    # and the log takes the batch once. The batch after it is a new one.
    assert await producer.append({"n": 1}) == _at(0)
    assert await producer.append({"n": 2}) == _at(1)
    assert await producer.append() == _at(1)
    ids = [id for id, _ in handle.sent]
    assert ids[0] == ids[1] != ids[2]
    assert _sequences([arg for _, arg in handle.sent]) == [(2, [1]), (2, [1]), (3, [2])]


async def test_a_batch_whose_update_failed_goes_out_before_the_next_one():
    handle = _UpdateHandle(lose_first_reply=True)
    producer = _producer(handle, "update")
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    # The pending batch lands first, at offset 0, so the new one follows it.
    assert await producer.append({"n": 2}, {"n": 3}) == _at(2)
    await producer.finish()
    sent = [arg for _, arg in handle.sent]
    assert _sequences(sent) == [(2, [1]), (2, [1]), (4, [2, 3]), (5, [4])]
    assert _wires(sent[-1])[0].kind == int(RecordKind.FINISH)


async def test_a_producer_falls_back_to_the_signal_without_a_publish_update():
    handle = _UpdateHandle(handler=False)
    producer = _producer(handle, "update")
    # Rejected twice across a task boundary means the workflow's worker
    # predates the Update; the batch goes by Signal, and so does every later
    # one, without asking again.
    assert await producer.append({"n": 1}) is None
    assert producer.transport == "signal"
    assert await producer.append({"n": 2}) is None
    assert len(handle.sent) == 2
    assert _sequences(handle.signalled) == [(2, [1]), (3, [2])]


class _Description:
    run_id = "the-only-run"
    status = WorkflowExecutionStatus.COMPLETED


class _StubHandle:
    """A workflow handle that answers describe and fails whatever the test names."""

    id = "stub"
    run_id = "the-only-run"

    def __init__(
        self,
        *,
        query_error: BaseException | None = None,
        events_error: BaseException | None = None,
    ) -> None:
        self._query_error = query_error
        self._events_error = events_error

    async def describe(self) -> Any:
        return _Description()

    async def start_update(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        # The run is closed, so its poll Update is gone with it and the read
        # goes on to the tail Query, which is what these cases are about.
        raise RPCError("no poll update", RPCStatusCode.NOT_FOUND, b"")

    async def query(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        assert self._query_error is not None
        raise self._query_error

    def fetch_history_events(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        error = self._events_error

        async def _events() -> AsyncIterator[Any]:
            events: tuple[Any, ...] = ()
            for event in events:
                yield event
            if error is not None:
                raise error

        return _events()


class _OneHandleClient:
    """A client that answers every handle request with the same handle."""

    data_converter = DataConverter.default

    def __init__(self, handle: Any) -> None:
        self._handle = handle

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None
    ) -> Any:
        del workflow_id, run_id
        return self._handle


def _handle_over(stub: _StubHandle) -> WorkflowStreamsHandle:
    return WorkflowStreamsHandle(
        _OneHandleClient(stub),  # type: ignore[arg-type]
        "wf",
        "the-only-run",
        timedelta(0),
    )


async def test_a_failing_tail_query_comes_back_as_a_stream_error():
    # The handler being absent is the one benign case; anything else is a
    # real failure, and the interface says a caller catches stream conditions
    # by meaning rather than by the client's own exception types.
    stub = _StubHandle(query_error=WorkflowQueryFailedError("the workflow rejected it"))
    with pytest.raises(StreamError):
        await take(_handle_over(stub).read(topic=DECISIONS, result_type=dict), 1)


async def test_a_missing_tail_handler_ends_the_read_instead_of_failing_it():
    stub = _StubHandle(
        query_error=WorkflowQueryFailedError(
            f"Query handler for 'x' {QUERY_HANDLER_NOT_FOUND}, known queries: []"
        )
    )
    # The workflow never opened a stream through this provider, so the run
    # holds no tail and the read is simply over.
    assert [r async for r in _handle_over(stub).read(topic=DECISIONS)] == []


async def test_a_failing_latest_query_comes_back_as_a_stream_error():
    stub = _StubHandle(query_error=WorkflowQueryFailedError("the workflow rejected it"))
    with pytest.raises(StreamError):
        await _handle_over(stub).latest(topic=DECISIONS)


async def test_a_missing_run_leaves_the_successor_lookup_as_a_stream_error():
    stub = _StubHandle(events_error=RPCError("gone", RPCStatusCode.NOT_FOUND, b""))
    with pytest.raises(StreamNotFoundError):
        await _handle_over(stub)._successor(stub)  # type: ignore[arg-type]


class _PlainHandle:
    """A workflow handle that accepts every Signal and remembers it."""

    id = "plain"

    def __init__(self) -> None:
        self.sent: list[PublishInput] = []

    async def signal(self, name: str, arg: PublishInput) -> None:
        del name
        self.sent.append(arg)


async def test_the_dedupe_sequence_names_where_the_records_end():
    # The shipped handler drops a batch whose sequence it has already passed,
    # so the sequence has to say how far this producer's records reach. A
    # count of signals does not: a retry that batches its records differently
    # from the send it repeats then carries a sequence the workflow has not
    # seen, and the records it already holds go in a second time.
    first = _PlainHandle()
    original = _producer(first)  # type: ignore[arg-type]
    await original.append({"n": 1}, {"n": 2})

    second = _PlainHandle()
    retry = _producer(second)  # type: ignore[arg-type]
    await retry.append({"n": 1})
    await retry.append({"n": 2})
    await retry.append({"n": 3})

    # The original ended at record 2, so its sequence is 3. Neither half of
    # the retry's re-split reaches past it, and only the new record does.
    assert _sequences(first.sent) == [(3, [1, 2])]
    assert _sequences(second.sent) == [(2, [1]), (3, [2]), (4, [3])]


@workflow.defn
class StartsWhenTold:
    """Opens a reader on ``inputs`` at the start a signal names and returns what it read."""

    def __init__(self) -> None:
        self._start: str | None = None

    @workflow.signal
    def begin(self, start: str) -> None:
        self._start = start

    @workflow.run
    async def run(self) -> list[Any]:
        await workflow.wait_condition(lambda: self._start is not None)
        if self._start == "end":
            reader = workflow.stream_reader(INPUTS, result_type=dict, after=END)
            want = 1
        else:
            reader = workflow.stream_reader(INPUTS, result_type=dict, last=2)
            want = 2
        values: list[Any] = []
        async for value in reader.values():
            values.append(value["n"])
            if len(values) == want:
                break
        return values


async def test_a_workflow_reader_starts_at_end_or_the_newest_records(
    client: Client, provider: WorkflowStreamsProvider
):
    # The log is workflow state, so both starts resolve against it on the
    # workflow thread; a cold cache replays every task and has to land the
    # reader on the same offset each time.
    async with new_worker(
        client, StartsWhenTold, plugins=[provider], max_cached_workflows=0
    ) as worker:
        for start, expected in (("last", [3, 4]), ("end", ["new"])):
            handle = await client.start_workflow(
                StartsWhenTold.run,
                id=f"ws-start-{uuid.uuid4().hex}",
                task_queue=worker.task_queue,
            )
            stream = provider.get_stream_handle(client, handle.id)
            producer = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
            await producer.append({"n": 1}, {"n": 2}, {"n": 3}, {"n": 4})
            await handle.signal(StartsWhenTold.begin, start)
            result = asyncio.ensure_future(handle.result())
            if start == "end":
                for _ in range(150):
                    try:
                        await producer.append({"n": "new"})
                    except StreamNotFoundError:
                        # The Update returns once the workflow took the
                        # batch, and reading it is what closes the workflow,
                        # so the next append can find it gone before the
                        # result has been noticed.
                        break
                    done, _ = await asyncio.wait({result}, timeout=0.2)
                    if done:
                        break
            assert await asyncio.wait_for(result, 30) == expected


async def test_a_standalone_activity_stream_is_refused(
    client: Client, provider: WorkflowStreamsProvider
):
    # The log lives inside a running workflow, so an activity outside any
    # workflow has no place to put a stream of its own here. The refusal is
    # the documented error, not an AttributeError or a stream silently put
    # somewhere else. An activity a workflow scheduled is served.
    with pytest.raises(StreamUnsupportedError, match="standalone"):
        provider.get_activity_stream_handle(client, "act")
    assert isinstance(
        provider.get_activity_stream_handle(client, "act", workflow_id="wf"),
        WorkflowStreamsActivityHandle,
    )


async def test_a_standalone_stream_is_refused(
    client: Client, provider: WorkflowStreamsProvider
):
    # Every log is a running workflow's state; a stream with no owner has no
    # workflow to live in. Both the create and the lookup say so.
    with pytest.raises(StreamUnsupportedError, match="standalone"):
        await provider.create_standalone_stream(client, "shared")
    with pytest.raises(StreamUnsupportedError, match="standalone"):
        provider.get_standalone_stream_handle(client, "shared")


async def test_a_handle_names_its_stream_as_a_ref(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_stream = provider.get_stream_handle(client, "wf", run_id="run-1")
    assert workflow_stream.ref(topic=INPUTS) == StreamRef.for_workflow(
        "wf", run_id="run-1", topic=INPUTS
    )
    own = provider.get_activity_stream_handle(client, "act", workflow_id="wf")
    assert own.ref(topic=TOKENS) == StreamRef.for_activity(
        "act", workflow_id="wf", topic=TOKENS
    )
    assert own.ref().topic == DEFAULT_TOPIC
    # An owned stream ends with its owner; only a standalone one closes.
    with pytest.raises(ValueError, match="standalone"):
        await own.close()


def test_a_record_carries_the_plaintext_hash_the_workflow_dedupes_by():
    handle = _UpdateHandle()
    producer = _producer(handle, "update")
    entries, _ = producer._entries(
        [(RecordKind.DATA, {"n": 1}), (RecordKind.FINISH, None)]
    )  # pyright: ignore[reportPrivateUsage]
    data, finish = _wires(PublishInput(items=entries))
    # A DATA record is stamped with the hash of its converted body, where the
    # workflow can read it without the body; FINISH has nothing to hash.
    assert data.metadata[CONTENT_HASH_KEY].data.decode() == content_hash(data.body)
    assert CONTENT_HASH_KEY not in finish.metadata

    # The workflow's identity for a batch is those hashes, so a body whose
    # bytes a codec changed is still the same batch, and a different value
    # is not.
    def batch(*values: dict) -> PublishInput:
        made, _ = _producer(handle, "update")._entries(  # pyright: ignore[reportPrivateUsage]
            [(RecordKind.DATA, value) for value in values]
        )
        return PublishInput(items=made, publisher_id="model#1", sequence=3)

    same, recoded, other = batch({"n": 1}), batch({"n": 1}), batch({"n": 2})
    record = _wires(recoded)[0]
    record.body.data = b"\x00" + record.body.data
    recoded.items[0].data = base64.b64encode(
        Payload(
            metadata={"encoding": b"binary/plain"}, data=record.SerializeToString()
        ).SerializeToString()
    ).decode("ascii")
    assert workflow_streams._content(same) == workflow_streams._content(recoded)  # pyright: ignore[reportPrivateUsage]
    assert workflow_streams._content(same) != workflow_streams._content(other)  # pyright: ignore[reportPrivateUsage]


async def test_external_storage_applies_at_the_envelope_the_workflow_reads_through(
    client: Client, provider: WorkflowStreamsProvider
):
    # Worker and clients share one converter, as a deployment's do. A batch
    # above the threshold leaves as a claim on the Update argument, the
    # worker redeems it, the workflow reads the value, and the outside
    # reader gets the poll response the same way.
    from tests.streams.test_streams_conformance import RecordingDriver

    driver = RecordingDriver()
    converter = dataclasses.replace(
        DataConverter.default,
        external_storage=ExternalStorage(drivers=[driver], payload_size_threshold=512),
    )
    config = client.config()
    config["data_converter"] = converter
    shared = Client(**config)
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(shared, EchoLoop, plugins=[provider]) as worker:
        handle = await shared.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(shared, workflow_id)
        producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await producer.append({"n": 1})
        assert driver.stored == 0
        await producer.append({"n": 2, "blob": "x" * 4096})
        assert driver.stored == 1
        await producer.finish()

        records = await take(stream.read(topic=DECISIONS, result_type=dict), 3)
        assert [r.value.get("echo") for r in records[:2]] == [1, 2]
        assert records[2].kind is RecordKind.FINISH
        assert driver.retrieved >= 1

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 2


TOKENS = "tokens"


@activity.defn
async def stream_then_finish(count: int) -> None:
    producer = activity.stream_handle(scope="activity").producer(topic=TOKENS)
    for n in range(count):
        await producer.append({"n": n})
    # Long enough for a reader polling every few milliseconds to see this
    # activity pending before it finishes.
    await asyncio.sleep(1)
    await producer.finish()


@workflow.defn
class RunsAnActivityThenLingers:
    """Runs the streaming activity by a fixed id, then waits to be released."""

    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self, count: int) -> None:
        await workflow.execute_activity(
            stream_then_finish,
            count,
            activity_id="streamer",
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.wait_condition(lambda: self._released)


async def test_an_activity_read_ends_with_the_activity_while_the_workflow_runs(
    client: Client, provider: WorkflowStreamsProvider
):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(
        client,
        RunsAnActivityThenLingers,
        activities=[stream_then_finish],
        plugins=[provider],
    ) as worker:
        handle = await client.start_workflow(
            RunsAnActivityThenLingers.run,
            2,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        own = provider.get_activity_stream_handle(
            client, "streamer", workflow_id=workflow_id
        )

        async def read_everything() -> list[Any]:
            return [r async for r in own.read(topic=TOKENS, result_type=dict)]

        records = await asyncio.wait_for(read_everything(), 30)
        assert [(r.kind, r.value) for r in records] == [
            (RecordKind.DATA, {"n": 0}),
            (RecordKind.DATA, {"n": 1}),
            (RecordKind.FINISH, None),
        ]
        # The producer is the activity, and the record carries the plain
        # topic name; the reserved name is the log's business.
        assert all(r.producer_id == "streamer" and r.attempt == 1 for r in records)
        assert all(r.topic == TOKENS for r in records)
        # The activity's end ended the read: the workflow is still running.
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING

        # The workflow's topic of the same name is another stream, and the
        # reserved name cannot be reached as a workflow topic.
        workflow_stream = provider.get_stream_handle(client, workflow_id)
        assert await workflow_stream.latest(topic=TOKENS) == BEGINNING
        with pytest.raises(ValueError, match="reserved"):
            workflow_stream.read(topic="activity/streamer/tokens")
        with pytest.raises(ValueError, match="reserved"):
            workflow_stream.producer(topic="activity/x", producer_id="p", attempt=1)

        await handle.signal(RunsAnActivityThenLingers.release)
        await handle.result()
