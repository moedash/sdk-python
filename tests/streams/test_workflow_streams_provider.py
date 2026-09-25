"""Conformance for the workflow_streams provider on the test environment's server.

Runs the interface loop over the shipped Option 0 transport: an outside
producer appends through the publish Signal, the workflow reads and
republishes through its own state, and an outside reader follows the poll
Update while the run is open and the tail Query once it has closed. The
outside-surface cases shared by every provider run from
``test_streams_conformance``; this file covers what the transport adds.
"""

from __future__ import annotations

import asyncio
import base64
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowQueryFailedError,
)
from temporalio.contrib.workflow_streams import PublishInput, WorkflowStream
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import (
    BEGINNING,
    RecordKind,
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    Supersession,
)
from temporalio.streams._wire import WireRecord
from temporalio.streams.providers.workflow_streams import (
    WorkflowStreamsHandle,
    WorkflowStreamsProducer,
    WorkflowStreamsProvider,
    _InstanceStream,
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
        # Positions are learnt at read time on this transport.
        assert await first.append({"n": 1}) is None
        # The retry of the same attempt re-sends its first batch.
        retry = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await retry.append({"n": 1})
        second = stream.producer(topic=INPUTS, producer_id="model", attempt=2)
        await second.append({"n": 2})

        records = await take(stream.read(topic=INPUTS, result_type=dict), 3)
        assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
        assert records[1].kind is RecordKind.SUPERSEDED
        assert records[1].supersession == Supersession("model", 1, 2)
        assert records[2].kind is RecordKind.DATA and records[2].attempt == 2
        assert all(r.topic == INPUTS for r in records)

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
    instance.stream = _Log()  # type: ignore[assignment]

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


def _producer(handle: _FlakyHandle) -> WorkflowStreamsProducer:
    return WorkflowStreamsProducer(
        handle, DataConverter.default.payload_converter, INPUTS, "model", 1
    )


async def test_a_retried_append_after_an_ambiguous_failure_writes_once():
    handle = _FlakyHandle()
    producer = _producer(handle)
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    await producer.append({"n": 1})
    await producer.append({"n": 2})
    # The retry carries the same signal sequence and the same record
    # sequence as the failed send, so the shipped dedupe drops the copy; the
    # batch after it continues the numbering.
    assert _sequences(handle.sent) == [(1, [0]), (1, [0]), (2, [1])]
    assert all(publish.publisher_id == "model#1" for publish in handle.sent)


async def test_a_batch_whose_signal_failed_goes_out_before_the_next_one():
    handle = _FlakyHandle()
    producer = _producer(handle)
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    await producer.append({"n": 2}, {"n": 3})
    await producer.finish()
    assert _sequences(handle.sent) == [(1, [0]), (1, [0]), (3, [1, 2]), (4, [3])]
    assert _wires(handle.sent[-1])[0].kind == int(RecordKind.FINISH)


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
            for event in ():
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

    # The original ended at record 1, so its sequence is 2. Neither half of
    # the retry's re-split reaches past it, and only the new record does.
    assert _sequences(first.sent) == [(2, [0, 1])]
    assert _sequences(second.sent) == [(1, [0]), (2, [1]), (3, [2])]
