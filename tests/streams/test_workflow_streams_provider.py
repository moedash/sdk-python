"""Conformance for the workflow_streams provider on the test environment's server.

Runs the interface loop over the shipped Option 0 transport: an outside
producer appends through the publish Signal, the workflow reads and
republishes through its own state, and an outside consumer follows the poll
Update while the run is open and the tail Query once it has closed.
"""

from __future__ import annotations

import asyncio
import base64
import uuid
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.contrib.workflow_streams import PublishInput
from temporalio.converter import DataConverter
from temporalio.streams import RecordKind, _frame
from temporalio.streams.providers.workflow_streams import WorkflowStreamsProducer
from tests.helpers import new_worker


@pytest.fixture(autouse=True)
def _workflow_streams_provider():  # pyright: ignore[reportUnusedFunction]
    streams.configure(provider="workflow_streams")


@workflow.defn
class EchoLoop:
    """Reads ``inputs``, echoes each value onto ``decisions``, ends on FINISH.

    Lingers until released so a reader can follow it while it runs; whoever
    arrives after it closed is served by the tail Query instead.
    """

    def __init__(self) -> None:
        self._released = False
        streams.prepare()

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> int:
        inputs = streams.reader("inputs", type=dict)
        decisions = streams.writer("decisions")
        seen = 0
        async for record in inputs:
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            assert isinstance(record.value, dict)
            seen += 1
            await decisions.publish({"echo": record.value["n"]})
        await decisions.finish()
        await workflow.wait_condition(lambda: self._released)
        streams.drain()
        await workflow.wait_condition(workflow.all_handlers_finished)
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


async def _feed(client: Client, workflow_id: str) -> None:
    producer = await streams.producer(
        client, workflow_id=workflow_id, stream="inputs", producer_id="model", attempt=1
    )
    await producer.append({"n": 1}, {"n": 2})
    await producer.append({"n": 3})
    await producer.finish()


ECHOED = [RecordKind.DATA, RecordKind.DATA, RecordKind.DATA, RecordKind.FINISH]


async def test_interface_loop_over_workflow_streams(client: Client):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await _feed(client, workflow_id)

        consumer = await streams.consumer(client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict), 4)
        assert [r.kind for r in records] == ECHOED
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3


async def test_retried_producer_dedupes_and_new_attempt_supersedes(client: Client):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )

        first = await streams.producer(
            client,
            workflow_id=workflow_id,
            stream="inputs",
            producer_id="model",
            attempt=1,
        )
        await first.append({"n": 1})
        # The retry of the same attempt re-sends its first batch.
        retry = await streams.producer(
            client,
            workflow_id=workflow_id,
            stream="inputs",
            producer_id="model",
            attempt=1,
        )
        assert await retry.append({"n": 1}) is None
        second = await streams.producer(
            client,
            workflow_id=workflow_id,
            stream="inputs",
            producer_id="model",
            attempt=2,
        )
        await second.append({"n": 2})

        consumer = await streams.consumer(
            client, workflow_id=workflow_id, stream="inputs"
        )
        records = await take(consumer.read(type=dict), 3)
        assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
        assert records[1].kind is RecordKind.SUPERSEDED
        assert isinstance(records[1].value, streams.Supersession)
        assert records[1].value.previous_attempt == 1
        assert records[2].kind is RecordKind.DATA and records[2].attempt == 2
        # Inbound records carry no topic; the stream's name is the address.
        assert all(r.topic == "" for r in records)

        await second.finish()
        await handle.signal(EchoLoop.release)
        await handle.result()


async def test_cold_cache_serves_each_record_once(client: Client):
    # Every task rebuilds the workflow from history, so the stream object
    # has to belong to the instance that is running: a stale one would carry
    # the previous instance's log and hand out every record twice.
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop, max_cached_workflows=0) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await _feed(client, workflow_id)

        consumer = await streams.consumer(client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict), 4)
        assert [r.kind for r in records] == ECHOED
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]
        inbound = await streams.consumer(
            client, workflow_id=workflow_id, stream="inputs"
        )
        arrived = await take(inbound.read(type=dict), 4)
        assert [r.kind for r in arrived] == ECHOED
        assert [r.value["n"] for r in arrived[:3]] == [1, 2, 3]

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3


async def test_a_closed_run_serves_its_tail_by_query(client: Client):
    workflow_id = f"streams-ws-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await _feed(client, workflow_id)
        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3

        # Nothing polled while the run was open. The poll Update is gone with
        # the run, so everything below arrives through the tail Query.
        consumer = await streams.consumer(client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict), 4)
        assert [r.kind for r in records] == ECHOED
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

        checkpoint = records[1].cursor
        resumed = await streams.consumer(client, workflow_id=workflow_id)
        again = await take(resumed.read(type=dict, after=checkpoint), 2)
        assert [r.value["echo"] for r in again[:1]] == [3]
        assert again[1].kind is RecordKind.FINISH


class _FlakyHandle:
    """A workflow handle whose first Signal is accepted and then reported failed."""

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


def _frames(
    publish: PublishInput,
) -> list[tuple[RecordKind, str, str, int, int, bytes]]:
    out = []
    for entry in publish.items:
        payload = Payload()
        payload.ParseFromString(base64.b64decode(entry.data))
        out.append(_frame.decode(payload.data))
    return out


def _sequences(sent: list[PublishInput]) -> list[tuple[int, list[int]]]:
    return [
        (publish.sequence, [frame[4] for frame in _frames(publish)]) for publish in sent
    ]


def _producer(handle: _FlakyHandle) -> WorkflowStreamsProducer:
    return WorkflowStreamsProducer(
        handle, DataConverter.default.payload_converter, "inputs", "", "model", 1
    )


async def test_a_retried_append_after_an_ambiguous_failure_writes_once():
    handle = _FlakyHandle()
    producer = _producer(handle)
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    await producer.append({"n": 1})
    await producer.append({"n": 2})
    # The retry carries the same signal sequence and the same frame sequence
    # as the failed send, so the shipped dedupe drops the copy; the batch
    # after it continues the numbering.
    assert _sequences(handle.sent) == [(1, [0]), (1, [0]), (2, [1])]


async def test_a_batch_whose_signal_failed_goes_out_before_the_next_one():
    handle = _FlakyHandle()
    producer = _producer(handle)
    with pytest.raises(ConnectionResetError):
        await producer.append({"n": 1})
    await producer.append({"n": 2}, {"n": 3})
    await producer.finish()
    assert _sequences(handle.sent) == [(1, [0]), (1, [0]), (2, [1, 2]), (3, [3])]
    assert _frames(handle.sent[-1])[0][0] is RecordKind.FINISH
