"""Live conformance for the workflow_streams provider.

Runs the interface loop over the shipped Option 0 transport against a real
server: an outside producer appends through the publish Signal, the workflow
reads and republishes through its own state, and an outside consumer follows
the poll Update. Gated behind ``STREAMS_LIVE=workflow_streams`` because it
needs a running server (``TEMPORAL_ADDRESS``, default ``localhost:7233``).
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.streams.providers.workflow_streams import drain
from temporalio.worker import Worker

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "workflow_streams",
    reason="needs a live server; run with STREAMS_LIVE=workflow_streams",
)


@workflow.defn
class EchoLoop:
    """Reads ``inputs``, echoes each value onto ``decisions``, ends on FINISH.

    Lingers until released, because an Option 0 stream dies with its
    workflow: a reader that arrives after close finds nothing, which is the
    transport limit the doc states rather than a defect to fix here.
    """

    def __init__(self) -> None:
        self._released = False

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
            seen += 1
            await decisions.publish({"echo": record.value["n"]})
        await decisions.finish()
        await workflow.wait_condition(lambda: self._released)
        drain()
        await workflow.wait_condition(workflow.all_handlers_finished)
        return seen


async def take(records, count: int, timeout: float = 30.0) -> list:
    out: list = []

    async def _collect() -> None:
        async for record in records:
            out.append(record)
            if len(out) >= count:
                return

    await asyncio.wait_for(_collect(), timeout)
    return out


async def test_interface_loop_over_workflow_streams():
    streams.configure(provider="workflow_streams")
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    )
    workflow_id = f"streams-ws-live-{uuid.uuid4().hex}"

    async with Worker(
        client,
        task_queue=f"tq-{workflow_id}",
        workflows=[EchoLoop],
        **streams.worker_options(),
    ):
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )

        producer = await streams.producer(
            client,
            workflow_id=workflow_id,
            stream="inputs",
            producer_id="model",
            attempt=1,
        )
        await producer.append({"n": 1}, {"n": 2})
        await producer.append({"n": 3})
        await producer.finish()

        consumer = await streams.consumer(client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict), 4)
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3


async def test_retried_producer_dedupes_and_new_attempt_supersedes():
    streams.configure(provider="workflow_streams")
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    )
    workflow_id = f"streams-ws-live-{uuid.uuid4().hex}"

    async with Worker(
        client,
        task_queue=f"tq-{workflow_id}",
        workflows=[EchoLoop],
        **streams.worker_options(),
    ):
        await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )

        first = await streams.producer(
            client, workflow_id=workflow_id, stream="inputs",
            producer_id="model", attempt=1,
        )
        await first.append({"n": 1})
        # The retry of the same attempt re-sends its first batch.
        retry = await streams.producer(
            client, workflow_id=workflow_id, stream="inputs",
            producer_id="model", attempt=1,
        )
        await retry.append({"n": 1})
        second = await streams.producer(
            client, workflow_id=workflow_id, stream="inputs",
            producer_id="model", attempt=2,
        )
        await second.append({"n": 2})

        consumer = await streams.consumer(
            client, workflow_id=workflow_id, stream="inputs"
        )
        records = await take(consumer.read(type=dict), 3)
        assert records[0].kind is RecordKind.DATA and records[0].attempt == 1
        assert records[1].kind is RecordKind.SUPERSEDED
        assert records[1].value.previous_attempt == 1
        assert records[2].kind is RecordKind.DATA and records[2].attempt == 2
