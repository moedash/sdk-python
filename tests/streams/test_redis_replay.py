"""Live checks for the client-side (Redis) provider inside a workflow.

The conformance suite covers the outside surface when ``STREAMS_LIVE=redis``.
This module runs the interface loop inside a workflow over the staged commit,
then queries a completed run, which replays it. Both need a dev server
(``TEMPORAL_ADDRESS``) and a Redis (``TEMPORAL_TEST_REDIS_URL`` or
``AI198_REDIS_URL``). The worker keeps a warm cache because the provider holds
the task open between records.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.worker import Worker
from tests.streams.test_streams_conformance import take
from tests.streams.test_streams_workflow import DECISIONS, INPUTS, ContractLoop

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "redis",
    reason="needs a live server and redis; run with STREAMS_LIVE=redis",
)


@pytest.fixture
async def live_client() -> AsyncIterator[Client]:
    # A prefix per case, because the store keeps what earlier cases wrote.
    streams.configure(provider="redis", key_prefix=f"streams-redis-{uuid.uuid4().hex}")
    try:
        yield await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    finally:
        await streams.close()


async def test_interface_loop_over_redis(live_client: Client):
    workflow_id = f"streams-redis-live-{uuid.uuid4().hex}"
    async with Worker(
        live_client,
        task_queue=f"tq-{workflow_id}",
        workflows=[ContractLoop],
        max_cached_workflows=100,
        **streams.worker_options(),
    ):
        handle = await live_client.start_workflow(
            ContractLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        producer = await streams.producer(
            live_client,
            workflow_id=workflow_id,
            stream=INPUTS,
            producer_id="model",
            attempt=1,
        )
        await producer.append({"n": 1}, {"n": 2})
        await producer.append({"n": 3})
        await producer.finish()

        consumer = await streams.consumer(live_client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict, topic=DECISIONS), 4, timeout=60)
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        assert [r.value["decided"] for r in records[:3]] == [1, 2, 3]
        assert await handle.result() == [
            {"kind": "decision", "n": 1, "attempt": 1},
            {"kind": "decision", "n": 2, "attempt": 1},
            {"kind": "decision", "n": 3, "attempt": 1},
            {"kind": "finish", "producer": "model"},
        ]


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
        out = streams.writer("out")
        await out.publish({"n": 0})
        await workflow.wait_condition(lambda: self._closed)
        for n in range(1, 4):
            await out.publish({"n": n})
        return 4

    @workflow.query
    def probe(self) -> int:
        return 1


async def test_query_after_completion_replays_the_final_task(live_client: Client):
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
        **streams.worker_options(),
    ):
        handle = await live_client.start_workflow(
            SignalWokenPublish.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        consumer = await streams.consumer(live_client, workflow_id=workflow_id)
        await take(consumer.read(type=dict, topic="out"), 1, timeout=60)
        await handle.signal(SignalWokenPublish.close)
        assert await handle.result() == 4
        assert await handle.query(SignalWokenPublish.probe) == 1
