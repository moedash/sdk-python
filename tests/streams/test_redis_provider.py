"""Live conformance for the client-side (Redis) provider.

Runs the same interface loop the other providers run, over the external-store
binding against a real Redis. The workflow publishes through the staged
commit, so it needs a warm workflow cache (the provider holds the task open
between records). Gated behind ``STREAMS_LIVE=redis``; needs a dev server
(``TEMPORAL_ADDRESS``) and a Redis (``AI198_REDIS_URL``).
"""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.worker import Worker

from tests.streams.test_workflow_streams_provider import EchoLoop, take

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "redis",
    reason="needs a live server and redis; run with STREAMS_LIVE=redis",
)


async def test_interface_loop_over_redis():
    streams.configure(
        provider="redis",
        url=os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6399"),
        key_prefix=f"streams-redis-live-{uuid.uuid4().hex}",
    )
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    )
    workflow_id = f"streams-redis-live-{uuid.uuid4().hex}"

    async with Worker(
        client,
        task_queue=f"tq-{workflow_id}",
        workflows=[EchoLoop],
        max_cached_workflows=100,
        **streams.worker_options(),
    ):
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )

        producer = await streams.producer(
            client, workflow_id=workflow_id, stream="inputs",
            producer_id="model", attempt=1,
        )
        await producer.append({"n": 1}, {"n": 2})
        await producer.append({"n": 3})
        await producer.finish()

        consumer = await streams.consumer(client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict, topic="decisions"), 4, timeout=60)
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3


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


async def test_query_after_completion_replays_the_final_task():
    # A query against a completed run replays it. The final task's publishes
    # were woken by a signal, which the activation applies before the replay
    # marker, so the marker's expectations have to be installed before that
    # task's code runs or the replay records nothing against a manifest of
    # three.
    streams.configure(
        provider="redis",
        url=os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6399"),
        key_prefix=f"streams-redis-live-{uuid.uuid4().hex}",
    )
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    )
    workflow_id = f"streams-redis-replay-{uuid.uuid4().hex}"

    async with Worker(
        client,
        task_queue=f"tq-{workflow_id}",
        workflows=[SignalWokenPublish],
        **streams.worker_options(),
    ):
        handle = await client.start_workflow(
            SignalWokenPublish.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
        )
        consumer = await streams.consumer(client, workflow_id=workflow_id)
        await take(consumer.read(type=dict, topic="out"), 1, timeout=60)
        await handle.signal(SignalWokenPublish.close)
        assert await handle.result() == 4
        assert await handle.query(SignalWokenPublish.probe) == 1
