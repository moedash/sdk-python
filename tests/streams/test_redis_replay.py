"""A query against a completed run replays it on the client-side provider.

Gated behind ``STREAMS_LIVE=redis``; needs a dev server (``TEMPORAL_ADDRESS``)
and a Redis (``AI198_REDIS_URL``).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.worker import Worker

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "redis",
    reason="needs a live server and redis; run with STREAMS_LIVE=redis",
)


async def take(records: Any, count: int, timeout: float = 5.0) -> list:
    out: list = []

    async def _collect() -> None:
        async for record in records:
            out.append(record)
            if len(out) >= count:
                return

    await asyncio.wait_for(_collect(), timeout)
    return out


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
    # The final task's publishes were woken by a signal, which the activation
    # applies before the replay marker, so the marker's expectations have to be
    # installed before that task's code runs or the replay records nothing
    # against a manifest of three.
    streams.configure(
        provider="redis",
        url=os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6399"),
        key_prefix=f"streams-redis-replay-{uuid.uuid4().hex}",
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
