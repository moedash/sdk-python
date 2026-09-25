"""Live checks for the client-side (Redis) provider inside a workflow.

The conformance suite covers the outside surface when ``STREAMS_LIVE=redis``.
This module runs the interface loop inside a workflow over the staged commit,
lets a read end with the workflow, shares a topic between an outside producer
and the workflow, and queries a completed run, which replays it. All need a
dev server (``TEMPORAL_ADDRESS``) and a Redis (``TEMPORAL_TEST_REDIS_URL`` or
``AI198_REDIS_URL``). The worker keeps a warm cache because the transport
holds the task open between records.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.streams.providers.redis import RedisStreams
from temporalio.worker import Worker
from tests.streams.test_streams_conformance import take
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
            return [
                r.value async for r in stream.read(topic=DECISIONS)
            ]

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
