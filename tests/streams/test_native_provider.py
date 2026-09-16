"""Live conformance for the server-side (native) provider.

Runs the same interface loop the other providers run, against a Temporal
server that carries the stream service. Gated behind ``STREAMS_LIVE=native``;
``TEMPORAL_ADDRESS`` must point at a server built from ``moedash/temporal``
``moe/AI-198-server-side-streams``.
"""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio import streams
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.worker import Worker
from tests.streams.test_workflow_streams_provider import EchoLoop, take

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "native",
    reason="needs a stream-carrying server; run with STREAMS_LIVE=native",
)


async def test_interface_loop_over_native_streams():
    streams.configure(provider="native")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    workflow_id = f"streams-native-live-{uuid.uuid4().hex}"

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
        records = await take(consumer.read(type=dict), 4, timeout=60)
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3
