"""Live conformance for the Nexus front.

The caller talks only to the stream endpoint over the server's Nexus HTTP
ingress; the handler worker delegates to the workflow_streams provider, so
this test is the provider-hiding demonstration: nothing on the caller side
names or could name the store. Gated behind ``STREAMS_LIVE=nexus`` because it
needs a dev server with an HTTP port and a registered Nexus endpoint.

Environment: ``TEMPORAL_ADDRESS`` (default ``localhost:7233``),
``TEMPORAL_HTTP`` (default ``http://127.0.0.1:7243``), and an endpoint named
``streams-e2e`` targeting task queue ``streams-handlers-e2e``.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid

import pytest

from temporalio import streams
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.streams._provider import instance
from temporalio.streams.providers.nexus import TemporalStreamsHandler
from temporalio.worker import Worker

from tests.streams.test_workflow_streams_provider import EchoLoop, take

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "nexus",
    reason="needs a live server and nexus endpoint; run with STREAMS_LIVE=nexus",
)

ENDPOINT = "streams-e2e"
HANDLER_TQ = "streams-handlers-e2e"


def _endpoint_id() -> str:
    # The HTTP ingress dispatches by endpoint id, not name.
    out = subprocess.run(
        [
            "temporal", "operator", "nexus", "endpoint", "get",
            "--name", ENDPOINT, "-o", "json",
            "--address", os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)["id"]


async def test_interface_loop_through_the_nexus_front():
    # The workflow worker and the handler worker both use the storage
    # provider; only the caller goes through the front.
    streams.configure(provider="workflow_streams")
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    )
    front = instance(
        "nexus",
        endpoint=_endpoint_id(),
        http_address=os.environ.get("TEMPORAL_HTTP", "http://127.0.0.1:7243"),
    )
    workflow_id = f"streams-nexus-live-{uuid.uuid4().hex}"

    async with Worker(
        client,
        task_queue=HANDLER_TQ,
        nexus_service_handlers=[
            TemporalStreamsHandler(client, provider="workflow_streams")
        ],
    ):
        async with Worker(
            client,
            task_queue=f"tq-{workflow_id}",
            workflows=[EchoLoop],
            **streams.worker_options(),
        ):
            handle = await client.start_workflow(
                EchoLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
            )

            producer = await front.producer(
                None,
                workflow_id=workflow_id,
                stream="inputs",
                producer_id="model",
                attempt=1,
            )
            await producer.append({"n": 1}, {"n": 2})
            await producer.append({"n": 3})
            await producer.finish()

            consumer = await front.consumer(None, workflow_id=workflow_id)
            records = await take(consumer.read(type=dict), 4, timeout=60)
            assert [r.kind for r in records] == [
                RecordKind.DATA,
                RecordKind.DATA,
                RecordKind.DATA,
                RecordKind.FINISH,
            ]
            assert [r.value["echo"] for r in records[:3]] == [1, 2, 3]

            # An opaque cursor from behind the front resumes a fresh reader
            # just past the record it names.
            checkpoint = records[0].cursor
            resumed = await front.consumer(None, workflow_id=workflow_id)
            again = await take(
                resumed.read(type=dict, after=checkpoint), 2, timeout=60
            )
            assert [r.value["echo"] for r in again[:2]] == [2, 3]

            await handle.signal(EchoLoop.release)
            assert await handle.result() == 3
