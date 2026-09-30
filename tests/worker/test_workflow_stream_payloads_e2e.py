"""How a body's encoding meets the server: retry identity and external storage.

A payload codec and an ``ExternalStorage`` driver both change the bytes a body
is stored as. The server must still recognize a retry, and a driver must be
applied on both halves of the native provider. Needs a Temporal server built
from the AI-198 branch:

    TEMPORAL_STREAM_TARGET=127.0.0.1:7333 uv run pytest tests/worker/test_workflow_stream_payloads_e2e.py
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Sequence
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.sdk.v1.external_storage_pb2 import ExternalStorageReference
from temporalio.client import Client
from temporalio.client_stream import StreamClient
from temporalio.converter import (
    DataConverter,
    ExternalStorage,
    JSONProtoPayloadConverter,
    PayloadCodec,
)
from temporalio.streams import RecordKind
from temporalio.streams.providers.native import CONTENT_HASH_KEY, NativeStreams
from temporalio.worker import Worker
from tests.streams.test_streams_conformance import take
from tests.test_extstore import InMemoryTestDriver

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream commands",
)

INPUTS = "inputs"
DECISIONS = "decisions"


class _NonceCodec(PayloadCodec):
    """Encodes to different bytes on every call, as a nonce-based cipher does.

    The plaintext is kept in the clear behind a counter so the test can read
    the stored bytes back and see that two encodings of one value differ.
    """

    def __init__(self) -> None:
        self.calls = 0

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.calls += 1
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=f"{self.calls}:".encode() + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for p in payloads:
            if p.metadata.get("encoding", b"") != b"binary/nonce":
                out.append(p)
                continue
            out.append(Payload.FromString(p.data.split(b":", 1)[1]))
        return out


@workflow.defn
class Echo:
    """Reads ``inputs`` and publishes each value on ``decisions``, until told to stop."""

    def __init__(self) -> None:
        self._stop = False

    @workflow.signal
    def stop(self) -> None:
        self._stop = True

    @workflow.run
    async def run(self, count: int) -> list[Any]:
        inputs = workflow.stream_reader(INPUTS, result_type=dict)
        decisions = workflow.stream_writer(DECISIONS)
        seen: list[Any] = []
        async for value in inputs.values():
            decisions.publish({"echo": value})
            seen.append(value)
            if len(seen) >= count:
                break
        decisions.finish()
        await workflow.wait_condition(lambda: self._stop)
        return seen


async def _connect(converter: DataConverter, provider: NativeStreams) -> Client:
    plain = await Client.connect(TARGET or "")
    config = plain.config()
    config["data_converter"] = converter
    config["plugins"] = [provider]
    return Client(**config)


async def test_a_retried_append_under_a_nonce_codec_is_deduplicated() -> None:
    """Retry identity is the plaintext hash, not the encoded bytes.

    Two producers with the same identity append the same value at the same
    sequence, as a retry after a lost response would. The codec encodes each
    differently, so the server sees two different bodies and one hash, and
    writes the record once.
    """
    provider = NativeStreams()
    client = await _connect(DataConverter(payload_codec=_NonceCodec()), provider)
    raw = StreamClient.connect(TARGET or "")
    task_queue = "nonce-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "nonce-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Echo]):
            handle = await client.start_workflow(
                Echo.run, 1, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            first = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
            retry = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
            landed = await first.append({"n": 1})
            again = await retry.append({"n": 1})
            assert again == landed, "the repeat answers with the original position"

            echoed = await take(stream.read(topic=DECISIONS, result_type=dict), 1)
            assert [r.value for r in echoed] == [{"echo": {"n": 1}}]
            await handle.signal(Echo.stop)
            assert await asyncio.wait_for(handle.result(), 60) == [{"n": 1}]

        run_id = (await handle.describe()).run_id
        assert run_id is not None
        page = await raw.workflow_stream(workflow_id, INPUTS, owner_run_id=run_id).poll(
            from_offset=0, wait=False
        )
        bodies = [e.record for e in page.entries if e.record.HasField("body")]
        assert len(bodies) == 1, "written once"
        assert bodies[0].body.metadata["encoding"] == b"binary/nonce"
        assert bodies[0].metadata[CONTENT_HASH_KEY].data
    finally:
        await raw.close()
        await provider.close()


async def test_a_workflow_publish_carries_the_plaintext_hash() -> None:
    """The workflow's own records are stamped too, from the workflow thread."""
    provider = NativeStreams()
    client = await _connect(DataConverter.default, provider)
    raw = StreamClient.connect(TARGET or "")
    task_queue = "stamp-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "stamp-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Echo]):
            handle = await client.start_workflow(
                Echo.run, 1, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            await stream.producer(topic=INPUTS, producer_id="tool", attempt=1).append(
                {"n": 7}
            )
            await take(stream.read(topic=DECISIONS, result_type=dict), 1)
            await handle.signal(Echo.stop)
            await asyncio.wait_for(handle.result(), 60)

        run_id = (await handle.describe()).run_id
        assert run_id is not None
        page = await raw.workflow_stream(
            workflow_id, DECISIONS, owner_run_id=run_id
        ).poll(from_offset=0, wait=False)
        kinds = {e.record.kind for e in page.entries}
        assert len(page.entries) == 2, "one decision and the finish"
        data = [e.record for e in page.entries if e.record.HasField("body")]
        assert len(data) == 1 and data[0].metadata[CONTENT_HASH_KEY].data
        finish = [e.record for e in page.entries if not e.record.HasField("body")]
        assert CONTENT_HASH_KEY not in finish[0].metadata
        assert RecordKind.FINISH in {RecordKind(k) for k in kinds}
    finally:
        await raw.close()
        await provider.close()


async def test_external_storage_applies_on_both_halves() -> None:
    """A ``StorageDriver`` on the client offloads stream bodies on both paths.

    Every body is over the threshold. The outside producer's append is stored
    through the driver before it reaches the server; the workflow retrieves it
    on its task, publishes, and the worker's payload pass offloads that too, off
    the workflow thread, so the outside reader retrieves it. Read raw, the
    server holds references on both topics and no plaintext.
    """
    driver = InMemoryTestDriver()
    converter = DataConverter(
        external_storage=ExternalStorage(drivers=[driver], payload_size_threshold=0)
    )
    provider = NativeStreams()
    client = await _connect(converter, provider)
    raw = StreamClient.connect(TARGET or "")
    task_queue = "offload-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "offload-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Echo]):
            handle = await client.start_workflow(
                Echo.run, 1, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            producer = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
            await producer.append({"n": "plaintext-marker"})
            stores_after_append = driver._store_calls
            assert stores_after_append >= 1, "the outside append offloaded"

            echoed = await take(stream.read(topic=DECISIONS, result_type=dict), 1)
            assert [r.value for r in echoed] == [{"echo": {"n": "plaintext-marker"}}]
            await handle.signal(Echo.stop)
            assert await asyncio.wait_for(handle.result(), 60) == [
                {"n": "plaintext-marker"}
            ]

        run_id = (await handle.describe()).run_id
        assert run_id is not None
        for topic in (INPUTS, DECISIONS):
            page = await raw.workflow_stream(
                workflow_id, topic, owner_run_id=run_id
            ).poll(from_offset=0, wait=False)
            bodies = [e.record.body for e in page.entries if e.record.HasField("body")]
            assert len(bodies) == 1, topic
            assert b"plaintext-marker" not in bodies[0].data, topic
            reference = JSONProtoPayloadConverter().from_payload(
                bodies[0], ExternalStorageReference
            )
            assert reference.driver_name == driver.name(), topic
        # The workflow's publish was offloaded by the worker, after the append.
        assert driver._store_calls > stores_after_append
        # Both halves retrieved: the worker on its task and the outside reader.
        assert driver._retrieve_calls >= 2
        # The outside append was stored under the workflow that owns the stream.
        targets = [ctx.target for ctx in driver._store_contexts if ctx.target]
        assert any(t.id == workflow_id and t.run_id == run_id for t in targets)
    finally:
        await raw.close()
        await provider.close()
