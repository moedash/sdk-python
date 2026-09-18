"""Conformance for the Nexus front.

The caller talks only to the stream endpoint; the handler delegates to a
storage provider, so these tests are the provider-hiding demonstration:
nothing on the caller side names or could name the store.

The loop test runs over the server's Nexus HTTP ingress and is gated behind
``STREAMS_LIVE=nexus`` because it needs a dev server with an HTTP port and a
registered Nexus endpoint. Environment: ``TEMPORAL_ADDRESS`` (default
``localhost:7233``), ``TEMPORAL_HTTP`` (default ``http://127.0.0.1:7243``),
and an endpoint named ``streams-e2e`` targeting task queue
``streams-handlers-e2e``.

The codec test stands the endpoint up in this process instead, because what
it checks is the bytes the caller puts on the wire.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import json
import os
import subprocess
import uuid
from collections.abc import Sequence
from typing import Any, cast

import pytest

import temporalio.converter
from temporalio import streams
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.streams import _frame as frame
from temporalio.streams._provider import instance
from temporalio.streams.providers import memory, nexus
from temporalio.streams.providers._nexus_generated import AppendInput, ReadInput
from temporalio.streams.providers.nexus import TemporalStreamsHandler
from temporalio.worker import Worker
from tests.streams.test_workflow_streams_provider import EchoLoop, take

live_only = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "nexus",
    reason="needs a live server and nexus endpoint; run with STREAMS_LIVE=nexus",
)

ENDPOINT = "streams-e2e"
HANDLER_TQ = "streams-handlers-e2e"
APPEND_OPERATION = nexus._APPEND_OPERATION  # pyright: ignore[reportPrivateUsage]


def _endpoint_id() -> str:
    # The HTTP ingress dispatches by endpoint id, not name.
    out = subprocess.run(
        [
            "temporal",
            "operator",
            "nexus",
            "endpoint",
            "get",
            "--name",
            ENDPOINT,
            "-o",
            "json",
            "--address",
            os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)["id"]


@live_only
async def test_interface_loop_through_the_nexus_front():
    # The workflow worker and the handler worker both use the storage
    # provider; only the caller goes through the front.
    streams.configure(provider="workflow_streams")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
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
            again = await take(resumed.read(type=dict, after=checkpoint), 2, timeout=60)
            assert [r.value["echo"] for r in again[:2]] == [2, 3]

            await handle.signal(EchoLoop.release)
            assert await handle.result() == 3


class ScrambleCodec(temporalio.converter.PayloadCodec):
    """Flips every byte of a payload, so the middle holds nothing readable."""

    KEY = 0x5A

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Wrap each payload as opaque bytes nothing downstream can read."""
        return [
            Payload(
                metadata={"encoding": b"binary/plain"},
                data=self._flip(payload.SerializeToString()),
            )
            for payload in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Recover the payloads :meth:`encode` wrapped."""
        out: list[Payload] = []
        for payload in payloads:
            inner = Payload()
            inner.ParseFromString(self._flip(payload.data))
            out.append(inner)
        return out

    @classmethod
    def _flip(cls, data: bytes) -> bytes:
        return bytes(byte ^ cls.KEY for byte in data)


async def _dispatch(
    handler: TemporalStreamsHandler, operation: str, request: Any
) -> Any:
    context = cast("Any", None)  # neither operation looks at it
    if operation == APPEND_OPERATION:
        return await handler.append(context, request)
    return await handler.read(context, request)


def _in_process_endpoint(
    handler: TemporalStreamsHandler,
    posted: list[bytes],
    answered: list[bytes],
) -> Any:
    """Serve the caller's posts from ``handler``, recording both directions."""
    contract = temporalio.converter.DataConverter.default.payload_converter
    loop = asyncio.get_running_loop()

    def post(url: str, body: bytes) -> bytes:
        posted.append(body)
        operation = url.rsplit("/", 1)[1]
        request_type = AppendInput if operation == APPEND_OPERATION else ReadInput
        request: Any = contract.from_payloads(
            [Payload(metadata={"encoding": b"json/plain"}, data=body)], [request_type]
        )[0]
        if request_type is ReadInput:
            # This endpoint has no other traffic to wait for, so park briefly
            # rather than for the contract's default.
            request.wait_ms = 200
        answer = asyncio.run_coroutine_threadsafe(
            _dispatch(handler, operation, request), loop
        ).result()
        raw = contract.to_payloads([answer])[0].data
        answered.append(raw)
        return raw

    return post


async def _loop_through_an_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    codec: temporalio.converter.PayloadCodec | None,
) -> tuple[list[Any], list[bytes], list[bytes]]:
    memory.reset()
    streams.configure(provider="memory")
    posted: list[bytes] = []
    answered: list[bytes] = []
    handler = TemporalStreamsHandler(None, provider="memory")
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, posted, answered))
    front = instance(
        "nexus",
        endpoint="in-process",
        data_converter=dataclasses.replace(
            temporalio.converter.DataConverter.default, payload_codec=codec
        ),
    )
    workflow_id = "wf-codec"

    producer = await front.producer(
        None,
        workflow_id=workflow_id,
        stream="inputs",
        producer_id="model",
        attempt=1,
    )
    await producer.append({"secret": "tuna"})
    await producer.finish()

    consumer = await front.consumer(None, workflow_id=workflow_id, stream="inputs")
    records = await take(consumer.read(type=dict), 2, timeout=30)
    memory.reset()
    return records, posted, answered


def _appended_bytes(posted: list[bytes]) -> bytes:
    out = b""
    for body in posted:
        for encoded in json.loads(body).get("payloads", []):
            out += base64.b64decode(encoded)
    return out


def _answered_bodies(answered: list[bytes]) -> bytes:
    out = b""
    for body in answered:
        for record in json.loads(body).get("records", []):
            out += frame.decode(base64.b64decode(record["frame"]))[5]
    return out


async def test_the_caller_encodes_records_with_the_configured_codec(
    monkeypatch: pytest.MonkeyPatch,
):
    records, posted, answered = await _loop_through_an_endpoint(
        monkeypatch, ScrambleCodec()
    )

    # The value survives the hop, so the codec is applied on both sides.
    assert [record.kind for record in records] == [RecordKind.DATA, RecordKind.FINISH]
    assert records[0].value == {"secret": "tuna"}
    # Neither direction carried the plaintext.
    assert b"tuna" not in _appended_bytes(posted)
    assert b"tuna" not in _answered_bodies(answered)


async def test_without_a_codec_the_same_records_go_out_in_the_clear(
    monkeypatch: pytest.MonkeyPatch,
):
    # The contrast is the point: the bytes above differ because of the codec,
    # not because the transport obscures them anyway.
    records, posted, answered = await _loop_through_an_endpoint(monkeypatch, None)

    assert records[0].value == {"secret": "tuna"}
    assert b"tuna" in _appended_bytes(posted)
    assert b"tuna" in _answered_bodies(answered)
