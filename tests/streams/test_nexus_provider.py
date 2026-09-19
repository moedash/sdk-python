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

Everything else stands the endpoint up in this process, because what it
checks is the bytes the caller puts on the wire and the handler's own rules.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

import nexusrpc
import nexusrpc.handler
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
from temporalio.streams.providers.nexus import (
    StreamEndpointError,
    TemporalStreamsHandler,
)
from temporalio.worker import Worker
from tests.streams.test_workflow_streams_provider import EchoLoop, take

live_only = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "nexus",
    reason="needs a live server and nexus endpoint; run with STREAMS_LIVE=nexus",
)

ENDPOINT = "streams-e2e"
HANDLER_TQ = "streams-handlers-e2e"
APPEND_OPERATION = nexus._APPEND_OPERATION  # pyright: ignore[reportPrivateUsage]
READ_OPERATION = nexus._READ_OPERATION  # pyright: ignore[reportPrivateUsage]


@live_only
async def test_interface_loop_through_the_nexus_front():
    # The workflow worker and the handler worker both use the storage
    # provider; only the caller goes through the front, and it names the
    # endpoint the way an operator does, by name.
    streams.configure(provider="workflow_streams")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    front = instance(
        "nexus",
        endpoint=ENDPOINT,
        client=client,
        http_address=os.environ.get("TEMPORAL_HTTP", "http://127.0.0.1:7243"),
        wait_ms=5000,
    )
    workflow_id = f"streams-nexus-live-{uuid.uuid4().hex}"
    handler = TemporalStreamsHandler(client, provider="workflow_streams")

    async with Worker(client, task_queue=HANDLER_TQ, nexus_service_handlers=[handler]):
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
        await handler.close()


class ScrambleCodec(temporalio.converter.PayloadCodec):
    """Flips every byte and labels the result with an encoding only it can read."""

    KEY = 0x5A

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Wrap each payload as opaque bytes nothing downstream can read."""
        return [
            Payload(
                metadata={"encoding": b"binary/scrambled"},
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


class _NeverCancelled(nexusrpc.handler.OperationTaskCancellation):
    def is_cancelled(self) -> bool:
        return False

    def cancellation_reason(self) -> str | None:
        return None

    def wait_until_cancelled_sync(self, timeout: float | None = None) -> bool:
        del timeout
        return False

    async def wait_until_cancelled(self) -> None:
        await asyncio.Event().wait()


def _context(
    operation: str, deadline: datetime | None = None
) -> nexusrpc.handler.StartOperationContext:
    return nexusrpc.handler.StartOperationContext(
        service=nexus._SERVICE_NAME,  # pyright: ignore[reportPrivateUsage]
        operation=operation,
        headers={},
        task_cancellation=_NeverCancelled(),
        request_deadline=deadline,
        request_id=uuid.uuid4().hex,
    )


async def _dispatch(
    handler: TemporalStreamsHandler, operation: str, request: Any
) -> Any:
    if operation == APPEND_OPERATION:
        return await handler.append(_context(operation), request)
    return await handler.read(_context(operation), request)


def _in_process_endpoint(
    handler: TemporalStreamsHandler,
    posted: list[bytes],
    answered: list[bytes],
    *,
    fail_after_applying: list[str] | None = None,
) -> Any:
    """Serve the caller's posts from ``handler``, recording both directions.

    ``fail_after_applying`` names operations whose first call is applied and
    then reported as a transport failure, the ambiguous case a retry has to
    survive.
    """
    contract = temporalio.converter.DataConverter.default.payload_converter
    loop = asyncio.get_running_loop()
    failing = set(fail_after_applying or [])

    def post(
        url: str, body: bytes, headers: Mapping[str, str], timeout_ms: int
    ) -> bytes:
        del headers, timeout_ms
        posted.append(body)
        operation = url.rsplit("/", 1)[1]
        request_type = AppendInput if operation == APPEND_OPERATION else ReadInput
        request: Any = contract.from_payloads(
            [Payload(metadata={"encoding": b"json/plain"}, data=body)], [request_type]
        )[0]
        try:
            answer = asyncio.run_coroutine_threadsafe(
                _dispatch(handler, operation, request), loop
            ).result()
        except nexusrpc.HandlerError as error:
            raise StreamEndpointError(str(error), status=400) from error
        raw = contract.to_payloads([answer])[0].data
        answered.append(raw)
        if operation in failing:
            failing.discard(operation)
            raise StreamEndpointError("connection reset after the handler answered")
        return raw

    return post


def _front(codec: temporalio.converter.PayloadCodec | None) -> Any:
    return instance(
        "nexus",
        endpoint="in-process",
        # This endpoint has no other traffic to wait for, so park briefly
        # rather than for the contract's default.
        wait_ms=200,
        data_converter=dataclasses.replace(
            temporalio.converter.DataConverter.default, payload_codec=codec
        ),
    )


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
    front = _front(codec)
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

    # The value survives the hop although the handler cannot read the
    # encoding, so the body passed through it untouched and the codec ran on
    # the caller alone.
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


def _append(
    workflow_id: str, batch_index: int, *values: Any, finish: bool = False
) -> AppendInput:
    converter = temporalio.converter.DataConverter.default.payload_converter
    return AppendInput(
        workflow_id=workflow_id,
        stream="inputs",
        producer_id="model",
        attempt=1,
        batch_index=batch_index,
        payloads=[
            converter.to_payloads([value])[0].SerializeToString() for value in values
        ],
        finish=finish,
    )


async def _stored(workflow_id: str) -> list[Any]:
    consumer = await streams.consumer(None, workflow_id=workflow_id, stream="inputs")
    out: list[Any] = []
    async for record in consumer.read(type=dict):
        if record.kind is RecordKind.DATA:
            out.append(record.value)
        if await consumer.latest() == record.cursor:
            break
    return out


@pytest.fixture
def _memory_store():  # pyright: ignore[reportUnusedFunction]
    memory.reset()
    streams.configure(provider="memory")
    yield
    memory.reset()


@pytest.mark.usefixtures("_memory_store")
async def test_a_handler_without_state_for_a_producer_refuses_to_continue_it():
    # A second handler instance stands in for a restarted or load-balanced
    # handler worker: it shares the store but not the dedupe state.
    first = TemporalStreamsHandler(None, provider="memory")
    second = TemporalStreamsHandler(None, provider="memory")
    await _dispatch(first, APPEND_OPERATION, _append("wf", 1, {"n": 1}))
    with pytest.raises(nexusrpc.HandlerError) as failed:
        await _dispatch(second, APPEND_OPERATION, _append("wf", 2, {"n": 2}))
    assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
    assert failed.value.retryable_override is False
    # The store holds the first batch and nothing was silently lost or doubled.
    assert await _stored("wf") == [{"n": 1}]


@pytest.mark.usefixtures("_memory_store")
async def test_the_handler_drops_repeats_and_rejects_gaps():
    handler = TemporalStreamsHandler(None, provider="memory")
    await _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1}))
    await _dispatch(handler, APPEND_OPERATION, _append("wf", 2, {"n": 2}))
    # An exact repeat is dropped with a success answer and no position.
    repeat = await _dispatch(handler, APPEND_OPERATION, _append("wf", 2, {"n": 2}))
    assert repeat.cursor is None
    with pytest.raises(nexusrpc.HandlerError) as failed:
        await _dispatch(handler, APPEND_OPERATION, _append("wf", 4, {"n": 4}))
    assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
    assert await _stored("wf") == [{"n": 1}, {"n": 2}]


@pytest.mark.usefixtures("_memory_store")
async def test_a_malformed_payload_is_the_callers_fault():
    handler = TemporalStreamsHandler(None, provider="memory")
    request = _append("wf", 1)
    request.payloads = [b"\xff\xfe not a payload"]
    with pytest.raises(nexusrpc.HandlerError) as failed:
        await _dispatch(handler, APPEND_OPERATION, request)
    assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST


@pytest.mark.usefixtures("_memory_store")
async def test_the_caller_retries_an_ambiguous_append_under_the_same_index(
    monkeypatch: pytest.MonkeyPatch,
):
    posted: list[bytes] = []
    handler = TemporalStreamsHandler(None, provider="memory")
    monkeypatch.setattr(
        nexus,
        "_post",
        _in_process_endpoint(
            handler, posted, [], fail_after_applying=[APPEND_OPERATION]
        ),
    )
    front = _front(None)
    producer = await front.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    with pytest.raises(StreamEndpointError):
        await producer.append({"n": 1})
    await producer.append({"n": 1})
    await producer.append({"n": 2})
    await producer.finish()
    indexes = [json.loads(body)["batch_index"] for body in posted]
    # The retry re-sent index 1, which the handler dropped as a repeat, so
    # the store holds each record once.
    assert indexes == [1, 1, 2, 3]
    assert await _stored("wf") == [{"n": 1}, {"n": 2}]


@pytest.mark.usefixtures("_memory_store")
async def test_a_failed_append_goes_out_before_the_next_one(
    monkeypatch: pytest.MonkeyPatch,
):
    posted: list[bytes] = []
    handler = TemporalStreamsHandler(None, provider="memory")
    monkeypatch.setattr(
        nexus,
        "_post",
        _in_process_endpoint(
            handler, posted, [], fail_after_applying=[APPEND_OPERATION]
        ),
    )
    front = _front(None)
    producer = await front.producer(
        None, workflow_id="wf", stream="inputs", producer_id="model", attempt=1
    )
    with pytest.raises(StreamEndpointError):
        await producer.append({"n": 1})
    # The caller moves on without retrying; the pending batch is replayed
    # first under its own index and the new one takes the next.
    await producer.append({"n": 2})
    indexes = [json.loads(body)["batch_index"] for body in posted]
    assert indexes == [1, 1, 2]
    assert await _stored("wf") == [{"n": 1}, {"n": 2}]


@pytest.mark.usefixtures("_memory_store")
async def test_consecutive_reads_share_one_parked_subscription():
    handler = TemporalStreamsHandler(None, provider="memory")
    await _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1}, {"n": 2}))
    first = await _dispatch(
        handler,
        READ_OPERATION,
        ReadInput(workflow_id="wf", stream="inputs", max_records=1, wait_ms=200),
    )
    assert len(first.records) == 1
    parked = handler._subscriptions  # pyright: ignore[reportPrivateUsage]
    pump = next(iter(parked.values())).pump
    # A call that resumes where the last one stopped drains the same
    # subscription instead of parking a second read on the store.
    second = await _dispatch(
        handler,
        READ_OPERATION,
        ReadInput(
            workflow_id="wf",
            stream="inputs",
            after_token=first.next_token,
            max_records=1,
            wait_ms=200,
        ),
    )
    assert len(second.records) == 1
    assert second.next_token != first.next_token
    assert len(parked) == 1 and next(iter(parked.values())).pump is pump
    # A call from somewhere else replaces it.
    await _dispatch(
        handler,
        READ_OPERATION,
        ReadInput(workflow_id="wf", stream="inputs", wait_ms=200),
    )
    assert len(parked) == 1 and next(iter(parked.values())).pump is not pump
    await handler.close()
    assert not parked


@pytest.mark.usefixtures("_memory_store")
async def test_the_read_wait_is_cut_to_the_request_deadline():
    handler = TemporalStreamsHandler(None, provider="memory")
    started = asyncio.get_running_loop().time()
    answer = await handler.read(
        _context(READ_OPERATION, datetime.now(timezone.utc) + timedelta(seconds=1)),
        ReadInput(workflow_id="wf", stream="inputs", wait_ms=60000),
    )
    assert answer.records == [] and answer.next_token == ""
    assert asyncio.get_running_loop().time() - started < 5
    await handler.close()
