"""Conformance for the Nexus front.

The caller talks only to the stream endpoint; the handler fronts a storage
provider's own handles, so these tests are the provider-hiding demonstration:
nothing on the caller side names or could name the store.

The live tests run over the server's Nexus HTTP ingress and are gated behind
``STREAMS_LIVE=nexus`` because they need a dev server with an HTTP port.
Environment: ``TEMPORAL_ADDRESS`` (default ``localhost:7233``) and
``TEMPORAL_HTTP`` (default ``http://127.0.0.1:7243``). Each live test
registers its own Nexus endpoint through the operator service and deletes it
at the end.

Everything else stands the endpoint up in this process, because what it
checks is the bytes the caller puts on the wire and the handler's own rules.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import dataclasses
import gc
import http.client
import json
import os
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest import mock

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.converter
from temporalio import nexus as temporal_nexus
from temporalio import streams, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import (
    CreateNexusEndpointRequest,
    DeleteNexusEndpointRequest,
)
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamProducerError,
    StreamRef,
    StreamUnsupportedError,
)
from temporalio.streams._ref import open_ref
from temporalio.streams._wire import WireRecord
from temporalio.streams.providers import nexus
from temporalio.streams.providers._nexus_generated import AppendInput, ReadInput
from temporalio.streams.providers._nexus_generated import StreamRef as WireStreamRef
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.streams.providers.nexus import (
    NexusStreamHandle,
    NexusStreams,
    TemporalStreamsHandler,
)
from temporalio.streams.providers.workflow_streams import WorkflowStreamsProvider
from temporalio.worker import Worker
from tests.helpers import new_worker
from tests.streams.test_workflow_streams_provider import EchoLoop, take
from tests.streams.test_workflow_streams_provider import _feed as feed_echo_loop

live_only = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "nexus",
    reason="needs a live server and nexus endpoint; run with STREAMS_LIVE=nexus",
)

APPEND_OPERATION = nexus._APPEND_OPERATION  # pyright: ignore[reportPrivateUsage]
READ_OPERATION = nexus._READ_OPERATION  # pyright: ignore[reportPrivateUsage]
INPUTS = "inputs"
DECISIONS = "decisions"


async def _live_client() -> Client:
    return await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))


def _live_front(endpoint: str) -> NexusStreams:
    return NexusStreams(
        endpoint=endpoint,
        http_address=os.environ.get("TEMPORAL_HTTP", "http://127.0.0.1:7243"),
        read_wait=timedelta(seconds=5),
    )


@contextlib.asynccontextmanager
async def _own_endpoint(
    client: Client, name: str, task_queue: str
) -> AsyncIterator[str]:
    """Register a Nexus endpoint routed to ``task_queue`` for one test's life."""
    created = await client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=name,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(
                        namespace=client.namespace, task_queue=task_queue
                    )
                ),
            )
        )
    )
    try:
        yield name
    finally:
        await client.operator_service.delete_nexus_endpoint(
            DeleteNexusEndpointRequest(
                id=created.endpoint.id, version=created.endpoint.version
            )
        )


@live_only
async def test_interface_loop_through_the_nexus_front():
    # The workflow worker and the handler worker both use the storage
    # provider; only the caller goes through the front, and it names the
    # endpoint the way an operator does, by name.
    store = WorkflowStreamsProvider()
    client = await _live_client()
    workflow_id = f"streams-nexus-live-{uuid.uuid4().hex}"
    handler = TemporalStreamsHandler(store, client)
    handler_tq = f"handlers-{workflow_id}"

    async with (
        _own_endpoint(client, workflow_id, handler_tq) as endpoint,
        Worker(client, task_queue=handler_tq, nexus_service_handlers=[handler]),
    ):
        front = _live_front(endpoint)
        async with Worker(
            client,
            task_queue=f"tq-{workflow_id}",
            workflows=[EchoLoop],
            plugins=[store],
        ):
            handle = await client.start_workflow(
                EchoLoop.run, id=workflow_id, task_queue=f"tq-{workflow_id}"
            )

            stream = front.get_stream_handle(client, workflow_id)
            producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            await producer.append({"n": 1}, {"n": 2})
            await producer.append({"n": 3})
            await producer.finish()

            records = await take(stream.read(topic=DECISIONS, result_type=dict), 4, 60)
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
            again = await take(
                stream.read(topic=DECISIONS, result_type=dict, after=checkpoint), 2, 60
            )
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
        return [Payload.FromString(self._flip(payload.data)) for payload in payloads]

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
    contract = (
        temporalio.converter.DataConverter.default._get_internal_payload_converter()
    )
    loop = asyncio.get_running_loop()
    failing = set(fail_after_applying or [])

    def post(
        url: str, body: bytes, headers: Mapping[str, str], timeout: timedelta
    ) -> bytes:
        del headers, timeout
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
            # The ingress answers a handler error with a failure body and the
            # status its type maps to.
            status = 404 if error.type is nexusrpc.HandlerErrorType.NOT_FOUND else 400
            raise nexus._EndpointFailure(  # pyright: ignore[reportPrivateUsage]
                json.dumps({"message": str(error)}), status
            ) from error
        raw = contract.to_payloads([answer])[0].data
        answered.append(raw)
        if operation in failing:
            failing.discard(operation)
            raise nexus._EndpointFailure(  # pyright: ignore[reportPrivateUsage]
                "connection reset after the handler answered", None
            )
        return raw

    return post


def _front(codec: temporalio.converter.PayloadCodec | None) -> NexusStreams:
    return NexusStreams(
        endpoint="in-process",
        # This endpoint has no other traffic to wait for, so park briefly
        # rather than for the contract's default.
        read_wait=timedelta(milliseconds=200),
        data_converter=dataclasses.replace(
            temporalio.converter.DataConverter.default, payload_codec=codec
        ),
    )


async def _loop_through_an_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    codec: temporalio.converter.PayloadCodec | None,
) -> tuple[list[Any], list[bytes], list[bytes]]:
    store = MemoryStreams()
    posted: list[bytes] = []
    answered: list[bytes] = []
    handler = TemporalStreamsHandler(store, None)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, posted, answered))
    stream = _front(codec).get_stream_handle(None, "wf-codec")

    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    await producer.append({"secret": "tuna"})
    await producer.finish()

    records = await take(stream.read(topic=INPUTS, result_type=dict), 2, timeout=30)
    await handler.close()
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
            wire = WireRecord.FromString(base64.b64decode(record["record"]))
            out += wire.body.SerializeToString()
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


def _ref(workflow_id: str, topic: str = INPUTS) -> WireStreamRef:
    return WireStreamRef(kind="workflow", workflow_id=workflow_id, topic=topic)


def _append(
    workflow_id: str,
    batch_index: int,
    *values: Any,
    sequence: int | None = None,
    finish: bool = False,
) -> AppendInput:
    converter = temporalio.converter.DataConverter.default.payload_converter
    return AppendInput(
        stream=_ref(workflow_id),
        producer_id="model",
        attempt=1,
        # Batches of one, numbered in step with the batch index unless a
        # case wants them out of step.
        sequence=batch_index - 1 if sequence is None else sequence,
        batch_index=batch_index,
        payloads=[
            converter.to_payloads([value])[0].SerializeToString() for value in values
        ],
        finish=finish,
    )


async def _stored(store: MemoryStreams, workflow_id: str) -> list[Any]:
    stream = store.get_stream_handle(None, workflow_id)
    end = await stream.latest(topic=INPUTS)
    if end == BEGINNING:
        return []
    out: list[Any] = []
    async for record in stream.read(topic=INPUTS, result_type=dict):
        if record.kind is RecordKind.DATA:
            out.append(record.value)
        if record.cursor == end:
            break
    return out


async def test_a_handler_without_state_for_a_producer_refuses_to_continue_it():
    # A second handler instance stands in for a restarted or load-balanced
    # handler worker: it shares the store but not the dedupe state.
    store = MemoryStreams()
    first = TemporalStreamsHandler(store, None)
    second = TemporalStreamsHandler(store, None)
    await _dispatch(first, APPEND_OPERATION, _append("wf", 1, {"n": 1}))
    with pytest.raises(nexusrpc.HandlerError) as failed:
        await _dispatch(second, APPEND_OPERATION, _append("wf", 2, {"n": 2}))
    assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
    assert failed.value.retryable_override is False
    # Named so the caller can raise the same class.
    assert str(failed.value).startswith("StreamProducerError: ")
    # The store holds the first batch and nothing was silently lost or doubled.
    assert await _stored(store, "wf") == [{"n": 1}]


async def test_the_handler_answers_repeats_and_rejects_gaps():
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    await _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1}))
    second = await _dispatch(handler, APPEND_OPERATION, _append("wf", 2, {"n": 2}))
    # A repeat of the last batch is written once and answers with where the
    # original landed, so a retrying caller checkpoints the same position.
    repeat = await _dispatch(handler, APPEND_OPERATION, _append("wf", 2, {"n": 2}))
    assert repeat.cursor == second.cursor
    with pytest.raises(nexusrpc.HandlerError) as skipped:
        await _dispatch(handler, APPEND_OPERATION, _append("wf", 4, {"n": 4}))
    assert skipped.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
    with pytest.raises(nexusrpc.HandlerError) as behind:
        await _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1}))
    assert str(behind.value).startswith("StreamProducerError: ")
    with pytest.raises(nexusrpc.HandlerError) as renumbered:
        await _dispatch(
            handler, APPEND_OPERATION, _append("wf", 3, {"n": 3}, sequence=7)
        )
    assert "sequence 7 does not continue at 2" in str(renumbered.value)
    assert await _stored(store, "wf") == [{"n": 1}, {"n": 2}]


async def test_a_malformed_payload_is_the_callers_fault():
    handler = TemporalStreamsHandler(MemoryStreams(), None)
    request = _append("wf", 1)
    request.payloads = [b"\xff\xfe not a payload"]
    with pytest.raises(nexusrpc.HandlerError) as failed:
        await _dispatch(handler, APPEND_OPERATION, request)
    assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST


async def test_the_caller_retries_an_ambiguous_append_under_the_same_index(
    monkeypatch: pytest.MonkeyPatch,
):
    posted: list[bytes] = []
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    monkeypatch.setattr(
        nexus,
        "_post",
        _in_process_endpoint(
            handler, posted, [], fail_after_applying=[APPEND_OPERATION]
        ),
    )
    stream = _front(None).get_stream_handle(None, "wf")
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    # A transport failure is an RPCError, never the transport's own type.
    with pytest.raises(RPCError) as failed:
        await producer.append({"n": 1})
    assert failed.value.status == RPCStatusCode.UNAVAILABLE
    landed = await producer.append({"n": 1})
    await producer.append({"n": 2})
    await producer.finish()
    indexes = [json.loads(body)["batch_index"] for body in posted]
    sequences = [json.loads(body)["sequence"] for body in posted]
    # The retry re-sent index 1, which the handler answered as a repeat with
    # the original's position, so the store holds each record once.
    assert indexes == [1, 1, 2, 3]
    assert sequences == [0, 0, 1, 2]
    assert landed == await _memory_position(store, "wf", 0)
    assert await _stored(store, "wf") == [{"n": 1}, {"n": 2}]


async def _memory_position(
    store: MemoryStreams, workflow_id: str, index: int
) -> Cursor:
    records = await take(
        store.get_stream_handle(None, workflow_id).read(topic=INPUTS, result_type=dict),
        index + 1,
    )
    return records[index].cursor


async def test_a_failed_append_goes_out_before_the_next_one(
    monkeypatch: pytest.MonkeyPatch,
):
    posted: list[bytes] = []
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    monkeypatch.setattr(
        nexus,
        "_post",
        _in_process_endpoint(
            handler, posted, [], fail_after_applying=[APPEND_OPERATION]
        ),
    )
    stream = _front(None).get_stream_handle(None, "wf")
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    with pytest.raises(RPCError):
        await producer.append({"n": 1})
    # The caller moves on without retrying; the pending batch is replayed
    # first under its own index and the new one takes the next.
    await producer.append({"n": 2})
    indexes = [json.loads(body)["batch_index"] for body in posted]
    assert indexes == [1, 1, 2]
    assert await _stored(store, "wf") == [{"n": 1}, {"n": 2}]


async def test_a_stream_condition_crosses_the_endpoint_under_its_own_class(
    monkeypatch: pytest.MonkeyPatch,
):
    handler = TemporalStreamsHandler(MemoryStreams(), None)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, [], []))
    stream = _front(None).get_stream_handle(None, "wf")
    # The token is opaque on this side, so the store behind the endpoint is
    # what refuses it, and the refusal arrives on the first read as the class
    # the store raised.
    with pytest.raises(StreamCursorError):
        await take(stream.read(topic=INPUTS, after=Cursor("elsewhere:1")), 1)
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    await producer.append({"n": 1})
    # A second handler has no state for the attempt; the caller learns that
    # as a producer conflict.
    fresh = TemporalStreamsHandler(MemoryStreams(), None)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(fresh, [], []))
    with pytest.raises(StreamProducerError):
        await producer.append({"n": 2})
    await handler.close()


def test_the_front_has_no_workflow_half():
    with pytest.raises(StreamUnsupportedError):
        _front(None).workflow_provider()


async def test_the_front_registers_on_a_client(client: Client):
    # The same accessor as any provider, so code outside a workflow does not
    # change when the store moves behind an endpoint.
    config = client.config()
    config["plugins"] = [_front(None)]
    registered = Client(**config)
    assert isinstance(registered.get_stream_handle("wf"), NexusStreamHandle)


async def test_consecutive_reads_share_one_parked_subscription():
    handler = TemporalStreamsHandler(MemoryStreams(), None)
    await _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1}, {"n": 2}))
    first = await _dispatch(
        handler,
        READ_OPERATION,
        ReadInput(stream=_ref("wf"), max_records=1, wait_ms=200),
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
            stream=_ref("wf"),
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
        ReadInput(stream=_ref("wf"), wait_ms=200),
    )
    assert len(parked) == 1 and next(iter(parked.values())).pump is not pump
    await handler.close()
    assert not parked


async def test_the_read_wait_is_cut_to_the_request_deadline():
    handler = TemporalStreamsHandler(MemoryStreams(), None)
    started = asyncio.get_running_loop().time()
    answer = await handler.read(
        _context(READ_OPERATION, datetime.now(timezone.utc) + timedelta(seconds=1)),
        ReadInput(stream=_ref("wf"), wait_ms=60000),
    )
    assert answer.records == [] and answer.next_token == "" and not answer.done
    assert asyncio.get_running_loop().time() - started < 5
    await handler.close()


async def test_the_read_ends_when_the_store_ends_it(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    # Behind the endpoint sits the Workflow Streams store, whose read ends
    # once the run has closed and its tail is served. The front carries that
    # end to the caller, whose loop leaves by itself.
    store = WorkflowStreamsProvider(poll_cooldown=timedelta(milliseconds=20))
    handler = TemporalStreamsHandler(store, client)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, [], []))
    workflow_id = f"streams-nexus-{uuid.uuid4().hex}"
    async with new_worker(client, EchoLoop, plugins=[store]) as worker:
        handle = await client.start_workflow(
            EchoLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        await feed_echo_loop(store, client, workflow_id)
        await handle.signal(EchoLoop.release)
        assert await handle.result() == 3

        stream = _front(None).get_stream_handle(None, workflow_id)

        async def read_everything() -> list[Any]:
            return [
                (r.kind, r.value)
                async for r in stream.read(topic=DECISIONS, result_type=dict)
            ]

        records = await asyncio.wait_for(read_everything(), 30)
    await handler.close()
    assert records == [
        (RecordKind.DATA, {"echo": 1}),
        (RecordKind.DATA, {"echo": 2}),
        (RecordKind.DATA, {"echo": 3}),
        (RecordKind.FINISH, None),
    ]


class _SlowStore(MemoryStreams):
    """The memory store with a real await inside ``append``.

    The handler's repeat check and its commit have an await between them.
    Without a gate there, two in-flight copies of one batch both finish the
    check before either commits and both reach the store.
    """

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.appends = 0

    def get_stream_handle(
        self, client: Any, workflow_id: str, *, run_id: str | None = None
    ) -> Any:
        handle = super().get_stream_handle(client, workflow_id, run_id=run_id)
        outer = self
        make_producer = handle.producer

        def producer(**kwargs: Any) -> Any:
            delegate = make_producer(**kwargs)
            inner_append = delegate.append

            async def append(*values: Any) -> Any:
                outer.appends += 1
                await outer.gate.wait()
                return await inner_append(*values)

            delegate.append = append  # type: ignore[method-assign]
            return delegate

        handle.producer = producer  # type: ignore[method-assign]
        return handle


async def test_two_copies_of_one_batch_reach_the_store_once():
    store = _SlowStore()
    handler = TemporalStreamsHandler(store, None)
    both = asyncio.gather(
        _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1})),
        _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1})),
    )
    await asyncio.sleep(0.1)
    store.gate.set()
    first, second = await asyncio.wait_for(both, 10)
    # One of them wrote and the other was answered from the state it left.
    # Counted at the store because a store that dedupes by itself would hide
    # a second commit the handler should never have made.
    assert store.appends == 1
    assert first.cursor == second.cursor
    assert await _stored(store, "wf") == [{"n": 1}]


async def test_a_finished_batch_can_be_retried():
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    await _dispatch(handler, APPEND_OPERATION, _append("wf", 1, {"n": 1}))
    finished = await _dispatch(
        handler, APPEND_OPERATION, _append("wf", 2, {"n": 2}, finish=True)
    )
    # The caller never saw the answer. The byte-identical retry has to be
    # answerable, because a finish the caller cannot repeat is a batch it can
    # neither complete nor abandon.
    again = await _dispatch(
        handler, APPEND_OPERATION, _append("wf", 2, {"n": 2}, finish=True)
    )
    assert again.cursor == finished.cursor
    assert await _stored(store, "wf") == [{"n": 1}, {"n": 2}]
    # And the attempt is over: a batch after the finish is refused rather
    # than landing behind the marker.
    with pytest.raises(nexusrpc.HandlerError) as after:
        await _dispatch(handler, APPEND_OPERATION, _append("wf", 3, {"n": 3}))
    assert "already finished" in str(after.value)


class _FailingReadStore(MemoryStreams):
    """A store whose read fails once, the way a transient store failure does."""

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    def get_stream_handle(
        self, client: Any, workflow_id: str, *, run_id: str | None = None
    ) -> Any:
        handle = super().get_stream_handle(client, workflow_id, run_id=run_id)
        outer = self

        def read(**kwargs: Any) -> Any:
            outer.reads += 1
            if outer.reads == 1:

                async def failing() -> AsyncIterator[Any]:
                    for record in cast("list[Any]", []):
                        yield record
                    raise RuntimeError("the store went away")

                return failing()
            return MemoryStreams.get_stream_handle(
                outer, client, workflow_id, run_id=run_id
            ).read(**kwargs)

        handle.read = read  # type: ignore[method-assign]
        return handle


async def test_a_failed_read_is_not_reported_as_the_end_of_the_stream():
    store = _FailingReadStore()
    handler = TemporalStreamsHandler(store, None)
    await (
        store.get_stream_handle(None, "wf")
        .producer(topic=INPUTS, producer_id="model", attempt=1)
        .append({"n": 1})
    )

    request = ReadInput(stream=_ref("wf"), wait_ms=200, max_records=10)
    with pytest.raises(RuntimeError, match="the store went away"):
        await _dispatch(handler, READ_OPERATION, request)
    # The retry from the same token has to re-subscribe rather than be
    # answered done=True off the subscription the failed pump left behind.
    answer = await _dispatch(handler, READ_OPERATION, request)
    assert [WireRecord.FromString(r.record).sequence for r in answer.records] == [1]
    assert answer.done is False
    await handler.close()


async def test_the_handler_does_not_grow_a_lock_per_address():
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    for index in range(25):
        workflow_id = f"wf-{index}"
        await _dispatch(
            handler, APPEND_OPERATION, _append(workflow_id, 1, {"n": index})
        )
        await _dispatch(
            handler,
            READ_OPERATION,
            ReadInput(stream=_ref(workflow_id), wait_ms=0, max_records=1),
        )
    gc.collect()
    # Held weakly, so an address nobody is reading or appending to leaves
    # nothing behind. A strong map would hold one entry per address forever.
    assert len(handler._read_locks) == 0  # pyright: ignore[reportPrivateUsage]
    assert len(handler._append_locks) == 0  # pyright: ignore[reportPrivateUsage]
    await handler.close()


def test_the_read_bounds_are_checked_where_they_are_set():
    # The contract accepts a wait up to a minute and a batch up to a
    # thousand; past that the endpoint answers with a payload validation
    # error, which is neither a stream condition nor an RPC failure.
    with pytest.raises(ValueError, match="read_wait"):
        NexusStreams(endpoint="e", read_wait=timedelta(minutes=2))
    with pytest.raises(ValueError, match="read_wait"):
        NexusStreams(endpoint="e", read_wait=timedelta(seconds=-1))
    with pytest.raises(ValueError, match="max_records"):
        NexusStreams(endpoint="e", max_records=0)
    with pytest.raises(ValueError, match="max_records"):
        NexusStreams(endpoint="e", max_records=1001)
    NexusStreams(endpoint="e", read_wait=timedelta(seconds=60), max_records=1000)


def test_a_socket_failure_never_reaches_the_caller_as_a_timeout():
    # urllib wraps only the request in URLError, so a read timeout comes out
    # of getresponse() as builtins.TimeoutError, which on 3.11 and later is
    # asyncio.TimeoutError and would be taken for the caller's own deadline.
    def _raise(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise TimeoutError("the socket read timed out")

    with mock.patch("urllib.request.urlopen", _raise):
        with pytest.raises(nexus._EndpointFailure):  # pyright: ignore[reportPrivateUsage]
            nexus._post(  # pyright: ignore[reportPrivateUsage]
                "http://127.0.0.1:1/x", b"{}", {}, timedelta(seconds=1)
            )

    def _disconnect(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise http.client.RemoteDisconnected("the endpoint hung up")

    with mock.patch("urllib.request.urlopen", _disconnect):
        with pytest.raises(nexus._EndpointFailure):  # pyright: ignore[reportPrivateUsage]
            nexus._post(  # pyright: ignore[reportPrivateUsage]
                "http://127.0.0.1:1/x", b"{}", {}, timedelta(seconds=1)
            )

    # A url urllib cannot even build a request from does not escape either.
    with pytest.raises(nexus._EndpointFailure):  # pyright: ignore[reportPrivateUsage]
        nexus._post("not a url", b"{}", {}, timedelta(seconds=1))  # pyright: ignore[reportPrivateUsage]


async def test_the_front_carries_every_read_start(monkeypatch: pytest.MonkeyPatch):
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, [], []))
    stream = _front(None).get_stream_handle(None, "wf-starts")
    producer = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    # last= rides the contract to the store behind the endpoint.
    newest = await take(
        stream.read(topic=INPUTS, result_type=dict, last=2), 2, timeout=30
    )
    assert [record.value for record in newest] == [{"n": 2}, {"n": 3}]

    # END is the endpoint's newest position when the read starts.
    at_end = stream.read(topic=INPUTS, result_type=dict, after=END)
    first = asyncio.ensure_future(at_end.__anext__())
    try:
        for _ in range(100):
            await producer.append({"n": "new"})
            done, _ = await asyncio.wait({first}, timeout=0.1)
            if done:
                break
        record = await asyncio.wait_for(first, 30)
    finally:
        await at_end.aclose()
    assert record.value == {"n": "new"}

    # A token resumes a read and last_n starts one, so the two are refused.
    with pytest.raises(nexusrpc.HandlerError) as both:
        await _dispatch(
            handler,
            READ_OPERATION,
            ReadInput(
                stream=_ref("wf-starts"),
                after_token=newest[0].cursor.token,
                last_n=1,
            ),
        )
    assert both.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
    await handler.close()


async def test_an_activity_owner_crosses_the_endpoint(monkeypatch: pytest.MonkeyPatch):
    # The reference carries the owner, so an activity's own stream is reached
    # through the same two operations and lands on the store's activity
    # accessor, apart from the workflow's topics of the same name.
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, [], []))
    front = _front(None)
    own = front.get_activity_stream_handle(None, "act", workflow_id="wf")
    await own.producer(topic=INPUTS, producer_id="model", attempt=1).append({"n": 1})
    records = await take(own.read(topic=INPUTS, result_type=dict), 1, timeout=30)
    assert records[0].value == {"n": 1}
    assert await _stored(store, "wf") == []
    stored = store.get_activity_stream_handle(None, "act", workflow_id="wf")
    assert [r.value async for r in _take_data(stored, 1)] == [{"n": 1}]
    await handler.close()


async def _take_data(stream: Any, count: int) -> AsyncIterator[Any]:
    async for record in stream.read(topic=INPUTS, result_type=dict):
        if record.kind is RecordKind.DATA:
            yield record
            count -= 1
        if count == 0:
            return


async def test_an_owner_the_store_cannot_host_is_refused_under_its_own_class(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    # Whether an owner is supported is the store's answer, not the front's:
    # the Workflow Streams store hosts only the streams of a workflow's
    # activities, so a standalone activity's are refused, and its refusal
    # reaches the caller as the class it raised.
    handler = TemporalStreamsHandler(WorkflowStreamsProvider(), client)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, [], []))
    own = _front(None).get_activity_stream_handle(None, "act")
    with pytest.raises(StreamUnsupportedError, match="activity"):
        await own.producer(topic=INPUTS, producer_id="model", attempt=1).append(
            {"n": 1}
        )
    with pytest.raises(StreamUnsupportedError, match="activity"):
        await take(own.read(topic=INPUTS), 1)
    # No provider addresses a standalone stream yet, so that owner is refused
    # for every store.
    standalone = WireStreamRef(kind="standalone", stream_id="s-1", topic=INPUTS)
    with pytest.raises(nexusrpc.HandlerError) as refused:
        await _dispatch(handler, READ_OPERATION, ReadInput(stream=standalone))
    assert str(refused.value).startswith("StreamUnsupportedError: ")
    await handler.close()


async def test_a_reference_missing_its_owner_id_is_the_callers_fault():
    handler = TemporalStreamsHandler(MemoryStreams(), None)
    for ref in (
        WireStreamRef(kind="workflow", topic=INPUTS),
        WireStreamRef(kind="activity", workflow_id="wf", topic=INPUTS),
        WireStreamRef(kind="standalone", topic=INPUTS),
        WireStreamRef(kind="workflow", workflow_id="wf", topic=""),
    ):
        with pytest.raises(nexusrpc.HandlerError) as failed:
            await _dispatch(handler, READ_OPERATION, ReadInput(stream=ref))
        assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
        with pytest.raises(nexusrpc.HandlerError) as failed:
            request = _append("wf", 1, {"n": 1})
            request.stream = ref
            await _dispatch(handler, APPEND_OPERATION, request)
        assert failed.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST


async def test_a_handle_names_its_stream_as_a_ref_that_opens_on_the_front(
    monkeypatch: pytest.MonkeyPatch,
):
    store = MemoryStreams()
    handler = TemporalStreamsHandler(store, None)
    monkeypatch.setattr(nexus, "_post", _in_process_endpoint(handler, [], []))
    front = _front(None)
    handle = front.get_stream_handle(None, "wf", run_id="r1")
    ref = handle.ref(topic=INPUTS)
    assert ref == StreamRef.for_workflow("wf", run_id="r1", topic=INPUTS)
    # Plain data: the default converter carries it the way an operation
    # result or a workflow argument is carried.
    converter = temporalio.converter.DataConverter.default.payload_converter
    carried = converter.from_payloads([converter.to_payloads([ref])[0]], [StreamRef])
    assert carried == [ref]
    # Opened on the front, the ref reaches the same stream and its topic is
    # the handle's default; the client accessor goes through open_ref.
    opened = open_ref(front, cast(Client, None), ref)
    await opened.producer(producer_id="model", attempt=1).append({"n": 1})
    records = await take(handle.read(topic=INPUTS, result_type=dict), 1, timeout=30)
    assert records[0].value == {"n": 1}
    assert opened.ref() == ref
    # Every owner kind names itself.
    assert front.get_activity_stream_handle(
        None, "act", workflow_id="wf"
    ).ref() == StreamRef.for_activity("act", workflow_id="wf")
    assert front.get_standalone_stream_handle(None, "s-1").ref(
        topic="t"
    ) == StreamRef.for_standalone("s-1", topic="t")
    await handler.close()


async def test_the_front_refuses_what_the_contract_cannot_carry():
    # No create and no seal operation: an owned stream cannot be closed by
    # anyone, and a standalone stream is created and sealed on the store.
    front = _front(None)
    with pytest.raises(ValueError, match="only a standalone stream"):
        await front.get_stream_handle(None, "wf").close()
    with pytest.raises(StreamUnsupportedError, match="seals"):
        await front.get_standalone_stream_handle(None, "s-1").close()
    with pytest.raises(StreamUnsupportedError, match="creates"):
        await front.create_standalone_stream(None, "s-1")
    with pytest.raises(ValueError, match="stream_id"):
        front.get_standalone_stream_handle(None, "")


# ---------------------------------------------------------------------------
# A stream as an operation result and as an operation input (June s8 b).
# ---------------------------------------------------------------------------


@dataclass
class ScoreUpdate:
    """One score change."""

    home: int
    away: int


@dataclass
class GameRequest:
    """Which game to start, and where its workflow runs."""

    game_id: str
    task_queue: str


@dataclass
class ScorePost:
    """A score for the stream given, and whether it is the last one."""

    stream: StreamRef
    score: ScoreUpdate
    last: bool = False


@dataclass
class CallerInput:
    """The endpoint to call, the game to ask for, and the scores to post."""

    endpoint: str
    game_id: str
    task_queue: str
    scores: list[ScoreUpdate]


SCORES = streams.topic("scores", ScoreUpdate)
COMMANDS = streams.topic("commands", ScoreUpdate)


@workflow.defn
class ScoreboardGame:
    """Reads posted scores on ``commands`` and publishes each on ``scores``."""

    @workflow.run
    async def run(self) -> int:
        """Relay every posted score until the poster finishes, then finish."""
        posted = 0
        scores = workflow.stream_writer(SCORES)
        async for record in workflow.stream_reader(COMMANDS):
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is RecordKind.DATA:
                assert record.value is not None
                scores.publish(record.value)
                posted += 1
        scores.finish()
        return posted


@nexusrpc.service
class ScoreboardService:
    """Starts a game and hands back its stream; takes a stream to post to."""

    start_game: nexusrpc.Operation[GameRequest, StreamRef]
    post_score: nexusrpc.Operation[ScorePost, None]


@nexusrpc.handler.service_handler(service=ScoreboardService)
class ScoreboardHandler:
    """Serves the two operations as a client of the stream front."""

    @nexusrpc.handler.sync_operation
    async def start_game(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: GameRequest
    ) -> StreamRef:
        """Start the game workflow and return a ref to its ``scores`` topic."""
        client = temporal_nexus.client()
        # Reusing a running game makes a retried start hand back the same ref.
        await client.start_workflow(
            ScoreboardGame.run,
            id=input.game_id,
            task_queue=input.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return client.get_stream_handle(input.game_id).ref(topic=SCORES)

    @nexusrpc.handler.sync_operation
    async def post_score(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ScorePost
    ) -> None:
        """Append the score to the stream the caller named."""
        # The request id is the producer, so a retried post writes once.
        producer = (
            temporal_nexus.client()
            .get_stream_handle(input.stream)
            .producer(producer_id=f"post-{ctx.request_id}", attempt=1)
        )
        await producer.append(input.score)
        if input.last:
            await producer.finish()


@workflow.defn
class ScoreboardCaller:
    """Calls the operations and passes the ref on; it never reads the stream."""

    @workflow.run
    async def run(self, input: CallerInput) -> StreamRef:
        """Start a game, post the scores to it, and return where to read them."""
        scoreboard = workflow.create_nexus_client(
            service=ScoreboardService, endpoint=input.endpoint
        )
        ref = await scoreboard.execute_operation(
            ScoreboardService.start_game,
            GameRequest(input.game_id, input.task_queue),
        )
        for index, score in enumerate(input.scores):
            await scoreboard.execute_operation(
                ScoreboardService.post_score,
                ScorePost(
                    ref.with_topic(COMMANDS), score, last=index == len(input.scores) - 1
                ),
            )
        return ref


@live_only
async def test_an_operation_returns_a_stream_ref_and_another_takes_one():
    # June s8 (b) without the emulation: the operation's result type is the
    # SDK's StreamRef, the calling workflow passes it on as data, the client
    # opens it on the front it carries and long-polls it, and a second
    # operation appends to a stream it was given as input. The handlers are
    # clients of the front like any process; only the game workflow's worker
    # holds the store.
    store = WorkflowStreamsProvider()
    plain = await _live_client()
    game_id = f"streams-nexus-ref-{uuid.uuid4().hex}"
    handler_tq = f"handlers-{game_id}"
    stream_handler = TemporalStreamsHandler(store, plain)

    async with (
        _own_endpoint(plain, game_id, handler_tq) as endpoint,
        Worker(
            plain,
            task_queue=handler_tq,
            nexus_service_handlers=[stream_handler],
        ),
    ):
        front = _live_front(endpoint)
        config = plain.config()
        config["plugins"] = [front]
        fronted = Client(**config)
        async with (
            Worker(
                fronted,
                task_queue=f"scoreboard-{game_id}",
                nexus_service_handlers=[ScoreboardHandler()],
            ),
            Worker(
                plain,
                task_queue=f"tq-{game_id}",
                workflows=[ScoreboardGame, ScoreboardCaller],
                plugins=[store],
            ),
        ):
            # The scoreboard service sits behind its own endpoint.
            async with _own_endpoint(
                plain, f"scoreboard-{game_id}", f"scoreboard-{game_id}"
            ) as scoreboard_endpoint:
                posted = [ScoreUpdate(1, 0), ScoreUpdate(1, 1), ScoreUpdate(2, 1)]
                ref = await plain.execute_workflow(
                    ScoreboardCaller.run,
                    CallerInput(scoreboard_endpoint, game_id, f"tq-{game_id}", posted),
                    id=f"{game_id}-caller",
                    task_queue=f"tq-{game_id}",
                )
                assert ref == StreamRef.for_workflow(game_id, topic=SCORES)

                # The ref is plain data the client opens on its own provider,
                # here the front, and reads with the endpoint's long poll. It
                # names the topic, not the record type, so the reader says
                # what to decode as, the way a runtime topic name does.
                records = await take(
                    fronted.get_stream_handle(ref).read(result_type=ScoreUpdate), 4, 60
                )
                assert [record.kind for record in records] == [
                    RecordKind.DATA,
                    RecordKind.DATA,
                    RecordKind.DATA,
                    RecordKind.FINISH,
                ]
                assert [record.value for record in records[:3]] == posted
                assert await plain.get_workflow_handle(game_id).result() == 3
        await stream_handler.close()
