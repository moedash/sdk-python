"""The Nexus front: the outside surface behind one Temporal-authenticated endpoint.

Two halves in one module. :class:`NexusStreams` is a provider that implements
the outside half only: it hands out handles that append and read through the
endpoint, and it has no workflow half, because a workflow's publishes and
reads ride the Workflow Task and cannot cross an RPC.
:class:`TemporalStreamsHandler` runs in a worker next to any storage provider
and serves two sync operations, ``append`` and ``read``, by fronting that
provider's own handles, so the store behind the endpoint is invisible to
callers and an operator switches it without touching them.

The wire types and the service definition come from
``temporal_streams.nexusrpc.yaml``, so any language nexgen targets can be
handed the same contract. A record crosses as the serialized
``temporal.api.stream.v1.StreamRecord``, the same bytes every store keeps, so
a caller in another language decodes it with the api protos alone. What stays
hand-written here is what the generator cannot express yet: the handler's
dedupe and long-poll collect loop, and the caller's batching, cursor handling
and codec.

Reads hand out batches and an append carries one batch per call, because a
Nexus operation per record costs too much for token streams. Record bodies
cross the handler untouched: it reads the store as raw payloads and forwards
them as they are, so a codec that changes the payload encoding survives the
hop and the handler's worker never needs the key. Supersession records are
not transported: the caller's reader re-synthesizes them from the attempts it
observes, which is the policy module's job on every provider. Cursors pass
through opaque, so the caller cannot tell which store produced them; a
foreign one is refused by the store behind the endpoint and reaches the
caller on the first read.

Both operations address a stream by a reference, ``StreamRef`` on the wire:
the owner (a workflow, an activity, or a standalone stream), the ids that
name it, and the topic. The handler maps the reference onto the store's own
accessor for that owner and refuses an owner the store cannot host with
:class:`temporalio.streams.StreamUnsupportedError`, so a reference is good
behind any endpoint that serves the owner. The same reference is what an
operation of the application's own returns to hand a stream to its caller;
:meth:`NexusStreamHandle.ref` makes one and ``client.get_stream_handle(ref)``
opens it.

The handler keeps one parked read per stream reference and serves
consecutive calls from it, so an idle caller does not leave one abandoned
long poll on the store per call. A call whose token does not match the parked
position replaces the subscription, and an idle one is released after a
minute; that is the residual cost.

Two stated prototype limits. Append deduplication lives in handler memory, by
batch index per producer attempt, so a handler that has no state for a
producer attempt refuses to continue it rather than starting a fresh delegate
whose numbering the store would drop as a repeat; the caller opens a new
attempt, which readers report as a supersession. And any caller the endpoint
admits may touch any owner's streams in the namespace; the endpoint's own
authorization is the boundary.

A failure from the endpoint reaches the caller as the
:class:`temporalio.streams.StreamError` the store raised, when the handler
named one, and as :class:`temporalio.service.RPCError` otherwise, never as an
HTTP or urllib exception.

The third piece is the consumer side of a Nexus operation.
:class:`StreamConsumerOperation`, built with :func:`stream_consumer_operation`,
is an asynchronous operation handler whose input is a ``StreamRef`` and whose
result is what a consume function folded the stream's records into. It does
not poll. On start it registers a callback listener on the stream's
notification channel, with a URL the hosting process serves and a header that
names the operation, and reads the stream from its start through the client's
provider, the front when the client carries one. The server posts every
notification on the channel to that URL, the handler reads from its cursor to
the head, and the close completes the operation through the caller's
completion callback. The delivery is what the server's channel library posts:
``POST`` to the listener's URL, the ``Notification`` as protobuf JSON with
``Content-Type: application/json``, the listener's own headers and the
channel's name in ``Temporal-Notification-Channel``. A process that hosts the
handler feeds each such request to :meth:`StreamConsumerOperation.deliver`;
``nexus_consumer_service`` in this package is the standalone one.
"""

from __future__ import annotations

import asyncio
import email.utils
import http.client
import inspect
import json
import logging
import time
import urllib.error
import urllib.request
import uuid
import weakref
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, NoReturn, TypeVar, cast

import nexusrpc
import nexusrpc.handler
from google.protobuf import json_format
from google.protobuf.message import DecodeError

import temporalio.api.notification.v1
import temporalio.client
import temporalio.converter
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.operatorservice.v1 import ListNexusEndpointsRequest
from temporalio.client import Callback, Client, ClientConfig
from temporalio.common import RawValue
from temporalio.service import ConnectConfig, RPCError, RPCStatusCode, ServiceClient
from temporalio.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.streams._provider import StreamHandle, StreamProducer, StreamProvider
from temporalio.streams._record import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamRecord,
    check_read_start,
)
from temporalio.streams._ref import StreamOwnerKind, StreamRef
from temporalio.streams._topic import StreamTopic, resolve_topic
from temporalio.streams._wire import (
    RecordDecoder,
    WireRecord,
    producer_identity,
    to_wire,
)
from temporalio.streams.providers._nexus_generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
)
from temporalio.streams.providers._nexus_generated import StreamRef as WireStreamRef
from temporalio.workflow import Notification

__all__ = [
    "NOTIFICATION_CHANNEL_HEADER",
    "STREAM_CONSUMER_TOKEN_HEADER",
    "ConsumerState",
    "Delivery",
    "NexusProducer",
    "NexusStreamHandle",
    "NexusStreams",
    "StreamConsumerOperation",
    "TemporalStreamsHandler",
    "WireStreamRef",
    "stream_consumer_operation",
]

T = TypeVar("T")
S = TypeVar("S")

# The reference's identity on the handler, topic included, so one parked read
# and one producer state key on exactly what the wire names.
_StreamKey = tuple[str, str, str, str, str, str]
_ProducerKey = tuple[_StreamKey, str, int]

_WORKFLOW_SIDE_ERROR = (
    "the nexus provider is an outside transport; a worker publishes and reads "
    "through a storage provider, so give the worker one of those"
)

# The contract leaves these unset, so the handler is the one place that says
# what an omitted read bound means. The wait is long because the handler
# shortens it to the request deadline anyway, and a shorter default only
# means more round trips on an idle stream.
_DEFAULT_MAX_RECORDS = 100
_DEFAULT_READ_WAIT = timedelta(seconds=30)
_DEADLINE_MARGIN = timedelta(milliseconds=500)
_DEFAULT_MAX_PRODUCERS = 10_000
_DEFAULT_SUBSCRIPTION_IDLE = timedelta(seconds=60)
_QUEUE_DEPTH = 1000
_APPEND_TIMEOUT = timedelta(seconds=30)
_ROUND_TRIP_MARGIN = timedelta(seconds=5)
# What ReadInput accepts in temporal_streams.nexusrpc.yaml. Repeated here so
# a caller's mistake is refused where it was made rather than on the wire.
_MIN_READ_WAIT = timedelta(0)
_MAX_READ_WAIT = timedelta(milliseconds=60_000)
_MIN_MAX_RECORDS = 1
_MAX_MAX_RECORDS = 1000

_definition = nexusrpc.get_service_definition(TemporalStreams)
if _definition is None:  # pragma: no cover
    raise RuntimeError("the generated stream service carries no nexus definition")
# Both halves read the wire names off the generated definition, so a rename in
# the contract cannot leave the caller posting to a path the handler no longer
# serves.
_SERVICE_NAME = _definition.name
_OPERATION_NAMES = {
    operation.method_name: operation.name
    for operation in _definition.operation_definitions.values()
}
_APPEND_OPERATION = _OPERATION_NAMES["append"]
_READ_OPERATION = _OPERATION_NAMES["read"]

# The stream conditions that cross the endpoint under their own name, so the
# caller raises the class the store raised.
_STREAM_ERRORS: dict[str, type[StreamError]] = {
    cls.__name__: cls
    for cls in (
        StreamError,
        StreamNotFoundError,
        StreamCursorError,
        StreamProducerError,
        StreamClosedError,
        StreamUnsupportedError,
    )
}
_HTTP_TO_RPC = {
    400: RPCStatusCode.INVALID_ARGUMENT,
    401: RPCStatusCode.UNAUTHENTICATED,
    403: RPCStatusCode.PERMISSION_DENIED,
    404: RPCStatusCode.NOT_FOUND,
    408: RPCStatusCode.DEADLINE_EXCEEDED,
    409: RPCStatusCode.ALREADY_EXISTS,
    412: RPCStatusCode.FAILED_PRECONDITION,
    429: RPCStatusCode.RESOURCE_EXHAUSTED,
    500: RPCStatusCode.INTERNAL,
    501: RPCStatusCode.UNIMPLEMENTED,
    503: RPCStatusCode.UNAVAILABLE,
    504: RPCStatusCode.DEADLINE_EXCEEDED,
}

_OutputT = TypeVar("_OutputT")

logger = logging.getLogger(__name__)


def _require_topic(topic: str) -> None:
    if not topic:
        raise ValueError("topic must not be empty")


def _check_ref(ref: WireStreamRef) -> None:
    """Refuse a reference whose owner lacks the id that names it."""
    _require_topic(ref.topic)
    if ref.kind == "workflow" and not ref.workflow_id:
        raise ValueError("a stream reference with a workflow owner needs workflow_id")
    if ref.kind == "activity" and not ref.activity_id:
        raise ValueError("a stream reference with an activity owner needs activity_id")
    if ref.kind == "standalone" and not ref.stream_id:
        raise ValueError("a stream reference with a standalone owner needs stream_id")


def _stream_key(ref: WireStreamRef) -> _StreamKey:
    return (
        ref.kind,
        ref.workflow_id or "",
        ref.run_id or "",
        ref.activity_id or "",
        ref.stream_id or "",
        ref.topic,
    )


@dataclass(frozen=True)
class _Address:
    """The owner a handle is bound to; a topic completes it into a reference."""

    kind: StreamOwnerKind
    workflow_id: str | None = None
    run_id: str | None = None
    activity_id: str | None = None
    stream_id: str | None = None

    def wire(self, topic: str) -> WireStreamRef:
        return WireStreamRef(
            kind=self.kind,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            activity_id=self.activity_id,
            stream_id=self.stream_id,
            topic=topic,
        )

    def ref(self, topic: str) -> StreamRef:
        # The same members under the SDK's own name, so what an operation
        # returns is what client.get_stream_handle(ref) opens.
        return StreamRef(
            self.kind,
            topic,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            activity_id=self.activity_id,
            stream_id=self.stream_id,
        )


def _handler_error(error: StreamError) -> nexusrpc.HandlerError:
    """Carry a stream condition across the endpoint under its own class name."""
    kind = (
        nexusrpc.HandlerErrorType.NOT_FOUND
        if isinstance(error, StreamNotFoundError)
        else nexusrpc.HandlerErrorType.BAD_REQUEST
    )
    return nexusrpc.HandlerError(
        f"{type(error).__name__}: {error}", type=kind, retryable_override=False
    )


def _bad_request(message: str) -> nexusrpc.HandlerError:
    return nexusrpc.HandlerError(
        message, type=nexusrpc.HandlerErrorType.BAD_REQUEST, retryable_override=False
    )


@dataclass
class _Subscription:
    """One parked read on the store, shared by consecutive calls."""

    records: asyncio.Queue[StreamRecord[Any]]
    position: str
    last_used: float
    last_n: int | None = None
    """The newest-N start this subscription was opened with, when it had no token."""
    pump: asyncio.Task[None] | None = None
    failure: BaseException | None = None
    done: bool = False


@dataclass
class _ProducerState:
    delegate: StreamProducer
    batch_index: int
    sequence: int
    last_cursor: str | None
    finished: bool = False


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream endpoint by fronting one storage provider's handles.

    The provider is an explicit instance the application constructed, so a
    caller and a handler can coexist in one process, and so an operator can
    run handlers for a new store next to handlers for the old one.
    """

    def __init__(
        self,
        provider: StreamProvider,
        client: Client | None,
        *,
        max_producers: int = _DEFAULT_MAX_PRODUCERS,
        subscription_idle: timedelta = _DEFAULT_SUBSCRIPTION_IDLE,
    ) -> None:
        """Serve the endpoint out of ``provider``'s handles, opened with ``client``.

        ``max_producers`` bounds the dedupe state kept per producer attempt;
        the oldest is evicted, and a producer evicted mid-life gets a clear
        refusal on its next batch rather than a silent drop.
        ``subscription_idle`` is how long a parked read outlives its last
        caller.
        """
        self._provider = provider
        self._client = client
        self._producers: OrderedDict[_ProducerKey, _ProducerState] = OrderedDict()
        self._max_producers = max_producers
        self._subscriptions: dict[_StreamKey, _Subscription] = {}
        # Held weakly, and by the call that is using one for as long as it
        # runs. A strong map would keep an entry per address ever read or
        # appended to, and nothing would ever reach it again.
        self._read_locks: weakref.WeakValueDictionary[_StreamKey, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._append_locks: weakref.WeakValueDictionary[_ProducerKey, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._subscription_idle = subscription_idle.total_seconds()

    def _stream(self, ref: WireStreamRef) -> StreamHandle:
        """The store's handle for the owner ``ref`` names.

        Raises:
            StreamUnsupportedError: The store has no accessor for that owner.
        """
        # A storage provider wants a client; the memory provider, which the
        # tests front, accepts none, so the cast only lies where it is unread.
        client = cast(Client, self._client)
        if ref.kind == "workflow":
            return self._provider.get_stream_handle(
                client, cast(str, ref.workflow_id), run_id=ref.run_id or None
            )
        if ref.kind == "activity":
            return self._provider.get_activity_stream_handle(
                client,
                cast(str, ref.activity_id),
                workflow_id=ref.workflow_id or None,
                run_id=ref.run_id or None,
            )
        return self._provider.get_standalone_stream_handle(
            client, cast(str, ref.stream_id)
        )

    @nexusrpc.handler.sync_operation
    async def append(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Append on the caller's account, writing a repeated batch once.

        Raises:
            nexusrpc.HandlerError: ``BAD_REQUEST`` when the batch index or
                sequence does not continue the producer attempt, when the
                attempt is one this handler has no state for, when a payload
                is not a serialized ``Payload``, or when the store cannot host
                the reference's owner; ``NOT_FOUND`` when the store has no
                such owner.
        """
        try:
            return await self._append(input)
        except StreamError as error:
            raise _handler_error(error) from error
        except ValueError as error:
            raise _bad_request(str(error)) from error

    async def _append(self, input: AppendInput) -> AppendOutput:
        _check_ref(input.stream)
        key: _ProducerKey = (
            _stream_key(input.stream),
            input.producer_id,
            input.attempt,
        )
        # The repeat check and the commit have an await between them, so two
        # in-flight copies of one batch would both pass the check and both
        # reach the store. The read path serialises per address for the same
        # reason.
        lock = self._append_locks.setdefault(key, asyncio.Lock())
        async with lock:
            return await self._append_locked(input, key)

    async def _append_locked(
        self, input: AppendInput, key: _ProducerKey
    ) -> AppendOutput:
        state = self._producers.get(key)
        if state is None:
            if input.batch_index != 1 or input.sequence != 0:
                # A fresh delegate would restart its numbering at zero, and
                # the store would drop that as a repeat of the first batch.
                # Refusing here turns a silent loss into a failure the caller
                # can act on by opening a new attempt.
                raise StreamProducerError(
                    f"producer {input.producer_id!r} attempt {input.attempt} resumed "
                    f"at batch {input.batch_index} on a handler that has no state for "
                    "it; open a new attempt"
                )
            delegate = self._stream(input.stream).producer(
                topic=input.stream.topic,
                producer_id=input.producer_id,
                attempt=input.attempt,
            )
            state = _ProducerState(delegate, 0, 0, None)
        elif input.batch_index == state.batch_index:
            # The last batch again: the store already holds it, and where it
            # landed is the answer the original got.
            self._producers.move_to_end(key)
            return AppendOutput(cursor=state.last_cursor)
        elif input.batch_index < state.batch_index:
            raise StreamProducerError(
                f"batch {input.batch_index} was written and is no longer the last "
                f"for producer {input.producer_id!r} attempt {input.attempt}"
            )
        elif input.batch_index != state.batch_index + 1:
            raise StreamProducerError(
                f"batch {input.batch_index} skips ahead of {state.batch_index} for "
                f"producer {input.producer_id!r} attempt {input.attempt}"
            )
        elif state.finished:
            raise StreamProducerError(
                f"producer {input.producer_id!r} attempt {input.attempt} already "
                "finished this topic; open a new attempt"
            )
        if input.sequence != state.sequence:
            raise StreamProducerError(
                f"sequence {input.sequence} does not continue at {state.sequence} for "
                f"producer {input.producer_id!r} attempt {input.attempt}"
            )
        payloads = self._payloads(input.payloads)
        cursor: str | None = None
        if payloads:
            appended = await state.delegate.append(*(RawValue(p) for p in payloads))
            cursor = appended.token if appended is not None else None
        if input.finish:
            await state.delegate.finish()
        # Recorded only once the store accepted the batch, so a failed append
        # is not mistaken for a repeat when the caller retries it. A finish is
        # recorded the same way rather than dropping the state: a lost
        # response would otherwise leave the caller with a batch it can
        # neither repeat nor abandon.
        state.batch_index = input.batch_index
        state.sequence += len(payloads)
        state.last_cursor = cursor
        state.finished = state.finished or bool(input.finish)
        self._producers[key] = state
        self._producers.move_to_end(key)
        while len(self._producers) > self._max_producers:
            self._producers.popitem(last=False)
        return AppendOutput(cursor=cursor)

    @staticmethod
    def _payloads(encoded: list[bytes] | None) -> list[Payload]:
        payloads = []
        for index, raw in enumerate(encoded or []):
            try:
                payloads.append(Payload.FromString(raw))
            except DecodeError as error:
                raise _bad_request(
                    f"payloads[{index}] is not a serialized Temporal Payload: {error}"
                ) from error
        return payloads

    @nexusrpc.handler.sync_operation
    async def read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        """Answer with the records after the caller's token, or time out.

        Raises:
            nexusrpc.HandlerError: ``BAD_REQUEST`` when the store refuses the
                token or cannot host the reference's owner, ``NOT_FOUND`` when
                it has no such owner.
        """
        try:
            return await self._read(ctx, input)
        except StreamError as error:
            raise _handler_error(error) from error
        except ValueError as error:
            raise _bad_request(str(error)) from error

    async def _read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        _check_ref(input.stream)
        topic = input.stream.topic
        stream = self._stream(input.stream)
        if input.latest_only:
            return ReadOutput(next_token=(await stream.latest(topic=topic)).token)
        max_records = (
            _DEFAULT_MAX_RECORDS if input.max_records is None else input.max_records
        )
        wait = (
            _DEFAULT_READ_WAIT.total_seconds()
            if input.wait_ms is None
            else input.wait_ms / 1000
        )
        if ctx.request_deadline is not None:
            # The server answers the caller with a timeout at the deadline
            # whatever this call does, so collecting past it only holds the
            # slot for an answer nobody receives.
            deadline = ctx.request_deadline
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            left = deadline - datetime.now(timezone.utc) - _DEADLINE_MARGIN
            wait = max(0.0, min(wait, left.total_seconds()))
        after = input.after_token or ""
        if after and input.last_n is not None:
            raise ValueError(
                "pass either after_token or last_n, not both: a token resumes a "
                "read and last_n starts one"
            )
        key = _stream_key(input.stream)
        await self._expire_subscriptions()
        lock = self._read_locks.setdefault(key, asyncio.Lock())
        async with lock:
            subscription = self._subscriptions.get(key)
            if (
                subscription is None
                or subscription.position != after
                or (not after and subscription.last_n != input.last_n)
            ):
                if subscription is not None:
                    await self._drop(key)
                subscription = self._subscribe(key, stream, topic, after, input.last_n)
            try:
                records, next_token = await self._drain(subscription, max_records, wait)
            except Exception:
                # The pump failed. Keeping the subscription would answer the
                # caller's retry from the same token with end of stream, so a
                # transient read failure would read as the stream ending.
                await self._drop(key)
                raise
            subscription.position = next_token
            subscription.last_used = time.monotonic()
            done = (
                subscription.done
                and subscription.records.empty()
                and subscription.failure is None
            )
            if done:
                # The store ended the read: the run or chain is closed and the
                # tail has been handed over. Nothing more will arrive on it.
                await self._drop(key)
        return ReadOutput(records=records, next_token=next_token, done=done)

    def _subscribe(
        self,
        key: _StreamKey,
        stream: StreamHandle,
        topic: str,
        after: str,
        last_n: int | None,
    ) -> _Subscription:
        # Raw payloads: the handler forwards what the store holds without
        # decoding it, so an encoding only the caller's codec understands
        # passes through untouched. A foreign token is refused right here.
        source = (
            stream.read(topic=topic, last=last_n, result_type=RawValue)
            if last_n is not None
            else stream.read(
                topic=topic,
                after=Cursor(after) if after else BEGINNING,
                result_type=RawValue,
            )
        )
        subscription = _Subscription(
            records=asyncio.Queue(maxsize=_QUEUE_DEPTH),
            position=after,
            last_used=time.monotonic(),
            last_n=last_n,
        )

        async def pump() -> None:
            try:
                async for record in source:
                    if record.kind is RecordKind.SUPERSEDED:
                        continue
                    await subscription.records.put(record)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                subscription.failure = error
            finally:
                subscription.done = True
                await source.aclose()

        subscription.pump = asyncio.create_task(pump())
        self._subscriptions[key] = subscription
        return subscription

    async def _drain(
        self, subscription: _Subscription, max_records: int, wait: float
    ) -> tuple[list[RecordWire], str]:
        records: list[RecordWire] = []
        next_token = subscription.position
        queue = subscription.records
        deadline = time.monotonic() + wait
        while len(records) < max_records:
            if queue.empty():
                remaining = deadline - time.monotonic()
                if records or remaining <= 0:
                    break
                if subscription.done:
                    if subscription.failure is None:
                        break
                    # Honour the wait even though nothing can arrive, so a
                    # caller polling a failed stream does not spin on the
                    # endpoint.
                    await asyncio.sleep(remaining)
                    break
                try:
                    record = await asyncio.wait_for(queue.get(), remaining)
                except asyncio.TimeoutError:
                    break
            else:
                record = queue.get_nowait()
            records.append(self._wire(record))
            next_token = record.cursor.token
        if not records and subscription.failure is not None:
            # Left on the subscription rather than cleared: the caller is
            # answered with the failure and the subscription is dropped, so
            # there is nothing for a second reader of it to be misled by.
            raise subscription.failure
        return records, next_token

    @staticmethod
    def _wire(record: StreamRecord[Any]) -> RecordWire:
        # The record read as RawValue carries the stored body untouched; the
        # default converter passes a RawValue through, so no encoding happens
        # here.
        wire = to_wire(
            temporalio.converter.DataConverter.default.payload_converter,
            topic=record.topic,
            kind=record.kind,
            value=record.value,
            producer_id=record.producer_id,
            attempt=record.attempt,
            sequence=record.sequence,
        )
        return RecordWire(token=record.cursor.token, record=wire.SerializeToString())

    async def close(self) -> None:
        """Release every parked read.

        Call it when the worker hosting this handler stops, so subscriptions
        waiting on the store do not outlive it.
        """
        for key in list(self._subscriptions):
            await self._drop(key)

    async def _drop(self, key: _StreamKey) -> None:
        subscription = self._subscriptions.pop(key, None)
        if subscription is None or subscription.pump is None:
            return
        subscription.pump.cancel()
        try:
            await subscription.pump
        except (asyncio.CancelledError, Exception):
            pass

    async def _expire_subscriptions(self) -> None:
        cutoff = time.monotonic() - self._subscription_idle
        for key in [k for k, s in self._subscriptions.items() if s.last_used < cutoff]:
            lock = self._read_locks.get(key)
            if lock is not None and lock.locked():
                continue
            await self._drop(key)


class _EndpointFailure(Exception):
    """What the transport saw: the endpoint's answer, or the reason it gave none."""

    def __init__(self, detail: str, status: int | None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status = status


def _translate(failure: _EndpointFailure) -> Exception:
    """The exception a caller raises for an endpoint failure.

    The handler names a stream condition by its class in the failure message,
    so the same class is raised here; anything else is an ``RPCError`` with
    the status the HTTP answer maps to, or ``UNAVAILABLE`` when there was none.
    """
    message = failure.detail
    try:
        parsed = json.loads(message)
        if isinstance(parsed, dict) and isinstance(parsed.get("message"), str):
            message = parsed["message"]
    except ValueError:
        pass
    if failure.status is not None:
        for name, cls in _STREAM_ERRORS.items():
            prefix = f"{name}: "
            if message.startswith(prefix):
                return cls(message[len(prefix) :])
        status = _HTTP_TO_RPC.get(failure.status, RPCStatusCode.UNKNOWN)
    else:
        status = RPCStatusCode.UNAVAILABLE
    return RPCError(message, status, b"")


def _post(
    url: str, body: bytes, headers: Mapping[str, str], timeout: timedelta
) -> bytes:
    timeout_ms = int(timeout.total_seconds() * 1000)
    # Everything is inside the try, because urllib wraps only the request in
    # URLError: a bad url raises from Request(), and a socket timeout or a
    # dropped connection raises from getresponse() and read() as
    # builtins.TimeoutError or an http.client exception. On 3.11 and later
    # TimeoutError is asyncio.TimeoutError, so letting one out would be
    # indistinguishable from the caller's own wait_for expiring.
    try:
        request = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                # Tells the server how long the handler may park, so it does
                # not time the call out ahead of a wait the caller asked for.
                "Request-Timeout": f"{timeout_ms}ms",
                **headers,
            },
            method="POST",
        )
        with urllib.request.urlopen(
            request, timeout=(timeout + _ROUND_TRIP_MARGIN).total_seconds()
        ) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise _EndpointFailure(
            error.read().decode(errors="replace"), error.code
        ) from error
    except urllib.error.URLError as error:
        raise _EndpointFailure(
            f"stream endpoint unreachable at {url}: {error.reason}", None
        ) from error
    except (TimeoutError, http.client.HTTPException, OSError, ValueError) as error:
        raise _EndpointFailure(
            f"stream endpoint at {url} did not answer: {error!r}", None
        ) from error


class _Front:
    """Everything a handle needs to reach one endpoint."""

    def __init__(self, streams: NexusStreams, client: Client | None) -> None:
        self._streams = streams
        self._client = client
        data_converter = streams._data_converter
        if data_converter is None:
            data_converter = (
                client.data_converter
                if client is not None
                else temporalio.converter.DataConverter.default
            )
        self.converter = data_converter.payload_converter
        self.codec = data_converter.payload_codec
        self.read_wait = streams._read_wait
        self.max_records = streams._max_records

    async def _base_url(self) -> str:
        endpoint_id = await self._streams._endpoint_id(self._client)
        return (
            f"{self._streams._http_address}/nexus/endpoints/{endpoint_id}"
            f"/services/{_SERVICE_NAME}"
        )

    async def invoke(
        self,
        operation: str,
        request: Any,
        output: type[_OutputT],
        *,
        timeout: timedelta,
    ) -> _OutputT:
        # The contract types carry their own JSON encoding, so the raw caller
        # and the worker serving the operation agree on the body without
        # either of them spelling the fields out. That encoding is a transfer
        # type hook, which only the internal converter applies.
        contract = (
            temporalio.converter.DataConverter.default._get_internal_payload_converter()
        )
        url = f"{await self._base_url()}/{operation}"
        try:
            raw = await asyncio.to_thread(
                _post,
                url,
                contract.to_payloads([request])[0].data,
                self._streams._headers,
                timeout,
            )
        except _EndpointFailure as failure:
            raise _translate(failure) from failure
        payload = Payload(metadata={"encoding": b"json/plain"}, data=raw or b"{}")
        return contract.from_payloads([payload], [output])[0]


class NexusProducer(Generic[T]):
    """Appends through the stream endpoint; the store behind it does the rest.

    Calls are serialized and the batch index is committed only once the
    endpoint answered. A batch whose call raised stays pending and is sent
    again under the same index, either when the caller retries the same
    values or ahead of whatever the caller sends next, so an ambiguous
    failure writes the batch once and loses nothing.
    """

    def __init__(
        self,
        front: _Front,
        stream: WireStreamRef,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to the stream ``stream`` names behind the endpoint."""
        self._front = front
        self._stream = stream
        self._producer_id = producer_id
        self._attempt = attempt
        self._batch_index = 0
        self._sequence = 0
        self._last: Cursor | None = BEGINNING
        self._pending: tuple[AppendInput, tuple[bytes, ...], bool] | None = None
        self._lock = asyncio.Lock()

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    async def append(self, *values: T) -> Cursor | None:
        """Append ``values`` through the endpoint and return where the last one landed.

        A repeat answers with the original's position and an empty call with
        the last one; ``None`` when the store behind the endpoint learns
        positions only at read time.
        """
        if not values:
            return self._last
        payloads = self._front.converter.to_payloads(list(values))
        answer = await self._call(payloads, finish=False)
        self._last = Cursor(answer.cursor) if answer.cursor else None
        return self._last

    async def finish(self) -> None:
        """Write ``FINISH`` for this producer on this topic."""
        await self._call([], finish=True)

    async def _call(self, payloads: list[Payload], *, finish: bool) -> AppendOutput:
        # Identity is taken before the codec runs: a codec may encrypt with a
        # fresh nonce each time, and the retry has to be recognised anyway.
        identity = tuple(payload.SerializeToString() for payload in payloads)
        if self._front.codec is not None:
            # The endpoint is the edge of this process, so a configured codec
            # runs here rather than at the handler: whoever hosts the endpoint
            # never holds the plaintext.
            payloads = await self._front.codec.encode(payloads)
        encoded = [payload.SerializeToString() for payload in payloads]
        async with self._lock:
            if self._pending is not None and self._pending[1:] != (identity, finish):
                # The caller moved on from a batch the endpoint may or may
                # not have applied. It goes first under its own index, so the
                # handler drops it if it landed and writes it if not, and the
                # new batch takes the index after it.
                await self._send(*self._pending)
            elif self._pending is not None:
                return await self._send(*self._pending)
            request = AppendInput(
                stream=self._stream,
                producer_id=self._producer_id,
                attempt=self._attempt,
                sequence=self._sequence,
                batch_index=self._batch_index + 1,
                payloads=encoded,
                finish=finish,
            )
            return await self._send(request, identity, finish)

    async def _send(
        self, request: AppendInput, identity: tuple[bytes, ...], finish: bool
    ) -> AppendOutput:
        self._pending = (request, identity, finish)
        answer = await self._front.invoke(
            _APPEND_OPERATION, request, AppendOutput, timeout=_APPEND_TIMEOUT
        )
        self._batch_index = request.batch_index
        self._sequence = request.sequence + len(request.payloads or []) + int(finish)
        self._pending = None
        return answer


class NexusStreamHandle:
    """One owner's stream through the endpoint, re-synthesizing supersession."""

    def __init__(self, front: _Front, address: _Address) -> None:
        """Address the owner ``address`` names; each call completes it with a topic."""
        self._front = front
        self._address = address

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` from where the read starts, one endpoint batch at a time.

        ``BEGINNING`` and ``last=`` are resolved by the store behind the
        endpoint on the first call. ``END`` is the endpoint's newest position,
        asked for with a ``latest_only`` call when the read starts, and the
        read resumes after it, so it yields only what is appended from then on.

        The token is opaque here, so a cursor from another store is refused by
        the store behind the endpoint and raises
        :class:`temporalio.streams.StreamCursorError` on the first iteration.

        ``aclose()`` on the result releases nothing at the endpoint at once.
        The contract carries no unsubscribe operation, so the handler cannot
        be told; it reclaims the parked read when it has gone idle, which is
        a minute by default. Until then the subscription and its long poll on
        the store stay, and a caller that stops and starts many reads on one
        topic should expect that lag rather than an immediate release.
        """
        check_read_start(after, last)
        topic, result_type = resolve_topic(topic, result_type)
        return self._read(topic, after, last, result_type)

    async def _read(
        self, topic: str, after: Cursor, last: int | None, result_type: type | None
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        if after == END:
            after = await self.latest(topic=topic)
        # Where last= lands is the store's to say, so BEGINNING stands in for
        # the position before it: a resume from it may repeat a record, where
        # anything later could skip one.
        decoder = RecordDecoder(
            self._front.converter, result_type, after=after, warn=logger.warning
        )
        token = after.token
        last_n = last
        stream = self._address.wire(topic)
        while True:
            answer = await self._front.invoke(
                _READ_OPERATION,
                ReadInput(
                    stream=stream,
                    after_token=token,
                    last_n=last_n,
                    max_records=self._front.max_records,
                    wait_ms=int(self._front.read_wait.total_seconds() * 1000),
                ),
                ReadOutput,
                timeout=self._front.read_wait + _ROUND_TRIP_MARGIN,
            )
            for wire in answer.records or []:
                cursor = Cursor(wire.token)
                try:
                    record = WireRecord.FromString(wire.record)
                except DecodeError as error:
                    # Same answer as every other reader: skip and say so.
                    logger.warning("skipping stream record at %s: %s", cursor, error)
                    continue
                if self._front.codec is not None and record.HasField("body"):
                    record.body.CopyFrom(
                        (await self._front.codec.decode([record.body]))[0]
                    )
                for out in decoder.decode(cursor, record):
                    yield out
            token = answer.next_token or token
            if token:
                last_n = None
            if answer.done:
                return

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The newest position on ``topic`` behind the endpoint, for following from now."""
        topic, _ = resolve_topic(topic)
        answer = await self._front.invoke(
            _READ_OPERATION,
            ReadInput(stream=self._address.wire(topic), latest_only=True),
            ReadOutput,
            timeout=_APPEND_TIMEOUT,
        )
        token = answer.next_token or ""
        return Cursor(token) if token else BEGINNING

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str = "",
        attempt: int = 0,
    ) -> NexusProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return NexusProducer(
            self._front, self._address.wire(topic), producer_id, attempt
        )

    def ref(self, *, topic: str | StreamTopic[Any] | None = None) -> StreamRef:
        """A :class:`temporalio.streams.StreamRef` to ``topic`` of this owner.

        Plain data naming the owner as this handle addresses it and the
        topic, with no cursor and no endpoint, so an operation can return it
        and whoever receives it opens it with ``client.get_stream_handle(ref)``
        on the front or on the store itself.
        """
        name, _ = resolve_topic(topic)
        return self._address.ref(name)

    async def close(self) -> None:
        """Refused: the contract carries no operation that seals a stream.

        Raises:
            ValueError: The handle is on a workflow's or an activity's stream,
                which ends with its owner.
            StreamUnsupportedError: The handle is on a standalone stream; seal
                it on the store's own handle.
        """
        if self._address.kind != "standalone":
            raise ValueError(
                "only a standalone stream can be closed; this handle is on an owner's "
                "stream, which ends when the owner does"
            )
        raise StreamUnsupportedError(
            "the stream endpoint has no operation that seals a standalone stream; "
            "close it on the store's own handle"
        )


class NexusStreams(StreamProvider, temporalio.client.Plugin):
    """The outside half of a provider, over one Nexus endpoint.

    A client plugin but not a worker plugin: a workflow publishes and reads
    through the storage provider its worker was given, and this front only
    serves code outside a workflow, so ``Client.connect(plugins=[front])``
    makes ``client.get_stream_handle()`` go through the endpoint while a
    worker built from that client is left without a provider. The endpoint
    hides which store sits behind it.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        http_address: str = "http://127.0.0.1:7243",
        headers: Mapping[str, str] | None = None,
        read_wait: timedelta = _DEFAULT_READ_WAIT,
        max_records: int = _DEFAULT_MAX_RECORDS,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> None:
        """Point the front at one endpoint.

        Args:
            endpoint: The Nexus endpoint's name, resolved to its id through
                the operator service of the client a handle is opened with.
                When a handle is opened without a client it has to be the id,
                because the HTTP ingress dispatches by id.
            http_address: The server's Nexus HTTP ingress.
            headers: Sent on every request; where an authorization header
                belongs.
            read_wait: How long the handler may park a read before answering
                with what it has.
            max_records: The most records one read answer carries.
            data_converter: Overrides the client's converter for record
                bodies. A payload codec configured here runs on this side of
                the endpoint, so records are encoded before they leave the
                process and the handler's worker never holds the key.
        """
        if not endpoint:
            raise ValueError("endpoint must not be empty")
        # Checked here rather than on the first read, because the contract
        # refuses an out-of-range value with a payload validation error that
        # is neither a StreamError nor an RPCError, a long way from the line
        # that got it wrong.
        if not _MIN_READ_WAIT <= read_wait <= _MAX_READ_WAIT:
            raise ValueError(
                f"read_wait must be between {_MIN_READ_WAIT} and {_MAX_READ_WAIT}, "
                f"got {read_wait}"
            )
        if read_wait.microseconds % 1000:
            raise ValueError(
                f"read_wait is carried in whole milliseconds, got {read_wait}"
            )
        if not _MIN_MAX_RECORDS <= max_records <= _MAX_MAX_RECORDS:
            raise ValueError(
                f"max_records must be between {_MIN_MAX_RECORDS} and "
                f"{_MAX_MAX_RECORDS}, got {max_records}"
            )
        self._endpoint = endpoint
        self._http_address = http_address.rstrip("/")
        self._headers = dict(headers or {})
        self._read_wait = read_wait
        self._max_records = max_records
        self._data_converter = data_converter
        self._resolved: str | None = None
        self._resolving = asyncio.Lock()

    def workflow_provider(self) -> NoReturn:
        """Raise: this front has no workflow half."""
        raise StreamUnsupportedError(_WORKFLOW_SIDE_ERROR)

    def get_stream_handle(
        self, client: Client | None, workflow_id: str, *, run_id: str | None = None
    ) -> NexusStreamHandle:
        """A handle on ``workflow_id``'s stream through the endpoint.

        ``client`` is not used for transport: it resolves the endpoint's name
        and supplies the data converter when none was configured.
        """
        return NexusStreamHandle(
            _Front(self, client), _Address("workflow", workflow_id, run_id)
        )

    def get_activity_stream_handle(
        self,
        client: Client | None,
        activity_id: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
    ) -> NexusStreamHandle:
        """A handle on the streams ``activity_id`` owns, through the endpoint.

        The reference carries the activity owner across; whether the store
        behind the endpoint can host it is the store's answer, and a refusal
        reaches the caller as :class:`temporalio.streams.StreamUnsupportedError`
        on the first call.
        """
        return NexusStreamHandle(
            _Front(self, client),
            _Address("activity", workflow_id, run_id, activity_id),
        )

    def get_standalone_stream_handle(
        self, client: Client | None, stream_id: str
    ) -> NexusStreamHandle:
        """A handle on the standalone stream ``stream_id``, through the endpoint.

        The reference carries the standalone owner across; the store behind
        the endpoint answers whether it hosts one, and a refusal reaches the
        caller as :class:`temporalio.streams.StreamUnsupportedError` on the
        first call.
        """
        if not stream_id:
            raise ValueError("stream_id must not be empty")
        return NexusStreamHandle(
            _Front(self, client), _Address("standalone", stream_id=stream_id)
        )

    async def create_standalone_stream(
        self,
        client: Client | None,
        stream_id: str,
        *,
        retention: timedelta | None = None,
        max_records: int | None = None,
        max_bytes: int | None = None,
    ) -> NoReturn:
        """Refused: the contract carries no operation that creates a stream.

        Raises:
            StreamUnsupportedError: Always. Create the stream on the store
                behind the endpoint and hand its reference to callers.
        """
        raise StreamUnsupportedError(
            "the stream endpoint has no operation that creates a standalone stream; "
            "create it on the store behind the endpoint and pass its StreamRef"
        )

    async def close(self) -> None:
        """Nothing to release: each call opens and closes its own connection."""

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Set this front as the client's ``stream_provider``."""
        config["stream_provider"] = self
        return config

    async def connect_service_client(
        self,
        config: ConnectConfig,
        next: Callable[[ConnectConfig], Awaitable[ServiceClient]],
    ) -> ServiceClient:
        """Connect unchanged."""
        return await next(config)

    async def _endpoint_id(self, client: Client | None) -> str:
        if self._resolved is not None:
            return self._resolved
        async with self._resolving:
            if self._resolved is None:
                if client is None:
                    self._resolved = self._endpoint
                else:
                    found = await client.operator_service.list_nexus_endpoints(
                        ListNexusEndpointsRequest(name=self._endpoint)
                    )
                    if not found.endpoints:
                        raise ValueError(
                            f"no nexus endpoint is named {self._endpoint!r}"
                        )
                    self._resolved = found.endpoints[0].id
        return self._resolved


# ---------------------------------------------------------------------------
# A Nexus operation as a stream consumer.
# ---------------------------------------------------------------------------

NOTIFICATION_CHANNEL_HEADER = "Temporal-Notification-Channel"
"""The header the server adds to a channel's callback delivery, naming the channel."""

STREAM_CONSUMER_TOKEN_HEADER = "Temporal-Stream-Consumer-Token"
"""The header a consumer's listener registration carries, naming the operation.

The server sends a listener's own headers back on every delivery, so this is
how one delivery URL serves any number of operations.
"""

_COMPLETION_TIMEOUT = timedelta(seconds=30)
_CLOSED_KEY = "closed"
_FAILURE_CONTENT_TYPE = "application/json"
_CONTENT_TYPE_HEADER = "Content-Type"
_STATE_HEADER = "Nexus-Operation-State"
_TOKEN_HEADER = "Nexus-Operation-Token"
_START_TIME_HEADER = "Nexus-Operation-Start-Time"


ConsumeFunction = Callable[[StreamRecord[Any], S], "S | Awaitable[S]"]
ChannelRule = Callable[
    [StreamRef], "temporalio.client.ChannelAddress | tuple[str, str | None]"
]


def _address(where: Any) -> tuple[str, str | None]:
    """The channel name and the owner a rule answered with, as an address or a pair."""
    channel = getattr(where, "channel", None)
    if isinstance(channel, str):
        return channel, getattr(where, "workflow_id", None)
    channel, owner = where
    return channel, owner


@dataclass(frozen=True)
class ConsumerState(Generic[S]):
    """What a consumer operation holds for one token, read-only.

    .. warning::
       This API is experimental and unstable.
    """

    ref: StreamRef
    """The stream being consumed."""

    channel: str
    """The channel the listener is registered on."""

    owner: str | None
    """The workflow a linked channel belongs to, ``None`` for an independent one."""

    listener_id: str
    """The listener id the server assigned."""

    cursor: Cursor
    """Where the next read starts. :data:`BEGINNING` until a record was consumed."""

    value: S
    """What the consume function has folded the records into so far."""

    reads: int
    """How many read passes reached the store, the start's included."""

    deliveries: int
    """How many notifications were delivered for this operation."""

    records: int
    """How many records the consume function has been handed."""


@dataclass(frozen=True)
class Delivery:
    """What one delivered notification led to.

    .. warning::
       This API is experimental and unstable.
    """

    token: str | None
    """The operation the delivery named, when the headers carried one."""

    known: bool
    """Whether this process holds that operation. A delivery for one it does not is ignored."""

    read: bool
    """Whether the delivery led to a read. One that brought nothing new does not."""

    records: int
    """How many records that read handed to the consume function."""

    completed: bool
    """Whether the delivery closed the operation."""


@dataclass
class _Consumption(Generic[S]):
    ref: StreamRef
    channel: str
    owner: str | None
    listener_id: str
    callback_url: str | None
    callback_headers: dict[str, str]
    started: datetime
    value: S
    cursor: Cursor = BEGINNING
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    reads: int = 0
    deliveries: int = 0
    records: int = 0
    finished: bool = False
    done: bool = False
    opening: asyncio.Task[None] | None = None


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


def _parse_notification(body: bytes | str | Mapping[str, Any]) -> Notification:
    """The notification a delivery carries, from the JSON the server posts."""
    text = json.dumps(body) if isinstance(body, Mapping) else body
    if isinstance(text, bytes):
        text = text.decode()
    proto = json_format.Parse(
        text, temporalio.api.notification.v1.Notification(), ignore_unknown_fields=True
    )
    return Notification._from_proto(proto)


def _payload_content(payload: Payload) -> tuple[dict[str, str], bytes]:
    """The HTTP content headers and body that carry ``payload`` to the server.

    The mapping is the server's: a plain JSON payload travels as JSON, a null
    one as an empty body with no type, and anything else, a codec's output
    included, as the serialized payload itself.
    """
    metadata = {key: value.decode() for key, value in payload.metadata.items()}
    encoding = metadata.get("encoding")
    if set(metadata) == {"encoding"}:
        if encoding == "json/plain":
            return {_CONTENT_TYPE_HEADER: "application/json"}, payload.data
        if encoding == "binary/plain":
            return {_CONTENT_TYPE_HEADER: "application/octet-stream"}, payload.data
        if encoding == "binary/null":
            return {}, b""
    return (
        {_CONTENT_TYPE_HEADER: "application/x-temporal-payload"},
        payload.SerializeToString(),
    )


def _post_completion(
    url: str, body: bytes, headers: Mapping[str, str], timeout: timedelta
) -> None:
    """Post an operation completion to the caller's callback URL.

    Unlike :func:`_post` this sets no content type of its own, because a null
    result travels without one.

    Raises:
        _EndpointFailure: The callback answered with an error or not at all.
    """
    try:
        request = urllib.request.Request(
            url, data=body or None, headers=dict(headers), method="POST"
        )
        with urllib.request.urlopen(request, timeout=timeout.total_seconds()):
            return
    except urllib.error.HTTPError as error:
        raise _EndpointFailure(
            error.read().decode(errors="replace"), error.code
        ) from error
    except urllib.error.URLError as error:
        raise _EndpointFailure(
            f"completion callback unreachable at {url}: {error.reason}", None
        ) from error
    except (TimeoutError, http.client.HTTPException, OSError, ValueError) as error:
        raise _EndpointFailure(
            f"completion callback at {url} did not answer: {error!r}", None
        ) from error


class StreamConsumerOperation(nexusrpc.handler.OperationHandler[StreamRef, S]):
    """An asynchronous operation that consumes the stream its input names.

    The caller starts it with a :class:`temporalio.streams.StreamRef` and
    awaits its result. On start the handler registers a callback listener on
    the stream's notification channel, with the URL the hosting process
    serves and this operation's token in a header, and reads the stream from
    its start through the client's provider. Every notification the server
    delivers leads to one read from the cursor to the head, however many
    writes the channel folded into it, and a delivery that brings nothing new
    reads nothing. The operation completes through the caller's completion
    callback when the notification says the stream closed, when the read
    reaches ``FINISH`` or when the store ends the read, with what the consume
    function folded the records into. Cancel unregisters the listener and
    reports the operation canceled.

    The consume function is a reducer: it takes a record and the value so far
    and returns the new value, awaitable or not. It is handed every record
    kind and narrows on ``kind`` itself. An exception from it fails the
    operation. The cursor and the value live in this process keyed by the
    operation token, so a process that loses them loses the operation; a
    durable cursor is a follow-up.

    .. warning::
       This API is experimental and unstable.
    """

    def __init__(
        self,
        consume: ConsumeFunction[S],
        *,
        initial: Callable[[], S],
        listener_url: str,
        client: Client | None = None,
        channel_for: ChannelRule = temporalio.client.stream_channel,
        result_type: type | None = None,
        token_header: str = STREAM_CONSUMER_TOKEN_HEADER,
    ) -> None:
        """Consume with ``consume``, served at ``listener_url``.

        Args:
            consume: The reducer each record is handed with the value so far.
            initial: Makes the value an operation starts with.
            listener_url: Where the server posts the channel's notifications.
                The hosting process serves it and feeds each request to
                :meth:`deliver`.
            client: Registers the listener and opens the stream. Leave it
                unset in a handler hosted by a Temporal worker, where the
                operation context's client is used.
            channel_for: Names the channel for a ref and the workflow a
                linked channel belongs to, as a
                :class:`temporalio.client.ChannelAddress` or a
                ``(channel, workflow_id)`` pair. The default is
                :func:`temporalio.client.stream_channel`, the server's rule
                for native streams; a store that names channels its own
                way passes its rule.
            result_type: What record values are decoded as.
            token_header: The header carrying the operation token on the
                registration and so on every delivery.
        """
        if not listener_url:
            raise ValueError("listener_url must not be empty")
        self._consume = consume
        self._initial = initial
        self._listener_url = listener_url
        self._given_client = client
        self._channel_for = channel_for
        self._result_type = result_type
        self._token_header = token_header
        self._states: dict[str, _Consumption[S]] = {}

    @property
    def tokens(self) -> list[str]:
        """The operations this process holds, oldest first."""
        return list(self._states)

    def state(self, token: str) -> ConsumerState[S] | None:
        """What is held for ``token``, or ``None`` once it completed or never was."""
        held = self._states.get(token)
        if held is None:
            return None
        return ConsumerState(
            ref=held.ref,
            channel=held.channel,
            owner=held.owner,
            listener_id=held.listener_id,
            cursor=held.cursor,
            value=held.value,
            reads=held.reads,
            deliveries=held.deliveries,
            records=held.records,
        )

    def _client(self) -> Client:
        if self._given_client is not None:
            return self._given_client
        return temporalio.nexus.client()

    async def start(
        self, ctx: nexusrpc.handler.StartOperationContext, input: StreamRef
    ) -> nexusrpc.handler.StartOperationResultAsync:
        """Register as the stream's listener and start reading it.

        The read runs after the start answers, so a long stream does not hold
        the start request, and the first delivery waits on it.
        """
        client = self._client()
        token = uuid.uuid4().hex
        channel, owner = _address(self._channel_for(input))
        # Registered before the first read: a record appended between the
        # read and the registration would otherwise be missed, where one
        # appended between the registration and the read is read twice at
        # worst, once by the read and once by the delivery it provokes, and
        # the cursor makes the second a no-op.
        listener_id = await client.register_channel_listener(
            channel,
            Callback(url=self._listener_url, headers={self._token_header: token}),
            workflow_id=owner,
        )
        state: _Consumption[S] = _Consumption(
            ref=input,
            channel=channel,
            owner=owner,
            listener_id=listener_id,
            callback_url=ctx.callback_url,
            callback_headers=dict(ctx.callback_headers),
            started=datetime.now(timezone.utc),
            value=self._initial(),
        )
        self._states[token] = state
        state.opening = asyncio.create_task(self._open(token, state, client))
        return nexusrpc.handler.StartOperationResultAsync(token)

    async def _open(self, token: str, state: _Consumption[S], client: Client) -> None:
        try:
            async with state.lock:
                if state.done:
                    return
                await self._catch_up(token, state, client)
                if state.finished and not state.done:
                    await self._complete(token, state, client)
        except Exception:
            # The next delivery reads again from the cursor, so a failure here
            # costs nothing but the records it would have consumed early.
            logger.exception("the first read of %s failed", state.ref)

    async def deliver(
        self, headers: Mapping[str, str], body: bytes | str | Mapping[str, Any]
    ) -> Delivery:
        """Act on one notification the server posted to the listener URL.

        ``headers`` are the request's, in any case; ``body`` is the
        ``Notification`` as protobuf JSON, raw or already parsed. The delivery
        is matched to its operation by the token header. Reading from the
        cursor to the head happens under the operation's lock, so a delivery
        that arrives during the first read waits for it and then finds the
        cursor at the head.

        A failure to read raises, so the hosting process answers the server
        with an error and the server retries the delivery; a failure in the
        consume function fails the operation instead.
        """
        token = _header(headers, self._token_header)
        state = self._states.get(token) if token is not None else None
        if token is None or state is None:
            logger.warning(
                "ignoring a channel delivery for an operation this process does not hold"
            )
            return Delivery(token, known=False, read=False, records=0, completed=False)
        client = self._client()
        notification = _parse_notification(body)
        closed = await self._closed(notification, client)
        async with state.lock:
            if state.done:
                return Delivery(token, True, read=False, records=0, completed=True)
            state.deliveries += 1
            reads_before = state.reads
            records = await self._catch_up(token, state, client)
            completed = False
            if (closed or state.finished) and not state.done:
                await self._complete(token, state, client)
                completed = True
        return Delivery(
            token,
            True,
            read=state.reads > reads_before,
            records=records,
            completed=completed,
        )

    async def _closed(self, notification: Notification, client: Client) -> bool:
        payload = notification.metadata.get(_CLOSED_KEY)
        if payload is None:
            return False
        try:
            converter = client.data_converter
            if converter.payload_codec is not None:
                [payload] = await converter.payload_codec.decode([payload])
            return bool(converter.payload_converter.from_payload(payload, bool))
        except Exception:
            logger.warning(
                "could not decode the notification's closed flag", exc_info=True
            )
            return False

    async def _catch_up(
        self, token: str, state: _Consumption[S], client: Client
    ) -> int:
        """Read from the cursor to the head, consuming, and return how many records."""
        handle = client.get_stream_handle(state.ref)
        head = await handle.latest()
        if head == state.cursor:
            return 0
        state.reads += 1
        source = handle.read(after=state.cursor, result_type=self._result_type)
        count = 0
        exhausted = True
        try:
            async for record in source:
                try:
                    value = self._consume(record, state.value)
                    if inspect.isawaitable(value):
                        value = await value
                except Exception as error:
                    await self._complete(token, state, client, error=error)
                    return count
                state.value = cast(S, value)
                state.cursor = record.cursor
                state.records += 1
                count += 1
                if record.kind is RecordKind.FINISH:
                    state.finished = True
                    exhausted = False
                    break
                if record.cursor == head:
                    exhausted = False
                    break
        finally:
            await source.aclose()
        if exhausted:
            # The store ended the read: nothing more will arrive on it.
            state.finished = True
        return count

    async def _complete(
        self,
        token: str,
        state: _Consumption[S],
        client: Client,
        *,
        error: BaseException | None = None,
    ) -> None:
        state.done = True
        if error is None:
            await self._post_result(token, state, client)
        else:
            await self._post_failure(
                token, state, "failed", f"{type(error).__name__}: {error}"
            )
        await self._unregister(client, state)
        self._states.pop(token, None)

    async def cancel(
        self, ctx: nexusrpc.handler.CancelOperationContext, token: str
    ) -> None:
        """Unregister the listener and report the operation canceled.

        Raises:
            nexusrpc.HandlerError: ``NOT_FOUND`` when this process holds no
                operation under ``token``.
        """
        del ctx
        state = self._states.get(token)
        if state is None:
            raise nexusrpc.HandlerError(
                "no stream consumer operation is held under this token",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
                retryable_override=False,
            )
        client = self._client()
        if state.opening is not None and not state.opening.done():
            state.opening.cancel()
        async with state.lock:
            if state.done:
                return
            state.done = True
            await self._unregister(client, state)
            await self._post_failure(
                token, state, "canceled", "the caller canceled the stream consumer"
            )
            self._states.pop(token, None)

    async def close(self) -> None:
        """Unregister every listener this process still holds, completing nothing.

        Call it when the hosting process stops, so the server does not keep
        posting to a URL nobody serves.
        """
        client = self._given_client
        for token, state in list(self._states.items()):
            if state.opening is not None and not state.opening.done():
                state.opening.cancel()
            state.done = True
            if client is not None:
                await self._unregister(client, state)
            self._states.pop(token, None)

    async def _unregister(self, client: Client, state: _Consumption[S]) -> None:
        try:
            await client.unregister_channel_listener(
                state.channel, state.listener_id, workflow_id=state.owner
            )
        except RPCError:
            logger.warning(
                "could not unregister listener %s from channel %s",
                state.listener_id,
                state.channel,
                exc_info=True,
            )

    async def _post_result(
        self, token: str, state: _Consumption[S], client: Client
    ) -> None:
        converter = client.data_converter
        [payload] = converter.payload_converter.to_payloads([state.value])
        if converter.payload_codec is not None:
            [payload] = await converter.payload_codec.encode([payload])
        headers, body = _payload_content(payload)
        await self._post(token, state, "succeeded", headers, body)

    async def _post_failure(
        self, token: str, state: _Consumption[S], outcome: str, message: str
    ) -> None:
        await self._post(
            token,
            state,
            outcome,
            {_CONTENT_TYPE_HEADER: _FAILURE_CONTENT_TYPE},
            json.dumps({"message": message}).encode(),
        )

    async def _post(
        self,
        token: str,
        state: _Consumption[S],
        outcome: str,
        content: Mapping[str, str],
        body: bytes,
    ) -> None:
        if not state.callback_url:
            logger.warning(
                "operation %s %s with no completion callback to tell", token, outcome
            )
            return
        headers = {
            **state.callback_headers,
            _TOKEN_HEADER: token,
            _STATE_HEADER: outcome,
            _START_TIME_HEADER: email.utils.format_datetime(state.started, usegmt=True),
            **content,
        }
        try:
            await asyncio.to_thread(
                _post_completion, state.callback_url, body, headers, _COMPLETION_TIMEOUT
            )
        except _EndpointFailure:
            logger.exception("could not complete operation %s as %s", token, outcome)


def stream_consumer_operation(
    consume: ConsumeFunction[S],
    *,
    initial: Callable[[], S],
    listener_url: str,
    client: Client | None = None,
    channel_for: ChannelRule = temporalio.client.stream_channel,
    result_type: type | None = None,
    token_header: str = STREAM_CONSUMER_TOKEN_HEADER,
) -> StreamConsumerOperation[S]:
    """Turn ``consume`` into an asynchronous operation handler that consumes a stream.

    A service handler returns the result from an operation handler factory,
    and the hosting process feeds the deliveries it receives at
    ``listener_url`` to the same object's :meth:`StreamConsumerOperation.deliver`.
    See :class:`StreamConsumerOperation` for the arguments and the contract.
    """
    return StreamConsumerOperation(
        consume,
        initial=initial,
        listener_url=listener_url,
        client=client,
        channel_for=channel_for,
        result_type=result_type,
        token_header=token_header,
    )
