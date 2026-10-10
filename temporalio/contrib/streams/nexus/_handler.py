"""The stream service's handler over any stream provider."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import nexusrpc
import nexusrpc.handler
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError

import temporalio.api.failure.v1
import temporalio.converter
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import NexusHandlerErrorRetryBehavior
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._provider import StreamHandle, StreamProvider
from temporalio.contrib.streams._record import Cursor, RecordKind, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._wire import to_wire
from temporalio.contrib.streams.nexus._generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
)

__all__ = ["StreamAccess", "TemporalStreamsHandler"]

logger = logging.getLogger(__name__)

_DEFAULT_MAX_RECORDS = 100
# A read answer is a sync Nexus result, recorded in the caller's History, so it
# stays well below the server's blob size limit (2 MiB by default).
_DEFAULT_MAX_ANSWER_BYTES = 1 << 20
# One record must fit a read result on its own, so a record above this can never
# be read through the service.
_DEFAULT_MAX_RECORD_BYTES = 3 << 19
# What a body gains inside its stored record: topic, producer, attempt,
# sequence and the content hash.
_RECORD_OVERHEAD = 256
# How long a read waits for records the store already holds to be fetched and
# decoded, such as behind a slow payload codec.
_CATCH_UP_LIMIT = 10.0
# Room left inside the request deadline for the answer to travel back.
_DEADLINE_MARGIN = timedelta(milliseconds=500)

# The Nexus error type each stream condition crosses as. The condition's class
# name leads the message and is the cause's application error type, so a
# caller in any language can raise the class the store raised.
_ERROR_TYPES: tuple[tuple[type[StreamError], nexusrpc.HandlerErrorType, bool], ...] = (
    (StreamNotFoundError, nexusrpc.HandlerErrorType.NOT_FOUND, False),
    (StreamCursorError, nexusrpc.HandlerErrorType.BAD_REQUEST, False),
    (StreamProducerError, nexusrpc.HandlerErrorType.BAD_REQUEST, False),
    (StreamClosedError, nexusrpc.HandlerErrorType.BAD_REQUEST, False),
    (StreamUnsupportedError, nexusrpc.HandlerErrorType.NOT_IMPLEMENTED, False),
    # Any other refusal, such as a store out of memory: nothing was written,
    # and the same call gets the same answer until the store changes.
    (StreamRefusedError, nexusrpc.HandlerErrorType.INTERNAL, False),
    # The write may have landed. Repeating the same call is safe, because
    # the store deduplicates it, so the caller is told to retry.
    (StreamOutcomeUnknownError, nexusrpc.HandlerErrorType.UNAVAILABLE, True),
    (StreamStorageError, nexusrpc.HandlerErrorType.UNAVAILABLE, True),
)

# The Nexus failure metadata type under which the details are a
# temporal.api.failure.v1.Failure, which the failure converter reads back as is.
_TEMPORAL_FAILURE_TYPE = "temporal.api.failure.v1.Failure"

# The default converter passes a RawValue through untouched. The codec runs
# in the provider, with the Worker client's data converter: it encodes a body
# on the way into the store and decodes it on the way out.
_RAW_CONVERTER = temporalio.converter.DataConverter.default.payload_converter


def _handler_error(error: ValueError | StreamError) -> nexusrpc.HandlerError:
    if isinstance(error, ValueError):
        kind, retryable, name = (
            nexusrpc.HandlerErrorType.BAD_REQUEST,
            False,
            "ValueError",
        )
    else:
        kind, retryable = nexusrpc.HandlerErrorType.INTERNAL, True
        for error_class, error_kind, error_retryable in _ERROR_TYPES:
            if isinstance(error, error_class):
                kind, retryable = error_kind, error_retryable
                break
        name = type(error).__name__
    message = f"{name}: {error}"
    # The failure that crosses is spelled out here rather than derived from the
    # raised error, so the handler's stack frames stay on this side.
    failure = temporalio.api.failure.v1.Failure(
        cause=temporalio.api.failure.v1.Failure(
            message=str(error),
            application_failure_info=temporalio.api.failure.v1.ApplicationFailureInfo(
                type=name, non_retryable=not retryable
            ),
        ),
        nexus_handler_failure_info=temporalio.api.failure.v1.NexusHandlerFailureInfo(
            type=kind.name,
            retry_behavior=(
                NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
                if retryable
                else NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
            ),
        ),
    )
    return nexusrpc.HandlerError(
        message,
        type=kind,
        retryable_override=retryable,
        original_failure=nexusrpc.Failure(
            message=message,
            metadata={"type": _TEMPORAL_FAILURE_TYPE},
            details=MessageToDict(failure),
        ),
    )


def _crossing_size(size: int) -> int:
    """The bytes ``size`` record bytes take in a read result, as base64."""
    return 4 * ((size + 2) // 3)


@dataclasses.dataclass(frozen=True)
class StreamAccess:
    """One call the stream service is about to serve, for an authorizer.

    .. warning::
        This API is experimental.
    """

    operation: Literal["append", "read"]
    """What the caller asks to do."""

    stream: StreamRef
    """The stream the call names."""

    context: nexusrpc.handler.StartOperationContext
    """The Nexus call, with the caller's headers."""

    producer_id: str | None = None
    """The producer an append writes as. ``None`` for a read."""

    attempt: int | None = None
    """The producer attempt an append writes. ``None`` for a read."""


_StreamKey = tuple[str, str, str, str]


def _stream_key(ref: StreamRef) -> _StreamKey:
    return (ref.kind, ref.workflow_id, ref.run_id or "", ref.topic)


@dataclasses.dataclass(eq=False)
class _Subscription:
    """One open read on the store, carried from one read call to the next.

    It sits idle between calls under the cursor its last answer handed out,
    so the reader's next call, which resumes from that cursor, continues it
    instead of opening the store's read again.
    """

    key: _StreamKey
    records: AsyncGenerator[StreamRecord[Any], None]
    position: str
    handle: StreamHandle
    pending: asyncio.Task[StreamRecord[Any]] | None = None
    """A record fetch still in flight when the last call ran out of time. The
    next call waits on it rather than starting another, so a record the store
    already handed over is never dropped."""
    expiry: asyncio.TimerHandle | None = None
    ended: bool = False
    fetched: str = ""
    """The position of the last record fetched from the store, whether or not
    an answer carried it yet."""
    carried: RecordWire | None = None
    """A fetched record the last answer had no room for. It leads the next."""


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream service over ``provider``.

    Register it on a Worker with ``nexus_service_handlers=[handler]`` and
    point a Nexus endpoint at the Worker's task queue. The Worker's client
    opens the streams, so a stream lives in the Worker's namespace. That
    client carries the namespace's payload codec, the one the stream's
    producers and readers use: the store keeps each body encoded with it, a
    read decodes the bodies, and the read result crosses encoded as a whole
    like any Nexus result, so Workflow code that reads gets plain bodies with
    no codec of its own.

    A reader's subscription is kept between its calls and found again by the
    cursor its last answer handed out, so readers never share one and a
    second reader cannot take over the first's. A subscription no call has
    used for ``idle_timeout`` is closed, and beyond ``max_idle_subscriptions``
    the longest idle is closed first; a reader whose subscription was closed
    is answered from a fresh one on its next call, from its own cursor, so
    nothing is lost. Retries are deduplicated by the store, not here: a call
    carries the writer's sequence and the provider compares it with what it
    holds.

    A read answers with at most ``max_records`` records and, past its first
    record, at most ``max_answer_bytes`` of records as they cross, so a read's
    result stays below the server's blob size limit; a reader reads again
    from the cursor it got. A read with ``wait_ms`` 0 waits for no new record,
    but answers with every record the store held when it started, up to those
    limits.

    The handler writes and reads with its Worker's client and that client's
    store credentials, whatever caller sent the call. So every caller that
    may reach the endpoint gets the Worker's store rights, for every stream in
    the namespace and as any producer. Pass ``authorize`` to check each call:
    it gets a :class:`StreamAccess` and answers whether to serve it, and a
    refused call fails as unauthorized.

    A record whose bytes would take more than ``max_record_bytes`` in a read
    result can never be read through the service, so an append with one is
    refused. A record already in the store that is larger, written there
    directly, crosses as an error at its cursor, which a reader goes on past.
    Sizes count what crosses: the record bytes as base64, about 4/3 of them.

    Parked reads hold Nexus task slots for up to their ``wait_ms``. With many
    readers, serve appends and reads on separate task queues or endpoints, so
    an append doesn't wait for a slot behind parked reads.

    Give the Worker more Nexus pollers than the default
    (``nexus_task_poller_behavior``). A Nexus request is matched only to a
    waiting poll, and with the default five pollers spread over a task queue's
    partitions, an append was seen to wait behind parked reads until one of
    them answered.

    Call :meth:`close` when the Worker stops.
    """

    def __init__(
        self,
        provider: StreamProvider,
        *,
        idle_timeout: timedelta = timedelta(minutes=1),
        max_idle_subscriptions: int = 1000,
        max_answer_bytes: int = _DEFAULT_MAX_ANSWER_BYTES,
        max_record_bytes: int = _DEFAULT_MAX_RECORD_BYTES,
        authorize: Callable[[StreamAccess], Awaitable[bool]] | None = None,
    ) -> None:
        """Serve ``provider``.

        Raises:
            ValueError: ``idle_timeout`` is not positive,
                ``max_idle_subscriptions`` is below zero, or
                ``max_answer_bytes`` or ``max_record_bytes`` is not positive.
        """
        if idle_timeout <= timedelta(0):
            raise ValueError("idle_timeout must be positive")
        if max_idle_subscriptions < 0:
            raise ValueError("max_idle_subscriptions must not be negative")
        if max_answer_bytes <= 0:
            raise ValueError("max_answer_bytes must be positive")
        if max_record_bytes <= 0:
            raise ValueError("max_record_bytes must be positive")
        self._max_answer_bytes = max_answer_bytes
        self._max_record_bytes = max_record_bytes
        self._authorize = authorize
        self._closed = False
        self._provider = provider
        self._idle_timeout = idle_timeout.total_seconds()
        self._max_idle = max_idle_subscriptions
        # Idle subscriptions by the cursor a reader resumes from, oldest first.
        # Several can sit under one cursor when readers are at the same place.
        self._idle: OrderedDict[_Subscription, tuple[_StreamKey, str]] = OrderedDict()
        self._closing: set[asyncio.Task[None]] = set()

    @nexusrpc.handler.sync_operation
    async def append(
        self, ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Write one batch, or finish the producer, through the provider."""
        await self._check(
            StreamAccess(
                "append",
                input.stream,
                ctx,
                producer_id=input.producer_id,
                attempt=input.attempt,
            )
        )
        try:
            return await self._append(input)
        except (ValueError, StreamError) as error:
            raise _handler_error(error)

    @nexusrpc.handler.sync_operation
    async def read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        """Answer with the records after the caller's cursor, waiting for some."""
        await self._check(StreamAccess("read", input.stream, ctx))
        try:
            return await self._read(ctx, input)
        except (ValueError, StreamError) as error:
            raise _handler_error(error)

    async def close(self) -> None:
        """Close every idle subscription and wait for those already closing.

        A call still running releases its subscription when it answers.
        """
        self._closed = True
        for subscription in list(self._idle):
            self._release(subscription)
        if self._closing:
            await asyncio.gather(*self._closing, return_exceptions=True)

    async def _check(self, access: StreamAccess) -> None:
        if self._authorize is None or await self._authorize(access):
            return
        raise nexusrpc.HandlerError(
            f"the caller may not {access.operation} stream {access.stream.topic!r} "
            f"of Workflow {access.stream.workflow_id!r}",
            type=nexusrpc.HandlerErrorType.UNAUTHORIZED,
            retryable_override=False,
        )

    def _open(self, ref: StreamRef) -> StreamHandle:
        client: Client = temporalio.nexus.client()
        return self._provider.get_stream_handle(client, ref)

    async def _append(self, input: AppendInput) -> AppendOutput:
        payloads = input.payloads or []
        if input.finish and payloads:
            raise ValueError(
                "a finishing append carries no payloads; send the batch first, "
                "then finish at the next sequence"
            )
        if not input.finish and not payloads:
            raise ValueError("an append carries payloads or finishes")
        producer = self._open(input.stream).producer(
            producer_id=input.producer_id,
            attempt=input.attempt,
            next_sequence=input.sequence,
        )
        if input.finish:
            cursor = await producer.finish()
        else:
            values: list[RawValue] = []
            for index, body in enumerate(payloads):
                size = _crossing_size(len(body) + _RECORD_OVERHEAD)
                if size > self._max_record_bytes:
                    raise ValueError(
                        f"payload {index} would take {size} bytes in a read result, "
                        f"above the per-record limit of {self._max_record_bytes}"
                    )
                try:
                    values.append(RawValue(Payload.FromString(body)))
                except DecodeError as error:
                    raise ValueError(
                        f"payload {index} is not a serialized Payload: {error}"
                    ) from None
            cursor = await producer.append(*values)
        return AppendOutput(cursor=cursor.token)

    async def _read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        after = input.after_token or ""
        if input.latest_only:
            if after:
                raise ValueError(
                    "latest_only starts a read at the newest record; it takes no "
                    "after_token"
                )
            latest = await self._open(input.stream).latest()
            return ReadOutput(records=[], next_token=latest.token, done=False)
        max_records = input.max_records or _DEFAULT_MAX_RECORDS
        wait = (input.wait_ms or 0) / 1000
        ceiling = _CATCH_UP_LIMIT
        deadline = ctx.request_deadline
        if deadline is not None:
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            left = (
                deadline - datetime.now(timezone.utc) - _DEADLINE_MARGIN
            ).total_seconds()
            wait = max(0.0, min(wait, left))
            ceiling = max(0.0, min(ceiling, left))

        key = _stream_key(input.stream)
        subscription = self._take(key, after)
        if subscription is None:
            # Resolved here, so a cursor from another stream or store is
            # refused by this call.
            handle = self._open(input.stream)
            records = handle.read(
                after=Cursor(after) if after else BEGINNING, result_type=RawValue
            )
            subscription = _Subscription(
                key, records, after, handle=handle, fetched=after
            )
        try:
            collected = await self._collect(subscription, max_records, wait, ceiling)
        except BaseException:
            # A subscription whose read failed, or whose call was cancelled
            # mid-fetch, is not kept: the reader's next call resumes from its
            # own cursor on a fresh one.
            self._release(subscription)
            raise
        if collected:
            subscription.position = collected[-1].token
        done = subscription.ended and subscription.carried is None
        if done or self._closed:
            self._release(subscription)
        else:
            self._park(subscription)
        return ReadOutput(
            records=collected,
            next_token=subscription.position,
            done=done,
        )

    async def _collect(
        self,
        subscription: _Subscription,
        max_records: int,
        wait: float,
        ceiling: float,
    ) -> list[RecordWire]:
        """Up to ``max_records`` records within the answer's byte budget.

        First it waits up to ``wait`` for a record. Once it has one, or when
        it may not wait, it takes every record the store already held, as
        far as the newest one when that phase began: those need a fetch and
        a decode, not a new record, so it waits up to ``ceiling`` for them.
        """
        loop = asyncio.get_running_loop()
        end = loop.time() + wait
        collected: list[RecordWire] = []
        size = 0
        newest: str | None = None
        if subscription.carried is not None:
            collected.append(subscription.carried)
            size += _crossing_size(len(subscription.carried.record))
            subscription.carried = None
        while len(collected) < max_records and not subscription.ended:
            waiting = max(0.0, end - loop.time())
            if collected or waiting == 0:
                if newest is None:
                    newest = (await subscription.handle.latest()).token
                if subscription.fetched == newest:
                    break
                timeout = ceiling
            else:
                timeout = waiting
            if subscription.pending is None:
                subscription.pending = asyncio.ensure_future(
                    subscription.records.__anext__()
                )
            done, _ = await asyncio.wait({subscription.pending}, timeout=timeout)
            if not done:
                break
            fetch, subscription.pending = subscription.pending, None
            try:
                record = fetch.result()
            except StopAsyncIteration:
                subscription.ended = True
                break
            # A reader synthesizes supersessions from the attempts it sees, so
            # they are not transported.
            if record.kind is RecordKind.SUPERSEDED:
                continue
            subscription.fetched = record.cursor.token
            wire = _record_wire(record)
            wire_size = _crossing_size(len(wire.record))
            if wire_size > self._max_record_bytes:
                # No read result can carry it, so the reader is told why at
                # its cursor and goes on past it.
                wire = RecordWire(
                    token=wire.token,
                    record=b"",
                    error=(
                        f"the record would take {wire_size} bytes in a read result, "
                        f"above the per-record limit of {self._max_record_bytes}"
                    ),
                )
                wire_size = 0
            if collected and size + wire_size > self._max_answer_bytes:
                subscription.carried = wire
                break
            collected.append(wire)
            size += wire_size
        return collected

    def _take(self, key: _StreamKey, position: str) -> _Subscription | None:
        for subscription, (parked_key, parked_position) in self._idle.items():
            if parked_key == key and parked_position == position:
                del self._idle[subscription]
                if subscription.expiry is not None:
                    subscription.expiry.cancel()
                    subscription.expiry = None
                return subscription
        return None

    def _park(self, subscription: _Subscription) -> None:
        self._idle[subscription] = (subscription.key, subscription.position)
        subscription.expiry = asyncio.get_running_loop().call_later(
            self._idle_timeout, self._release, subscription
        )
        while len(self._idle) > self._max_idle:
            oldest = next(iter(self._idle))
            self._release(oldest)

    def _release(self, subscription: _Subscription) -> None:
        """Close ``subscription`` in the background and forget it."""
        self._idle.pop(subscription, None)
        if subscription.expiry is not None:
            subscription.expiry.cancel()
            subscription.expiry = None
        task = asyncio.ensure_future(_close_subscription(subscription))
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)


async def _close_subscription(subscription: _Subscription) -> None:
    # An in-flight fetch holds the generator, which refuses aclose while it
    # runs, so the fetch is cancelled and awaited first.
    if subscription.pending is not None:
        subscription.pending.cancel()
        with contextlib.suppress(BaseException):
            await subscription.pending
        subscription.pending = None
    try:
        await subscription.records.aclose()
    except Exception:
        logger.warning("closing a stream subscription failed", exc_info=True)


def _record_wire(record: StreamRecord[Any]) -> RecordWire:
    value = record.value if record.kind is RecordKind.DATA else None
    wire = to_wire(
        _RAW_CONVERTER,
        topic=record.topic,
        kind=record.kind,
        value=value,
        producer_id=record.producer_id,
        attempt=record.attempt,
        sequence=record.sequence,
    )
    return RecordWire(
        token=record.cursor.token, record=wire.SerializeToString(deterministic=True)
    )
