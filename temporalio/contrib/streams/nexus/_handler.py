"""The stream service's handler over any stream provider."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
from collections import OrderedDict
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import nexusrpc
import nexusrpc.handler
from google.protobuf.message import DecodeError

import temporalio.converter
import temporalio.nexus
from temporalio.api.common.v1 import Payload
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
from temporalio.exceptions import ApplicationError

__all__ = ["TemporalStreamsHandler"]

logger = logging.getLogger(__name__)

_DEFAULT_MAX_RECORDS = 100
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
    # The write may have landed. Repeating the same call is safe, because
    # the store deduplicates it, so the caller is told to retry.
    (StreamOutcomeUnknownError, nexusrpc.HandlerErrorType.UNAVAILABLE, True),
)

# The default converter passes a RawValue through untouched, so a body the
# caller's codec encoded is never decoded or re-encoded here.
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
    handler_error = nexusrpc.HandlerError(
        f"{name}: {error}", type=kind, retryable_override=retryable
    )
    handler_error.__cause__ = ApplicationError(
        str(error), type=name, non_retryable=not retryable
    )
    return handler_error


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
    pending: asyncio.Task[StreamRecord[Any]] | None = None
    """A record fetch still in flight when the last call ran out of time. The
    next call waits on it rather than starting another, so a record the store
    already handed over is never dropped."""
    expiry: asyncio.TimerHandle | None = None
    ended: bool = False


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream service over ``provider``.

    Register it on a Worker with ``nexus_service_handlers=[handler]`` and
    point a Nexus endpoint at the Worker's task queue. The Worker's client
    opens the streams, so a stream lives in the Worker's namespace. That
    client should carry no payload codec: record bodies arrive encoded by the
    writer's codec and leave that way, and the handler never decodes them.

    A reader's subscription is kept between its calls and found again by the
    cursor its last answer handed out, so readers never share one and a
    second reader cannot take over the first's. A subscription no call has
    used for ``idle_timeout`` is closed, and beyond ``max_idle_subscriptions``
    the longest idle is closed first; a reader whose subscription was closed
    is answered from a fresh one on its next call, from its own cursor, so
    nothing is lost. Retries are deduplicated by the store, not here: a call
    carries the writer's sequence and the provider compares it with what it
    holds.

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
    ) -> None:
        """Serve ``provider``.

        Raises:
            ValueError: ``idle_timeout`` is not positive or
                ``max_idle_subscriptions`` is below zero.
        """
        if idle_timeout <= timedelta(0):
            raise ValueError("idle_timeout must be positive")
        if max_idle_subscriptions < 0:
            raise ValueError("max_idle_subscriptions must not be negative")
        self._provider = provider
        self._idle_timeout = idle_timeout.total_seconds()
        self._max_idle = max_idle_subscriptions
        # Idle subscriptions by the cursor a reader resumes from, oldest first.
        # Several can sit under one cursor when readers are at the same place.
        self._idle: OrderedDict[_Subscription, tuple[_StreamKey, str]] = OrderedDict()
        self._closing: set[asyncio.Task[None]] = set()

    @nexusrpc.handler.sync_operation
    async def append(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Write one batch, or finish the producer, through the provider."""
        try:
            return await self._append(input)
        except (ValueError, StreamError) as error:
            raise _handler_error(error)

    @nexusrpc.handler.sync_operation
    async def read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        """Answer with the records after the caller's cursor, waiting for some."""
        try:
            return await self._read(ctx, input)
        except (ValueError, StreamError) as error:
            raise _handler_error(error)

    async def close(self) -> None:
        """Close every idle subscription and wait for those already closing."""
        for subscription in list(self._idle):
            self._release(subscription)
        if self._closing:
            await asyncio.gather(*self._closing, return_exceptions=True)

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
        deadline = ctx.request_deadline
        if deadline is not None:
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            left = deadline - datetime.now(timezone.utc) - _DEADLINE_MARGIN
            wait = max(0.0, min(wait, left.total_seconds()))

        key = _stream_key(input.stream)
        subscription = self._take(key, after)
        if subscription is None:
            # Resolved here, so a cursor from another stream or store is
            # refused by this call.
            records = self._open(input.stream).read(
                after=Cursor(after) if after else BEGINNING, result_type=RawValue
            )
            subscription = _Subscription(key, records, after)
        try:
            collected = await self._collect(subscription, max_records, wait)
        except BaseException:
            # A subscription whose read failed, or whose call was cancelled
            # mid-fetch, is not kept: the reader's next call resumes from its
            # own cursor on a fresh one.
            self._release(subscription)
            raise
        if collected:
            subscription.position = collected[-1].token
        if subscription.ended:
            self._release(subscription)
        else:
            self._park(subscription)
        return ReadOutput(
            records=collected,
            next_token=subscription.position,
            done=subscription.ended,
        )

    async def _collect(
        self, subscription: _Subscription, max_records: int, wait: float
    ) -> list[RecordWire]:
        loop = asyncio.get_running_loop()
        end = loop.time() + wait
        collected: list[RecordWire] = []
        while len(collected) < max_records and not subscription.ended:
            if subscription.pending is None:
                subscription.pending = asyncio.ensure_future(
                    subscription.records.__anext__()
                )
            # Once something is collected, only what is already at hand is
            # added: the caller asked to wait for records, not for a full batch.
            remaining = 0.0 if collected else max(0.0, end - loop.time())
            done, _ = await asyncio.wait({subscription.pending}, timeout=remaining)
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
            collected.append(_record_wire(record))
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
