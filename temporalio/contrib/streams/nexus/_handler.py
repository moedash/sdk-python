"""The stream service's handler over Core's stream service."""

from __future__ import annotations

import dataclasses
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Literal

import nexusrpc
import nexusrpc.handler
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError

import temporalio.api.failure.v1
import temporalio.converter
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import NexusHandlerErrorRetryBehavior
from temporalio.bridge.proto.streams import LatestRequest, ReadRecord, ReadRequest
from temporalio.common import RawValue
from temporalio.contrib.streams._body import decode_body
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
from temporalio.contrib.streams._handles import StreamHandle, get_stream_handle
from temporalio.contrib.streams._plugin import call
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams.nexus._generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
)

__all__ = ["StreamAccess", "TemporalStreamsHandler"]

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


_ReadKey = tuple[str, str, str, str, str]


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream service over the store of its Worker's client.

    Register it on a Worker with ``nexus_service_handlers=[handler]`` and
    point a Nexus endpoint at the Worker's task queue. The Worker's client
    must carry a stream store, and a stream lives in the client's namespace.
    That client carries the namespace's payload codec, the one the stream's
    producers and readers use: the store keeps each body encoded with it, a
    read decodes the bodies, and the read result crosses encoded as a whole
    like any Nexus result, so Workflow code that reads gets plain bodies with
    no codec of its own.

    Core keeps a read's progress in an opaque state between its calls. The
    handler holds each reader's state under the cursor its last answer handed
    out, so the reader's next call continues it. A state no call has used for
    ``idle_timeout`` is dropped, and beyond ``max_idle_reads`` the longest
    idle goes first. A reader whose state was dropped reads on from its own
    cursor, so nothing is lost. Retries are deduplicated by the store: a call
    carries the writer's sequence, and the store compares it with what it
    holds.

    A read answers with at most ``max_records`` records and, past its first
    record, at most ``max_answer_bytes`` of records as they cross, so a read's
    result stays below the server's blob size limit. A reader reads again
    from the cursor it got.

    The handler writes and reads with its Worker's client and that client's
    store, whatever caller sent the call. So every caller that may reach the
    endpoint gets the Worker's store rights, for every stream in the
    namespace and as any producer. Pass ``authorize`` to check each call: it
    gets a :class:`StreamAccess` and answers whether to serve it, and a
    refused call fails as unauthorized.

    A record whose bytes would take more than ``max_record_bytes`` in a read
    result can never be read through the service, so an append with one is
    refused. A record already in the store that is larger, written there
    directly, crosses as an error at its cursor, which a reader goes on past.
    So does a record whose body the codec can't decode. Sizes count what
    crosses: the record bytes as base64, about 4/3 of them.

    Parked reads hold Nexus task slots for up to their ``wait_ms``. With many
    readers, serve appends and reads on separate task queues or endpoints, so
    an append doesn't wait for a slot behind parked reads, and give the
    Worker more Nexus pollers than the default (``nexus_task_poller_behavior``).
    """

    def __init__(
        self,
        *,
        idle_timeout: timedelta = timedelta(minutes=1),
        max_idle_reads: int = 1000,
        max_answer_bytes: int = _DEFAULT_MAX_ANSWER_BYTES,
        max_record_bytes: int = _DEFAULT_MAX_RECORD_BYTES,
        authorize: Callable[[StreamAccess], Awaitable[bool]] | None = None,
    ) -> None:
        """Serve the store of the Worker's client.

        Raises:
            ValueError: ``idle_timeout`` is not positive, ``max_idle_reads``
                is below zero, or ``max_answer_bytes`` or ``max_record_bytes``
                is not positive.
        """
        if idle_timeout <= timedelta(0):
            raise ValueError("idle_timeout must be positive")
        if max_idle_reads < 0:
            raise ValueError("max_idle_reads must not be negative")
        if max_answer_bytes <= 0:
            raise ValueError("max_answer_bytes must be positive")
        if max_record_bytes <= 0:
            raise ValueError("max_record_bytes must be positive")
        self._idle_timeout = idle_timeout.total_seconds()
        self._max_idle = max_idle_reads
        self._max_answer_bytes = max_answer_bytes
        self._max_record_bytes = max_record_bytes
        self._authorize = authorize
        # Each reader's Core state and when it was parked, by the cursor the
        # reader resumes from, oldest first.
        self._states: OrderedDict[_ReadKey, tuple[bytes, float]] = OrderedDict()

    @nexusrpc.handler.sync_operation
    async def append(
        self, ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Write one batch, or finish the producer."""
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
        """Drop every held read state. A reader reads on from its own cursor."""
        self._states.clear()

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
        return get_stream_handle(temporalio.nexus.client(), ref)

    async def _append(self, input: AppendInput) -> AppendOutput:
        payloads = input.payloads or []
        if input.finish and payloads:
            raise ValueError(
                "a finishing append carries no payloads; send the batch first, "
                "then finish at the next sequence"
            )
        if not input.finish and not payloads:
            raise ValueError("an append carries payloads or finishes")
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
        producer = self._open(input.stream).producer(
            producer_id=input.producer_id,
            attempt=input.attempt,
            next_sequence=input.sequence,
        )
        cursor = await (producer.finish() if input.finish else producer.append(*values))
        return AppendOutput(cursor=cursor.token)

    async def _read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        after = input.after_token or ""
        handle = self._open(input.stream)
        address = handle._address(handle.ref.topic)
        client = temporalio.nexus.client()
        service = await handle._plugin._service_for(client)
        if input.latest_only:
            if after:
                raise ValueError(
                    "latest_only starts a read at the newest record; it takes no "
                    "after_token"
                )
            latest = await call(service.latest(LatestRequest(stream=address)))
            return ReadOutput(records=[], next_token=latest.cursor, done=False)
        wait = timedelta(milliseconds=input.wait_ms or 0)
        deadline = ctx.request_deadline
        if deadline is not None:
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            left = deadline - datetime.now(timezone.utc) - _DEADLINE_MARGIN
            wait = max(timedelta(0), min(wait, left))
        key = (*_ref_key(input.stream), after)
        request = ReadRequest(
            stream=address,
            after=after,
            max_records=input.max_records or _DEFAULT_MAX_RECORDS,
            state=self._take(key),
        )
        request.wait.FromTimedelta(wait)
        response = await call(service.read(request))
        records: list[RecordWire] = []
        size = 0
        cut = False
        for record in response.records:
            # A reader synthesizes supersessions from the attempts it sees, so
            # they are not transported.
            if record.HasField("superseded"):
                continue
            wire = await self._record_wire(client.data_converter, record)
            wire_size = _crossing_size(len(wire.record))
            if records and size + wire_size > self._max_answer_bytes:
                cut = True
                break
            records.append(wire)
            size += wire_size
        if cut:
            # Core's state is past the records left out, so the next call starts
            # a fresh read from the cursor this answer hands out.
            return ReadOutput(records=records, next_token=records[-1].token, done=False)
        if not response.done:
            self._park((*_ref_key(input.stream), response.cursor), response.state)
        return ReadOutput(
            records=records, next_token=response.cursor, done=response.done
        )

    async def _record_wire(
        self, converter: temporalio.converter.DataConverter, record: ReadRecord
    ) -> RecordWire:
        stored = record.stored
        if stored.HasField("body"):
            try:
                stored.body.CopyFrom(await decode_body(converter, stored.body))
            except Exception as error:
                return RecordWire(
                    token=record.cursor,
                    record=b"",
                    error=f"the record's body could not be decoded: {error}",
                )
        data = stored.SerializeToString(deterministic=True)
        wire_size = _crossing_size(len(data))
        if wire_size > self._max_record_bytes:
            # No read result can carry it, so the reader is told why at its
            # cursor and goes on past it.
            return RecordWire(
                token=record.cursor,
                record=b"",
                error=(
                    f"the record would take {wire_size} bytes in a read result, "
                    f"above the per-record limit of {self._max_record_bytes}"
                ),
            )
        return RecordWire(token=record.cursor, record=data)

    def _take(self, key: _ReadKey) -> bytes:
        state, parked = self._states.pop(key, (b"", 0.0))
        return state if time.monotonic() - parked < self._idle_timeout else b""

    def _park(self, key: _ReadKey, state: bytes) -> None:
        self._states[key] = (state, time.monotonic())
        self._states.move_to_end(key)
        while len(self._states) > self._max_idle:
            self._states.popitem(last=False)


def _ref_key(ref: StreamRef) -> tuple[str, str, str, str]:
    return (ref.kind, ref.workflow_id, ref.run_id or "", ref.topic)
