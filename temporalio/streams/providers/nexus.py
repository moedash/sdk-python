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

The handler keeps one parked read per ``(workflow, run, topic)`` and serves
consecutive calls from it, so an idle caller does not leave one abandoned
long poll on the store per call. A call whose token does not match the parked
position replaces the subscription, and an idle one is released after a
minute; that is the residual cost.

Two stated prototype limits. Append deduplication lives in handler memory, by
batch index per producer attempt, so a handler that has no state for a
producer attempt refuses to continue it rather than starting a fresh delegate
whose numbering the store would drop as a repeat; the caller opens a new
attempt, which readers report as a supersession. And any caller the endpoint
admits may touch any workflow's streams in the namespace; the endpoint's own
authorization is the boundary.

A failure from the endpoint reaches the caller as the
:class:`temporalio.streams.StreamError` the store raised, when the handler
named one, and as :class:`temporalio.service.RPCError` otherwise, never as an
HTTP or urllib exception.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import logging
import time
import urllib.error
import urllib.request
import weakref
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, NoReturn, TypeVar, cast

import nexusrpc
import nexusrpc.handler
from google.protobuf.message import DecodeError

import temporalio.client
import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.api.operatorservice.v1 import ListNexusEndpointsRequest
from temporalio.client import Client, ClientConfig
from temporalio.common import RawValue
from temporalio.service import ConnectConfig, RPCError, RPCStatusCode, ServiceClient
from temporalio.streams._errors import (
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.streams._provider import StreamHandle, StreamProducer, StreamProvider
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord
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

__all__ = [
    "NexusProducer",
    "NexusStreamHandle",
    "NexusStreams",
    "TemporalStreamsHandler",
]

T = TypeVar("T")

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
        self._producers: OrderedDict[tuple[str, str, str, str, int], _ProducerState] = (
            OrderedDict()
        )
        self._max_producers = max_producers
        self._subscriptions: dict[tuple[str, str, str], _Subscription] = {}
        # Held weakly, and by the call that is using one for as long as it
        # runs. A strong map would keep an entry per address ever read or
        # appended to, and nothing would ever reach it again.
        self._read_locks: weakref.WeakValueDictionary[
            tuple[str, str, str], asyncio.Lock
        ] = weakref.WeakValueDictionary()
        self._append_locks: weakref.WeakValueDictionary[
            tuple[str, str, str, str, int], asyncio.Lock
        ] = weakref.WeakValueDictionary()
        self._subscription_idle = subscription_idle.total_seconds()

    def _stream(self, workflow_id: str, run_id: str | None) -> StreamHandle:
        # A storage provider wants a client; the memory provider, which the
        # tests front, accepts none, so the cast only lies where it is unread.
        return self._provider.get_stream_handle(
            cast(Client, self._client), workflow_id, run_id=run_id or None
        )

    @nexusrpc.handler.sync_operation
    async def append(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Append on the caller's account, writing a repeated batch once.

        Raises:
            nexusrpc.HandlerError: ``BAD_REQUEST`` when the batch index or
                sequence does not continue the producer attempt, when the
                attempt is one this handler has no state for, or when a
                payload is not a serialized ``Payload``; ``NOT_FOUND`` when
                the store has no such workflow.
        """
        try:
            return await self._append(input)
        except StreamError as error:
            raise _handler_error(error) from error
        except ValueError as error:
            raise _bad_request(str(error)) from error

    async def _append(self, input: AppendInput) -> AppendOutput:
        _require_topic(input.topic)
        key = (
            input.workflow_id,
            input.run_id or "",
            input.topic,
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
        self, input: AppendInput, key: tuple[str, str, str, str, int]
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
            delegate = self._stream(input.workflow_id, input.run_id).producer(
                topic=input.topic, producer_id=input.producer_id, attempt=input.attempt
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
                token, ``NOT_FOUND`` when it has no such workflow.
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
        _require_topic(input.topic)
        stream = self._stream(input.workflow_id, input.run_id)
        if input.latest_only:
            return ReadOutput(next_token=(await stream.latest(topic=input.topic)).token)
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
        key = (input.workflow_id, input.run_id or "", input.topic)
        await self._expire_subscriptions()
        lock = self._read_locks.setdefault(key, asyncio.Lock())
        async with lock:
            subscription = self._subscriptions.get(key)
            if subscription is None or subscription.position != after:
                if subscription is not None:
                    await self._drop(key)
                subscription = self._subscribe(key, stream, input.topic, after)
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
        self, key: tuple[str, str, str], stream: StreamHandle, topic: str, after: str
    ) -> _Subscription:
        # Raw payloads: the handler forwards what the store holds without
        # decoding it, so an encoding only the caller's codec understands
        # passes through untouched. A foreign token is refused right here.
        source = stream.read(
            topic=topic,
            after=Cursor(after) if after else BEGINNING,
            result_type=RawValue,
        )
        subscription = _Subscription(
            records=asyncio.Queue(maxsize=_QUEUE_DEPTH),
            position=after,
            last_used=time.monotonic(),
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

    async def _drop(self, key: tuple[str, str, str]) -> None:
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
        workflow_id: str,
        run_id: str | None,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic`` on the workflow behind the endpoint."""
        self._front = front
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._topic = topic
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
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                topic=self._topic,
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
    """One workflow's stream through the endpoint, re-synthesizing supersession."""

    def __init__(self, front: _Front, workflow_id: str, run_id: str | None) -> None:
        """Address ``workflow_id``'s stream, pinned to ``run_id`` when one is given."""
        self._front = front
        self._workflow_id = workflow_id
        self._run_id = run_id

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` after ``after``, one endpoint batch at a time.

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
        topic, result_type = resolve_topic(topic, result_type)
        return self._read(topic, after, result_type)

    async def _read(
        self, topic: str, after: Cursor, result_type: type | None
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._front.converter, result_type, after=after, warn=logger.warning
        )
        token = after.token
        while True:
            answer = await self._front.invoke(
                _READ_OPERATION,
                ReadInput(
                    workflow_id=self._workflow_id,
                    run_id=self._run_id,
                    topic=topic,
                    after_token=token,
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
            if answer.done:
                return

    async def latest(self, *, topic: str | StreamTopic[Any]) -> Cursor:
        """The newest position on ``topic`` behind the endpoint, for following from now."""
        topic, _ = resolve_topic(topic)
        answer = await self._front.invoke(
            _READ_OPERATION,
            ReadInput(
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                topic=topic,
                latest_only=True,
            ),
            ReadOutput,
            timeout=_APPEND_TIMEOUT,
        )
        token = answer.next_token or ""
        return Cursor(token) if token else BEGINNING

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any],
        producer_id: str = "",
        attempt: int = 0,
    ) -> NexusProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return NexusProducer(
            self._front, self._workflow_id, self._run_id, topic, producer_id, attempt
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
        return NexusStreamHandle(_Front(self, client), workflow_id, run_id)

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
