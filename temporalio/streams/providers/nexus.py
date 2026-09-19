"""The Nexus front: the outside surface behind one Temporal-authenticated endpoint.

Two halves in one module. The provider half implements only :func:`producer`
and :func:`consumer`; the workflow side raises, naming a storage provider,
because a workflow's publishes and reads ride the Workflow Task and cannot
cross an RPC. The handler half runs in a worker configured with any storage
provider and serves two sync operations, ``append`` and ``read``, so the
store behind the endpoint is invisible to callers and an operator switches
it without touching them.

The wire types and the service definition come from
``temporal_streams.nexusrpc.yaml``, so any language nexgen targets can be
handed the same contract. What stays hand-written here is what the
generator cannot express yet: the handler's dedupe and long-poll collect
loop, and the caller's batching, cursor handling and codec.

Reads hand out batches and an append carries one batch per call, because a
Nexus operation per record costs too much for token streams. Record bodies
cross the handler untouched: it reads the store as raw payloads and frames
them as they are, so a codec that changes the payload encoding survives the
hop and the handler's worker never needs the key. Supersession records are
not transported: the caller's reader re-synthesizes them from the attempts
it observes, which is the policy module's job on every provider. Cursors
pass through opaque, so the caller cannot tell which store produced them.

The handler keeps one parked read per ``(workflow, stream, topic)`` and
serves consecutive calls from it, so an idle caller does not leave one
abandoned long poll on the store per call. A call whose token does not
match the parked position replaces the subscription, and an idle one is
released after a minute; that is the residual cost.

Two stated prototype limits. Append deduplication lives in handler memory,
by batch index per producer attempt, so a handler that has no state for a
producer attempt refuses to continue it rather than starting a fresh
delegate whose numbering the store would drop as a repeat; the caller opens
a new attempt, which readers report as a supersession. The store-level fix,
a sequence-explicit append in the contract, is listed in the design doc's
work table. And any caller the endpoint admits may touch any workflow's
streams in the namespace; the endpoint's own authorization is the boundary.
"""

from __future__ import annotations

import asyncio
import logging
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

import nexusrpc
import nexusrpc.handler
from google.protobuf.message import DecodeError

import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.api.operatorservice.v1 import ListNexusEndpointsRequest
from temporalio.common import RawValue
from temporalio.streams import _frame, _provider
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord
from temporalio.streams.providers._nexus_generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
)

_WORKFLOW_SIDE_ERROR = (
    "the nexus provider is an outside transport; a worker publishes and reads "
    "through a storage provider, so configure one of those on the worker"
)

# The contract leaves these unset, so the handler is the one place that says
# what an omitted read bound means. The wait is long because the handler
# shortens it to the request deadline anyway, and a shorter default only
# means more round trips on an idle stream.
_DEFAULT_MAX_RECORDS = 100
_DEFAULT_WAIT_MS = 30_000
_DEADLINE_MARGIN = timedelta(milliseconds=500)
_DEFAULT_MAX_PRODUCERS = 10_000
_DEFAULT_SUBSCRIPTION_IDLE = timedelta(seconds=60)
_QUEUE_DEPTH = 1000
_APPEND_TIMEOUT_MS = 30_000

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

_OutputT = TypeVar("_OutputT")

logger = logging.getLogger(__name__)


def _bad_request(message: str) -> nexusrpc.HandlerError:
    return nexusrpc.HandlerError(
        message,
        type=nexusrpc.HandlerErrorType.BAD_REQUEST,
        retryable_override=False,
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
    delegate: Any
    batch_index: int


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream endpoint, delegating to one storage provider.

    The delegate is an explicit instance rather than the process default, so
    a caller and a handler can coexist in one process, and so an operator can
    run handlers for a new store next to handlers for the old one.
    """

    def __init__(
        self,
        client: Any,
        provider: str,
        *,
        max_producers: int = _DEFAULT_MAX_PRODUCERS,
        subscription_idle: timedelta = _DEFAULT_SUBSCRIPTION_IDLE,
        **provider_options: Any,
    ) -> None:
        """Serve the endpoint out of the provider named by ``provider``.

        ``max_producers`` bounds the dedupe state kept per producer attempt;
        the oldest is evicted, and a producer evicted mid-life gets a clear
        refusal on its next batch rather than a silent drop.
        ``subscription_idle`` is how long a parked read outlives its last
        caller.
        """
        self._client = client
        self._delegate = _provider.instance(provider, **provider_options)
        self._producers: OrderedDict[tuple[str, str, str, int], _ProducerState] = (
            OrderedDict()
        )
        self._max_producers = max_producers
        self._subscriptions: dict[tuple[str, str, str], _Subscription] = {}
        self._read_locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self._subscription_idle = subscription_idle.total_seconds()

    @nexusrpc.handler.sync_operation
    async def append(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Append on the caller's account, dropping a repeated batch.

        Raises:
            nexusrpc.HandlerError: ``BAD_REQUEST`` when the batch index skips
                ahead, when it continues a producer attempt this handler has
                no state for, or when a payload is not a serialized
                ``Payload``.
        """
        topic = input.topic or ""
        key = (
            input.workflow_id,
            input.stream or f"@{topic}",
            input.producer_id,
            input.attempt,
        )
        state = self._producers.get(key)
        if state is None:
            if input.batch_index != 1:
                # A fresh delegate would restart its numbering at zero, and
                # the store would drop that as a repeat of the first batch.
                # Refusing here turns a silent loss into a failure the caller
                # can act on by opening a new attempt.
                raise _bad_request(
                    f"producer {input.producer_id!r} attempt {input.attempt} resumed "
                    f"at batch {input.batch_index} on a handler that has no state for "
                    "it; open a new attempt"
                )
            delegate = await self._delegate.producer(
                self._client,
                workflow_id=input.workflow_id,
                stream=input.stream,
                topic=topic,
                producer_id=input.producer_id,
                attempt=input.attempt,
            )
            state = _ProducerState(delegate, 0)
        elif input.batch_index <= state.batch_index:
            # A repeat, or a producer that restarted its numbering: either
            # way the store already holds the batch.
            self._producers.move_to_end(key)
            return AppendOutput()
        elif input.batch_index != state.batch_index + 1:
            raise _bad_request(
                f"batch {input.batch_index} skips ahead of {state.batch_index} for "
                f"producer {input.producer_id!r} attempt {input.attempt}"
            )
        payloads = self._payloads(input.payloads)
        cursor: str | None = None
        if payloads:
            appended = await state.delegate.append(*payloads)
            cursor = appended.token if appended is not None else None
        if input.finish:
            await state.delegate.finish()
            self._producers.pop(key, None)
            return AppendOutput(cursor=cursor)
        # Recorded only once the store accepted the batch, so a failed append
        # is not mistaken for a repeat when the caller retries it.
        state.batch_index = input.batch_index
        self._producers[key] = state
        self._producers.move_to_end(key)
        while len(self._producers) > self._max_producers:
            self._producers.popitem(last=False)
        return AppendOutput(cursor=cursor)

    @staticmethod
    def _payloads(encoded: list[bytes] | None) -> list[Payload]:
        payloads = []
        for index, raw in enumerate(encoded or []):
            payload = Payload()
            try:
                payload.ParseFromString(raw)
            except DecodeError as error:
                raise _bad_request(
                    f"payloads[{index}] is not a serialized Temporal Payload: {error}"
                ) from error
            payloads.append(payload)
        return payloads

    @nexusrpc.handler.sync_operation
    async def read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        """Answer with the records after the caller's token, or time out."""
        stream = input.stream or ""
        topic = input.topic or None
        if input.latest_only:
            delegate = await self._delegate.consumer(
                self._client, workflow_id=input.workflow_id, stream=stream
            )
            return ReadOutput(next_token=(await delegate.latest(topic=topic)).token)
        max_records = (
            _DEFAULT_MAX_RECORDS if input.max_records is None else input.max_records
        )
        wait = (_DEFAULT_WAIT_MS if input.wait_ms is None else input.wait_ms) / 1000
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
        key = (input.workflow_id, stream, topic or "")
        await self._expire_subscriptions()
        lock = self._read_locks.setdefault(key, asyncio.Lock())
        async with lock:
            subscription = self._subscriptions.get(key)
            if subscription is None or subscription.position != after:
                if subscription is not None:
                    await self._drop(key)
                subscription = await self._subscribe(
                    key, input.workflow_id, stream, topic, after
                )
            records, next_token = await self._drain(subscription, max_records, wait)
            subscription.position = next_token
            subscription.last_used = time.monotonic()
            if subscription.done and subscription.records.empty():
                # The store ended the read, as the Workflow Streams delegate
                # does when the run closes. Nothing more will arrive on it.
                await self._drop(key)
        return ReadOutput(records=records, next_token=next_token)

    async def _subscribe(
        self,
        key: tuple[str, str, str],
        workflow_id: str,
        stream: str,
        topic: str | None,
        after: str,
    ) -> _Subscription:
        delegate = await self._delegate.consumer(
            self._client, workflow_id=workflow_id, stream=stream
        )
        # Raw payloads: the handler frames what the store holds without
        # decoding it, so an encoding only the caller's codec understands
        # passes through untouched.
        source = delegate.read(
            after=Cursor(after) if after else BEGINNING, topic=topic, type=RawValue
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
                if isinstance(source, AsyncGenerator):
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
                    # Honour the wait even though nothing can arrive, so a
                    # caller polling a closed stream does not spin on the
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
            failure, subscription.failure = subscription.failure, None
            raise failure
        return records, next_token

    @staticmethod
    def _wire(record: StreamRecord[Any]) -> RecordWire:
        body = (
            record.value.payload.SerializeToString()
            if isinstance(record.value, RawValue)
            else b""
        )
        frame = _frame.encode(
            topic=record.topic,
            kind=record.kind,
            producer=record.producer,
            attempt=record.attempt,
            sequence=record.sequence,
            body=body,
        )
        return RecordWire(token=record.cursor.token, frame=frame)

    async def close(self) -> None:
        """Release every parked read.

        Call it when the worker hosting this handler stops, so subscriptions
        waiting on the store do not outlive it.
        """
        for key in list(self._subscriptions):
            await self._drop(key)
        self._read_locks.clear()

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
            self._read_locks.pop(key, None)


class StreamEndpointError(RuntimeError):
    """The stream endpoint refused a call or could not be reached.

    ``status`` is the HTTP status when the endpoint answered, and ``None``
    when the connection itself failed.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        """Carry the endpoint's answer, or the reason it gave none."""
        super().__init__(message)
        self.status = status


def _post(url: str, body: bytes, headers: Mapping[str, str], timeout_ms: int) -> bytes:
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            # Tells the server how long the handler may park, so it does not
            # time the call out ahead of a wait the caller asked for.
            "Request-Timeout": f"{timeout_ms}ms",
            **headers,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_ms / 1000 + 5) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise StreamEndpointError(
            f"stream endpoint call failed ({error.code}): {detail}", status=error.code
        ) from error
    except urllib.error.URLError as error:
        raise StreamEndpointError(
            f"stream endpoint unreachable at {url}: {error.reason}"
        ) from error


class _Front:
    """Everything the caller half needs to reach one endpoint."""

    def __init__(
        self,
        base_url: str,
        data_converter: temporalio.converter.DataConverter,
        headers: Mapping[str, str],
        wait_ms: int,
        max_records: int,
    ) -> None:
        self.base_url = base_url
        self.converter = data_converter.payload_converter
        self.codec = data_converter.payload_codec
        self.headers = dict(headers)
        self.wait_ms = wait_ms
        self.max_records = max_records

    async def invoke(
        self, operation: str, request: Any, output: type[_OutputT], *, timeout_ms: int
    ) -> _OutputT:
        # The contract types carry their own JSON encoding, so the raw caller
        # and the worker serving the operation agree on the body without
        # either of them spelling the fields out.
        contract = temporalio.converter.DataConverter.default.payload_converter
        raw = await asyncio.to_thread(
            _post,
            f"{self.base_url}/{operation}",
            contract.to_payloads([request])[0].data,
            self.headers,
            timeout_ms,
        )
        payload = Payload(metadata={"encoding": b"json/plain"}, data=raw or b"{}")
        return contract.from_payloads([payload], [output])[0]


class NexusProducer:
    """Appends through the stream endpoint; framing happens behind it.

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
        stream: str,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to one stream or topic on the endpoint."""
        self._front = front
        self._workflow_id = workflow_id
        self._stream = stream
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._batch_index = 0
        self._pending: tuple[AppendInput, tuple[bytes, ...], bool] | None = None
        self._lock = asyncio.Lock()

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    async def append(self, *values: Any) -> Cursor | None:
        """Append ``values`` through the endpoint."""
        if not values:
            return None
        payloads = [
            value
            if isinstance(value, Payload)
            else self._front.converter.to_payloads([value])[0]
            for value in values
        ]
        answer = await self._call(payloads, finish=False)
        return Cursor(answer.cursor) if answer.cursor else None

    async def finish(self) -> None:
        """Mark this producer done, so a reader stops waiting on it."""
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
            request = self._request(self._batch_index + 1, encoded, finish)
            return await self._send(request, identity, finish)

    def _request(
        self, batch_index: int, payloads: list[bytes], finish: bool
    ) -> AppendInput:
        return AppendInput(
            workflow_id=self._workflow_id,
            stream=self._stream,
            producer_id=self._producer_id,
            attempt=self._attempt,
            batch_index=batch_index,
            topic=self._topic,
            payloads=payloads,
            finish=finish,
        )

    async def _send(
        self, request: AppendInput, identity: tuple[bytes, ...], finish: bool
    ) -> AppendOutput:
        self._pending = (request, identity, finish)
        answer = await self._front.invoke(
            _APPEND_OPERATION, request, AppendOutput, timeout_ms=_APPEND_TIMEOUT_MS
        )
        self._batch_index = request.batch_index
        self._pending = None
        return answer


class NexusConsumer:
    """Reads batches from the stream endpoint, re-synthesizing supersession."""

    def __init__(self, front: _Front, workflow_id: str, stream: str) -> None:
        """Read what ``workflow_id`` publishes, through the endpoint."""
        self._front = front
        self._workflow_id = workflow_id
        self._stream = stream

    async def read(
        self,
        *,
        after: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records after ``after``, one endpoint batch at a time."""
        _provider.check_topic(self._stream, topic)
        attempts = AttemptTracker()
        token = after.token
        while True:
            answer = await self._front.invoke(
                _READ_OPERATION,
                ReadInput(
                    workflow_id=self._workflow_id,
                    stream=self._stream,
                    topic=topic or "",
                    after_token=token,
                    max_records=self._front.max_records,
                    wait_ms=self._front.wait_ms,
                ),
                ReadOutput,
                timeout_ms=self._front.wait_ms + 5000,
            )
            for wire in answer.records or []:
                cursor = Cursor(wire.token)
                try:
                    kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                        wire.frame
                    )
                except ValueError as error:
                    # Same answer as every other reader: skip and say so.
                    logger.warning("skipping stream record at %s: %s", cursor, error)
                    continue
                if topic is not None and frame_topic != topic:
                    continue
                superseded = attempts.note(source, attempt, cursor)
                if superseded is not None:
                    yield superseded
                value = (
                    await self._decode(body, type) if kind is RecordKind.DATA else None
                )
                yield StreamRecord(
                    value=value,
                    cursor=cursor,
                    kind=kind,
                    topic=frame_topic,
                    producer=source,
                    attempt=attempt,
                    sequence=sequence,
                )
            token = answer.next_token or token

    async def latest(self, *, topic: str | None = None) -> Cursor:
        """The cursor of the last record written, for following from now."""
        answer = await self._front.invoke(
            _READ_OPERATION,
            ReadInput(
                workflow_id=self._workflow_id,
                stream=self._stream,
                topic=topic or "",
                latest_only=True,
            ),
            ReadOutput,
            timeout_ms=_APPEND_TIMEOUT_MS,
        )
        token = answer.next_token or ""
        return Cursor(token) if token else BEGINNING

    async def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        if self._front.codec is not None:
            payload = (await self._front.codec.decode([payload]))[0]
        if as_type is None:
            return self._front.converter.from_payloads([payload])[0]
        return self._front.converter.from_payloads([payload], [as_type])[0]


class _NexusProvider:
    name = "nexus"

    def __init__(self) -> None:
        self._endpoint: str | None = None
        self._endpoint_id: str | None = None
        self._http_address = "http://127.0.0.1:7243"
        self._service = _SERVICE_NAME
        self._data_converter: temporalio.converter.DataConverter | None = None
        self._client: Any = None
        self._headers: dict[str, str] = {}
        self._wait_ms = _DEFAULT_WAIT_MS
        self._max_records = _DEFAULT_MAX_RECORDS

    def configure(self, **options: Any) -> None:
        """Point the caller half at one endpoint.

        ``endpoint`` is the endpoint's name when ``client`` is given, and is
        resolved to its id through the operator service on first use; without
        a client it has to be the id, because the HTTP ingress dispatches by
        id. ``headers`` go on every request, which is where an authorization
        header belongs. ``wait_ms`` and ``max_records`` are the read bounds
        the caller asks the handler for.
        """
        endpoint = options.pop("endpoint", None)
        self._http_address = options.pop("http_address", self._http_address)
        self._service = options.pop("service", _SERVICE_NAME)
        self._data_converter = options.pop("data_converter", None)
        self._client = options.pop("client", None)
        self._headers = dict(options.pop("headers", None) or {})
        self._wait_ms = int(options.pop("wait_ms", _DEFAULT_WAIT_MS))
        self._max_records = int(options.pop("max_records", _DEFAULT_MAX_RECORDS))
        if options:
            raise TypeError(
                "the nexus provider takes endpoint, http_address, service, "
                f"data_converter, client, headers, wait_ms and max_records, got "
                f"{sorted(options)}"
            )
        if not endpoint:
            raise TypeError(
                "the nexus provider needs endpoint=<nexus endpoint name> together with "
                "client=<Client>, or endpoint=<nexus endpoint id> on its own"
            )
        self._endpoint = endpoint
        self._endpoint_id = None

    def worker_options(self) -> dict[str, Any]:
        raise RuntimeError(_WORKFLOW_SIDE_ERROR)

    def open_read(
        self,
        stream: str,
        *,
        after: Cursor = BEGINNING,
        idle_timeout: timedelta | None = None,
    ) -> ReadSource:
        del stream, after, idle_timeout
        raise RuntimeError(_WORKFLOW_SIDE_ERROR)

    def open_write(self, topic: str) -> WriteSink:
        del topic
        raise RuntimeError(_WORKFLOW_SIDE_ERROR)

    async def _front(self, client: Any) -> _Front:
        if self._endpoint is None:
            raise RuntimeError("configure the nexus provider before opening handles")
        if self._endpoint_id is None:
            resolver = self._client if self._client is not None else client
            if resolver is None:
                self._endpoint_id = self._endpoint
            else:
                found = await resolver.operator_service.list_nexus_endpoints(
                    ListNexusEndpointsRequest(name=self._endpoint)
                )
                if not found.endpoints:
                    raise RuntimeError(f"no nexus endpoint is named {self._endpoint!r}")
                self._endpoint_id = found.endpoints[0].id
        base_url = (
            f"{self._http_address}/nexus/endpoints/{self._endpoint_id}"
            f"/services/{self._service}"
        )
        return _Front(
            base_url,
            self._converter(client),
            self._headers,
            self._wait_ms,
            self._max_records,
        )

    async def producer(
        self,
        client: Any,
        *,
        workflow_id: str,
        stream: str = "",
        topic: str = "",
        producer_id: str = "",
        attempt: int = 0,
    ) -> NexusProducer:
        return NexusProducer(
            await self._front(client), workflow_id, stream, topic, producer_id, attempt
        )

    async def consumer(
        self, client: Any, *, workflow_id: str, stream: str = ""
    ) -> NexusConsumer:
        return NexusConsumer(await self._front(client), workflow_id, stream)

    def _converter(self, client: Any) -> temporalio.converter.DataConverter:
        if self._data_converter is not None:
            return self._data_converter
        if client is None:
            return temporalio.converter.DataConverter.default
        return client.data_converter


_provider.register("nexus", _NexusProvider)
