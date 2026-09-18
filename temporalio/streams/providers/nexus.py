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
Nexus operation per record costs too much for token streams. Supersession
records are not transported: the caller's reader re-synthesizes them from
the attempts it observes, which is the policy module's job on every
provider. Cursors pass through opaque, so the caller cannot tell which store
produced them.

One stated prototype limit: append deduplication holds at the handler by
batch index per producer, so a handler restart during a producer's life can
double-append. The store-level fix, a sequence-explicit append in the IDL
contract, is listed in the design doc's work table.
"""

from __future__ import annotations

import asyncio
import base64
import urllib.error
import urllib.request
from collections.abc import AsyncGenerator, AsyncIterator
from datetime import timedelta
from typing import Any, TypeVar

import nexusrpc
import nexusrpc.handler

import temporalio.converter
from temporalio.api.common.v1 import Payload
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
# what an omitted read bound means.
_DEFAULT_MAX_RECORDS = 100
_DEFAULT_WAIT_MS = 5000

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


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream endpoint, delegating to one storage provider.

    The delegate is an explicit instance rather than the process default, so
    a caller and a handler can coexist in one process, and so an operator can
    run handlers for a new store next to handlers for the old one.
    """

    def __init__(self, client: Any, provider: str, **provider_options: Any) -> None:
        """Serve the endpoint out of the provider named by ``provider``."""
        self._client = client
        self._delegate = _provider.instance(provider, **provider_options)
        self._producers: dict[tuple[str, str, str, int], tuple[Any, int]] = {}

    @nexusrpc.handler.sync_operation
    async def append(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Append on the caller's account, dropping a repeated batch."""
        topic = input.topic or ""
        key = (
            input.workflow_id,
            input.stream or f"@{topic}",
            input.producer_id,
            input.attempt,
        )
        cached = self._producers.get(key)
        if cached is not None and input.batch_index <= cached[1]:
            return AppendOutput()
        if cached is None:
            delegate = await self._delegate.producer(
                self._client,
                workflow_id=input.workflow_id,
                stream=input.stream,
                topic=topic,
                producer_id=input.producer_id,
                attempt=input.attempt,
            )
        else:
            delegate = cached[0]
        self._producers[key] = (delegate, input.batch_index)
        cursor = ""
        if input.payloads:
            payloads = []
            for encoded in input.payloads:
                payload = Payload()
                payload.ParseFromString(base64.b64decode(encoded))
                payloads.append(payload)
            cursor = (await delegate.append(*payloads)).token
        if input.finish:
            await delegate.finish()
        return AppendOutput(cursor=cursor)

    @nexusrpc.handler.sync_operation
    async def read(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        """Answer with the records after the caller's token, or time out."""
        delegate = await self._delegate.consumer(
            self._client, workflow_id=input.workflow_id, stream=input.stream or ""
        )
        topic = input.topic or None
        if input.latest_only:
            return ReadOutput(next_token=(await delegate.latest(topic=topic)).token)
        max_records = (
            _DEFAULT_MAX_RECORDS if input.max_records is None else input.max_records
        )
        wait_ms = _DEFAULT_WAIT_MS if input.wait_ms is None else input.wait_ms
        converter = temporalio.converter.DataConverter.default.payload_converter
        records: list[RecordWire] = []
        next_token = input.after_token or ""
        subscription = delegate.read(
            after=Cursor(input.after_token) if input.after_token else BEGINNING,
            topic=topic,
        )

        async def collect() -> None:
            nonlocal next_token
            async for record in subscription:
                if record.kind is RecordKind.SUPERSEDED:
                    continue
                body = b""
                if record.kind is RecordKind.DATA:
                    body = converter.to_payloads([record.value])[0].SerializeToString()
                frame = _frame.encode(
                    topic=record.topic,
                    kind=record.kind,
                    producer=record.producer,
                    attempt=record.attempt,
                    sequence=record.sequence,
                    body=body,
                )
                records.append(
                    RecordWire(
                        token=record.cursor.token,
                        frame=base64.b64encode(frame).decode("ascii"),
                    )
                )
                next_token = record.cursor.token
                if len(records) >= max_records:
                    return

        try:
            # `wait_for` rather than `asyncio.timeout`, which 3.10 does not
            # have. Whatever was collected before the deadline is the answer.
            await asyncio.wait_for(collect(), wait_ms / 1000)
        except asyncio.TimeoutError:
            pass
        finally:
            # The delegate's read parks against its store, and this call
            # answers before the caller asks again, so let it go now rather
            # than when the collector is next swept.
            if isinstance(subscription, AsyncGenerator):
                await subscription.aclose()
        return ReadOutput(records=records, next_token=next_token)


def _post(url: str, body: bytes) -> bytes:
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"stream endpoint call failed ({error.code}): {detail}")


async def _invoke(url: str, request: Any, output: type[_OutputT]) -> _OutputT:
    # The contract types carry their own JSON encoding, so the raw caller and
    # the worker serving the operation agree on the body without either of
    # them spelling the fields out.
    contract = temporalio.converter.DataConverter.default.payload_converter
    raw = await asyncio.to_thread(_post, url, contract.to_payloads([request])[0].data)
    payload = Payload(metadata={"encoding": b"json/plain"}, data=raw or b"{}")
    return contract.from_payloads([payload], [output])[0]


class NexusProducer:
    """Appends through the stream endpoint; framing happens behind it."""

    def __init__(
        self,
        base_url: str,
        data_converter: temporalio.converter.DataConverter,
        workflow_id: str,
        stream: str,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to one stream or topic on the endpoint."""
        self._base_url = base_url
        self._converter = data_converter.payload_converter
        self._codec = data_converter.payload_codec
        self._workflow_id = workflow_id
        self._stream = stream
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._batch_index = 0

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    async def append(self, *values: Any) -> Cursor:
        """Append ``values`` through the endpoint."""
        payloads = [
            value
            if isinstance(value, Payload)
            else self._converter.to_payloads([value])[0]
            for value in values
        ]
        if self._codec is not None:
            # The endpoint is the edge of this process, so a configured codec
            # runs here rather than at the handler: whoever hosts the endpoint
            # never holds the plaintext.
            payloads = await self._codec.encode(payloads)
        encoded = [
            base64.b64encode(payload.SerializeToString()).decode()
            for payload in payloads
        ]
        return Cursor((await self._call(payloads=encoded)).cursor or "")

    async def finish(self) -> None:
        """Mark this producer done, so a reader stops waiting on it."""
        await self._call(finish=True)

    async def _call(
        self, payloads: list[str] | None = None, finish: bool = False
    ) -> AppendOutput:
        self._batch_index += 1
        return await _invoke(
            f"{self._base_url}/{_APPEND_OPERATION}",
            AppendInput(
                workflow_id=self._workflow_id,
                stream=self._stream,
                producer_id=self._producer_id,
                attempt=self._attempt,
                batch_index=self._batch_index,
                topic=self._topic,
                payloads=payloads or [],
                finish=finish,
            ),
            AppendOutput,
        )


class NexusConsumer:
    """Reads batches from the stream endpoint, re-synthesizing supersession."""

    def __init__(
        self,
        base_url: str,
        data_converter: temporalio.converter.DataConverter,
        workflow_id: str,
        stream: str,
    ) -> None:
        """Read what ``workflow_id`` publishes, through the endpoint."""
        self._base_url = base_url
        self._converter = data_converter.payload_converter
        self._codec = data_converter.payload_codec
        self._workflow_id = workflow_id
        self._stream = stream

    async def read(
        self,
        *,
        after: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncIterator[StreamRecord[Any]]:
        """Yield records after ``after``, one endpoint batch at a time."""
        attempts = AttemptTracker()
        token = after.token
        while True:
            answer = await _invoke(
                f"{self._base_url}/{_READ_OPERATION}",
                ReadInput(
                    workflow_id=self._workflow_id,
                    stream=self._stream,
                    topic=topic or "",
                    after_token=token,
                ),
                ReadOutput,
            )
            for wire in answer.records or []:
                try:
                    kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                        base64.b64decode(wire.frame)
                    )
                except ValueError:
                    continue
                if topic is not None and frame_topic != topic:
                    continue
                cursor = Cursor(wire.token)
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
        answer = await _invoke(
            f"{self._base_url}/{_READ_OPERATION}",
            ReadInput(
                workflow_id=self._workflow_id,
                stream=self._stream,
                topic=topic or "",
                latest_only=True,
            ),
            ReadOutput,
        )
        token = answer.next_token or ""
        return Cursor(token) if token else BEGINNING

    async def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        if self._codec is not None:
            payload = (await self._codec.decode([payload]))[0]
        if as_type is None:
            return self._converter.from_payloads([payload])[0]
        return self._converter.from_payloads([payload], [as_type])[0]


class _NexusProvider:
    name = "nexus"

    def __init__(self) -> None:
        self._base_url: str | None = None
        self._data_converter: temporalio.converter.DataConverter | None = None

    def configure(self, **options: Any) -> None:
        endpoint = options.pop("endpoint", None)
        http_address = options.pop("http_address", "http://127.0.0.1:7243")
        service = options.pop("service", _SERVICE_NAME)
        data_converter = options.pop("data_converter", None)
        if options:
            raise TypeError(
                "the nexus provider takes endpoint, http_address, service and "
                f"data_converter, got {sorted(options)}"
            )
        if not endpoint:
            raise TypeError("the nexus provider needs endpoint=<nexus endpoint name>")
        self._base_url = f"{http_address}/nexus/endpoints/{endpoint}/services/{service}"
        self._data_converter = data_converter

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

    def _url(self) -> str:
        if self._base_url is None:
            raise RuntimeError("configure the nexus provider before opening handles")
        return self._base_url

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
        if not producer_id:
            from temporalio import activity

            producer_id = activity.info().activity_id
            attempt = attempt or activity.info().attempt
        return NexusProducer(
            self._url(),
            self._converter(client),
            workflow_id,
            stream,
            topic,
            producer_id,
            attempt,
        )

    async def consumer(
        self, client: Any, *, workflow_id: str, stream: str = ""
    ) -> NexusConsumer:
        return NexusConsumer(self._url(), self._converter(client), workflow_id, stream)

    def _converter(self, client: Any) -> temporalio.converter.DataConverter:
        if self._data_converter is not None:
            return self._data_converter
        if client is None:
            return temporalio.converter.DataConverter.default
        return client.data_converter


_provider.register("nexus", _NexusProvider)
