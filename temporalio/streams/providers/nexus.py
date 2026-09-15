"""The Nexus front: the outside surface behind one Temporal-authenticated endpoint.

Two halves in one module. The provider half implements only :func:`producer`
and :func:`consumer`; the workflow side raises, naming a storage provider,
because a workflow's publishes and reads ride the Workflow Task and cannot
cross an RPC. The handler half runs in a worker configured with any storage
provider and serves two sync operations, ``append`` and ``read``, so the
store behind the endpoint is invisible to callers and an operator switches
it without touching them.

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
import dataclasses
import json
import urllib.error
import urllib.request
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.streams import _frame, _provider
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

_WORKFLOW_SIDE_ERROR = (
    "the nexus provider is an outside transport; a worker publishes and reads "
    "through a storage provider, so configure one of those on the worker"
)


@dataclasses.dataclass
class AppendInput:
    workflow_id: str
    stream: str
    producer_id: str
    attempt: int
    batch_index: int
    payloads: list[str] = dataclasses.field(default_factory=list)
    finish: bool = False


@dataclasses.dataclass
class AppendOutput:
    cursor: str = ""


@dataclasses.dataclass
class ReadInput:
    workflow_id: str
    stream: str = ""
    from_token: str = ""
    max_records: int = 100
    wait_ms: int = 5000


@dataclasses.dataclass
class RecordWire:
    token: str
    frame: str


@dataclasses.dataclass
class ReadOutput:
    records: list[RecordWire] = dataclasses.field(default_factory=list)
    next_token: str = ""


@nexusrpc.service
class TemporalStreams:
    append: nexusrpc.Operation[AppendInput, AppendOutput]
    read: nexusrpc.Operation[ReadInput, ReadOutput]


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler:
    """Serves the stream endpoint, delegating to one storage provider.

    The delegate is an explicit instance rather than the process default, so
    a caller and a handler can coexist in one process, and so an operator can
    run handlers for a new store next to handlers for the old one.
    """

    def __init__(self, client: Any, provider: str, **provider_options: Any) -> None:
        self._client = client
        self._delegate = _provider.instance(provider, **provider_options)
        self._producers: dict[tuple[str, str, str, int], tuple[Any, int]] = {}

    @nexusrpc.handler.sync_operation
    async def append(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        key = (input.workflow_id, input.stream, input.producer_id, input.attempt)
        cached = self._producers.get(key)
        if cached is not None and input.batch_index <= cached[1]:
            return AppendOutput()
        if cached is None:
            delegate = await self._delegate.producer(
                self._client,
                workflow_id=input.workflow_id,
                stream=input.stream,
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
        delegate = await self._delegate.consumer(
            self._client, workflow_id=input.workflow_id, stream=input.stream
        )
        converter = temporalio.converter.DataConverter.default.payload_converter
        records: list[RecordWire] = []
        next_token = input.from_token
        subscription = delegate.read(
            start=Cursor(input.from_token) if input.from_token else BEGINNING
        )
        try:
            async with asyncio.timeout(input.wait_ms / 1000):
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
                    if len(records) >= input.max_records:
                        break
        except TimeoutError:
            pass
        finally:
            await subscription.aclose()
        return ReadOutput(records=records, next_token=next_token)


def _post(url: str, body: dict) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"stream endpoint call failed ({error.code}): {detail}")
    return json.loads(raw) if raw else {}


class NexusProducer:
    """Appends through the stream endpoint; framing happens behind it."""

    def __init__(
        self, base_url: str, converter: Any, workflow_id: str, stream: str,
        producer_id: str, attempt: int,
    ) -> None:
        self._base_url = base_url
        self._converter = converter
        self._workflow_id = workflow_id
        self._stream = stream
        self._producer_id = producer_id
        self._attempt = attempt
        self._batch_index = 0

    @property
    def attempt(self) -> int:
        return self._attempt

    async def append(self, *values: Any) -> Cursor:
        payloads = []
        for value in values:
            payload = (
                value
                if isinstance(value, Payload)
                else self._converter.to_payloads([value])[0]
            )
            payloads.append(base64.b64encode(payload.SerializeToString()).decode())
        return Cursor((await self._call(payloads=payloads)).get("cursor", ""))

    async def finish(self) -> None:
        await self._call(finish=True)

    async def _call(self, payloads: list[str] | None = None, finish: bool = False) -> dict:
        self._batch_index += 1
        return await asyncio.to_thread(
            _post,
            f"{self._base_url}/append",
            dataclasses.asdict(
                AppendInput(
                    workflow_id=self._workflow_id,
                    stream=self._stream,
                    producer_id=self._producer_id,
                    attempt=self._attempt,
                    batch_index=self._batch_index,
                    payloads=payloads or [],
                    finish=finish,
                )
            ),
        )


class NexusConsumer:
    """Reads batches from the stream endpoint, re-synthesizing supersession."""

    def __init__(self, base_url: str, converter: Any, workflow_id: str, stream: str) -> None:
        self._base_url = base_url
        self._converter = converter
        self._workflow_id = workflow_id
        self._stream = stream

    async def read(
        self,
        *,
        start: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncIterator[StreamRecord[Any]]:
        attempts = AttemptTracker()
        token = start.token
        last_yielded: str | None = None
        while True:
            raw = await asyncio.to_thread(
                _post,
                f"{self._base_url}/read",
                dataclasses.asdict(
                    ReadInput(
                        workflow_id=self._workflow_id,
                        stream=self._stream,
                        from_token=token,
                    )
                ),
            )
            for wire in raw.get("records", []):
                if wire["token"] == last_yielded:
                    # An opaque token cannot be advanced past itself, so a
                    # resumed batch starts with the record already yielded.
                    continue
                try:
                    kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                        base64.b64decode(wire["frame"])
                    )
                except ValueError:
                    continue
                if topic is not None and frame_topic != topic:
                    last_yielded = wire["token"]
                    continue
                cursor = Cursor(wire["token"])
                superseded = attempts.note(source, attempt, cursor)
                if superseded is not None:
                    yield superseded
                yield StreamRecord(
                    value=self._decode(body, type) if kind is RecordKind.DATA else None,
                    cursor=cursor,
                    kind=kind,
                    topic=frame_topic,
                    producer=source,
                    attempt=attempt,
                    sequence=sequence,
                )
                last_yielded = wire["token"]
            token = raw.get("next_token", token) or token

    def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        if as_type is None:
            return self._converter.from_payloads([payload])[0]
        return self._converter.from_payloads([payload], [as_type])[0]


class _NexusProvider:
    name = "nexus"

    def __init__(self) -> None:
        self._base_url: str | None = None

    def configure(self, **options: Any) -> None:
        endpoint = options.pop("endpoint", None)
        http_address = options.pop("http_address", "http://127.0.0.1:7243")
        service = options.pop("service", "TemporalStreams")
        if options:
            raise TypeError(
                "the nexus provider takes endpoint, http_address and service, "
                f"got {sorted(options)}"
            )
        if not endpoint:
            raise TypeError("the nexus provider needs endpoint=<nexus endpoint name>")
        self._base_url = (
            f"{http_address}/nexus/endpoints/{endpoint}/services/{service}"
        )

    def worker_options(self) -> dict[str, Any]:
        raise RuntimeError(_WORKFLOW_SIDE_ERROR)

    def open_read(
        self,
        stream: str,
        *,
        start: Cursor = BEGINNING,
        idle_timeout: timedelta | None = None,
    ) -> ReadSource:
        raise RuntimeError(_WORKFLOW_SIDE_ERROR)

    def open_write(self, topic: str) -> WriteSink:
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
        stream: str,
        producer_id: str = "",
        attempt: int = 0,
    ) -> NexusProducer:
        if not producer_id:
            from temporalio import activity

            producer_id = activity.info().activity_id
            attempt = attempt or activity.info().attempt
        return NexusProducer(
            self._url(),
            _converter(client),
            workflow_id,
            stream,
            producer_id,
            attempt,
        )

    async def consumer(
        self, client: Any, *, workflow_id: str, stream: str = ""
    ) -> NexusConsumer:
        return NexusConsumer(self._url(), _converter(client), workflow_id, stream)


def _converter(client: Any) -> Any:
    if client is None:
        return temporalio.converter.DataConverter.default.payload_converter
    return client.data_converter.payload_converter


_provider.register("nexus", _NexusProvider)
