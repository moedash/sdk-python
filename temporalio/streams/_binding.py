"""The client-side binding of the stream interface.

Streams live in a store the customer runs, Redis here. A workflow's publish is
buffered by the worker, staged invisibly under a token, and promoted only once
a marker in History proves the workflow task that produced it was accepted.
Consumption is recorded the same way, as ranges and boundaries in History.

This is the only module of the package that differs between the two
implementations.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.external_workflow_streams import (
    BEGINNING as PROVIDER_BEGINNING,
    AFTER,
    ExternalOutputStreamClient,
    ExternalStreamProducer,
    WakeNotAcknowledgedError,
    WorkflowChainKey,
    external_output_stream,
    external_stream,
)
from temporalio.streams import _frame
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

__all__ = [
    "Consumer",
    "Producer",
    "configure",
    "consumer",
    "open_read",
    "open_write",
    "producer",
    "worker_options",
]

PROVIDER = "external"

_backend: Any = None


def configure(**options: Any) -> None:
    """Name the provider for this process.

    Args:
        backend: A ``StreamBackend`` instance, such as ``RedisStreamBackend``.
            There is no registry to name one from out here, so the caller
            constructs it and hands it over.
    """
    global _backend
    unknown = sorted(set(options) - {"backend"})
    if unknown:
        raise TypeError(f"unknown provider options {unknown}")
    if "backend" not in options:
        raise TypeError("the client-side provider needs a backend")
    _backend = options["backend"]


def worker_options() -> dict[str, Any]:
    """What a ``Worker`` or ``Replayer`` needs to serve this provider."""
    return {"external_stream_backend": _require_backend()}


def _require_backend() -> Any:
    if _backend is None:
        raise RuntimeError(
            "no stream provider configured; call temporalio.streams.configure("
            "backend=...) before opening a producer, a consumer or a worker"
        )
    return _backend


class _ExternalReadSource:
    def __init__(self, subscription: Any) -> None:
        self._subscription = subscription
        self._records = subscription.records()

    async def next_batch(self) -> list[tuple[Cursor, bytes]]:
        # One record per batch. The provider reports readiness per record, so a
        # batch here would be this binding inventing a boundary the provider
        # never observed, and replay records boundaries.
        offset, body = await self._records.__anext__()
        return [(Cursor(str(offset)), body)]

    def close(self) -> None:
        self._subscription.close()


class _ExternalWriteSink:
    def __init__(self, topic: Any) -> None:
        self._topic = topic

    async def publish(self, frame: bytes) -> None:
        # Awaited for real here. The worker holds a bounded output batch, and a
        # publisher that has filled it waits for room rather than growing it.
        await self._topic.publish(frame)


def open_read(
    stream: str,
    *,
    start: Cursor = BEGINNING,
    idle_timeout: timedelta | None = None,
) -> ReadSource:
    """Subscribe the running workflow to its inbound stream ``stream``."""
    # The provider resolves the name under this run's chain key, so a start
    # position is only meaningful to a reader that already has one, and this
    # provider's first subscription always starts at the beginning of the
    # stream it minted.
    if start is not BEGINNING and start.token:
        raise ValueError(
            "the client-side provider starts a new subscription at the "
            "beginning of the stream it owns; resuming elsewhere is not "
            "supported yet"
        )
    options = external_stream
    if idle_timeout is not None:
        options = options.with_options(idle_timeout=idle_timeout)
    return _ExternalReadSource(options.topic(stream, type=bytes).subscribe())


def open_write(topic: str) -> WriteSink:
    """Bind ``topic`` on the stream the running workflow owns."""
    return _ExternalWriteSink(external_output_stream.topic(topic, type=bytes))


async def _chain_key(client: Client, workflow_id: str) -> WorkflowChainKey:
    description = await client.get_workflow_handle(workflow_id).describe()
    return WorkflowChainKey(
        client.namespace,
        workflow_id,
        description.raw_description.workflow_execution_info.first_run_id,
    )


class Producer:
    """Appends to a stream from outside workflow code.

    Every append is visible as soon as it is written. That is the point for an
    activity streaming model output, and it is why an activity carries its own
    identity: the retry of a failed attempt has no commit boundary to sort it
    out afterwards.
    """

    def __init__(
        self,
        topic: Any,
        converter: Any,
        stream: str,
        producer_id: str,
        attempt: int,
        client: Client | None = None,
        workflow_id: str = "",
    ) -> None:
        """Prefer :func:`producer`."""
        self._topic = topic
        self._converter = converter
        self._stream = stream
        self._producer_id = producer_id
        self._attempt = attempt
        self._client = client
        self._workflow_id = workflow_id
        self._sequence = 0

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    async def append(self, *values: Any) -> Cursor:
        """Append values and return where the first one landed."""
        first: Any = None
        for value in values:
            frame = _frame.encode(
                topic=self._stream,
                kind=RecordKind.DATA,
                producer=self._producer_id,
                attempt=self._attempt,
                sequence=self._sequence,
                body=self._encode(value),
            )
            self._sequence += 1
            offset = await self._topic.publish(frame)
            if first is None:
                first = offset
        return Cursor(str(first))

    async def finish(self) -> None:
        """Declare this stream complete."""
        frame = _frame.encode(
            topic=self._stream,
            kind=RecordKind.FINISH,
            producer=self._producer_id,
            attempt=self._attempt,
            sequence=self._sequence,
            body=b"",
        )
        self._sequence += 1
        try:
            await self._topic.publish(frame)
        except WakeNotAcknowledgedError:
            # The record is appended; what failed is telling a consumer that is
            # no longer there to be told. A terminal record most often races the
            # consumer acting on it, so treating an absent consumer as a failed
            # publish would make the ordinary ending look like an error.
            if not await self._consumer_has_gone():
                raise

    async def _consumer_has_gone(self) -> bool:
        """Whether the consuming execution is closing or closed.

        Asked rather than assumed, and asked for a few seconds rather than
        once: the server refuses the wake while the execution is closing, and
        at that moment its status is still the running one. A single look would
        read "running" and turn the ordinary ending into an error.
        """
        if self._client is None or not self._workflow_id:
            return False
        handle = self._client.get_workflow_handle(self._workflow_id)
        deadline = time.monotonic() + 5
        while True:
            description = await handle.describe()
            status = description.status
            if status is not None and status != WorkflowExecutionStatus.RUNNING:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.1)

    def _encode(self, value: Any) -> bytes:
        payload = (
            value
            if isinstance(value, Payload)
            else self._converter.to_payloads([value])[0]
        )
        return payload.SerializeToString()


async def producer(
    client: Client,
    *,
    workflow_id: str,
    stream: str,
    producer_id: str = "",
    attempt: int = 0,
) -> Producer:
    """Open a producer for the inbound stream ``stream`` of ``workflow_id``.

    Inside an activity, leave ``producer_id`` and ``attempt`` unset: the
    activity's own id and attempt are the right answer and are what let a
    reader tell a retry from a new generation.
    """
    if not producer_id:
        producer_id = activity.info().activity_id
    if not attempt:
        attempt = activity.info().attempt
    session = await ExternalStreamProducer.connect(
        backend=_require_backend(),
        workflow=await _chain_key(client, workflow_id),
        client=client,
        # The attempt is part of it. Deduplication answers "is this the same
        # append again", and a second attempt writing different words at the
        # same sequence is not: this provider rejects that outright. Folding
        # the attempt in keeps a retried append idempotent without letting a
        # new generation be refused as a conflicting duplicate.
        session_id=f"{producer_id}#{attempt}",
    )
    return Producer(
        session.topic(stream, type=bytes),
        client.data_converter.payload_converter,
        stream,
        producer_id,
        attempt,
        client,
        workflow_id,
    )


class Consumer:
    """Reads a stream from outside workflow code, resumably."""

    def __init__(self, client: Any, converter: Any) -> None:
        """Prefer :func:`consumer`."""
        self._client = client
        self._converter = converter

    async def read(
        self,
        *,
        start: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncIterator[StreamRecord[Any]]:
        """Yield records from ``start`` as they arrive.

        Applies the same supersession rule as a workflow reader, so a browser
        and a workflow watching one activity agree on which attempt is current.
        """
        if topic is None:
            raise ValueError(
                "this provider stores each topic separately, so an outside "
                "reader has to name one"
            )
        attempts = AttemptTracker()
        after = PROVIDER_BEGINNING if not start.token else AFTER(start.token)
        handle = self._client.topic(topic, type=bytes)
        async for item in handle.subscribe(after=after):
            cursor = Cursor(str(item.offset))
            try:
                kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                    item.data
                )
            except ValueError:
                continue
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

    def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        if as_type is None:
            return self._converter.from_payloads([payload])[0]
        return self._converter.from_payloads([payload], [as_type])[0]


async def consumer(
    client: Client, *, workflow_id: str, stream: str = ""
) -> Consumer:
    """Open a reader for what ``workflow_id`` publishes.

    An empty ``stream`` reads what the workflow wrote through
    :func:`temporalio.streams.writer`. This provider keeps inbound and outbound
    records in separate stores, so naming an inbound stream is not supported
    from here yet.
    """
    if stream:
        raise ValueError(
            "this provider separates inbound and outbound storage, so an "
            "outside reader cannot follow an inbound stream yet"
        )
    return Consumer(
        await ExternalOutputStreamClient.connect(
            backend=_require_backend(),
            workflow=await _chain_key(client, workflow_id),
            client=client,
        ),
        client.data_converter.payload_converter,
    )
