"""The server-side binding of the stream interface.

Streams live on the Temporal server. A workflow publishes with a command that
the server applies in the same transaction that accepts the workflow task, and
consumes ranges the server attaches to the workflow task it dispatches.

This is the only module of the package that differs between the two
implementations.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.client_stream import StreamClient
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

PROVIDER = "server"


def configure(**options: Any) -> None:
    """Name the provider for this process.

    Nothing to name here: the streams are on the server the client is already
    connected to. It exists so a process that switches providers changes one
    call rather than its structure.
    """
    if options:
        raise TypeError(
            f"the server-side provider takes no options, got {sorted(options)}"
        )


def worker_options() -> dict[str, Any]:
    """What a ``Worker`` or ``Replayer`` needs to serve this provider."""
    return {}

_RUN_STATE = "__temporal_streams_state"


class _Fanout:
    """One server subscription, shared by every reader of that stream.

    The server sends a range once. Whoever reads it first would otherwise take
    it from the others, so the pull happens here and the result goes to all
    live readers.
    """

    def __init__(self, stream: str) -> None:
        self._stream = stream
        self._subscribers: list[list[tuple[Cursor, bytes]]] = []
        self._pull: asyncio.Future[None] | None = None

    def attach(self) -> list[tuple[Cursor, bytes]]:
        queue: list[tuple[Cursor, bytes]] = []
        self._subscribers.append(queue)
        return queue

    def detach(self, queue: list[tuple[Cursor, bytes]]) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)

    async def next_batch(
        self, queue: list[tuple[Cursor, bytes]]
    ) -> list[tuple[Cursor, bytes]]:
        while not queue:
            pending = self._pull
            if pending is None:
                pending = self._pull = asyncio.ensure_future(self._run_pull())
            # Shielded because one reader going away must not cancel a range
            # the server will not send again.
            await asyncio.shield(pending)
        batch, queue[:] = list(queue), []
        return batch

    async def _run_pull(self) -> None:
        try:
            delivered = await workflow.read_stream_messages(self._stream)
            arrived = [
                (Cursor(str(message.offset)), message.body) for message in delivered
            ]
            for queue in self._subscribers:
                queue.extend(arrived)
        finally:
            self._pull = None


def _fanouts() -> dict[str, _Fanout]:
    instance = workflow.instance()
    state = getattr(instance, _RUN_STATE, None)
    if state is None:
        state = {}
        setattr(instance, _RUN_STATE, state)
    return state


class _NativeReadSource:
    def __init__(self, fanout: _Fanout) -> None:
        self._fanout = fanout
        self._queue = fanout.attach()

    async def next_batch(self) -> list[tuple[Cursor, bytes]]:
        return await self._fanout.next_batch(self._queue)

    def close(self) -> None:
        self._fanout.detach(self._queue)


class _NativeWriteSink:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    async def publish(self, frame: bytes) -> None:
        # Nothing is awaited. The server applies the payload when it accepts
        # the task, so there is no back pressure to wait on and no round trip
        # to pay for. The signature is awaitable because the contract allows a
        # provider that does hold a publisher back.
        workflow.add_stream_messages([frame], stream_id="", topic=self._topic)


def inbound_stream_id(workflow_id: str, stream: str) -> str:
    """The server-side id of a workflow's inbound stream.

    A workflow names its streams relative to itself, so the two providers can
    address the same thing without workflow code knowing how either of them
    stores it. Here that is a namespace-level stream id built from the pair,
    and the workflow id rather than the run id because the stream has to
    survive continue-as-new and reset.
    """
    return f"{workflow_id}:{stream}"


def open_read(
    stream: str,
    *,
    start: Cursor = BEGINNING,
    idle_timeout: timedelta | None = None,
) -> ReadSource:
    """Subscribe the running workflow to its inbound stream ``stream``."""
    # Accepted and ignored. Delivery arrives on workflow tasks the server
    # dispatches, so no worker is held between records and there is nothing for
    # an idle timeout to release.
    del idle_timeout
    stream_id = inbound_stream_id(workflow.info().workflow_id, stream)
    fanouts = _fanouts()
    fanout = fanouts.get(stream_id)
    if fanout is None:
        workflow.subscribe_stream(
            stream_id, start_offset=int(start.token) if start.token else 0
        )
        fanout = fanouts[stream_id] = _Fanout(stream_id)
    return _NativeReadSource(fanout)


def open_write(topic: str) -> WriteSink:
    """Bind ``topic`` on the stream the running workflow owns."""
    return _NativeWriteSink(topic)


def _reject_configured_codec(client: Client) -> None:
    """Refuse a namespace whose payloads are meant to be encoded.

    Stream bodies do not pass through the codec chain on this provider, so
    proceeding would store in the clear what the namespace expects encrypted,
    and would do it without saying so.
    """
    if client.data_converter.payload_codec is not None:
        raise RuntimeError(
            "this client has a payload codec configured, and server-side stream "
            "bodies do not pass through it"
        )


# Streams this process has already opened, so a second producer for the same
# stream reuses the handle rather than asking the server to create it again.
# The second create is answered correctly, and it is still a failed call the
# server logs, which is noise an operator has to learn to ignore.
_handles: dict[str, Any] = {}


# One channel per target and namespace, shared by every handle in the process.
# A channel is multiplexed and long lived, and callers open a handle per
# subscription, which would otherwise be a connection per subscription.
_channels: dict[tuple[int, str, str], StreamClient] = {}


def _stream_client(client: Client) -> StreamClient:
    target = client.service_client.config.target_host
    # Keyed on the loop as well: a gRPC channel belongs to the loop that made it.
    key = (id(asyncio.get_event_loop()), target, client.namespace)
    existing = _channels.get(key)
    if existing is None:
        existing = StreamClient.connect(target, client.namespace)
        _channels[key] = existing
    return existing


class Producer:
    """Appends to a stream from outside workflow code.

    Every append is visible as soon as it is written. That is the point for an
    activity streaming model output, and it is why an activity carries its own
    identity: the retry of a failed attempt has no commit boundary to sort it
    out afterwards.
    """

    def __init__(
        self,
        handle: Any,
        converter: Any,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Prefer :func:`producer`."""
        self._handle = handle
        self._converter = converter
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = 0

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    @property
    def _provider_id(self) -> str:
        """The identity the provider dedupes on.

        The attempt is part of it. Deduplication answers "is this the same
        append again", and a second attempt writing different words at the same
        sequence is not. Folding the attempt in keeps a retried append idempotent
        without letting a new generation be swallowed as a duplicate of the old
        one.
        """
        return f"{self._producer_id}#{self._attempt}" if self._attempt else self._producer_id

    async def append(self, *values: Any) -> Cursor:
        """Append values and return where the first one landed."""
        frames = []
        for value in values:
            frames.append(
                _frame.encode(
                    topic=self._topic,
                    kind=RecordKind.DATA,
                    producer=self._producer_id,
                    attempt=self._attempt,
                    sequence=self._sequence,
                    body=self._encode(value),
                )
            )
            self._sequence += 1
        offset = await self._handle.append(
            *frames,
            topic=self._topic,
            producer_id=self._provider_id,
            # The provider dedupes a retried append on this pair, which is a
            # different job from telling readers that a new generation started.
            sequence=self._sequence - len(frames),
        )
        return Cursor(str(offset))

    async def finish(self) -> None:
        """Declare this stream complete."""
        frame = _frame.encode(
            topic=self._topic,
            kind=RecordKind.FINISH,
            producer=self._producer_id,
            attempt=self._attempt,
            sequence=self._sequence,
            body=b"",
        )
        self._sequence += 1
        await self._handle.append(
            frame,
            topic=self._topic,
            producer_id=self._provider_id,
            sequence=self._sequence - 1,
        )

    def _encode(self, value: Any) -> bytes:
        payload = (
            value if isinstance(value, Payload) else self._converter.to_payloads([value])[0]
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
    _reject_configured_codec(client)
    if not producer_id:
        producer_id = activity.info().activity_id
    if not attempt:
        attempt = activity.info().attempt
    streams = _stream_client(client)
    stream_id = inbound_stream_id(workflow_id, stream)
    handle = _handles.get(stream_id)
    if handle is None:
        try:
            handle = await streams.create(stream_id)
        except Exception:
            # Created by whoever set the stream up. A producer opening a stream
            # it does not own is the ordinary case, not the exception.
            handle = streams.get(stream_id)
        _handles[stream_id] = handle
    return Producer(
        handle,
        client.data_converter.payload_converter,
        stream,
        producer_id,
        attempt,
    )


class Consumer:
    """Reads a stream from outside workflow code, resumably."""

    def __init__(self, handle: Any, converter: Any) -> None:
        """Prefer :func:`consumer`."""
        self._handle = handle
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
        attempts = AttemptTracker()
        async for message in self._handle.follow(
            from_offset=int(start.token) if start.token else 0
        ):
            cursor = Cursor(str(message.offset))
            try:
                kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                    message.data
                )
            except ValueError:
                continue
            if topic is not None and frame_topic != topic:
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
    :func:`temporalio.streams.writer`. Naming one reads that inbound stream
    instead, which is how a second consumer follows the same input.
    """
    _reject_configured_codec(client)
    streams = _stream_client(client)
    if stream:
        handle: Any = streams.get(inbound_stream_id(workflow_id, stream))
    else:
        handle = streams.workflow_stream(workflow_id)
    return Consumer(handle, client.data_converter.payload_converter)
