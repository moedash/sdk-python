"""The server-side (native) provider.

Streams live on the Temporal server. A workflow publishes with a command that
the server applies in the same transaction that accepts the workflow task, and
consumes ranges the server attaches to the workflow task it dispatches.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import grpc

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.client_stream import StreamClient, close_shared_clients, shared_client
from temporalio.streams import _frame, _provider
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._ids import inbound_stream_id
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

_RUN_STATE = "__temporal_streams_state"

logger = logging.getLogger(__name__)


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
_handles: dict[tuple[str, str, str], Any] = {}


def _stream_client(client: Client) -> StreamClient:
    return shared_client(client.service_client.config.target_host, client.namespace)


async def _owner_run(client: Client, workflow_id: str) -> str:
    """The run whose stream a handle on ``workflow_id`` should address.

    Resolved once, when the handle opens. Left to the server, a follower whose
    workflow continued as new would be redirected to the successor, whose
    stream starts empty, and its offset read as "caught up" rather than as a
    position on the run it was watching. A producer is pinned for the same
    reason: an activity's output belongs to the run that scheduled it.
    """
    return (await client.get_workflow_handle(workflow_id).describe()).run_id


class NativeProducer:
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
        """Bind this producer to ``topic`` on the stream ``handle`` names."""
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
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: Any) -> Cursor | None:
        """Append ``values`` and return the last one's cursor, or ``None`` if nothing landed."""
        if not values:
            return None
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
        appended = await self._handle.append(
            *frames,
            topic=self._topic,
            producer_id=self._provider_id,
            # The provider dedupes a retried append on this pair, which is a
            # different job from telling readers that a new generation started.
            sequence=self._sequence - len(frames),
        )
        if appended.deduplicated:
            return None
        return Cursor(str(appended.next_offset - 1))

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
            value
            if isinstance(value, Payload)
            else self._converter.to_payloads([value])[0]
        )
        return payload.SerializeToString()


class NativeConsumer:
    """Reads a stream from outside workflow code, resumably."""

    def __init__(self, handle: Any, converter: Any, stream: str, address: str) -> None:
        """Read the stream ``handle`` names, from anywhere.

        ``stream`` is the inbound name the caller used, empty for the owner's
        stream; ``address`` is what the server calls it, for log lines.
        """
        self._handle = handle
        self._converter = converter
        self._stream = stream
        self._address = address

    async def read(
        self,
        *,
        after: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records after ``after`` as they arrive.

        Applies the same supersession rule as a workflow reader, so a browser
        and a workflow watching one activity agree on which attempt is current.
        """
        _provider.check_topic(self._stream, topic)
        attempts = AttemptTracker()
        async for message in self._handle.follow(
            from_offset=int(after.token) + 1 if after.token else 0
        ):
            cursor = Cursor(str(message.offset))
            try:
                kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                    message.data
                )
            except ValueError as error:
                # Same answer as the workflow-side reader: skip and say so.
                logger.warning(
                    "skipping record %s of stream %s: %s", cursor, self._address, error
                )
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

    async def latest(self, *, topic: str | None = None) -> Cursor:
        """The cursor of the last record written, for following from now."""
        del topic  # one server-side log per stream, whatever the topic
        try:
            state = await self._handle.describe()
        except grpc.aio.AioRpcError as error:
            # A stream nobody has published to does not exist yet, and that
            # is the same answer as an empty one. Any other failure is not:
            # a reader that took it for "empty" would replay the whole stream.
            if error.code() is not grpc.StatusCode.NOT_FOUND:
                raise
            return BEGINNING
        return (
            Cursor(str(state.head_offset - 1)) if state.head_offset > 0 else BEGINNING
        )

    def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        if as_type is None:
            return self._converter.from_payloads([payload])[0]
        return self._converter.from_payloads([payload], [as_type])[0]


class _NativeProvider:
    name = "native"

    def configure(self, **options: Any) -> None:
        """Take no options.

        The streams are on the server the client is already connected to.
        This exists so a process that switches providers changes one call
        rather than its structure.
        """
        if options:
            raise TypeError(
                f"the native provider takes no options, got {sorted(options)}"
            )

    def worker_options(self) -> dict[str, Any]:
        return {}

    async def close(self) -> None:
        """Close the channels this process opened to the stream service."""
        await close_shared_clients()

    def open_read(
        self,
        stream: str,
        *,
        after: Cursor = BEGINNING,
        idle_timeout: timedelta | None = None,
    ) -> ReadSource:
        # Accepted and ignored. Delivery arrives on workflow tasks the server
        # dispatches, so no worker is held between records and there is nothing
        # for an idle timeout to release.
        del idle_timeout
        stream_id = inbound_stream_id(workflow.info().workflow_id, stream)
        fanouts = _fanouts()
        fanout = fanouts.get(stream_id)
        if fanout is None:
            workflow.subscribe_stream(
                stream_id, start_offset=int(after.token) + 1 if after.token else 0
            )
            fanout = fanouts[stream_id] = _Fanout(stream_id)
        return _NativeReadSource(fanout)

    def open_write(self, topic: str) -> WriteSink:
        return _NativeWriteSink(topic)

    async def producer(
        self,
        client: Client,
        *,
        workflow_id: str,
        stream: str = "",
        topic: str = "",
        producer_id: str = "",
        attempt: int = 0,
    ) -> NativeProducer:
        """Open a producer on ``workflow_id``'s account.

        ``stream`` names an inbound stream, a namespace-level stream the
        server keys by the pair. With none, ``topic`` names a topic on the
        stream the workflow publishes, which the server lets any producer
        append to. An inbound record carries no topic: the stream's name is
        its whole address.
        """
        _reject_configured_codec(client)
        streams = _stream_client(client)
        converter = client.data_converter.payload_converter
        if not stream:
            handle: Any = streams.workflow_stream(
                workflow_id, owner_run_id=await _owner_run(client, workflow_id)
            )
            return NativeProducer(handle, converter, topic, producer_id, attempt)
        stream_id = inbound_stream_id(workflow_id, stream)
        key = (client.service_client.config.target_host, client.namespace, stream_id)
        handle = _handles.get(key)
        if handle is None:
            try:
                handle = await streams.create(stream_id)
            except Exception:
                # Created by whoever set the stream up. A producer opening a
                # stream it does not own is the ordinary case, not the
                # exception.
                handle = streams.get(stream_id)
            _handles[key] = handle
        return NativeProducer(handle, converter, topic, producer_id, attempt)

    async def consumer(
        self, client: Client, *, workflow_id: str, stream: str = ""
    ) -> NativeConsumer:
        """Open a reader for what ``workflow_id`` publishes.

        An empty ``stream`` reads what the workflow wrote through
        :func:`temporalio.streams.writer`. Naming one reads that inbound
        stream instead, which is how a second consumer follows the same input.
        """
        _reject_configured_codec(client)
        streams = _stream_client(client)
        address = inbound_stream_id(workflow_id, stream)
        if stream:
            handle: Any = streams.get(address)
        else:
            handle = streams.workflow_stream(
                workflow_id, owner_run_id=await _owner_run(client, workflow_id)
            )
        return NativeConsumer(
            handle, client.data_converter.payload_converter, stream, address
        )


_provider.register("native", _NativeProvider)
