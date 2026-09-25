"""The server-side (native) provider.

Streams live on the Temporal server, beside the workflow that owns them. A
topic is one owned stream named after the topic, created by whoever touches it
first: the workflow publishes to it with a command the server applies in the
transaction that accepts the Workflow Task, subscribes to it by name and reads
the ranges the server delivers on its Workflow Tasks; outside code appends and
reads through the stream service, and the workflow's records and an outside
producer's land in one log in the order the server accepted them.

A cursor names the run as well as the offset, because an owned stream belongs
to one run and a successor's starts over at zero. A handle without a run id
reads run after run, learning from the poll that a run's stream is closed and
from the run's close event who came next; with a run id it is pinned.

Prototype support for AI-198. It needs a server built from that branch and
opens its own gRPC channel to it, because sdk-core does not know the stream
service yet, which is also why it does not support TLS or API keys.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import Any, Generic, TypeVar

from temporalio import workflow
from temporalio.client import Client, WorkflowHistoryEventFilterType
from temporalio.client_stream import (
    StreamClient,
    WorkflowStreamHandle,
    close_shared_clients,
    shared_client,
)
from temporalio.converter import PayloadCodec, PayloadConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._errors import StreamCursorError, StreamNotFoundError
from temporalio.streams._provider import ReadSource, WriteSink
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord
from temporalio.streams._topic import StreamTopic, resolve_topic
from temporalio.streams._wire import (
    RecordDecoder,
    WireRecord,
    cursor_position,
    mint_cursor,
    producer_identity,
    to_wire,
)
from temporalio.streams.providers import ProviderPlugin

__all__ = ["NativeProducer", "NativeStreamHandle", "NativeStreams"]

T = TypeVar("T")

_PROVIDER = "native"

logger = logging.getLogger(__name__)


def _require_topic(topic: str) -> None:
    if not topic:
        raise ValueError("topic must not be empty")


def _cursor(run_id: str, offset: int) -> Cursor:
    return mint_cursor(_PROVIDER, f"{run_id}:{offset}")


def _position(after: Cursor) -> tuple[str, int] | None:
    """The ``(run id, offset)`` a cursor of this provider names, or ``None`` for BEGINNING."""
    token = cursor_position(after, provider=_PROVIDER)
    if token is None:
        return None
    run_id, _, offset = token.rpartition(":")
    try:
        if not run_id:
            raise ValueError
        return run_id, int(offset)
    except ValueError:
        raise StreamCursorError(
            f"cursor {after.token!r} does not name a run and an offset on the "
            "native provider"
        ) from None


async def _encode_body(codec: PayloadCodec | None, record: WireRecord) -> WireRecord:
    # The worker's payload visitor runs a codec over the bodies a workflow
    # publishes and receives; the outside half has no such pass, so it applies
    # the client's codec here or the two sides would not agree.
    if codec is None or not record.HasField("body"):
        return record
    encoded = await codec.encode([record.body])
    record.body.CopyFrom(encoded[0])
    return record


async def _decode_body(codec: PayloadCodec | None, record: WireRecord) -> WireRecord:
    if codec is None or not record.HasField("body"):
        return record
    decoded = await codec.decode([record.body])
    record.body.CopyFrom(decoded[0])
    return record


class _NativeReadSource:
    """One subscription of the running workflow, fed by delivered ranges."""

    def __init__(self, stream_id: str, run_id: str) -> None:
        self._stream_id = stream_id
        self._run_id = run_id
        self._closed = False

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        if self._closed:
            raise StopAsyncIteration
        delivered = await workflow._read_stream_records(self._stream_id)
        if self._closed:
            # Closed while this was parked; the buffer woke it with nothing.
            raise StopAsyncIteration
        return [(_cursor(self._run_id, item.offset), item.record) for item in delivered]

    def close(self) -> None:
        """Stop reading, and stop keeping what the server keeps delivering.

        The server has no unsubscribe command, so ranges keep arriving on
        every Workflow Task for the life of the run. What this ends is the
        reading and the keeping: nothing further is held for this stream, so
        a run that closes a reader early does not grow for the rest of its
        life. The subscription itself, and the delivery it costs each task,
        stay until the run ends.
        """
        if self._closed:
            return
        self._closed = True
        workflow._close_stream_records(self._stream_id)


class _NativeWriteSink:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    def publish(self, record: WireRecord) -> None:
        # Held by the runtime until the task completes, when the task's
        # records on this topic become one command the server applies with
        # the task: rule 1 through the server's own commit.
        workflow._append_stream_records([record], stream_id=self._topic)


class _NativeWorkflowProvider:
    """The workflow half: the server's commands and delivered ranges."""

    def open_reader(self, topic: str, *, after: Cursor) -> ReadSource:
        _require_topic(topic)
        run_id = workflow.info().run_id
        start = 0
        named = _position(after)
        if named is not None:
            if named[0] != run_id:
                raise StreamCursorError(
                    f"cursor {after.token!r} names another run; a run's stream is its own"
                )
            start = named[1] + 1
        workflow._subscribe_stream(topic, start_offset=start)
        return _NativeReadSource(topic, run_id)

    def open_writer(self, topic: str) -> WriteSink:
        _require_topic(topic)
        return _NativeWriteSink(topic)

    def on_workflow_start(self) -> None:
        pass

    async def on_workflow_finish(self) -> None:
        pass


class NativeProducer(Generic[T]):
    """Appends to a topic from outside workflow code.

    Every append is visible as soon as the server accepts it. That is the
    point for an activity streaming model output, and it is why an activity
    carries its own identity: the retry of a failed attempt has no commit
    boundary to sort it out afterwards.
    """

    def __init__(
        self,
        handle: WorkflowStreamHandle,
        pin: Any,
        codec: PayloadCodec | None,
        converter: PayloadConverter,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic`` on the stream ``handle`` names."""
        self._handle = handle
        self._pin = pin
        self._codec = codec
        self._converter = converter
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        # One-based, because zero on the wire says the producer does not
        # number its records and this one does.
        self._sequence = 1
        self._last = BEGINNING

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    @property
    def _writer(self) -> str:
        # The server dedupes on this and the sequence. The attempt is part of
        # it so a retried append is dropped while a new generation writing
        # different words at the same sequence is not.
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: T) -> Cursor:
        """Append ``values`` and return the cursor of the last record as stored.

        A repeat returns where the original landed, because the server
        answers a deduplicated batch with the original offsets; an empty call
        returns the position of this producer's last record.
        """
        if not values:
            return self._last
        return await self._write(
            [
                to_wire(
                    self._converter,
                    topic=self._topic,
                    kind=RecordKind.DATA,
                    value=value,
                    producer_id=self._producer_id,
                    attempt=self._attempt,
                    sequence=self._sequence + index,
                )
                for index, value in enumerate(values)
            ]
        )

    async def finish(self) -> None:
        """Write ``FINISH`` for this producer on this topic."""
        await self._write(
            [
                to_wire(
                    self._converter,
                    topic=self._topic,
                    kind=RecordKind.FINISH,
                    producer_id=self._producer_id,
                    attempt=self._attempt,
                    sequence=self._sequence,
                )
            ]
        )

    async def _write(self, records: list[WireRecord]) -> Cursor:
        # Pinned before the first write, so every cursor this producer hands
        # out names the run its records landed in.
        if not self._handle.owner_run_id:
            self._handle.pin(await self._pin())
        for record in records:
            await _encode_body(self._codec, record)
        appended = await self._handle.append(
            *records, producer_id=self._writer, sequence=self._sequence
        )
        self._sequence += len(records)
        self._last = _cursor(self._handle.owner_run_id, appended.next_offset - 1)
        return self._last


class NativeStreamHandle:
    """One workflow's topics from outside, over the stream service."""

    def __init__(
        self,
        client: Client,
        workflow_id: str,
        run_id: str | None,
        *,
        opened: set[tuple[str, str]] | None = None,
    ) -> None:
        """Address ``workflow_id``'s topics, pinned to ``run_id`` when one is given.

        ``opened`` is where this handle records the shared channel it used, so
        the provider that made it closes that one and no other.
        """
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._opened = set() if opened is None else opened
        self._converter = client.data_converter.payload_converter
        self._codec = client.data_converter.payload_codec
        self._streams: StreamClient | None = None

    def _service(self) -> StreamClient:
        # Resolved on first use, because the shared channel belongs to the
        # running loop and a handle may be made before there is one.
        if self._streams is None:
            key = (
                self._client.service_client.config.target_host,
                self._client.namespace,
            )
            self._streams = shared_client(*key)
            self._opened.add(key)
        return self._streams

    def _stream(self, topic: str, run_id: str) -> WorkflowStreamHandle:
        return self._service().workflow_stream(
            self._workflow_id, topic, owner_run_id=run_id
        )

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` after ``after`` until the chain, or the pinned run, closes."""
        topic, result_type = resolve_topic(topic, result_type)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        named = _position(after)
        if named is not None and self._run_id is not None and named[0] != self._run_id:
            raise StreamCursorError(
                f"cursor {after.token!r} names another run than this handle is pinned to"
            )
        return self._read(topic, named, after, result_type)

    async def _read(
        self,
        topic: str,
        named: tuple[str, int] | None,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        if named is not None:
            run_id, offset = named[0], named[1] + 1
        else:
            run_id, offset = self._run_id or await self._first_run(), 0
        while True:
            stream = self._stream(topic, run_id)
            while True:
                page = await stream.poll(from_offset=offset)
                for entry in page.entries:
                    record = await _decode_body(self._codec, entry.record)
                    for out in decoder.decode(_cursor(run_id, entry.offset), record):
                        yield out
                offset = page.next_offset
                # On a pinned stream the server reports the run's end as closed,
                # and a closed stream is finished once its head is delivered.
                if page.closed and offset >= page.head_offset:
                    break
            if self._run_id is not None:
                return
            successor = await self._successor(run_id)
            if successor is None:
                return
            run_id, offset = successor, 0

    async def latest(self, *, topic: str | StreamTopic[Any]) -> Cursor:
        """The cursor of the newest record on ``topic``, naming the run it was read from.

        An empty topic on the chain's first run is the beginning of the
        stream; on a successor it is a position of its own, because
        ``BEGINNING`` would send a chain-following read back to the first run.
        """
        topic, _ = resolve_topic(topic)
        run_id = self._run_id or await self._current_run()
        try:
            head = (await self._stream(topic, run_id).describe()).head_offset
        except StreamNotFoundError:
            # A topic nobody has written yet does not exist on the server,
            # which is the same answer as an empty one.
            head = 0
        if head > 0:
            return _cursor(run_id, head - 1)
        if self._run_id is None and await self._predecessor(run_id) is not None:
            return _cursor(run_id, -1)
        return BEGINNING

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any],
        producer_id: str = "",
        attempt: int = 0,
    ) -> NativeProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return NativeProducer(
            self._stream(topic, self._run_id or ""),
            self._current_run,
            self._codec,
            self._converter,
            topic,
            producer_id,
            attempt,
        )

    async def _current_run(self) -> str:
        try:
            description = await self._client.get_workflow_handle(
                self._workflow_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} was not found"
                ) from error
            raise
        assert description.run_id is not None
        return description.run_id

    async def _first_run(self) -> str:
        """The oldest retained run of the chain, walking back from the latest."""
        run_id = await self._current_run()
        while True:
            previous = await self._predecessor(run_id)
            if previous is None:
                return run_id
            run_id = previous

    async def _predecessor(self, run_id: str) -> str | None:
        handle = self._client.get_workflow_handle(self._workflow_id, run_id=run_id)
        try:
            async for event in handle.fetch_history_events(page_size=1):
                attributes = event.workflow_execution_started_event_attributes
                return attributes.continued_execution_run_id or None
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
        # The run's History is gone: the chain's retained part starts here.
        return None

    async def _successor(self, run_id: str) -> str | None:
        handle = self._client.get_workflow_handle(self._workflow_id, run_id=run_id)
        events = handle.fetch_history_events(
            event_filter_type=WorkflowHistoryEventFilterType.CLOSE_EVENT
        )
        try:
            async for event in events:
                if event.HasField(
                    "workflow_execution_continued_as_new_event_attributes"
                ):
                    attributes = (
                        event.workflow_execution_continued_as_new_event_attributes
                    )
                    return attributes.new_execution_run_id or None
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
        return None


class NativeStreams(ProviderPlugin):
    """The server-side provider.

    Takes no options: the streams are on the server the client is already
    connected to. Construct one, pass it to the worker as a plugin and open
    handles from it anywhere else; :meth:`close` releases the channels this
    provider opened to the stream service.
    """

    def __init__(self) -> None:
        """Create the provider."""
        # What this provider's handles opened, so closing it leaves another
        # provider's channels on the same loop alone.
        self._opened: set[tuple[str, str]] = set()

    def workflow_provider(self) -> _NativeWorkflowProvider:
        """The workflow half, over the server's commands and delivered ranges."""
        return _NativeWorkflowProvider()

    def get_stream_handle(
        self, client: Client, workflow_id: str, *, run_id: str | None = None
    ) -> NativeStreamHandle:
        """A handle on ``workflow_id``'s topics; without ``run_id`` it follows the chain."""
        return NativeStreamHandle(client, workflow_id, run_id, opened=self._opened)

    async def close(self) -> None:
        """Close the channels this provider opened to the stream service.

        The application calls this; no worker or client owns the provider's
        lifetime, because one provider serves the workers built from a client
        and every handle opened outside them.
        """
        await close_shared_clients(*self._opened)
        self._opened.clear()
