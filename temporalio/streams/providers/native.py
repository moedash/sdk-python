"""The server-side (native) provider.

Streams live on the Temporal server, beside the workflow that owns them. A
topic is one owned stream named after the topic, created by whoever touches it
first: the workflow publishes to it with a command the server applies in the
transaction that accepts the Workflow Task, subscribes to it by name and reads
the ranges the server delivers on its Workflow Tasks; outside code appends and
reads through the stream service, and the workflow's records and an outside
producer's land in one log in the order the server accepted them. The default
topic, :data:`temporalio.streams.DEFAULT_TOPIC`, is the server's default
stream: the server resolves an unnamed stream to that same name, so the
provider sends the name explicitly and a record's topic and its stream's name
never differ.

An activity owns topics of its own, apart from its workflow's: a standalone
activity is its own owner on the server, and an activity a workflow scheduled
is addressed through that workflow. They are one stream per activity
execution, so a retry writes to the same stream, and the server ends them when
the activity reaches a terminal status.

A cursor names the run as well as the offset, because an owned stream belongs
to one run and a successor's starts over at zero. A handle without a run id
reads run after run, learning from the poll that a run's stream is closed and
from the run's close event who came next; with a run id it is pinned.

Prototype support for AI-198. It needs a server built from that branch and
reaches the stream service on a channel of its own, opened with the client's
connection settings (target, TLS, API key, headers, retries), because sdk-core
does not know the service yet.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import AsyncGenerator, Callable
from typing import Any, Generic, TypeVar

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import StreamStartPosition
from temporalio.client import Client, WorkflowHistoryEventFilterType
from temporalio.client_stream import (
    Page,
    SharedKey,
    StreamClient,
    WorkflowStreamHandle,
    close_shared_clients,
    shared_client,
    shared_key,
)
from temporalio.converter import (
    DataConverter,
    StorageDriverStoreContext,
    StorageDriverWorkflowInfo,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._errors import StreamCursorError, StreamNotFoundError
from temporalio.streams._provider import ReadSource, WriteSink
from temporalio.streams._record import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamRecord,
    check_read_start,
)
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

__all__ = [
    "CONTENT_HASH_KEY",
    "NativeActivityStreamHandle",
    "NativeProducer",
    "NativeStreamHandle",
    "NativeStreams",
]

T = TypeVar("T")

_PROVIDER = "native"

CONTENT_HASH_KEY = "temporal.io/content-hash"
"""The record metadata key carrying the hex SHA-256 of the plaintext body."""

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


def _fingerprint(record: WireRecord) -> WireRecord:
    """Stamp ``record`` with the hash of its body as converted, before a codec or offload.

    The server deduplicates a producer's repeat on this when present, so a
    codec that encrypts with a fresh nonce per call cannot turn a retry into
    a divergent write. A record without a body has nothing to compare.
    """
    if not record.HasField("body"):
        return record
    digest = hashlib.sha256(record.body.SerializeToString()).hexdigest()
    record.metadata[CONTENT_HASH_KEY].CopyFrom(
        Payload(metadata={"encoding": b"binary/plain"}, data=digest.encode())
    )
    return record


async def _encode_body(converter: DataConverter, record: WireRecord) -> WireRecord:
    # The worker's payload pass runs the codec and then the external store over
    # the bodies a workflow publishes and receives; the outside half has no such
    # pass, so it applies the client's data converter here in the same order,
    # or the two sides would not agree.
    if not record.HasField("body"):
        return record
    encoded = await converter._encode_payload_sequence([record.body])
    stored = await converter._external_store_payload_sequence(encoded)
    record.body.CopyFrom(stored[0])
    return record


async def _decode_body(converter: DataConverter, record: WireRecord) -> WireRecord:
    if not record.HasField("body"):
        return record
    retrieved = await converter._external_retrieve_payload_sequence([record.body])
    decoded = await converter._decode_payload_sequence(retrieved)
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
        # the task: rule 1 through the server's own commit. The body is still
        # plaintext here; the worker's payload pass runs after the task.
        workflow._append_stream_records([_fingerprint(record)], stream_name=self._topic)


class _NativeWorkflowProvider:
    """The workflow half: the server's commands and delivered ranges."""

    def open_reader(
        self, topic: str, *, after: Cursor, last: int | None = None
    ) -> ReadSource:
        check_read_start(after, last)
        _require_topic(topic)
        run_id = workflow.info().run_id
        # The server resolves the position when it registers the subscription
        # and records the offset on the subscribed event, so replay never
        # resolves it again.
        if last is not None:
            start = StreamStartPosition(last_n=last)
        elif after == END:
            start = StreamStartPosition(tail=True)
        elif after == BEGINNING:
            start = StreamStartPosition(earliest=True)
        else:
            named = _position(after)
            assert named is not None
            if named[0] != run_id:
                raise StreamCursorError(
                    f"cursor {after.token!r} names another run; a run's stream is its own"
                )
            start = StreamStartPosition(offset=named[1] + 1)
        workflow._subscribe_stream(topic, start=start)
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
        converter: DataConverter,
        store_target: Callable[[str], StorageDriverWorkflowInfo],
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic`` on the stream ``handle`` names.

        ``converter`` is the client's data converter, applied to every body
        as the worker applies it to a workflow's own records. ``store_target``
        names the execution an offloaded body is stored under, given the run
        the producer pinned.
        """
        self._handle = handle
        self._pin = pin
        self._converter = converter
        self._store_target = store_target
        self._bound: DataConverter | None = None
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
                    self._converter.payload_converter,
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
                    self._converter.payload_converter,
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
        if self._bound is None:
            # An offloaded body is stored under the execution that owns the
            # stream, as the worker stores a workflow's own.
            self._bound = self._converter._with_store_context(
                StorageDriverStoreContext(
                    target=self._store_target(self._handle.owner_run_id)
                )
            )
        for record in records:
            await _encode_body(self._bound, _fingerprint(record))
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
        opened: set[SharedKey] | None = None,
    ) -> None:
        """Address ``workflow_id``'s topics, pinned to ``run_id`` when one is given.

        ``opened`` is where this handle records the shared channel it used, so
        the provider that made it closes that one and no other.
        """
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._opened = set() if opened is None else opened
        self._data_converter = client.data_converter
        self._converter = client.data_converter.payload_converter
        self._streams: StreamClient | None = None

    def _service(self) -> StreamClient:
        # Resolved on first use, because the shared channel belongs to the
        # running loop and a handle may be made before there is one.
        if self._streams is None:
            self._streams = shared_client(self._client)
            self._opened.add(shared_key(self._client))
        return self._streams

    def _stream(self, topic: str, run_id: str) -> WorkflowStreamHandle:
        return self._service().workflow_stream(
            self._workflow_id, topic, owner_run_id=run_id
        )

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` from where the read starts until the chain, or the pinned run, closes.

        ``BEGINNING`` is the oldest record the chain's first retained run
        still holds. ``END`` and ``last=`` start on the current run, or the
        pinned one: an earlier run of a chain has ended and holds neither the
        tail nor the newest records. The server resolves each on the first
        poll, in the read that serves it.
        """
        check_read_start(after, last)
        topic, result_type = resolve_topic(topic, result_type)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        named = None if after == END else _position(after)
        if named is not None and self._run_id is not None and named[0] != self._run_id:
            raise StreamCursorError(
                f"cursor {after.token!r} names another run than this handle is pinned to"
            )
        start: StreamStartPosition | None = None
        if last is not None:
            start = StreamStartPosition(last_n=last)
        elif after == END:
            start = StreamStartPosition(tail=True)
        elif named is None:
            start = StreamStartPosition(earliest=True)
        return self._read(topic, named, start, after, result_type)

    async def _read(
        self,
        topic: str,
        named: tuple[str, int] | None,
        start: StreamStartPosition | None,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder: RecordDecoder | None = None
        offset = 0
        if named is not None:
            run_id, offset = named[0], named[1] + 1
        elif start is not None and start.WhichOneof("position") != "earliest":
            run_id = self._run_id or await self._current_run()
        else:
            run_id = self._run_id or await self._first_run()
        while True:
            stream = self._stream(topic, run_id)
            while True:
                page = await stream.poll(from_offset=offset, start=start)
                if decoder is None:
                    decoder = RecordDecoder(
                        self._converter,
                        result_type,
                        after=self._previous(run_id, page, start, after),
                        warn=logger.warning,
                    )
                start = None
                for entry in page.entries:
                    record = await _decode_body(self._data_converter, entry.record)
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

    @staticmethod
    def _previous(
        run_id: str, page: Page, start: StreamStartPosition | None, after: Cursor
    ) -> Cursor:
        """The position before the first record a read yields.

        A synthesized record is positioned there. After ``END`` or ``last=``
        it is only known once the server resolved the start, from the first
        page. It names a run so a chain-following resume stays on this one.
        """
        if start is None or start.WhichOneof("position") == "earliest":
            return after
        first = page.entries[0].offset if page.entries else page.next_offset
        return _cursor(run_id, first - 1)

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
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
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str = "",
        attempt: int = 0,
    ) -> NativeProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return NativeProducer(
            self._stream(topic, self._run_id or ""),
            self._current_run,
            self._data_converter,
            self._store_target,
            topic,
            producer_id,
            attempt,
        )

    def _store_target(self, run_id: str) -> StorageDriverWorkflowInfo:
        """The execution an offloaded body of this owner's stream is stored under."""
        return StorageDriverWorkflowInfo(
            namespace=self._client.namespace, id=self._workflow_id, run_id=run_id
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


class NativeActivityStreamHandle(NativeStreamHandle):
    """The topics one activity owns, from outside, over the stream service.

    An activity's streams belong to one activity execution, not to a chain of
    runs: a retry writes to the same stream and a read ends when the activity
    reaches a terminal status. So the handle pins the execution on first use
    and never follows a successor. A standalone activity is its own owner; an
    activity a workflow scheduled is reached through that workflow's run.
    """

    def __init__(
        self,
        client: Client,
        activity_id: str,
        workflow_id: str | None,
        run_id: str | None,
        *,
        opened: set[SharedKey] | None = None,
    ) -> None:
        """Address ``activity_id``'s topics, pinned to ``run_id`` when one is given."""
        super().__init__(client, workflow_id or "", run_id, opened=opened)
        self._activity_id = activity_id

    def _stream(self, topic: str, run_id: str) -> WorkflowStreamHandle:
        return self._service().activity_stream(
            self._activity_id, topic, workflow_id=self._workflow_id, run_id=run_id
        )

    def _store_target(self, run_id: str) -> StorageDriverWorkflowInfo:
        # A workflow's activity stores under that workflow, as the worker does
        # for its activities; a standalone activity has no workflow to name.
        if self._workflow_id:
            return super()._store_target(run_id)
        return StorageDriverWorkflowInfo(namespace=self._client.namespace)

    async def _current_run(self) -> str:
        if self._workflow_id:
            return await super()._current_run()
        try:
            description = await self._client.get_activity_handle(
                self._activity_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"activity {self._activity_id!r} was not found"
                ) from error
            raise
        assert description.activity_run_id is not None
        return description.activity_run_id

    async def _first_run(self) -> str:
        return await self._current_run()

    async def _predecessor(self, run_id: str) -> str | None:
        return None

    async def _successor(self, run_id: str) -> str | None:
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
        self._opened: set[SharedKey] = set()

    def workflow_provider(self) -> _NativeWorkflowProvider:
        """The workflow half, over the server's commands and delivered ranges."""
        return _NativeWorkflowProvider()

    def get_stream_handle(
        self, client: Client, workflow_id: str, *, run_id: str | None = None
    ) -> NativeStreamHandle:
        """A handle on ``workflow_id``'s topics; without ``run_id`` it follows the chain."""
        return NativeStreamHandle(client, workflow_id, run_id, opened=self._opened)

    def get_activity_stream_handle(
        self,
        client: Client,
        activity_id: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
    ) -> NativeActivityStreamHandle:
        """A handle on the topics ``activity_id`` owns, apart from any workflow's.

        Without ``workflow_id`` the activity is a standalone one and ``run_id``
        pins its run; with one it is that workflow's activity and ``run_id``
        pins the workflow's run.
        """
        return NativeActivityStreamHandle(
            client, activity_id, workflow_id, run_id, opened=self._opened
        )

    async def close(self) -> None:
        """Close the channels this provider opened to the stream service.

        The application calls this; no worker or client owns the provider's
        lifetime, because one provider serves the workers built from a client
        and every handle opened outside them.
        """
        await close_shared_clients(*self._opened)
        self._opened.clear()
