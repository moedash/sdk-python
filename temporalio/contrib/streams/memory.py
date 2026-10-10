"""The in-process reference provider, for tests.

.. warning::
    This module is experimental and may change in future versions.

It exists so the conformance suite can run the whole outside surface without
a store, and to show in one file what a provider owes. Its limits:

- It is not replay-safe and not durable. Everything lives in process memory.
  Do not use it to show recovery, and do not use it in production.
- Streams are keyed by namespace, Workflow id and topic, not by run, so a
  reader that follows the chain sees every run's records. A ``run_id`` on a
  ref only decides which run's close ends a read.
- A read learns that the owner closed by describing the Workflow through the
  handle's client. A handle opened with no client reads until the caller
  stops.
- It keeps every record until :meth:`MemoryStreams.truncate` drops the oldest
  ones, which stands in for a store's retention in tests.

The outside surface (producer identity, retry deduplication, positions,
``SUPERSEDED``, stream-bound cursors, expired cursors) is faithful, which is
what the conformance suite checks.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

from google.protobuf.message import DecodeError

import temporalio.converter
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams._body import (
    content_fingerprint,
    decode_body,
    encode_body,
)
from temporalio.contrib.streams._cursor import (
    BEGINNING,
    END,
    cursor_position,
    mint_cursor,
    stream_hash,
)
from temporalio.contrib.streams._errors import (
    StreamCursorError,
    StreamExpiredError,
    StreamProducerError,
)
from temporalio.contrib.streams._plugin import StreamProviderPlugin
from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import RecordDecoder, WireRecord, to_wire
from temporalio.service import RPCError, RPCStatusCode

__all__ = ["MemoryProducer", "MemoryStreamHandle", "MemoryStreams"]

_PROVIDER = "memory"
_DEFAULT_NAMESPACE = "default"

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _wake(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


@dataclass(frozen=True)
class _Batch:
    """The newest batch one producer attempt wrote: its high-water mark."""

    sequence: int
    first: int
    count: int
    content: bytes


class _Topic:
    """One topic's records, and the readers parked on its tail."""

    def __init__(self) -> None:
        # The retained records, the first of which sits at offset ``base``.
        # Offsets are never reused, so a cursor keeps naming the same record
        # after truncation drops the ones before it.
        self.base = 0
        self.records: list[bytes] = []
        # One high-water mark per producer attempt, so the state stays
        # bounded however long the producer writes.
        self.sessions: dict[tuple[str, int], _Batch] = {}
        # Each waiter is parked with its loop, because an append may run on
        # another thread, and waking a foreign loop's future needs
        # call_soon_threadsafe.
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []

    @property
    def head(self) -> int:
        """The offset the next record lands at."""
        return self.base + len(self.records)

    def append(
        self,
        wires: list[WireRecord],
        *,
        session: tuple[str, int] | None = None,
        sequence: int = 0,
        content: bytes = b"",
    ) -> tuple[int, int]:
        """Store ``wires`` and return where they landed as ``(first offset, count)``.

        With a ``session``, a repeat of that session's newest batch carrying
        the same ``content`` stores nothing and returns where the original
        landed.

        Raises:
            StreamProducerError: ``sequence`` is the newest batch's with
                different content, or below it.
        """
        if session is not None:
            held = self.sessions.get(session)
            if held is not None:
                if sequence == held.sequence:
                    if content != held.content:
                        raise StreamProducerError(
                            f"producer {session[0]!r} attempt {session[1]} already "
                            f"used sequence {sequence} with different content"
                        )
                    return held.first, held.count
                if sequence < held.sequence:
                    raise StreamProducerError(
                        f"producer {session[0]!r} attempt {session[1]} sent sequence "
                        f"{sequence}, below the newest one the store holds, "
                        f"{held.sequence}"
                    )
        first = self.head
        self.records.extend(
            wire.SerializeToString(deterministic=True) for wire in wires
        )
        if session is not None:
            self.sessions[session] = _Batch(sequence, first, len(wires), content)
        self._wake_waiters()
        return first, len(wires)

    def _wake_waiters(self) -> None:
        waiters, self._waiters = self._waiters, []
        for loop, future in waiters:
            loop.call_soon_threadsafe(_wake, future)

    def at(self, offset: int) -> bytes:
        """The retained record at ``offset``."""
        return self.records[offset - self.base]

    def truncate(self, keep: int) -> None:
        """Drop all but the newest ``keep`` records."""
        drop = max(0, len(self.records) - keep)
        self.base += drop
        del self.records[:drop]

    async def wait_past(self, offset: int, timeout: float | None) -> None:
        """Wait until a record exists at ``offset``, or ``timeout`` passes."""
        if self.head > offset:
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._waiters.append((loop, future))
        try:
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            # Dropped on every exit, cancellation included, so a reader that
            # stops while parked leaves nothing behind on the topic.
            self._waiters = [w for w in self._waiters if w[1] is not future]


class MemoryProducer(Generic[T]):
    """A producer on one topic of the memory provider."""

    def __init__(
        self,
        store: _Topic,
        converter: temporalio.converter.DataConverter,
        topic: str,
        stream: str,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> None:
        """Bind this producer to ``topic``'s ``store``."""
        self._store = store
        self._converter = converter
        self._topic = topic
        self._stream = stream
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = next_sequence
        self._last = BEGINNING

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The attempt this producer writes."""
        return self._attempt

    async def append(self, *values: T) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamProducer.append`."""
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

    async def finish(self) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamProducer.finish`."""
        return await self._write(
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

    async def _write(self, wires: list[WireRecord]) -> Cursor:
        content = content_fingerprint(wires)
        for wire in wires:
            await encode_body(self._converter, wire)
        first, count = self._store.append(
            wires,
            session=(self._producer_id, self._attempt),
            sequence=self._sequence,
            content=content,
        )
        self._sequence += len(wires)
        self._last = mint_cursor(_PROVIDER, self._stream, str(first + count - 1))
        return self._last


class MemoryStreamHandle:
    """A Workflow's stream on the memory provider."""

    def __init__(
        self, streams: MemoryStreams, client: Client | None, ref: StreamRef
    ) -> None:
        """Address the stream ``ref`` names in ``streams``."""
        self._streams = streams
        self._client = client
        self._ref = ref
        self._namespace = client.namespace if client is not None else _DEFAULT_NAMESPACE
        self._converter = (
            client.data_converter
            if client is not None
            else temporalio.converter.DataConverter.default
        )

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on."""
        return self._ref

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.read`."""
        name, result_type = self._resolve(topic, result_type)
        store = self._streams._topic(self._namespace, self._ref.workflow_id, name)
        stream = self._stream(name)
        # Resolved here so a bad cursor fails this call, not the first
        # iteration of the generator.
        start = self._start(store, stream, after)
        previous = (
            mint_cursor(_PROVIDER, stream, str(start - 1)) if start else BEGINNING
        )
        return self._read(store, stream, start, previous, result_type)

    async def _read(
        self,
        store: _Topic,
        stream: str,
        offset: int,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._converter.payload_converter,
            result_type,
            after=after,
            warn=logger.warning,
        )
        closed = False
        while True:
            while offset < store.head:
                if offset < store.base:
                    raise StreamExpiredError(
                        f"the records from offset {offset} were dropped while this "
                        f"read was behind; the topic now starts at {store.base}"
                    )
                cursor = mint_cursor(_PROVIDER, stream, str(offset))
                raw = store.at(offset)
                offset += 1
                try:
                    wire = WireRecord.FromString(raw)
                except DecodeError as error:
                    logger.warning("skipping stream record at %s: %s", cursor, error)
                    continue
                await decode_body(self._converter, wire)
                for record in decoder.decode(cursor, wire):
                    yield record
            if closed:
                return
            # One more pass after learning the owner closed, so a record that
            # landed between the scan and the describe is still delivered.
            closed = await self._closed()
            if not closed:
                await store.wait_past(
                    offset,
                    None
                    if self._client is None
                    else self._streams._poll.total_seconds(),
                )

    async def _closed(self) -> bool:
        if self._client is None:
            return False
        try:
            description = await self._client.get_workflow_handle(
                self._ref.workflow_id, run_id=self._ref.run_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                # A producer may write before the owner exists, so there is
                # nothing to follow yet; keep waiting.
                return False
            raise
        status = description.status
        if status is None or status == WorkflowExecutionStatus.RUNNING:
            return False
        # Following the chain, a run that continued as new is not the end:
        # the next describe without a run id finds its successor.
        return not (
            self._ref.run_id is None
            and status == WorkflowExecutionStatus.CONTINUED_AS_NEW
        )

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamHandle.latest`."""
        name, _ = self._resolve(topic, None)
        head = self._streams._topic(self._namespace, self._ref.workflow_id, name).head
        return (
            mint_cursor(_PROVIDER, self._stream(name), str(head - 1))
            if head
            else BEGINNING
        )

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> MemoryProducer[Any]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.producer`."""
        if not producer_id:
            raise ValueError("producer_id must not be empty")
        for label, number in (("attempt", attempt), ("next_sequence", next_sequence)):
            if isinstance(number, bool) or not isinstance(number, int) or number < 1:
                raise ValueError(
                    f"{label} must be an int of at least 1, got {number!r}"
                )
        name, _ = self._resolve(topic, None)
        store = self._streams._topic(self._namespace, self._ref.workflow_id, name)
        return MemoryProducer(
            store,
            self._converter,
            name,
            self._stream(name),
            producer_id,
            attempt,
            next_sequence,
        )

    def _resolve(
        self, topic: str | StreamTopic[Any] | None, result_type: type | None
    ) -> tuple[str, type | None]:
        return resolve_topic(self._ref.topic if topic is None else topic, result_type)

    def _stream(self, topic: str) -> str:
        return stream_hash(
            self._namespace, self._ref.kind, self._ref.workflow_id, topic
        )

    def _start(self, store: _Topic, stream: str, after: Cursor) -> int:
        """The offset a read starts at, resolved against what ``store`` holds now."""
        if after == END:
            return store.head
        position = cursor_position(after, provider=_PROVIDER, stream=stream)
        if position is None:
            return store.base
        try:
            start = int(position) + 1
        except ValueError:
            raise StreamCursorError(
                f"cursor {after.token!r} does not name a position on the memory provider"
            ) from None
        if start < 0 or start > store.head:
            raise StreamCursorError(
                f"cursor {after.token!r} names a position this stream never held"
            )
        if start < store.base:
            raise StreamExpiredError(
                f"cursor {after.token!r} names a record no longer retained; the "
                f"topic starts at offset {store.base}"
            )
        return start


class MemoryStreams(StreamProviderPlugin):
    """The in-memory provider, one list per topic.

    Pass the same instance to the client, or to the Worker, and to any code
    that opens handles from it directly; two instances share nothing.
    """

    def __init__(
        self, *, poll_interval: timedelta = timedelta(milliseconds=100)
    ) -> None:
        """Create an empty provider.

        Args:
            poll_interval: How often an outside reader with nothing to read
                asks whether the owner closed.
        """
        super().__init__("temporalio.contrib.streams.MemoryStreams")
        self._poll = poll_interval
        self._topics: dict[tuple[str, str, str], _Topic] = {}

    def get_stream_handle(
        self, client: Client | None, ref: StreamRef
    ) -> MemoryStreamHandle:
        """A handle on the stream ``ref`` names.

        ``client`` may be ``None`` here, unlike on a storage provider. Then
        the handle uses the default data converter and the ``default``
        namespace, and a read waits until the caller stops it.
        """
        return MemoryStreamHandle(self, client, ref)

    async def close(self) -> None:
        """Nothing to release: the provider holds no connection."""

    def truncate(
        self, workflow_id: str, topic: str, *, keep: int, namespace: str = "default"
    ) -> None:
        """Drop all but the newest ``keep`` records of a topic. For tests.

        Stands in for a store's retention. Offsets are kept, so a cursor from
        before still names its record, and a read from ``BEGINNING`` starts
        at the oldest one left.
        """
        self._topic(namespace, workflow_id, topic).truncate(keep)

    def _topic(self, namespace: str, workflow_id: str, topic: str) -> _Topic:
        key = (namespace, workflow_id, topic)
        found = self._topics.get(key)
        if found is None:
            found = self._topics[key] = _Topic()
        return found
