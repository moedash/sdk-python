"""The in-process reference provider, for tests.

.. warning::
    This module is experimental and may change in future versions.

It exists so the conformance suite can run the whole outside surface without
a store, and to show in one file what a provider owes. Its limits:

- It is not replay-safe and not durable. Everything lives in process memory.
  Do not use it to show recovery, and do not use it in production.
- Streams are keyed by namespace, Workflow id, run chain and topic, as a
  storage provider keys them, so a reader that follows the chain sees every
  run's records, and a reused Workflow id starts a new stream. A ``run_id``
  on a ref picks the chain that run belongs to, and decides which run's
  close ends a read. A handle finds its chain by describing the Workflow;
  without a client, or before the owner exists, it uses the chain this
  provider saw last for the Workflow id. Records written before any chain
  was known go to the first chain that shows up.
- A read learns that the owner closed by describing the Workflow through the
  handle's client. A handle opened with no client reads until the caller
  stops, or until the topic is closed.
- It keeps every record until :meth:`MemoryStreams.truncate` drops the oldest
  ones, which stands in for a store's retention in tests.
- A Workflow's own publish is staged in process memory and promoted when
  History shows its commit, as on any provider. A stage is lost with the
  process, so output a Workflow Task committed just before a crash is never
  promoted.

The outside surface (producer identity, retry deduplication, positions,
``SUPERSEDED``, stream-bound cursors, expired cursors) is faithful, which is
what the conformance suite checks.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
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
    StreamClosedError,
    StreamCursorError,
    StreamExpiredError,
    StreamProducerError,
)
from temporalio.contrib.streams._notify import chain_first_run_id
from temporalio.contrib.streams._output import StagedBatch, StageRef
from temporalio.contrib.streams._plugin import StreamProviderPlugin
from temporalio.contrib.streams._provider import StreamProducer
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
        self.closed = False
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
        refuse_closed: bool = False,
    ) -> tuple[int, int]:
        """Store ``wires`` and return where they landed as ``(first offset, count)``.

        With a ``session``, a repeat of that session's newest batch carrying
        the same ``content`` stores nothing and returns where the original
        landed, even once the topic is closed. A Workflow's promoted output
        lands on a closed topic too; a producer's batch, ``refuse_closed``,
        does not.

        Raises:
            StreamProducerError: ``sequence`` is the newest batch's with
                different content, or below it.
            StreamClosedError: The topic is closed and ``refuse_closed``.
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
        if refuse_closed and self.closed:
            raise StreamClosedError("the stream has closed")
        first = self.head
        self.records.extend(
            wire.SerializeToString(deterministic=True) for wire in wires
        )
        if session is not None:
            self.sessions[session] = _Batch(sequence, first, len(wires), content)
        self._wake_waiters()
        return first, len(wires)

    def close(self) -> None:
        """Close the topic and wake its readers."""
        self.closed = True
        self._wake_waiters()

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
        store: Callable[[], Awaitable[tuple[_Topic, str]]],
        converter: temporalio.converter.DataConverter,
        topic: str,
        stream: str,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> None:
        """Bind this producer to ``topic``; ``store`` finds its records.

        The store is found once, at the first write: a producer writes to one
        run chain.
        """
        self._find_store = store
        self._store: _Topic | None = None
        self._written_chain: str | None = None
        """The run chain this producer's writes go to, once it has written."""
        self._converter = converter
        self._topic = topic
        self._stream = stream
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = next_sequence
        self._last = BEGINNING
        # A batch reads the sequence, then awaits the codec, then writes;
        # calls one at a time keep each batch's sequences its own.
        self._lock = asyncio.Lock()

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
        async with self._lock:
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
        async with self._lock:
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
        if self._store is None:
            self._store, self._written_chain = await self._find_store()
        first, count = self._store.append(
            wires,
            session=(self._producer_id, self._attempt),
            sequence=self._sequence,
            content=content,
            refuse_closed=True,
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
        stream = self._stream(name)
        # Parsed here so a cursor from another stream or provider fails this
        # call. Where it falls in the store is known once the chain is.
        position = self._position(stream, after)
        return self._read(name, stream, after, position, result_type)

    async def _read(
        self,
        name: str,
        stream: str,
        after: Cursor,
        position: int | None,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        store = await self._store(name)
        offset = self._start(store, after, position)
        after = mint_cursor(_PROVIDER, stream, str(offset - 1)) if offset else BEGINNING
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
            closed = await self._closed(store)
            if not closed:
                await store.wait_past(
                    offset,
                    None
                    if self._client is None
                    else self._streams._poll.total_seconds(),
                )

    async def _closed(self, store: _Topic) -> bool:
        if store.closed or self._client is None:
            return store.closed
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
        head = (await self._store(name)).head
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
    ) -> StreamProducer[Any]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.producer`."""
        if not producer_id:
            raise ValueError("producer_id must not be empty")
        for label, number in (("attempt", attempt), ("next_sequence", next_sequence)):
            if isinstance(number, bool) or not isinstance(number, int) or number < 1:
                raise ValueError(
                    f"{label} must be an int of at least 1, got {number!r}"
                )
        name, _ = self._resolve(topic, None)

        async def store() -> tuple[_Topic, str]:
            return await self._store_and_chain(name)

        return self._streams._notified_producer(
            self._client,
            self._ref,
            name,
            MemoryProducer(
                store,
                self._converter,
                name,
                self._stream(name),
                producer_id,
                attempt,
                next_sequence,
            ),
        )

    async def _store(self, topic: str) -> _Topic:
        """The records of ``topic`` on the chain this handle's ref names."""
        return (await self._store_and_chain(topic))[0]

    async def _store_and_chain(self, topic: str) -> tuple[_Topic, str]:
        """The records of ``topic`` and the chain they belong to."""
        chain = None
        if self._client is not None:
            try:
                chain = await chain_first_run_id(self._client, self._ref)
            except RPCError as error:
                # A producer may write before the owner exists, or name a run
                # the server does not know.
                if error.status not in (
                    RPCStatusCode.NOT_FOUND,
                    RPCStatusCode.INVALID_ARGUMENT,
                ):
                    raise
        store = self._streams._topic(
            self._namespace, self._ref.workflow_id, topic, chain=chain
        )
        if chain is None:
            chain = self._streams._chains.get(
                (self._namespace, self._ref.workflow_id), ""
            )
        return store, chain

    def _resolve(
        self, topic: str | StreamTopic[Any] | None, result_type: type | None
    ) -> tuple[str, type | None]:
        return resolve_topic(self._ref.topic if topic is None else topic, result_type)

    def _stream(self, topic: str) -> str:
        return stream_hash(
            self._namespace, self._ref.kind, self._ref.workflow_id, topic
        )

    def _position(self, stream: str, after: Cursor) -> int | None:
        """The offset ``after`` names, or ``None`` for ``BEGINNING`` and ``END``."""
        if after == END:
            return None
        position = cursor_position(after, provider=_PROVIDER, stream=stream)
        if position is None:
            return None
        try:
            return int(position)
        except ValueError:
            raise StreamCursorError(
                f"cursor {after.token!r} does not name a position on the memory provider"
            ) from None

    def _start(self, store: _Topic, after: Cursor, position: int | None) -> int:
        """The offset a read starts at, resolved against what ``store`` holds now."""
        if after == END:
            return store.head
        if position is None:
            return store.base
        start = position + 1
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
    """The in-memory provider, one list per topic. For tests only.

    It keeps everything in process memory: nothing survives the process,
    and it is not meant for production.

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
        self._topics: dict[tuple[str, str, str, str], _Topic] = {}
        # The newest run chain seen for each namespace and Workflow id.
        self._chains: dict[tuple[str, str], str] = {}
        self._stages: dict[str, StagedBatch] = {}

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

    async def _stage(self, batch: StagedBatch) -> str:
        token = uuid.uuid4().hex
        self._stages[token] = batch
        return token

    async def _promote(self, stage: StageRef) -> None:
        batch = self._stages.pop(stage.token, None)
        if batch is None:
            return
        warned: set[str] = set()
        for record in batch.records:
            store = self._topic(
                stage.namespace,
                stage.workflow_id,
                record.topic,
                chain=stage.first_run_id or None,
            )
            if store.closed and record.topic not in warned:
                warned.add(record.topic)
                self._warn_closed_topic(stage.workflow_id, record.topic)
            store.append([record])

    async def _abort(self, stage: StageRef) -> None:
        self._stages.pop(stage.token, None)

    async def _close_topic(
        self, client: Client, ref: StreamRef, topic: str, first_run_id: str
    ) -> None:
        self._topic(
            client.namespace, ref.workflow_id, topic, chain=first_run_id
        ).close()

    def truncate(
        self, workflow_id: str, topic: str, *, keep: int, namespace: str = "default"
    ) -> None:
        """Drop all but the newest ``keep`` records of a topic. For tests.

        Stands in for a store's retention, on every run chain of the Workflow
        id. Offsets are kept, so a cursor from before still names its record,
        and a read from ``BEGINNING`` starts at the oldest one left.
        """
        for (held_namespace, held_id, _, held_topic), store in self._topics.items():
            if (held_namespace, held_id, held_topic) == (namespace, workflow_id, topic):
                store.truncate(keep)

    def _topic(
        self, namespace: str, workflow_id: str, topic: str, *, chain: str | None = None
    ) -> _Topic:
        """One topic's store on run chain ``chain``.

        Without a chain, the one seen last for the Workflow id. The first
        chain seen for a Workflow id takes over what producers wrote before
        any chain was known, so a producer may write before its owner starts.
        """
        if chain is None:
            chain = self._chains.get((namespace, workflow_id), "")
        elif chain:
            if (namespace, workflow_id) not in self._chains:
                for held in [
                    key
                    for key in self._topics
                    if key[:3] == (namespace, workflow_id, "")
                ]:
                    self._topics[(namespace, workflow_id, chain, held[3])] = (
                        self._topics.pop(held)
                    )
            self._chains[(namespace, workflow_id)] = chain
        key = (namespace, workflow_id, chain, topic)
        found = self._topics.get(key)
        if found is None:
            found = self._topics[key] = _Topic()
        return found
