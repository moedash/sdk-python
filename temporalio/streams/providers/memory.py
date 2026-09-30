"""The in-process reference provider.

Exists so the conformance suite can exercise the whole surface without a
store, and to document in one file what a provider owes. Its limits, stated
so nobody mistakes it for evidence:

- It is not replay-safe. Workflow-side state lives in plain process memory,
  so run it with a warm workflow cache and do not use it to demonstrate
  recovery.
- A workflow's publish becomes visible at ``publish`` time rather than at
  task acceptance, and a failed task's records stay, so it only approximates
  rule 1 of the contract.
- Topics are keyed by workflow id rather than by run, so a successor run's
  reader from ``BEGINNING`` sees the chain's records. A ``run_id`` on a
  handle only decides which run's close ends a read.
- It learns that a workflow closed by describing it, so a handle opened
  without a client reads until the caller closes it.
- An activity's own streams are keyed by the activity, not by its run. A
  standalone activity's read ends when describing it shows a terminal
  status. A workflow's activity is described through its workflow: the read
  ends once the activity has been seen pending and is no longer, or the
  workflow closed, so a read opened after the activity already finished
  waits for the workflow.
- It keeps every record until :meth:`MemoryStreams.truncate` drops the
  oldest ones, which stands in for a store's retention in tests.
- A standalone stream lives here with its policy and a sealed flag.
  ``retention``, ``max_records`` and ``max_bytes`` are applied when a record
  is appended, so a stream nobody writes to keeps records past their
  retention. A read on it ends when it is sealed and the tail delivered, and
  a read, ``latest`` or ``producer`` on a stream id that does not exist
  raises :class:`temporalio.streams.StreamNotFoundError` at the call rather
  than waiting for the stream to be created.
- The outside path encodes and decodes bodies through the client's data
  converter, codec and external storage included, and fingerprints a retry
  over the converted bytes first. The workflow half has no client, so a
  workflow's own publish is stored as the payload converter produced it and
  a workflow-side read hands records over as stored.

The outside surface (producer identity, retry deduplication, positions,
supersession, cursors) is faithful, which is what the conformance tests lean
on. One list per topic; a topic is written by the workflow and by outside
producers alike and read from either side.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

from google.protobuf.message import DecodeError

import temporalio.converter
from temporalio import workflow
from temporalio.client import ActivityExecutionStatus, Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._body import content_fingerprint, decode_body, encode_body
from temporalio.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamNotFoundError,
    StreamProducerError,
)
from temporalio.streams._ids import topic_key
from temporalio.streams._provider import ReadSource, WriteSink
from temporalio.streams._record import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamRecord,
    check_read_start,
)
from temporalio.streams._ref import StreamRef
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

__all__ = ["MemoryProducer", "MemoryStreamHandle", "MemoryStreams"]

_PROVIDER = "memory"

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _wake(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


@dataclass(frozen=True)
class _Policy:
    """What a standalone stream retains, applied as records are appended."""

    retention: timedelta | None = None
    max_records: int | None = None
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.retention is not None and self.retention <= timedelta(0):
            raise ValueError("retention must be positive")
        if self.max_records is not None and self.max_records <= 0:
            raise ValueError("max_records must be positive")
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise ValueError("max_bytes must be positive")


class _Standalone:
    """One standalone stream: its policy, its seal and its topics."""

    def __init__(self, policy: _Policy) -> None:
        self.policy = policy
        self.sealed = False
        self.topics: dict[str, _Topic] = {}


class _Topic:
    """One topic's records, and the waiters parked on its tail."""

    def __init__(self, policy: _Policy | None = None, *, sealed: bool = False) -> None:
        self.policy = policy
        self.sealed = sealed
        # The retained records, the first of which sits at offset ``base``.
        # Offsets are never reused, so a cursor keeps naming the same record
        # after truncation drops the ones before it.
        self.base = 0
        self.records: list[bytes] = []
        # When each retained record landed, for a retention policy.
        self.stamps: list[float] = []
        # Dedupe identity is (producer#attempt, first sequence of the append),
        # the same pair the storage providers use, mapped to where the batch
        # landed and a digest of what it held, so a repeat answers with the
        # original position and a divergent one is told apart from it.
        self.seen: dict[tuple[str, int], tuple[int, int, bytes]] = {}
        # Each waiter is parked with the loop it belongs to. A workflow's
        # publish runs on the workflow thread, and waking a foreign loop's
        # future from there needs call_soon_threadsafe or the loop stays
        # blocked in select until unrelated I/O happens to wake it.
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []

    def append(
        self,
        wires: list[WireRecord],
        *,
        writer: str | None = None,
        sequence: int = 0,
        content: bytes | None = None,
    ) -> tuple[int, int]:
        """Store ``wires`` and return where they landed as ``(first offset, count)``.

        With a ``writer``, a repeat of ``(writer, sequence)`` carrying the same
        content stores nothing and returns where the original landed.
        ``content`` is the fingerprint the repeat is matched by; a producer
        takes it over the records before their bodies are encoded, and
        without one it is taken over ``wires`` as they are.

        Raises:
            StreamProducerError: ``(writer, sequence)`` is held with different
                content.
            StreamClosedError: The stream was sealed.
        """
        if self.sealed:
            raise StreamClosedError("the stream is closed and takes no more records")
        key = (writer or "", sequence)
        bodies = [wire.SerializeToString(deterministic=True) for wire in wires]
        if content is None:
            content = content_fingerprint(wires)
        if writer is not None:
            held = self.seen.get(key)
            if held is not None:
                first, count, seen_content = held
                if seen_content != content:
                    raise StreamProducerError(
                        f"producer sequence {sequence} already used with different "
                        f"content by {writer!r}"
                    )
                return first, count
        first = self.head
        now = time.time()
        self.records.extend(bodies)
        self.stamps.extend([now] * len(bodies))
        if writer is not None:
            self.seen[key] = (first, len(wires), content)
        self._apply_policy(now)
        self._wake_waiters()
        return first, len(wires)

    def _wake_waiters(self) -> None:
        waiters, self._waiters = self._waiters, []
        for loop, future in waiters:
            loop.call_soon_threadsafe(_wake, future)

    def _apply_policy(self, now: float) -> None:
        policy = self.policy
        if policy is None:
            return
        drop = 0
        if policy.max_records is not None:
            drop = max(drop, len(self.records) - policy.max_records)
        if policy.max_bytes is not None:
            held = sum(len(record) for record in self.records)
            while drop < len(self.records) and held > policy.max_bytes:
                held -= len(self.records[drop])
                drop += 1
        if policy.retention is not None:
            floor = now - policy.retention.total_seconds()
            while drop < len(self.records) and self.stamps[drop] < floor:
                drop += 1
        if drop:
            self._drop(drop)

    def seal(self) -> None:
        """Take no more records, and let parked readers see the end."""
        self.sealed = True
        self._wake_waiters()

    @property
    def head(self) -> int:
        """The offset the next record lands at."""
        return self.base + len(self.records)

    def at(self, offset: int) -> bytes:
        """The retained record at ``offset``."""
        return self.records[offset - self.base]

    def truncate(self, keep: int) -> None:
        """Drop all but the newest ``keep`` records."""
        self._drop(max(0, len(self.records) - keep))

    def _drop(self, count: int) -> None:
        self.base += count
        del self.records[:count]
        del self.stamps[:count]

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
            # aclose()s while parked here leaves nothing behind on the topic.
            self._waiters = [w for w in self._waiters if w[1] is not future]


def _parse(cursor: Cursor, raw: bytes, warn: Any) -> WireRecord | None:
    try:
        return WireRecord.FromString(raw)
    except DecodeError as error:
        # Same answer as an undecodable body: skip and say so, so one bad
        # record cannot pin a reader.
        warn("skipping stream record at %s: %s", cursor, error)
        return None


class _MemReadSource:
    """Workflow-side read that wakes by polling a timer.

    A real provider wakes the workflow by delivering; polling is the price of
    having no delivery path, and it is why this provider is for tests.
    """

    def __init__(self, store: _Topic, start: int, poll: timedelta) -> None:
        self._store = store
        self._offset = start
        self._poll = poll
        self._closed = False

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        while not self._closed:
            head = self._store.head
            if head > self._offset:
                batch: list[tuple[Cursor, WireRecord]] = []
                for offset in range(max(self._offset, self._store.base), head):
                    cursor = mint_cursor(_PROVIDER, str(offset))
                    wire = _parse(
                        cursor, self._store.at(offset), workflow.logger.warning
                    )
                    if wire is not None:
                        batch.append((cursor, wire))
                self._offset = head
                if batch:
                    return batch
                continue
            await workflow.sleep(self._poll)
        raise StopAsyncIteration

    def close(self) -> None:
        self._closed = True


class _MemWriteSink:
    def __init__(self, store: _Topic) -> None:
        self._store = store

    def publish(self, record: WireRecord) -> None:
        # Visible at once rather than at task acceptance: the documented gap
        # between this provider and rule 1.
        self._store.append([record])


class _MemoryWorkflowProvider:
    """The workflow half. Nothing to install and nothing to release."""

    def __init__(self, streams: MemoryStreams) -> None:
        self._streams = streams

    def open_reader(
        self, topic: str, *, after: Cursor, last: int | None = None
    ) -> ReadSource:
        check_read_start(after, last)
        store = self._streams._topic(workflow.info().workflow_id, topic)
        start = self._streams._start(store, after, last)
        return _MemReadSource(store, start, self._streams._poll)

    def open_writer(self, topic: str) -> WriteSink:
        return _MemWriteSink(self._streams._topic(workflow.info().workflow_id, topic))

    def on_workflow_start(self) -> None:
        pass

    async def on_workflow_finish(self) -> None:
        pass


class MemoryProducer(Generic[T]):
    """The outside producer, faithful to the contract."""

    def __init__(
        self,
        store: _Topic,
        converter: temporalio.converter.DataConverter,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic``'s ``store``."""
        self._store = store
        self._converter = converter
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = 0
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
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: T) -> Cursor:
        """Append ``values`` and return the cursor of the last record as stored.

        A repeat returns where the original landed; an empty call returns
        the position of this producer's last record.
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

    async def _write(self, wires: list[WireRecord]) -> Cursor:
        # The fingerprint comes first, over the converted records, so a codec
        # that encrypts with a fresh nonce cannot make a retry look divergent.
        content = content_fingerprint(wires)
        for wire in wires:
            await encode_body(self._converter, wire)
        first, count = self._store.append(
            wires, writer=self._writer, sequence=self._sequence, content=content
        )
        self._sequence += len(wires)
        self._last = mint_cursor(_PROVIDER, str(first + count - 1))
        return self._last


class MemoryStreamHandle:
    """One owner's stream from outside, with the shared reader rules.

    The owner is ``workflow_id``'s workflow, or with ``activity_id`` an
    activity: a standalone one without ``workflow_id``, or one that workflow
    scheduled. With ``stream_id`` the handle is on a standalone stream, which
    has no owner.
    """

    def __init__(
        self,
        streams: MemoryStreams,
        client: Client | None,
        workflow_id: str | None,
        run_id: str | None,
        activity_id: str | None = None,
        stream_id: str | None = None,
    ) -> None:
        """Address the owner's topics in ``streams``."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._activity_id = activity_id
        self._stream_id = stream_id
        self._seen_pending = False
        self._converter = (
            client.data_converter
            if client is not None
            else temporalio.converter.DataConverter.default
        )

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` from where the read starts until the owner closes.

        ``END`` and ``last=`` are resolved by this call, against what the
        topic holds when it is made. On a standalone stream the read ends
        once the stream is sealed and the tail delivered.
        """
        check_read_start(after, last)
        name, result_type = resolve_topic(topic, result_type)
        store = self._store(name)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        start = self._streams._start(store, after, last)
        # The decoder positions a synthesized record at the one before it, so
        # it is told the position before the first record this read yields.
        previous = mint_cursor(_PROVIDER, str(start - 1)) if start else BEGINNING
        return self._read(store, start, previous, result_type)

    async def _read(
        self,
        store: _Topic,
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
                    raise StreamCursorError(
                        f"offset {offset} was truncated while this read was behind; "
                        f"the topic now starts at {store.base}"
                    )
                cursor = mint_cursor(_PROVIDER, str(offset))
                wire = _parse(cursor, store.at(offset), logger.warning)
                offset += 1
                if wire is None:
                    continue
                await decode_body(self._converter, wire)
                for record in decoder.decode(cursor, wire):
                    yield record
            if closed:
                return
            # One more pass after learning the workflow closed, so a record
            # that landed between the scan and the describe is not lost.
            closed = await self._closed()
            if not closed:
                await store.wait_past(
                    offset,
                    None
                    if self._client is None
                    else self._streams._poll.total_seconds(),
                )

    def _store(self, topic: str) -> _Topic:
        if self._stream_id is not None:
            return self._streams._standalone_topic(self._stream_id, topic)
        if self._activity_id is not None:
            return self._streams._activity_topic(
                self._workflow_id, self._activity_id, topic
            )
        assert self._workflow_id is not None
        return self._streams._topic(self._workflow_id, topic)

    async def _closed(self) -> bool:
        if self._stream_id is not None:
            return self._streams._standalone_stream(self._stream_id).sealed
        if self._client is None:
            return False
        try:
            if self._activity_id is not None and self._workflow_id is None:
                activity = await self._client.get_activity_handle(
                    self._activity_id, run_id=self._run_id
                ).describe()
                return activity.status != ActivityExecutionStatus.RUNNING
            assert self._workflow_id is not None
            description = await self._client.get_workflow_handle(
                self._workflow_id, run_id=self._run_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                # A producer may write before the owner exists; there is
                # nothing to follow yet, so keep waiting.
                return False
            raise
        status = description.status
        if self._activity_id is not None:
            pending = any(
                info.activity_id == self._activity_id
                for info in description.raw_description.pending_activities
            )
            if pending:
                self._seen_pending = True
            elif self._seen_pending:
                return True
            # The activity's streams are not the workflow's chain: a run that
            # continued as new took its activities with it.
            return status is not None and status != WorkflowExecutionStatus.RUNNING
        if status is None or status == WorkflowExecutionStatus.RUNNING:
            return False
        # Following the chain, a run that continued as new is not the end:
        # the next describe without a run id finds its successor.
        return not (
            self._run_id is None and status == WorkflowExecutionStatus.CONTINUED_AS_NEW
        )

    async def latest(self, *, topic: str | StreamTopic[Any]) -> Cursor:
        """The cursor of the newest record on ``topic``, for following from now."""
        name, _ = resolve_topic(topic)
        head = self._store(name).head
        return mint_cursor(_PROVIDER, str(head - 1)) if head else BEGINNING

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any],
        producer_id: str = "",
        attempt: int = 0,
    ) -> MemoryProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        name, _ = resolve_topic(topic)
        store = self._store(name)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return MemoryProducer(store, self._converter, name, producer_id, attempt)

    def ref(self, *, topic: str | StreamTopic[Any] | None = None) -> StreamRef:
        """A ref to ``topic`` of this owner's stream, pinned as this handle is."""
        if self._stream_id is not None:
            return StreamRef.for_standalone(self._stream_id, topic=topic)
        if self._activity_id is not None:
            return StreamRef.for_activity(
                self._activity_id,
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                topic=topic,
            )
        assert self._workflow_id is not None
        return StreamRef.for_workflow(
            self._workflow_id, run_id=self._run_id, topic=topic
        )

    async def close(self) -> None:
        """Seal a standalone stream. An owned stream ends with its owner, not by a caller."""
        if self._stream_id is None:
            raise ValueError(
                "only a standalone stream can be closed; this handle is on an owned "
                "stream, which ends when its workflow or activity does"
            )
        self._streams._seal(self._stream_id)


class MemoryStreams(ProviderPlugin):
    """The in-memory provider, one list per topic.

    Construct one and pass the same instance to the worker and to the code
    that opens handles; two instances share nothing.
    """

    def __init__(
        self, *, poll_interval: timedelta = timedelta(milliseconds=100)
    ) -> None:
        """Create an empty provider.

        Args:
            poll_interval: How often a workflow-side reader with nothing to
                read checks again, and how often an outside reader asks
                whether the workflow closed.
        """
        self._poll = poll_interval
        self._topics: dict[str, _Topic] = {}
        self._activity_topics: dict[tuple[str | None, str, str], _Topic] = {}
        self._standalone: dict[str, _Standalone] = {}

    def reset(self) -> None:
        """Drop every topic and every standalone stream. For tests."""
        self._topics.clear()
        self._activity_topics.clear()
        self._standalone.clear()

    def truncate(self, workflow_id: str, topic: str, *, keep: int) -> None:
        """Drop all but the newest ``keep`` records of a topic. For tests.

        Stands in for a store's retention: offsets are kept, so a cursor from
        before still names its record, and a read from ``BEGINNING`` starts
        at the oldest one left.
        """
        self._topic(workflow_id, topic).truncate(keep)

    def workflow_provider(self) -> _MemoryWorkflowProvider:
        """The workflow half, over this provider's topics."""
        return _MemoryWorkflowProvider(self)

    def get_stream_handle(
        self, client: Client | None, workflow_id: str, *, run_id: str | None = None
    ) -> MemoryStreamHandle:
        """A handle on ``workflow_id``'s topics.

        ``client`` may be ``None`` here, unlike on a storage provider; then
        the handle cannot see the workflow close and a read waits until the
        caller closes it.
        """
        return MemoryStreamHandle(self, client, workflow_id, run_id)

    def get_activity_stream_handle(
        self,
        client: Client | None,
        activity_id: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
    ) -> MemoryStreamHandle:
        """A handle on the topics ``activity_id`` owns, apart from any workflow's.

        As on :meth:`get_stream_handle`, ``client`` may be ``None``, and then
        a read waits until the caller closes it.
        """
        return MemoryStreamHandle(self, client, workflow_id, run_id, activity_id)

    async def create_standalone_stream(
        self,
        client: Client | None,
        stream_id: str,
        *,
        retention: timedelta | None = None,
        max_records: int | None = None,
        max_bytes: int | None = None,
    ) -> MemoryStreamHandle:
        """Create the standalone stream ``stream_id``, or find it with the same policy.

        The policy is applied on every append to any of the stream's topics.
        ``client`` may be ``None``, as on the other handles.

        Raises:
            ValueError: ``stream_id`` is empty, a bound is not positive, or
                the stream exists with a different policy.
        """
        if not stream_id:
            raise ValueError("stream_id must not be empty")
        policy = _Policy(retention, max_records, max_bytes)
        existing = self._standalone.get(stream_id)
        if existing is None:
            self._standalone[stream_id] = _Standalone(policy)
        elif existing.policy != policy:
            raise ValueError(
                f"standalone stream {stream_id!r} exists with policy "
                f"{existing.policy}, not {policy}"
            )
        return MemoryStreamHandle(self, client, None, None, stream_id=stream_id)

    def get_standalone_stream_handle(
        self, client: Client | None, stream_id: str
    ) -> MemoryStreamHandle:
        """A handle on the standalone stream ``stream_id``.

        Nothing is checked here: a ``read``, ``latest`` or ``producer`` on a
        stream that was never created raises
        :class:`temporalio.streams.StreamNotFoundError` at the call.
        """
        return MemoryStreamHandle(self, client, None, None, stream_id=stream_id)

    async def close(self) -> None:
        """Nothing to release: the provider holds no connection."""

    def _topic(self, workflow_id: str, topic: str) -> _Topic:
        if not topic:
            raise ValueError("topic must not be empty")
        key = topic_key(workflow_id, topic)
        found = self._topics.get(key)
        if found is None:
            found = self._topics[key] = _Topic()
        return found

    def _activity_topic(
        self, workflow_id: str | None, activity_id: str, topic: str
    ) -> _Topic:
        # Kept apart from the workflow topics, so no workflow id can name an
        # activity's stream.
        if not topic:
            raise ValueError("topic must not be empty")
        key = (workflow_id, activity_id, topic)
        found = self._activity_topics.get(key)
        if found is None:
            found = self._activity_topics[key] = _Topic()
        return found

    def _standalone_stream(self, stream_id: str) -> _Standalone:
        stream = self._standalone.get(stream_id)
        if stream is None:
            raise StreamNotFoundError(
                f"standalone stream {stream_id!r} does not exist; create it with "
                "client.create_stream"
            )
        return stream

    def _standalone_topic(self, stream_id: str, topic: str) -> _Topic:
        stream = self._standalone_stream(stream_id)
        if not topic:
            raise ValueError("topic must not be empty")
        found = stream.topics.get(topic)
        if found is None:
            found = stream.topics[topic] = _Topic(stream.policy, sealed=stream.sealed)
        return found

    def _seal(self, stream_id: str) -> None:
        stream = self._standalone_stream(stream_id)
        stream.sealed = True
        for store in stream.topics.values():
            store.seal()

    def _start(self, store: _Topic, after: Cursor, last: int | None) -> int:
        """The offset a read starts at, resolved against what ``store`` holds now."""
        if last is not None:
            return max(store.base, store.head - last)
        if after == END:
            return store.head
        position = cursor_position(after, provider=_PROVIDER)
        if position is None:
            return store.base
        try:
            start = int(position) + 1
        except ValueError:
            raise StreamCursorError(
                f"cursor {after.token!r} does not name a position on the memory provider"
            ) from None
        if start < store.base:
            raise StreamCursorError(
                f"cursor {after.token!r} names a record no longer retained; the "
                f"topic starts at offset {store.base}"
            )
        return start
