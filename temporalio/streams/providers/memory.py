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

The outside surface (producer identity, retry deduplication, positions,
supersession, cursors) is faithful, which is what the conformance tests lean
on. One list per topic; a topic is written by the workflow and by outside
producers alike and read from either side.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar

from google.protobuf.message import DecodeError

import temporalio.converter
from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._errors import StreamCursorError, StreamProducerError
from temporalio.streams._ids import topic_key
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

__all__ = ["MemoryProducer", "MemoryStreamHandle", "MemoryStreams"]

_PROVIDER = "memory"

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _wake(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


def _fingerprint(bodies: list[bytes]) -> bytes:
    """A digest of one append's content, length-delimited so a split cannot collide."""
    digest = hashlib.sha256()
    for body in bodies:
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    return digest.digest()


class _Topic:
    """One topic's records, and the waiters parked on its tail."""

    def __init__(self) -> None:
        self.records: list[bytes] = []
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
    ) -> tuple[int, int]:
        """Store ``wires`` and return where they landed as ``(first offset, count)``.

        With a ``writer``, a repeat of ``(writer, sequence)`` carrying the same
        content stores nothing and returns where the original landed.

        Raises:
            StreamProducerError: ``(writer, sequence)`` is held with different
                content.
        """
        key = (writer or "", sequence)
        # Deterministic so the digest of one append does not depend on how
        # protobuf happened to order a payload's metadata map.
        bodies = [wire.SerializeToString(deterministic=True) for wire in wires]
        content = _fingerprint(bodies)
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
        first = len(self.records)
        self.records.extend(bodies)
        if writer is not None:
            self.seen[key] = (first, len(wires), content)
        waiters, self._waiters = self._waiters, []
        for loop, future in waiters:
            loop.call_soon_threadsafe(_wake, future)
        return first, len(wires)

    async def wait_past(self, offset: int, timeout: float | None) -> None:
        """Wait until a record exists at ``offset``, or ``timeout`` passes."""
        if len(self.records) > offset:
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
            records = self._store.records
            if len(records) > self._offset:
                batch: list[tuple[Cursor, WireRecord]] = []
                for offset in range(self._offset, len(records)):
                    cursor = mint_cursor(_PROVIDER, str(offset))
                    wire = _parse(cursor, records[offset], workflow.logger.warning)
                    if wire is not None:
                        batch.append((cursor, wire))
                self._offset = len(records)
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

    def open_reader(self, topic: str, *, after: Cursor) -> ReadSource:
        start = self._streams._offset_after(after)
        store = self._streams._topic(workflow.info().workflow_id, topic)
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
        converter: temporalio.converter.PayloadConverter,
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
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: T) -> Cursor:
        """Append ``values`` and return the cursor of the last record as stored.

        A repeat of the same content returns where the original landed; an
        empty call returns the position of this producer's last record.

        Raises:
            StreamProducerError: This sequence is held with different content.
        """
        if not values:
            return self._last
        return self._write(
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
        self._write(
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

    def _write(self, wires: list[WireRecord]) -> Cursor:
        first, count = self._store.append(
            wires, writer=self._writer, sequence=self._sequence
        )
        self._sequence += len(wires)
        self._last = mint_cursor(_PROVIDER, str(first + count - 1))
        return self._last


class MemoryStreamHandle:
    """One workflow's stream from outside, with the shared reader rules."""

    def __init__(
        self,
        streams: MemoryStreams,
        client: Client | None,
        workflow_id: str,
        run_id: str | None,
    ) -> None:
        """Address ``workflow_id``'s topics in ``streams``."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._converter = (
            client.data_converter.payload_converter
            if client is not None
            else temporalio.converter.DataConverter.default.payload_converter
        )

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` after ``after`` until the workflow closes."""
        name, result_type = resolve_topic(topic, result_type)
        store = self._streams._topic(self._workflow_id, name)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        start = self._streams._offset_after(after)
        return self._read(store, start, after, result_type)

    async def _read(
        self,
        store: _Topic,
        offset: int,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        closed = False
        while True:
            records = store.records
            while offset < len(records):
                cursor = mint_cursor(_PROVIDER, str(offset))
                wire = _parse(cursor, records[offset], logger.warning)
                offset += 1
                if wire is None:
                    continue
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

    async def _closed(self) -> bool:
        if self._client is None:
            return False
        handle = self._client.get_workflow_handle(
            self._workflow_id, run_id=self._run_id
        )
        try:
            description = await handle.describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                # A producer may write before the workflow exists; there is
                # nothing to follow yet, so keep waiting.
                return False
            raise
        status = description.status
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
        count = len(self._streams._topic(self._workflow_id, name).records)
        return mint_cursor(_PROVIDER, str(count - 1)) if count else BEGINNING

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any],
        producer_id: str = "",
        attempt: int = 0,
    ) -> MemoryProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        name, _ = resolve_topic(topic)
        store = self._streams._topic(self._workflow_id, name)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return MemoryProducer(store, self._converter, name, producer_id, attempt)


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

    def reset(self) -> None:
        """Drop every topic. For tests."""
        self._topics.clear()

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

    def _offset_after(self, after: Cursor) -> int:
        position = cursor_position(after, provider=_PROVIDER)
        if position is None:
            return 0
        try:
            return int(position) + 1
        except ValueError:
            raise StreamCursorError(
                f"cursor {after.token!r} does not name a position on the memory provider"
            ) from None
