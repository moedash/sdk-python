"""The in-process reference provider.

Exists so the conformance suite can exercise the whole surface without a
store, and to document in one file what a provider owes. Two honest limits,
both stated so nobody mistakes this for evidence:

- It is not replay-safe. Workflow-side state lives in plain process memory,
  so run it with a warm workflow cache and do not use it to demonstrate
  recovery.
- A workflow's publish becomes visible at ``publish`` time rather than at
  task acceptance, so it only approximates rule 1 of the contract.

The outside surface (producer, consumer, dedupe, supersession, cursors) is
faithful, which is what the conformance tests lean on.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import temporalio.converter
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.streams import _frame, _provider
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

_DEFAULT_POLL = timedelta(milliseconds=100)

logger = logging.getLogger(__name__)


def _wake(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


class _MemoryStream:
    def __init__(self) -> None:
        self.frames: list[bytes] = []
        # Dedupe identity is (producer#attempt, first sequence of the append),
        # the same pair the storage providers use.
        self.seen: dict[tuple[str, int], int] = {}
        # Each waiter is parked with the loop it belongs to. A workflow's
        # publish runs on the workflow thread, and waking a foreign loop's
        # future from there needs call_soon_threadsafe or the loop stays
        # blocked in select until unrelated I/O happens to wake it.
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []

    def append(
        self, frames: list[bytes], *, producer_id: str, sequence: int
    ) -> int | None:
        """Store ``frames`` and return the first one's offset, or ``None`` for a repeat."""
        key = (producer_id, sequence)
        if key in self.seen:
            return None
        offset = len(self.frames)
        self.frames.extend(frames)
        self.seen[key] = offset
        waiters, self._waiters = self._waiters, []
        for loop, future in waiters:
            loop.call_soon_threadsafe(_wake, future)
        return offset

    async def wait_past(self, offset: int) -> None:
        while len(self.frames) <= offset:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[None] = loop.create_future()
            self._waiters.append((loop, future))
            await future


_streams: dict[str, _MemoryStream] = {}


def _stream(stream_id: str) -> _MemoryStream:
    found = _streams.get(stream_id)
    if found is None:
        found = _streams[stream_id] = _MemoryStream()
    return found


def reset() -> None:
    """Drop every stream. For tests."""
    _streams.clear()


def _inbound_id(workflow_id: str, stream: str) -> str:
    return f"{workflow_id}:{stream}" if stream else workflow_id


def _converter(client: Any) -> Any:
    if client is None:
        return temporalio.converter.DataConverter.default.payload_converter
    return client.data_converter.payload_converter


class _MemReadSource:
    """Workflow-side read that wakes by polling a timer.

    A real provider wakes the workflow by delivering; polling is the price of
    having no delivery path, and it is why this provider is for tests.
    """

    def __init__(self, store: _MemoryStream, start: int, poll: timedelta) -> None:
        self._store = store
        self._cursor = start
        self._poll = poll
        self._closed = False

    async def next_batch(self) -> list[tuple[Cursor, bytes]]:
        while not self._closed:
            frames = self._store.frames
            if len(frames) > self._cursor:
                batch = [
                    (Cursor(str(offset)), frames[offset])
                    for offset in range(self._cursor, len(frames))
                ]
                self._cursor = len(frames)
                return batch
            await workflow.sleep(self._poll)
        raise StopAsyncIteration

    def close(self) -> None:
        self._closed = True


class _MemWriteSink:
    def __init__(self, store: _MemoryStream, topic: str) -> None:
        self._store = store
        self._topic = topic
        self._sequence = 0

    async def publish(self, frame: bytes) -> None:
        self._store.append(
            [frame],
            producer_id=f"__workflow__:{self._topic}",
            sequence=self._sequence,
        )
        self._sequence += 1


class MemoryProducer:
    """The outside producer, faithful to the contract."""

    def __init__(
        self,
        store: _MemoryStream,
        converter: Any,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic`` on ``store``."""
        self._store = store
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
        offset = self._store.append(
            frames,
            producer_id=self._provider_id,
            sequence=self._sequence - len(frames),
        )
        if offset is None:
            return None
        return Cursor(str(offset + len(frames) - 1))

    async def finish(self) -> None:
        """Mark this producer done, so a reader stops waiting on it."""
        frame = _frame.encode(
            topic=self._topic,
            kind=RecordKind.FINISH,
            producer=self._producer_id,
            attempt=self._attempt,
            sequence=self._sequence,
            body=b"",
        )
        self._sequence += 1
        self._store.append(
            [frame], producer_id=self._provider_id, sequence=self._sequence - 1
        )

    def _encode(self, value: Any) -> bytes:
        payload = (
            value
            if isinstance(value, Payload)
            else self._converter.to_payloads([value])[0]
        )
        return payload.SerializeToString()


class MemoryConsumer:
    """The outside reader, with the shared supersession rule."""

    def __init__(self, store: _MemoryStream, converter: Any, stream: str) -> None:
        """Read whatever ``store`` holds, now and as it grows."""
        self._store = store
        self._converter = converter
        self._stream = stream

    async def read(
        self,
        *,
        after: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records after ``after``, waiting for ones not written yet."""
        _provider.check_topic(self._stream, topic)
        attempts = AttemptTracker()
        offset = int(after.token) + 1 if after.token else 0
        while True:
            await self._store.wait_past(offset)
            frames = self._store.frames
            while offset < len(frames):
                cursor = Cursor(str(offset))
                frame = frames[offset]
                offset += 1
                try:
                    kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                        frame
                    )
                except ValueError as error:
                    # Same answer as the workflow-side reader: skip and say so.
                    logger.warning("skipping stream record at %s: %s", cursor, error)
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
        del topic  # one store per stream, so the position is topic-independent
        count = len(self._store.frames)
        return Cursor(str(count - 1)) if count else BEGINNING

    def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        if as_type is None:
            return self._converter.from_payloads([payload])[0]
        return self._converter.from_payloads([payload], [as_type])[0]


class _MemoryProvider:
    name = "memory"

    def __init__(self) -> None:
        self._poll = _DEFAULT_POLL

    def configure(self, **options: Any) -> None:
        poll = options.pop("poll_interval", None)
        if poll is not None:
            self._poll = poll
        if options:
            raise TypeError(
                f"the memory provider takes only poll_interval, got {sorted(options)}"
            )

    def worker_options(self) -> dict[str, Any]:
        return {}

    def open_read(
        self,
        stream: str,
        *,
        after: Cursor = BEGINNING,
        idle_timeout: timedelta | None = None,
    ) -> ReadSource:
        # Ignored: this provider never releases the worker, it polls. Reading
        # idle_timeout as the poll period would give the parameter a second
        # meaning that a port copying the reference would copy too.
        del idle_timeout
        store = _stream(_inbound_id(workflow.info().workflow_id, stream))
        return _MemReadSource(
            store, int(after.token) + 1 if after.token else 0, self._poll
        )

    def open_write(self, topic: str) -> WriteSink:
        store = _stream(_inbound_id(workflow.info().workflow_id, ""))
        return _MemWriteSink(store, topic)

    async def producer(
        self,
        client: Any,
        *,
        workflow_id: str,
        stream: str = "",
        topic: str = "",
        producer_id: str = "",
        attempt: int = 0,
    ) -> MemoryProducer:
        # With no inbound stream named, the target is the store the workflow's
        # own writer appends to, and the frame carries the topic. An inbound
        # record carries none: the stream's name is its whole address.
        return MemoryProducer(
            _stream(_inbound_id(workflow_id, stream)),
            _converter(client),
            topic,
            producer_id,
            attempt,
        )

    async def consumer(
        self, client: Any, *, workflow_id: str, stream: str = ""
    ) -> MemoryConsumer:
        return MemoryConsumer(
            _stream(_inbound_id(workflow_id, stream)), _converter(client), stream
        )


_provider.register("memory", _MemoryProvider)
