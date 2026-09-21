"""Streams from inside workflow code.

.. warning::
    This module is experimental and may change in future versions.

The reader and writer here are the same on every provider. They convert
values, synthesize supersession and buffer nothing the provider did not hand
them; everything provider-specific sits behind the ``ReadSource`` and
``WriteSink`` that the provider's workflow half opens. The provider itself
comes from the worker, the way the payload converter does.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any, Generic, TypeVar, cast, overload

from temporalio.streams._provider import ReadSource, WorkflowStreamProvider, WriteSink
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord
from temporalio.streams._wire import RecordDecoder, to_wire
from temporalio.workflow._context import _Runtime, payload_converter
from temporalio.workflow._sandbox import logger

__all__ = ["StreamReader", "StreamWriter", "stream_reader", "stream_writer"]

T = TypeVar("T")


class _WorkflowStreams:
    """The stream state a workflow instance carries: its provider half and open readers."""

    def __init__(self, provider: WorkflowStreamProvider) -> None:
        self.provider = provider
        self.readers: dict[str, StreamReader[Any]] = {}


class StreamReader(Generic[T]):
    """Reads one topic of this workflow's stream from inside workflow code.

    The reader is the async iterator: ``async for record in reader`` yields
    every kind of record, including the supersession the reader synthesizes
    when a producer's newer attempt appears. Check ``record.kind``, or
    iterate :meth:`values` when the workflow only wants data. Iteration ends
    when the reader is closed or the provider ends the subscription. One loop
    per reader: two loops on one reader share its buffer and interleave.
    """

    def __init__(
        self,
        source: ReadSource,
        *,
        topic: str,
        result_type: type | None,
        after: Cursor,
        on_close: Callable[[], None],
    ) -> None:
        """Prefer :func:`temporalio.workflow.stream_reader`."""
        self._source = source
        self._topic = topic
        self._result_type = result_type
        self._decoder = RecordDecoder(
            payload_converter(), result_type, after=after, warn=logger.warning
        )
        self._pending: deque[StreamRecord[T]] = deque()
        self._lock = asyncio.Lock()
        self._closed = False
        self._ended = False
        self._on_close = on_close

    @property
    def topic(self) -> str:
        """The topic this reader is subscribed to."""
        return self._topic

    def __aiter__(self) -> StreamReader[T]:
        """The reader is its own iterator."""
        return self

    async def __anext__(self) -> StreamRecord[T]:
        """The next record, waiting for one to arrive."""
        while True:
            if self._pending:
                return self._pending.popleft()
            if self._closed or self._ended:
                raise StopAsyncIteration
            await self._fill()

    async def _fill(self) -> None:
        # Two loops on one reader must not race the source, so one batch
        # fetch is in flight at a time and the second loop takes what the
        # first one buffered.
        async with self._lock:
            if self._pending or self._closed or self._ended:
                return
            try:
                batch = await self._source.next_batch()
            except StopAsyncIteration:
                # The provider ended the subscription. Iteration stops rather
                # than raising, so a workflow that reads to the end of a
                # finished stream leaves the loop instead of failing its task.
                self._ended = True
                return
            for cursor, wire in batch:
                self._pending.extend(self._decoder.decode(cursor, wire))

    async def values(self) -> AsyncIterator[T]:
        """Iterate the data values, dropping control records."""
        async for record in self:
            if record.kind is RecordKind.DATA:
                yield cast("T", record.value)

    def close(self) -> None:
        """End the subscription. Idempotent.

        A later :func:`temporalio.workflow.stream_reader` on the same topic
        opens a new subscription, which is a new command.
        """
        if self._closed:
            return
        self._closed = True
        self._source.close()
        self._on_close()


class StreamWriter:
    """Publishes to one topic of this workflow's stream.

    A workflow can only publish transactionally to its own stream, on every
    provider. Writing to somebody else's stream is an activity's job, and it
    gets the weaker guarantee that goes with doing I/O.
    """

    def __init__(self, sink: WriteSink, topic: str) -> None:
        """Prefer :func:`temporalio.workflow.stream_writer`."""
        self._sink = sink
        self._topic = topic
        self._finished = False

    @property
    def topic(self) -> str:
        """The topic this writer is bound to."""
        return self._topic

    def publish(self, value: Any) -> None:
        """Append ``value`` to this topic.

        Synchronous, because there is nothing to wait for inside a task: the
        record becomes visible when this Workflow Task is accepted, and never
        at all if the task fails, so a reader cannot see a decision the
        workflow did not commit. A :class:`temporalio.common.RawValue` passes
        through pre-encoded.

        Raises:
            ValueError: :meth:`finish` was already called on this writer.
        """
        if self._finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        self._sink.publish(
            to_wire(
                payload_converter(),
                topic=self._topic,
                kind=RecordKind.DATA,
                value=value,
            )
        )

    def finish(self) -> None:
        """Write ``FINISH`` for this workflow on this topic. Idempotent.

        Says this workflow has nothing more to send on the topic. It does not
        say the workflow succeeded, and it does not end anyone's read.
        """
        if self._finished:
            return
        self._finished = True
        self._sink.publish(
            to_wire(payload_converter(), topic=self._topic, kind=RecordKind.FINISH)
        )


@overload
def stream_reader(
    topic: str, *, result_type: type[T], after: Cursor = ...
) -> StreamReader[T]: ...


@overload
def stream_reader(
    topic: str, *, result_type: None = None, after: Cursor = ...
) -> StreamReader[Any]: ...


def stream_reader(
    topic: str, *, result_type: type | None = None, after: Cursor = BEGINNING
) -> StreamReader[Any]:
    """Subscribe this workflow to ``topic`` of its own stream.

    One subscription per topic per run. A second call for the same topic
    returns the reader already open on it, so records go to whichever loop
    pulls first; such a call may pass neither ``after`` nor a different
    ``result_type``. Adding a reader on a new topic is a new command, so gate
    it with :func:`temporalio.workflow.patched` as you would a timer. A
    reader in a successor run starts a new subscription: nothing crosses
    continue-as-new implicitly.

    Args:
        topic: The topic, relative to this workflow's stream.
        result_type: The value type, used as the decode hint.
            :class:`temporalio.common.RawValue` returns the payload untouched.
        after: Resume strictly after this record. Honoured on the first
            subscription of a run, because after that the recorded
            observations decide.

    Raises:
        ValueError: ``topic`` is empty, or a reader on it is already open and
            this call asked for a different position or type.
        temporalio.streams.StreamCursorError: ``after`` was minted by another
            provider.
    """
    if not topic:
        raise ValueError("topic must not be empty")
    state: _WorkflowStreams = _Runtime.current().workflow_streams()
    existing = state.readers.get(topic)
    if existing is not None:
        if after != BEGINNING or result_type is not existing._result_type:
            raise ValueError(
                f"topic {topic!r} already has a reader in this run; a second "
                "stream_reader shares it and takes no after= or other result_type="
            )
        return existing
    source = state.provider.open_reader(topic, after=after)

    def forget() -> None:
        state.readers.pop(topic, None)

    reader: StreamReader[Any] = StreamReader(
        source, topic=topic, result_type=result_type, after=after, on_close=forget
    )
    state.readers[topic] = reader
    return reader


def stream_writer(topic: str) -> StreamWriter:
    """Publish to ``topic`` of this workflow's stream.

    Args:
        topic: The topic name. Encoding follows each published value.

    Raises:
        ValueError: ``topic`` is empty.
    """
    if not topic:
        raise ValueError("topic must not be empty")
    provider = _Runtime.current().workflow_streams().provider
    return StreamWriter(provider.open_writer(topic), topic)
