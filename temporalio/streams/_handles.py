"""The handles workflow code holds.

Everything provider-specific sits behind the two protocols at the top. A
handle converts values, frames records, filters topics and synthesizes
supersession; a binding only moves bytes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Generic, Protocol, TypeVar

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.streams import _frame
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import Cursor, RecordKind, StreamRecord

__all__ = ["ReadSource", "StreamReader", "StreamWriter", "WriteSink"]

T = TypeVar("T")


class ReadSource(Protocol):
    """One subscription, as a provider supplies it."""

    async def next_batch(self) -> list[tuple[Cursor, bytes]]:
        """The next records, waiting until there is at least one.

        A batch rather than a record because delivery boundaries are what a
        provider actually records, and flattening them here keeps that out of
        the contract.

        Raises:
            StopAsyncIteration: This subscription has ended.
        """
        ...

    def close(self) -> None:
        """End the subscription. Idempotent."""
        ...


class WriteSink(Protocol):
    """One topic on the stream the running workflow owns."""

    async def publish(self, frame: bytes) -> None:
        """Accept one framed record into this workflow task's output.

        Awaitable because a provider may hold the publisher back when its
        batch is full. It does not mean the record is visible: it becomes
        visible when the workflow task is accepted.
        """
        ...


def _encode_value(value: Any) -> bytes:
    converter = workflow.payload_converter()
    payload = value if isinstance(value, Payload) else converter.to_payloads([value])[0]
    return payload.SerializeToString()


def _decode_value(body: bytes, as_type: type | None) -> Any:
    converter = workflow.payload_converter()
    payload = Payload()
    payload.ParseFromString(body)
    if as_type is None:
        return converter.from_payloads([payload])[0]
    return converter.from_payloads([payload], [as_type])[0]


class StreamWriter(Generic[T]):
    """Publishes to a topic on the stream this workflow owns.

    A workflow can only publish transactionally to a stream it owns, on both
    providers. Writing to somebody else's stream is an activity's job, and it
    gets the weaker guarantee that goes with doing I/O.
    """

    def __init__(self, sink: WriteSink, topic: str) -> None:
        """Prefer :func:`temporalio.streams.writer`."""
        self._sink = sink
        self._topic = topic
        self._finished = False

    @property
    def topic(self) -> str:
        """The topic this writer is bound to."""
        return self._topic

    async def publish(self, value: T) -> None:
        """Append ``value`` to this topic.

        Returning does not mean the record is visible. It becomes visible when
        this workflow task is accepted, and never at all if the task fails, so
        a reader cannot see a decision the workflow did not commit.
        """
        if self._finished:
            raise RuntimeError(f"topic {self._topic!r} was already finished")
        await self._sink.publish(
            _frame.encode(
                topic=self._topic,
                kind=RecordKind.DATA,
                producer="",
                attempt=0,
                sequence=-1,
                body=_encode_value(value),
            )
        )

    async def finish(self) -> None:
        """Declare this topic complete.

        Says the writer has nothing more to send. It does not say the workflow
        or the activity behind it succeeded, and a reader that treats it that
        way will accept output from an attempt that later timed out.
        """
        if self._finished:
            return
        self._finished = True
        await self._sink.publish(
            _frame.encode(
                topic=self._topic,
                kind=RecordKind.FINISH,
                producer="",
                attempt=0,
                sequence=-1,
                body=b"",
            )
        )


class StreamReader(Generic[T]):
    """Reads a stream from inside workflow code.

    Iterating yields every kind of record, including the supersession the
    reader synthesizes when a producer's newer attempt appears. Check
    ``record.kind``, or use :meth:`values` when the workflow genuinely only
    wants data.
    """

    def __init__(
        self,
        source: ReadSource,
        *,
        topic: str | None = None,
        type: type | None = None,
    ) -> None:
        """Prefer :func:`temporalio.streams.reader`."""
        self._source = source
        self._topic = topic
        self._type = type
        self._pending: list[StreamRecord[Any]] = []
        self._attempts = AttemptTracker()
        self._closed = False

    def __aiter__(self) -> AsyncIterator[StreamRecord[T]]:
        """Iterate records until the reader is closed."""
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[StreamRecord[T]]:
        while not self._closed:
            try:
                record = await self.next()
            except StopAsyncIteration:
                # The provider ended the subscription. Iteration stops rather
                # than raising, so a workflow that reads to the end of a
                # finished stream leaves the loop instead of failing its task.
                return
            yield record

    async def values(self) -> AsyncIterator[T]:
        """Iterate the data values, dropping control records."""
        async for record in self:
            if record.kind is RecordKind.DATA:
                yield record.value

    async def next(self) -> StreamRecord[T]:
        """The next record, waiting for one if there is none buffered.

        Raises:
            StopAsyncIteration: The provider ended the subscription.
        """
        while not self._pending:
            await self._fill()
        return self._pending.pop(0)

    async def _fill(self) -> None:
        for cursor, frame in await self._source.next_batch():
            kind, topic, producer, attempt, sequence, body = _frame.decode(frame)
            if self._topic is not None and topic != self._topic:
                continue
            superseded = self._attempts.note(producer, attempt, cursor)
            if superseded is not None:
                self._pending.append(superseded)
            self._pending.append(
                StreamRecord(
                    value=(
                        _decode_value(body, self._type)
                        if kind is RecordKind.DATA
                        else None
                    ),
                    cursor=cursor,
                    kind=kind,
                    topic=topic,
                    producer=producer,
                    attempt=attempt,
                    sequence=sequence,
                )
            )

    def close(self) -> None:
        """End the subscription. Idempotent."""
        self._closed = True
        self._source.close()
