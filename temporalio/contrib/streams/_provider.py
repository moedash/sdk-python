"""What a provider implements.

A provider is the store behind a stream. It hands out a
:class:`StreamHandle` per stream, and a handle reads, positions and makes
producers. A provider moves ``temporal.sdk.streams.v1.StreamRecord`` protos;
the shared helpers in this package convert values, synthesize
``SUPERSEDED``, mint stream-bound cursors and run bodies through the data
converter, so every provider behaves the same where the contract says so.

What a provider owes a record's body: the handle converts a value into the
body with the payload converter, then takes the retry fingerprint
(:func:`temporalio.contrib.streams._body.content_fingerprint`) over the converted
records, then runs each body through
:func:`temporalio.contrib.streams._body.encode_body`, which stamps the plaintext
hash and applies the codec and external storage. On a read it runs
:func:`temporalio.contrib.streams._body.decode_body` before the record reaches the
reader.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload

from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._record import Cursor, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic

if TYPE_CHECKING:
    from temporalio.client import Client

__all__ = ["StreamHandle", "StreamProducer", "StreamProvider"]

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)


class StreamProducer(Protocol[T_contra]):
    """Appends to one topic as one producer attempt.

    Every append is visible as soon as the store accepts it. Each record
    carries the producer id, the attempt and a sequence that starts at one
    for each attempt, which lets the store deduplicate a retry and lets a
    reader tell a retry from a new attempt.
    """

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        ...

    @property
    def attempt(self) -> int:
        """The attempt this producer writes."""
        ...

    async def append(self, *values: T_contra) -> Cursor:
        """Append ``values`` as one batch and return the cursor of its last record.

        The batch lands whole and in order. The producer moves its sequence
        past the batch only when the append succeeds, so a call that raised
        :class:`temporalio.contrib.streams.StreamOutcomeUnknownError` can be
        repeated with the same values: the store answers a repeat of the
        batch it holds last for this producer attempt with the original
        position and writes nothing.

        An empty call writes nothing and returns the cursor of this
        producer's last record, or ``BEGINNING`` when it has written none.

        Calls on one producer run one at a time, in the order they were
        made, so concurrent calls take consecutive sequences. A call that was
        cancelled may or may not have written its batch, and the producer
        cannot tell: start a new attempt (a new producer with a higher
        ``attempt``) rather than continuing this one.

        Raises:
            StreamProducerError: The sequence was already used with different
                content, or it is below the newest one the store holds for
                this producer attempt.
            StreamOutcomeUnknownError: The store may or may not have written
                the batch. Retry the same values on this producer.
            StreamRefusedError: The store refused the batch, for example
                because it is out of memory. Nothing was written.
            StreamClosedError: The stream refuses appends.
        """
        ...

    async def finish(self) -> Cursor:
        """Write ``FINISH`` for this producer on this topic and return its cursor.

        Says this producer has nothing more to send. It does not say the
        work behind it succeeded, and it does not end anyone's read.
        """
        ...


class StreamHandle(Protocol):
    """One stream, addressed by topic, from outside Workflow code.

    A handle on a Workflow's stream follows the run chain unless its
    :attr:`ref` is pinned to a run. A topic is a
    :class:`temporalio.contrib.streams.StreamTopic`, which carries the record
    type, or a plain string with ``result_type=`` for a name decided at
    runtime. A call that names no topic addresses the topic of :attr:`ref`.
    """

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on, as data another process can open."""
        ...

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records on ``topic`` after ``after`` as they arrive.

        ``BEGINNING`` starts at the oldest record the topic retains, and
        ``END`` at whatever is appended after the read starts. Any other
        cursor came from a record of this stream, and the read resumes just
        past it, so a reader that keeps the last cursor it handled and hands
        it back sees every stored record once. A resumed read knows the
        attempt of the record at its cursor, so a new attempt of that
        producer still arrives as ``SUPERSEDED``. Earlier attempts of other
        producers are not known to it. The read ends when the owner's run
        chain is closed and every retained record has been delivered. The
        result is a generator, so a caller that stops early calls
        ``aclose()`` on it.

        Raises:
            ValueError: ``result_type`` was passed with a topic definition,
                or the topic is empty.
            StreamCursorError: ``after`` came from another provider or
                another stream. Raised by this call, not by the first
                iteration.
            StreamExpiredError: ``after`` names a record the store no longer
                retains, or the read fell behind retention while it ran.
        """
        ...

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The cursor of the newest record on ``topic``, or ``BEGINNING`` when empty.

        ``read(after=latest())`` yields only what is appended after this call
        returned, which is how a client positions itself before it sends
        something the Workflow answers on the stream.
        """
        ...

    @overload
    def producer(
        self, *, topic: StreamTopic[T], producer_id: str, attempt: int
    ) -> StreamProducer[T]: ...

    @overload
    def producer(
        self, *, topic: str | None = None, producer_id: str, attempt: int
    ) -> StreamProducer[Any]: ...

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str,
        attempt: int,
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as ``producer_id`` in ``attempt``.

        A process that restarts its work raises ``attempt``, which starts a
        new sequence and tells readers that what the earlier attempt wrote is
        superseded. Keep one producer object per ``(topic, producer_id,
        attempt)``. Each object numbers its own records from one, so a second
        object for the same session is refused as stale.

        Raises:
            ValueError: ``producer_id`` is empty or ``attempt`` is below one.
        """
        ...


class StreamProvider(Protocol):
    """A store that holds streams.

    In this release only a Workflow owns a stream. The provider keeps the
    stream for the owner's run chain, so a handle that is not pinned to a run
    reads across Continue-as-New.
    """

    def get_stream_handle(self, client: Client, ref: StreamRef) -> StreamHandle:
        """A handle on the stream ``ref`` names.

        ``client`` is how the handle learns that the owner closed and which
        data converter to encode bodies with.
        """
        ...

    async def close(self) -> None:
        """Release what this provider holds for the process.

        The application calls this, not the Worker and not the client: one
        provider serves every Worker built from a client and every handle
        opened outside them, so none of those owns its lifetime.
        """
        ...
