"""What a provider implements, in two halves.

:class:`WorkflowStreamProvider` runs on the workflow thread and must keep the
contract's first two rules: publishes commit with the Workflow Task, and reads
are recorded observations. Nothing it needs may do I/O. :class:`StreamProvider`
is the half a process holds: it makes the workflow half for a worker and hands
out :class:`StreamHandle` objects to code outside a workflow. A Python provider
usually implements both on one class; the split is what lets a language whose
workflow code is bundled separately name the two halves in two packages.

A provider only moves ``temporal.api.stream.v1.StreamRecord`` protos. The
handles around it convert values, synthesize supersession and mint cursors,
and turn a :class:`temporalio.streams.StreamTopic` into the plain name the
provider sees, through :func:`temporalio.streams.resolve_topic`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload

from temporalio.api.stream.v1 import StreamRecord as WireRecord
from temporalio.streams._record import BEGINNING, Cursor, StreamRecord
from temporalio.streams._topic import StreamTopic

if TYPE_CHECKING:
    from temporalio.client import Client

__all__ = [
    "ReadSource",
    "StreamHandle",
    "StreamProducer",
    "StreamProvider",
    "WorkflowStreamProvider",
    "WriteSink",
]

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)


class StreamProducer(Protocol[T_contra]):
    """Appends to one topic from outside workflow code.

    Every append is visible as soon as the store accepts it, and carries the
    producer id, attempt and sequence that let a reader tell a retried append
    from a new generation. The type parameter is the topic definition's
    value type; a producer on a string-named topic takes any value.
    """

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        ...

    @property
    def attempt(self) -> int:
        """The generation this producer is writing, or 0 when undeclared."""
        ...

    async def append(self, *values: T_contra) -> Cursor | None:
        """Append ``values`` and return the cursor of the last record as the store holds it.

        A repeat of an earlier append (same producer, attempt and sequence)
        carrying the same content is written once and returns the position the
        original landed at. A repeat carrying different content is a conflict,
        not a retry: it raises :class:`StreamProducerError` and writes nothing,
        because the store cannot tell which of the two the reader was meant to
        see. A provider that cannot compare content says so in its own
        documentation rather than picking one silently.

        An empty call writes nothing and returns the same value a repeat would:
        the position of this producer's last record, or ``BEGINNING`` when it
        has written none. ``None`` means one thing only: this provider learns
        positions at read time, and a caller that needs one positions itself
        with :meth:`StreamHandle.latest`.

        Raises:
            StreamProducerError: The attempt or sequence conflicts with what
                the store holds.
        """
        ...

    async def finish(self) -> None:
        """Write ``FINISH`` for this producer on this topic.

        Says this producer has nothing more to send. It does not say the
        activity behind it succeeded, and it does not end anyone's read.
        """
        ...


class StreamHandle(Protocol):
    """One workflow's stream, addressed by topic, from outside workflow code.

    A handle follows the workflow's execution chain unless it was opened with
    a ``run_id``, in which case it is pinned to that run. A topic is a
    :class:`temporalio.streams.StreamTopic` definition, which carries the
    record type, or a plain string with ``result_type=`` for a name decided
    at runtime. A transport failure surfaces as
    :class:`temporalio.service.RPCError`, never as the transport's own
    exception type.
    """

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self, *, topic: str, after: Cursor = ..., result_type: type[T]
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self, *, topic: str, after: Cursor = ..., result_type: None = None
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records on ``topic`` after ``after`` as they arrive.

        ``BEGINNING`` yields everything the topic retains. Any other cursor
        came from a record a reader saw, and reading resumes just past it, so
        a reader that stores the last cursor it handled and hands it back
        sees every record exactly once. The read ends when the owning
        execution, or its chain, is closed and every retained record after
        ``after`` has been delivered; until then it waits. The result is a
        generator, so a caller that stops early can ``aclose()`` it and
        release whatever the provider parked against the store.

        Raises:
            ValueError: ``result_type`` was passed with a topic definition,
                or the topic is empty.
            StreamCursorError: ``after`` came from another provider or names
                a record no longer retained. Raised by this call, not by the
                first iteration.
            StreamNotFoundError: The workflow or topic does not exist or is
                past retention.
        """
        ...

    async def latest(self, *, topic: str | StreamTopic[Any]) -> Cursor:
        """The cursor of the newest record on ``topic``, or ``BEGINNING`` when empty.

        For a reader that wants to follow from now: ``read(after=latest())``
        yields only what is published after this call returned, which is how
        a client that is about to send a message positions itself before
        sending, without the workflow having to report a position.
        """
        ...

    @overload
    def producer(
        self, *, topic: StreamTopic[T], producer_id: str = ..., attempt: int = ...
    ) -> StreamProducer[T]: ...

    @overload
    def producer(
        self, *, topic: str, producer_id: str = ..., attempt: int = ...
    ) -> StreamProducer[Any]: ...

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any],
        producer_id: str = "",
        attempt: int = 0,
    ) -> StreamProducer[Any]:
        """A producer on ``topic``.

        Inside an activity, leave ``producer_id`` and ``attempt`` unset: the
        activity's own id and attempt are the right answer, and they are what
        let a reader tell a retry from a new generation. Outside one,
        ``producer_id`` is required and an empty one raises ``ValueError``.
        """
        ...


class ReadSource(Protocol):
    """One subscription, as a provider supplies it to the workflow thread."""

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        """The next records with their positions, waiting until there is at least one.

        A batch rather than a record because delivery boundaries are what a
        provider actually records, and flattening them here keeps that out of
        the contract. A record that cannot be parsed into a ``StreamRecord``
        proto is the provider's to skip.

        Raises:
            StopAsyncIteration: This subscription has ended.
        """
        ...

    def close(self) -> None:
        """End the subscription. Idempotent."""
        ...


class WriteSink(Protocol):
    """One topic of the running workflow's stream, as a provider binds it."""

    def publish(self, record: WireRecord) -> None:
        """Take one record into this Workflow Task's output.

        Synchronous: there is nothing to wait for inside a task, because the
        task is the visibility boundary. The provider commits what it buffered
        when the task completes and drops it when the task fails. A record
        the provider cannot stage raises :class:`temporalio.streams.StreamError`
        and fails the task, loudly.
        """
        ...


class WorkflowStreamProvider(Protocol):
    """The half of a provider that runs on the workflow thread.

    Imports nothing that does I/O. The worker creates one per workflow
    instance through :meth:`StreamProvider.workflow_provider`, so state kept
    here dies with the instance the way handlers do. It sees topics by name;
    the definitions are resolved before it is called.
    """

    def open_reader(self, topic: str, *, after: Cursor) -> ReadSource:
        """Subscribe the running workflow to ``topic`` of its own stream.

        Raises:
            StreamCursorError: ``after`` was minted by another provider.
        """
        ...

    def open_writer(self, topic: str) -> WriteSink:
        """Bind ``topic`` of the running workflow's stream for publishing."""
        ...

    def on_workflow_start(self) -> None:
        """Called before the workflow function runs.

        A provider that serves outside readers through handlers on the
        workflow registers them here, before the first task completes.
        """
        ...

    async def on_workflow_finish(self) -> None:
        """Called after the workflow function returns, raises or continues as new.

        A provider that parked an outside reader against the run lets go
        here, so the workflow can close.
        """
        ...


class StreamProvider(Protocol):
    """What a store ships. Also a :class:`temporalio.worker.Plugin` when it serves workers.

    Construct one, pass it to ``Client.connect(plugins=[provider])`` so the
    client and the workers built from it carry it, or to
    ``Worker(plugins=[provider])`` and ``Replayer(plugins=[provider])`` for a
    worker alone, and open handles from it anywhere else. Nothing is global:
    two workers in one process may hold two providers.
    """

    def workflow_provider(self) -> WorkflowStreamProvider:
        """The half that serves one workflow instance on its thread."""
        ...

    def get_stream_handle(
        self, client: Client, workflow_id: str, *, run_id: str | None = None
    ) -> StreamHandle:
        """A handle on ``workflow_id``'s stream.

        Without ``run_id`` it follows the execution chain, so a consumer keeps
        reading across continue-as-new; with one it is pinned to that run.
        """
        ...

    async def close(self) -> None:
        """Release what this provider holds for the process.

        A provider that keeps a connection pool or an HTTP session open needs
        a moment where the process says it is done; this is it. A provider
        that holds nothing returns at once.
        """
        ...
