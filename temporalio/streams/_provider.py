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
provider sees, through :func:`temporalio.streams.resolve_topic`. What it owes
a record's body on the way to and from its store, :class:`StreamProvider`
lists and :func:`temporalio.streams.encode_body` and
:func:`temporalio.streams.decode_body` do.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload

from temporalio.api.stream.v1 import StreamRecord as WireRecord
from temporalio.streams._record import BEGINNING, Cursor, StreamRecord
from temporalio.streams._topic import StreamTopic

if TYPE_CHECKING:
    from temporalio.client import Client
    from temporalio.streams._ref import StreamRef

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
    """One owner's stream, addressed by topic, from outside workflow code.

    The owner is a workflow, an activity, or a standalone stream that has an
    id of its own and no owner. A handle on a workflow follows its execution
    chain unless it was opened with a ``run_id``, in which case it is pinned
    to that run. A topic is a :class:`temporalio.streams.StreamTopic`
    definition, which carries the record type, or a plain string with
    ``result_type=`` for a name decided at runtime. A transport failure
    surfaces as :class:`temporalio.service.RPCError`, never as the
    transport's own exception type.

    A handle is bound to its client and provider. To hand a stream to another
    process, :meth:`ref` names it as a :class:`temporalio.streams.StreamRef`,
    which is plain data; the receiver opens it with
    ``temporalio.client.Client.get_stream_handle`` or
    ``temporalio.activity.stream_handle``, and calls that name no topic
    on that handle address the ref's topic.
    """

    @overload
    def read(
        self,
        *,
        topic: StreamTopic[T],
        after: Cursor = ...,
        last: int | None = None,
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        last: int | None = None,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        last: int | None = None,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records on ``topic`` after ``after`` as they arrive.

        Without ``topic`` it reads :data:`temporalio.streams.DEFAULT_TOPIC`.
        ``BEGINNING`` yields everything the topic retains, starting at the
        oldest record it still holds. ``END`` yields only what is appended
        after the read starts. ``last=N`` starts at the newest ``N`` records,
        or at all of them when there are fewer; it counts records of every
        kind, so a ``FINISH`` among them leaves fewer than ``N`` values, and
        it is exclusive with a cursor. Any other cursor came from a record a
        reader saw, and reading resumes just past it, so a reader that stores
        the last cursor it handled and hands it back sees every record
        exactly once; that is the only way to resume. The read ends when the owning
        execution, or its chain, is closed and every retained record after
        ``after`` has been delivered; until then it waits. The result is a
        generator, so a caller that stops early should ``aclose()`` it. How
        much that releases is the provider's to say: one that holds only
        local state lets go at once, and one that parked something on a
        store it cannot un-park says in its own documentation what it
        releases and when. Read the provider's ``read`` before relying on an
        immediate release.

        Raises:
            ValueError: ``result_type`` was passed with a topic definition,
                the topic is empty, ``last`` is not positive, or ``last`` was
                passed with a cursor.
            StreamCursorError: ``after`` came from another provider or names
                a record no longer retained. Raised by this call, not by the
                first iteration.
            StreamUnsupportedError: The provider cannot start a read where
                ``END`` or ``last=`` asks. A provider that raises it says so
                in its own documentation.
            StreamNotFoundError: The workflow or topic does not exist or is
                past retention.
        """
        ...

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The cursor of the newest record on ``topic``, or ``BEGINNING`` when empty.

        Without ``topic`` it answers for :data:`temporalio.streams.DEFAULT_TOPIC`.

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
        self, *, topic: str | None = None, producer_id: str = ..., attempt: int = ...
    ) -> StreamProducer[Any]: ...

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str = "",
        attempt: int = 0,
    ) -> StreamProducer[Any]:
        """A producer on ``topic``, or on the default topic without one.

        Inside an activity, leave ``producer_id`` and ``attempt`` unset: the
        activity's own id and attempt are the right answer, and they are what
        let a reader tell a retry from a new generation. Outside one,
        ``producer_id`` is required and an empty one raises ``ValueError``.
        """
        ...

    def ref(self, *, topic: str | StreamTopic[Any] | None = None) -> StreamRef:
        """A :class:`temporalio.streams.StreamRef` to ``topic`` of this owner.

        Without ``topic`` it names :data:`temporalio.streams.DEFAULT_TOPIC`,
        or the topic this handle was opened from a ref with. The ref carries
        the owner exactly as this handle addresses it, a ``run_id`` included
        when the handle is pinned, and no cursor or provider name, so it can
        travel as a workflow argument, an activity result or a Nexus
        operation input or result and be opened wherever a client is.
        """
        ...

    async def close(self) -> None:
        """Seal the standalone stream this handle is on.

        A sealed stream takes no more records: a later ``append`` raises
        :class:`temporalio.streams.StreamClosedError`, while everything it
        retains stays readable and a read on it ends once that tail has been
        delivered. Idempotent. Only a standalone stream can be closed here,
        because an owned stream ends with its owner.

        Raises:
            ValueError: This handle is on a workflow's or an activity's
                stream.
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

    def open_reader(
        self, topic: str, *, after: Cursor, last: int | None = None
    ) -> ReadSource:
        """Subscribe the running workflow to ``topic`` of its own stream.

        ``after`` and ``last`` mean what they mean on
        :meth:`StreamHandle.read`, and arrive already checked. Where a start
        is resolved has to be something replay reproduces, so a provider
        resolves it in the store and records the result, never by reading
        the store from the workflow thread.

        Raises:
            StreamCursorError: ``after`` was minted by another provider.
            StreamUnsupportedError: The provider cannot start where ``END``
                or ``last`` asks.
        """
        ...

    def open_writer(self, topic: str) -> WriteSink:
        """Bind ``topic`` of the running workflow's stream for publishing."""
        ...

    def on_workflow_start(self) -> None:
        """Called before the workflow function runs, and before the first task's handlers.

        After the workflow's own ``__init__`` and before any Signal or Update
        of the first task is handled, which the SDK does ahead of the
        workflow function. A provider that serves outside readers through
        handlers on the workflow registers them here, so an Update that
        arrives with the first task finds them.
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

    **What a provider owes a record's body.** The handles convert a value
    into the body with the payload converter and no more; what the SDK does
    to every other payload it sends, the codec and external storage, the
    provider owes the body too, through the client's data converter, so the
    :class:`temporalio.converter.ExternalStorage` drivers an application
    configured apply to stream bodies as well. It does that in one order.
    First it takes the retry fingerprint, the identity a repeated append is
    matched by, over the converted bytes, before the codec and before any
    offload, so a codec that encrypts with a fresh nonce cannot turn a retry
    into a divergent write; the plaintext hash also rides the record under
    :data:`temporalio.streams.CONTENT_HASH_KEY`, where the store can read it.
    Then it encodes the body and offloads it, and on a read it does the
    reverse before the record reaches a reader. A workflow's own publish is
    converted on the workflow thread and no further: the codec and the offload
    run when the provider commits the task's batch, off that thread.
    :func:`temporalio.streams.encode_body`,
    :func:`temporalio.streams.decode_body` and
    :func:`temporalio.streams.content_fingerprint` are that rule in code.

    **Standalone streams.** A stream can have an id of its own and no owner.
    It is created on purpose, with :meth:`create_standalone_stream` and a
    retention policy, and sealed on purpose, with the handle's ``close``. It
    is addressed by topic like an owner's streams; how a provider lays its
    topics out in the store is its own. A provider whose store cannot hold a
    stream without an owner raises
    :class:`temporalio.streams.StreamUnsupportedError` from both standalone
    calls.
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

    async def create_standalone_stream(
        self,
        client: Client,
        stream_id: str,
        *,
        retention: timedelta | None = None,
        max_records: int | None = None,
        max_bytes: int | None = None,
    ) -> StreamHandle:
        """Create the standalone stream ``stream_id`` and return a handle on it.

        The three policy arguments bound what the stream retains: records
        older than ``retention``, beyond the newest ``max_records``, or past
        ``max_bytes`` of stored records are dropped, and ``None`` leaves that
        bound to the provider's default. Creating a stream that exists with
        the same policy returns a handle on it, so a retried create is
        harmless.

        Raises:
            ValueError: ``stream_id`` is empty, a bound is not positive, or
                the stream exists with a different policy.
            StreamUnsupportedError: The provider's store cannot hold a stream
                without an owner.
        """
        ...

    def get_standalone_stream_handle(
        self, client: Client, stream_id: str
    ) -> StreamHandle:
        """A handle on the standalone stream ``stream_id``, which must exist.

        Nothing here creates the stream: the first ``read``, ``latest`` or
        ``producer`` on a stream that does not exist raises
        :class:`temporalio.streams.StreamNotFoundError`, unless the provider
        can wait for the stream to be created, in which case a ``read`` parks
        until the first write and says so in its own documentation.

        Raises:
            StreamUnsupportedError: The provider's store cannot hold a stream
                without an owner.
        """
        ...

    async def close(self) -> None:
        """Release what this provider holds for the process.

        A provider that keeps a connection pool or an HTTP session open needs
        a moment where the process says it is done; this is it. A provider
        that holds nothing returns at once.

        The application calls this, not the worker and not the client. One
        provider serves the workers built from a client and every handle
        opened outside them, so no single one of those owns its lifetime and
        a worker shutting down would close a connection its siblings are
        still reading through. A provider that outlives the process it was
        made in is the application's to close.
        """
        ...
