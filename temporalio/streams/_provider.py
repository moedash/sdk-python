"""The provider registry: where a process names its stream provider.

A provider only moves bytes. The handles, records, framing and supersession
around it are shared, so every provider module is small: it implements
:class:`StreamProvider` and registers a factory under a short name when it is
imported. One :func:`configure` call decides which factory serves this
process; :func:`worker_options` makes the same choice for a worker when
exactly one provider is registered.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Callable, Protocol, TypeVar, overload

from temporalio import activity
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._record import BEGINNING, Cursor, StreamRecord

__all__ = [
    "Consumer",
    "Producer",
    "StreamProvider",
    "StreamProviderLifecycle",
    "check_topic",
    "close",
    "configure",
    "consumer",
    "drain",
    "instance",
    "open_read",
    "open_write",
    "prepare",
    "producer",
    "register",
    "registered",
    "worker_options",
]

T = TypeVar("T")


class Producer(Protocol):
    """Appends to a stream from outside workflow code.

    Every append is visible as soon as the provider accepts it, and carries
    the producer id, attempt and sequence that let a reader tell a retried
    append from a new generation.
    """

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        ...

    async def append(self, *values: Any) -> Cursor | None:
        """Append values and return the cursor of the last one written.

        The last rather than the first, so ``read(after=appended)`` yields
        only what came later. ``None`` when nothing was written, because the
        batch was empty or a repeat the provider dropped, and on a transport
        that learns positions only at read time; a caller that needs to
        position itself on such a transport asks :meth:`Consumer.latest`.
        """
        ...

    async def finish(self) -> None:
        """Declare this producer's output complete."""
        ...


class Consumer(Protocol):
    """Reads a stream from outside workflow code, resumably."""

    @overload
    def read(
        self,
        *,
        after: Cursor = ...,
        topic: str | None = ...,
        type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        after: Cursor = ...,
        topic: str | None = ...,
        type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        after: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records after ``after`` as they arrive.

        ``BEGINNING`` yields everything the stream retains. Any other cursor
        came from a record this or another reader saw, and reading resumes
        just past it, so a reader that stores the last cursor it handled and
        hands it back sees every record exactly once. ``topic`` filters the
        stream the workflow publishes; an inbound stream has no topics, and
        naming one there raises ``ValueError``. The result is a generator so
        a caller that stops early can ``aclose()`` it and release whatever
        the provider parked against the store.
        """
        ...

    async def latest(self, *, topic: str | None = None) -> Cursor:
        """The cursor of the newest record, or ``BEGINNING`` when there is none.

        For a reader that wants to follow from now: ``read(after=latest())``
        yields only what is published after this call returned, which is how
        a client that is about to send a message positions itself before
        sending, without the workflow having to report a position. ``topic``
        is for the provider that keeps each topic in its own store.
        """
        ...


class StreamProvider(Protocol):
    """What a provider module implements.

    The workflow-side pair (:meth:`open_read`, :meth:`open_write`) runs inside
    workflow code and must keep the contract's first two rules: publishes
    commit with the workflow task, and reads are recorded observations. The
    outside pair (:meth:`producer`, :meth:`consumer`) runs anywhere and moves
    framed bytes. A transport-only provider may serve just the outside pair
    and raise on the workflow side, naming the provider a worker should use.

    A provider whose transport parks something against the running workflow
    also implements :class:`StreamProviderLifecycle`.
    """

    name: str

    def configure(self, **options: Any) -> None:
        """Accept this provider's process-level options."""
        ...

    def worker_options(self) -> dict[str, Any]:
        """What a ``Worker`` or ``Replayer`` needs to serve this provider."""
        ...

    def open_read(
        self,
        stream: str,
        *,
        after: Cursor = BEGINNING,
        idle_timeout: timedelta | None = None,
    ) -> ReadSource:
        """Subscribe the running workflow to its inbound stream ``stream``."""
        ...

    def open_write(self, topic: str) -> WriteSink:
        """Bind ``topic`` on the stream the running workflow owns."""
        ...

    async def producer(
        self,
        client: Any,
        *,
        workflow_id: str,
        stream: str = "",
        topic: str = "",
        producer_id: str = "",
        attempt: int = 0,
    ) -> Producer:
        """Open a producer that appends on ``workflow_id``'s account.

        ``stream`` names one of the workflow's inbound streams. With no
        ``stream``, ``topic`` names a topic on the stream the workflow itself
        publishes, which is how an activity puts its live output next to the
        workflow's own records for the same outside reader. ``producer_id``
        is never empty here: the package resolved it, from the activity
        context when the caller left it unset.
        """
        ...

    async def consumer(
        self, client: Any, *, workflow_id: str, stream: str = ""
    ) -> Consumer:
        """Open a reader on ``workflow_id``'s own stream, or its inbound ``stream``."""
        ...


class StreamProviderLifecycle(Protocol):
    """The hooks a provider adds when it needs the workflow's own lifetime.

    Separate from :class:`StreamProvider` because most transports need none
    of them, and a provider is not asked to carry empty methods to say so.
    :func:`prepare`, :func:`drain` and :func:`close` each call the hook when
    the provider defines it.
    """

    def prepare(self) -> None:
        """Install whatever this provider needs before the workflow runs.

        A provider that serves outside readers through handlers on the
        workflow itself has to register them before the first task completes,
        or a reader that arrives early finds nothing to talk to.
        """
        ...

    def drain(self) -> None:
        """Release anything this provider parked on the workflow's behalf.

        A provider that parks an outside reader against the running workflow,
        as the Workflow Streams transport does with its long-poll update, has
        to let go before the workflow can return.
        """
        ...

    async def close(self) -> None:
        """Release what this provider holds for the process.

        A provider that keeps a connection pool or an HTTP session open needs
        a moment where the process says it is done; this is it.
        """
        ...


_factories: dict[str, Callable[[], StreamProvider]] = {}
_active: StreamProvider | None = None


def register(name: str, factory: Callable[[], StreamProvider]) -> None:
    """Make ``factory`` selectable as ``configure(provider=name)``."""
    _factories[name] = factory


def registered() -> list[str]:
    """The provider names this process can configure."""
    return sorted(_factories)


def _make(name: str | None) -> StreamProvider:
    if name is None:
        if len(_factories) == 1:
            name = next(iter(_factories))
        else:
            raise RuntimeError(
                f"name a provider: configure(provider=...) with one of {registered()}"
            )
    factory = _factories.get(name)
    if factory is None:
        raise RuntimeError(
            f"no stream provider {name!r} is registered; available: {registered()}"
        )
    return factory()


def configure(provider: str | None = None, **options: Any) -> None:
    """Name the provider for this process and hand it its options.

    ``provider`` may be omitted when exactly one provider is registered.
    """
    global _active
    _active = instance(provider, **options)


def instance(provider: str | None = None, **options: Any) -> StreamProvider:
    """A configured provider that is not installed as the process default.

    For code that serves one provider while the process is configured with
    another, such as a Nexus stream handler delegating to its store.
    """
    chosen = _make(provider)
    chosen.configure(**options)
    return chosen


def _current() -> StreamProvider:
    if _active is None:
        # Building a provider is process setup. Doing it on the first
        # workflow's thread would race a second workflow thread for the
        # global and hide the choice inside workflow code.
        raise RuntimeError(
            "no stream provider is configured; call streams.configure() or "
            "streams.worker_options() before opening a stream"
        )
    return _active


def worker_options() -> dict[str, Any]:
    """What a ``Worker`` or ``Replayer`` needs to serve this provider.

    Every worker calls this, so it is where the default is resolved when
    :func:`configure` was not called and exactly one provider is registered.
    """
    if _active is None:
        configure()
    return _current().worker_options()


def open_read(
    stream: str,
    *,
    after: Cursor = BEGINNING,
    idle_timeout: timedelta | None = None,
) -> ReadSource:
    """Subscribe the running workflow to its inbound stream ``stream``."""
    return _current().open_read(stream, after=after, idle_timeout=idle_timeout)


def open_write(topic: str) -> WriteSink:
    """Bind ``topic`` on the stream the running workflow owns."""
    return _current().open_write(topic)


def prepare() -> None:
    """Let the provider install what it needs, before the workflow runs.

    Call it from the workflow's constructor. It is a no-op on providers that
    need nothing, so workflow code can call it unconditionally and stay
    portable.
    """
    provider = _current()
    install = getattr(provider, "prepare", None)
    if install is not None:
        install()


def drain() -> None:
    """Release anything the provider parked on this workflow's behalf.

    Call it before a workflow that read a stream returns. It is a no-op on
    providers that park nothing, so workflow code can call it unconditionally
    and stay portable.
    """
    provider = _current()
    release = getattr(provider, "drain", None)
    if release is not None:
        release()


async def close() -> None:
    """Release what the provider holds for this process.

    Await it when the process is done with streams. It is a no-op on
    providers that hold nothing, and on a process that never configured one,
    so a shutdown path can call it unconditionally.
    """
    if _active is None:
        return
    release = getattr(_active, "close", None)
    if release is not None:
        await release()


def check_topic(stream: str, topic: str | None) -> None:
    """Reject a topic filter on an inbound stream.

    An inbound record carries no topic; the stream's name is its whole
    address, so a filter there could only ever match nothing. Every provider
    calls this at the top of its ``read`` so the answer is the same on all
    of them.
    """
    if stream and topic is not None:
        raise ValueError(
            f"inbound stream {stream!r} has no topics; topic= applies to the "
            "stream the workflow publishes"
        )


async def producer(
    client: Any,
    *,
    workflow_id: str,
    stream: str = "",
    topic: str = "",
    producer_id: str = "",
    attempt: int = 0,
) -> Producer:
    """Open a producer that appends on ``workflow_id``'s account.

    Name either an inbound ``stream`` of the workflow, or a ``topic`` on the
    stream the workflow publishes. Inside an activity, leave ``producer_id``
    and ``attempt`` unset: the activity's own id and attempt are the right
    answer, and they are what let a reader tell a retry from a new generation.
    """
    if bool(stream) == bool(topic):
        raise ValueError(
            "name exactly one of stream (an inbound stream of the workflow) or "
            "topic (a topic on the stream the workflow publishes)"
        )
    if not producer_id:
        if not activity.in_activity():
            raise ValueError(
                "producer_id is required outside an activity; inside one it "
                "defaults to the activity's id and attempt"
            )
        info = activity.info()
        producer_id = info.activity_id
        attempt = attempt or info.attempt
    return await _current().producer(
        client,
        workflow_id=workflow_id,
        stream=stream,
        topic=topic,
        producer_id=producer_id,
        attempt=attempt,
    )


async def consumer(client: Any, *, workflow_id: str, stream: str = "") -> Consumer:
    """Open a reader on ``workflow_id``'s own stream, or its inbound ``stream``.

    With no ``stream`` the reader follows what the workflow publishes, topic
    by topic. With one, it follows the inbound stream of that name, which is
    how a process watches what producers hand the workflow.
    """
    return await _current().consumer(client, workflow_id=workflow_id, stream=stream)
