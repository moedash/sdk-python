"""The provider registry: where a process names its stream provider.

A provider only moves bytes. The handles, records, framing and supersession
around it are shared, so every provider module is small: it implements
:class:`StreamProvider` and registers a factory under a short name when it is
imported. One :func:`configure` call decides which factory serves this
process; when exactly one provider is registered, it is the default and the
call can be omitted.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any, Callable, Protocol

from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._record import BEGINNING, Cursor, StreamRecord

__all__ = [
    "Consumer",
    "Producer",
    "StreamProvider",
    "configure",
    "consumer",
    "drain",
    "open_read",
    "open_write",
    "prepare",
    "producer",
    "register",
    "registered",
    "worker_options",
]


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

    async def append(self, *values: Any) -> Cursor:
        """Append values and return where the first one landed."""
        ...

    async def finish(self) -> None:
        """Declare this producer's output complete."""
        ...


class Consumer(Protocol):
    """Reads a stream from outside workflow code, resumably."""

    def read(
        self,
        *,
        start: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncIterator[StreamRecord[Any]]:
        """Yield records from ``start`` as they arrive."""
        ...


class StreamProvider(Protocol):
    """What a provider module implements.

    The workflow-side pair (:meth:`open_read`, :meth:`open_write`) runs inside
    workflow code and must keep the contract's first two rules: publishes
    commit with the workflow task, and reads are recorded observations. The
    outside pair (:meth:`producer`, :meth:`consumer`) runs anywhere and moves
    framed bytes. A transport-only provider may serve just the outside pair
    and raise on the workflow side, naming the provider a worker should use.
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
        start: Cursor = BEGINNING,
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
        stream: str,
        producer_id: str = "",
        attempt: int = 0,
    ) -> Producer:
        """Open a producer for ``workflow_id``'s inbound stream ``stream``."""
        ...

    async def consumer(
        self, client: Any, *, workflow_id: str, stream: str = ""
    ) -> Consumer:
        """Open a reader for what ``workflow_id`` publishes."""
        ...

    def prepare(self) -> None:
        """Install whatever this provider needs before the workflow runs.

        Optional. A provider that serves outside readers through handlers on
        the workflow itself has to register them before the first task
        completes, or a reader that arrives early finds nothing to talk to.
        """
        ...

    def drain(self) -> None:
        """Release anything this provider parked on the workflow's behalf.

        Optional. A provider that parks an outside reader against the running
        workflow, as the Workflow Streams transport does with its long-poll
        update, has to let go before the workflow can return.
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
                "name a provider: configure(provider=...) with one of "
                f"{registered()}"
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
    global _active
    if _active is None:
        chosen = _make(None)
        chosen.configure()
        _active = chosen
    return _active


def worker_options() -> dict[str, Any]:
    """What a ``Worker`` or ``Replayer`` needs to serve this provider."""
    return _current().worker_options()


def open_read(
    stream: str,
    *,
    start: Cursor = BEGINNING,
    idle_timeout: timedelta | None = None,
) -> ReadSource:
    """Subscribe the running workflow to its inbound stream ``stream``."""
    return _current().open_read(stream, start=start, idle_timeout=idle_timeout)


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


async def producer(
    client: Any,
    *,
    workflow_id: str,
    stream: str,
    producer_id: str = "",
    attempt: int = 0,
) -> Producer:
    """Open a producer for the inbound stream ``stream`` of ``workflow_id``."""
    return await _current().producer(
        client,
        workflow_id=workflow_id,
        stream=stream,
        producer_id=producer_id,
        attempt=attempt,
    )


async def consumer(client: Any, *, workflow_id: str, stream: str = "") -> Consumer:
    """Open a reader for what ``workflow_id`` publishes."""
    return await _current().consumer(client, workflow_id=workflow_id, stream=stream)
