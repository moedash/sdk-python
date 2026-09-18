"""A stream a workflow can read, decide on, and write.

The contract, in five statements:

1. **A workflow publishes only to its own stream, and it publishes
   transactionally.** :meth:`StreamWriter.publish` returns without the record
   being visible. The record appears when the workflow task is accepted, and
   never if the task fails, so no reader can see a decision the workflow did
   not commit.
2. **Reading is an observation, and the SDK records it.** What
   :class:`StreamReader` handed to workflow code, including the boundary where
   it found nothing, is committed with the commands that reading produced.
   Recovery re-supplies the same records in the same order.
3. **Anything that does I/O publishes on its own account.** An activity, a
   local activity, or an outside process writes through :func:`producer`,
   either into one of the workflow's inbound streams or onto a topic of the
   stream the workflow publishes, and its records are visible as soon as they
   are written. It carries a producer id, an attempt and a sequence so a retry
   can be told from a new generation.
4. **A cursor is opaque and belongs to its provider.** Hand it back to resume
   after the record it names. Do not compare two cursors or do arithmetic on
   one: one provider numbers records with integers and another with a
   millisecond-and-sequence pair. A reader that wants to follow from now asks
   the consumer for :meth:`Consumer.latest` instead of guessing a position.
5. **A workflow names its streams relative to itself, and the provider
   resolves them.** ``reader("inputs")`` is this workflow's inbound stream
   called ``inputs``; ``writer("decisions")`` is a topic it publishes on. One
   provider stores that as a namespace-level stream id and another as a name
   under the run's chain key, and workflow code does not have to know which.
   The provider itself is chosen when the worker is built.

Outside a workflow there is no worker to carry the choice, so a process calls
:func:`configure` once before it opens a :func:`producer` or a
:func:`consumer`, and passes :func:`worker_options` to its ``Worker`` and
``Replayer``. Those two calls are where a provider is named. Nothing else in
this module mentions one. Code that needs a second provider next to the
process default, such as a handler that serves one store while its process
talks to another, holds one from :func:`instance`.

Reading somebody else's stream is out of scope for the first release. It is
the topology neither prototype has evidence for, and leaving it out is what
lets both of them implement the rest.

What the contract does not promise: that a :attr:`RecordKind.FINISH` record
means the writing activity succeeded, that a superseded attempt's records can
be withdrawn, or that a stream outlives the retention its provider is
configured for.

Prototype support for AI-198. The public names are the proposal; providers
live under :mod:`temporalio.streams.providers`, one module each, registered
by name and chosen by :func:`configure`. Everything else is shared.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, TypeVar, overload

from temporalio.streams._handles import (
    ReadSource,
    StreamReader,
    StreamWriter,
    WriteSink,
)
from temporalio.streams._provider import (
    Consumer,
    Producer,
    StreamProvider,
    StreamProviderLifecycle,
    configure,
    consumer,
    drain,
    instance,
    open_read,
    open_write,
    prepare,
    producer,
    worker_options,
)
from temporalio.streams._record import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)

__all__ = [
    "BEGINNING",
    "Consumer",
    "Cursor",
    "Producer",
    "ReadSource",
    "RecordKind",
    "StreamProvider",
    "StreamProviderLifecycle",
    "StreamReader",
    "StreamRecord",
    "StreamWriter",
    "Supersession",
    "WriteSink",
    "configure",
    "consumer",
    "drain",
    "instance",
    "prepare",
    "producer",
    "reader",
    "worker_options",
    "writer",
]

T = TypeVar("T")


@overload
def reader(
    stream: str,
    *,
    type: type[T],
    after: Cursor = ...,
    idle_timeout: timedelta | None = ...,
) -> StreamReader[T]: ...


@overload
def reader(
    stream: str,
    *,
    type: None = None,
    after: Cursor = ...,
    idle_timeout: timedelta | None = ...,
) -> StreamReader[Any]: ...


def reader(
    stream: str,
    *,
    type: type | None = None,
    after: Cursor = BEGINNING,
    idle_timeout: timedelta | None = None,
) -> StreamReader[Any]:
    """Subscribe this workflow to its inbound stream ``stream``.

    Each call is a separate subscription with its own cursor, so adding,
    removing or reordering one renumbers the waits after it. Gate a change
    behind :func:`temporalio.workflow.patched` as you would for a timer.

    An inbound stream has no topics: its name is the whole address, and
    every record on it arrives with an empty ``topic``. Topics belong to the
    stream the workflow publishes, which :func:`consumer` reads from outside.

    Args:
        stream: The inbound stream's name, relative to this workflow.
        type: The value type, used as the decode hint.
        after: Resume after this record. Only honoured on the first
            subscription of a run, because after that the recorded cursor
            decides.
        idle_timeout: How long the workflow waits with nothing arriving before
            the provider is allowed to release the worker. ``None`` takes the
            provider's default.
    """
    return StreamReader(
        open_read(stream, after=after, idle_timeout=idle_timeout), type=type
    )


def writer(topic: str) -> StreamWriter[Any]:
    """Publish to ``topic`` on the stream this workflow owns.

    Args:
        topic: The topic name. Encoding follows each published value.
    """
    return StreamWriter(open_write(topic), topic)


# Importing the package registers every provider whose dependencies are
# present in this tree. Import order matters: the registry above must exist
# before a provider module can register with it.
from temporalio.streams import providers as providers  # noqa: E402
