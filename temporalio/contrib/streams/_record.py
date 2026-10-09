"""The value types the stream contract is expressed in.

Nothing here touches Temporal or a provider, so every provider shares it
unchanged.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Generic, TypeVar

__all__ = [
    "BEGINNING",
    "END",
    "Cursor",
    "RecordKind",
    "StreamRecord",
    "Supersession",
]

T = TypeVar("T")


@enum.unique
class RecordKind(enum.IntEnum):
    """What a record is.

    The stored kinds match ``temporal.sdk.streams.v1.StreamRecordKind``
    value for value, so a kind crosses the wire as the integer the proto
    holds. :attr:`SUPERSEDED` is never stored.
    """

    UNSPECIFIED = 0
    """The proto's zero value.

    A stored record whose writer set no kind is read as :attr:`DATA`, so a
    reader never sees this kind on a record.
    """

    DATA = 1
    """Carries a value published by a Workflow or a producer."""

    FINISH = 2
    """The producer in ``producer_id`` will write nothing more on this topic.

    An empty ``producer_id`` names the owning Workflow. It does not end a
    read and says nothing about the producer's outcome: an Activity can
    still fail after it wrote ``FINISH``.
    """

    SUPERSEDED = 3
    """A later attempt of the same producer started writing.

    The reader synthesizes it from the records it observed. No store holds
    it, so every provider reports a retry the same way. Its cursor is the
    position before the new attempt's first record, so a reader that resumes
    after it gets that record next.
    """


@dataclass(frozen=True)
class Cursor:
    """A position in one stream.

    Opaque on purpose. The token names the provider that minted it and the
    stream it belongs to, and that provider refuses a token from another
    provider or another stream with
    :class:`temporalio.contrib.streams.StreamCursorError`. Do not compare two
    cursors or do arithmetic on one: hand a cursor back to resume strictly
    after the record it names.
    """

    token: str

    def __str__(self) -> str:
        """The provider's position token."""
        return self.token


BEGINNING = Cursor("")
"""Read from the oldest record the stream still retains."""

END = Cursor("$end")
"""Read only what is appended after the read starts.

It is resolved when the read starts. To position a reader before the
reader's process writes something, use
:meth:`temporalio.contrib.streams.StreamHandle.latest` instead.
"""


@dataclass(frozen=True)
class Supersession:
    """What a :attr:`RecordKind.SUPERSEDED` record reports."""

    producer_id: str
    previous_attempt: int
    attempt: int


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """One record as a reader sees it.

    ``value`` is set on a :attr:`RecordKind.DATA` record and
    ``supersession`` on a :attr:`RecordKind.SUPERSEDED` one. Other kinds
    carry neither, so a consumer narrows on ``kind`` first.
    """

    kind: RecordKind
    cursor: Cursor
    topic: str
    producer_id: str = ""
    """Who wrote it, or empty when the owning Workflow wrote it."""
    attempt: int = 0
    """The producer's attempt, or 0 when the producer declared none."""
    sequence: int = 0
    """The producer's position within its attempt, or 0 when it does not number."""
    value: T | None = None
    """The published value. Set on ``DATA`` only."""
    supersession: Supersession | None = None
    """The attempt change being reported. Set on ``SUPERSEDED`` only."""
