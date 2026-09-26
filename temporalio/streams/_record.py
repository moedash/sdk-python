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
    "Cursor",
    "RecordKind",
    "StreamRecord",
    "Supersession",
]

T = TypeVar("T")


@enum.unique
class RecordKind(enum.IntEnum):
    """What a record is.

    Mirrors ``temporal.api.stream.v1.StreamRecordKind`` value for value, so a
    record's kind crosses the wire as the integer the proto holds.
    """

    UNSPECIFIED = 0
    """The proto's zero value.

    A stored record whose writer set no kind is read as :attr:`DATA`, as the
    proto defines it, so a reader never sees this kind on a record.
    """

    DATA = 1
    """Carries a value published by a workflow or a producer."""

    FINISH = 2
    """The producer named in ``producer_id`` will write nothing more on this topic.

    An empty ``producer_id`` names the owning workflow. It does not end a
    read, which ends when the owning execution or its chain is closed and the
    retained tail has been delivered, and it says nothing about the producer's
    outcome: an activity can still time out after writing it.
    """

    SUPERSEDED = 3
    """A later attempt of the same producer started writing.

    Synthesized by the reader from what it observed, never stored, so every
    provider delivers it identically and replay reproduces it without the
    provider's help. Its cursor is the position before the new attempt's
    first record, so resuming after it delivers that record next.
    """


@dataclass(frozen=True)
class Cursor:
    """A position in a stream, ordered by its provider rather than by value.

    Opaque on purpose. One provider numbers records with integers and another
    with a millisecond-and-sequence pair, so comparing tokens here would be
    right for one and wrong for the other. Hand a cursor back to resume after
    the record it names; nothing here advances one. The token starts with the
    name of the provider that minted it, and a provider refuses a token from
    another with :class:`temporalio.streams.StreamCursorError`.
    """

    token: str

    def __str__(self) -> str:
        """The provider's position token."""
        return self.token


BEGINNING = Cursor("")
"""Read from the oldest record the stream still retains."""


@dataclass(frozen=True)
class Supersession:
    """What a :attr:`RecordKind.SUPERSEDED` record reports."""

    producer_id: str
    previous_attempt: int
    attempt: int


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """One record as a reader sees it.

    ``value`` is set on a :attr:`RecordKind.DATA` record and ``supersession``
    on a :attr:`RecordKind.SUPERSEDED` one; every other kind carries neither.
    Each field means one thing, so a consumer narrows on ``kind`` and reads
    the field that kind promises.
    """

    kind: RecordKind
    cursor: Cursor
    topic: str
    producer_id: str = ""
    """Who wrote it, or empty when the owning workflow wrote it itself."""
    attempt: int = 0
    """The producer's attempt, or 0 when it did not declare one."""
    sequence: int = 0
    """The producer's position within its attempt, or 0 when it does not number."""
    value: T | None = None
    """The published value. Set on ``DATA`` only."""
    supersession: Supersession | None = None
    """The attempt change being reported. Set on ``SUPERSEDED`` only."""
