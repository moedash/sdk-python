"""The value types the stream contract is expressed in.

Nothing here touches Temporal or a provider, so both bindings share it
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
    """What a record is."""

    DATA = 1
    """Carries a value published by a workflow or a producer."""

    FINISH = 2
    """The writer of this topic declared it complete.

    Separate from the activity or workflow that wrote it having succeeded. A
    reader that treats it as proof of success is wrong: the activity can still
    time out after writing it.
    """

    SUPERSEDED = 3
    """A later attempt of the same producer started writing.

    Synthesized by the reader from what it observed, so both providers deliver
    it identically and replay reproduces it without the provider's help.
    """


@dataclass(frozen=True)
class Cursor:
    """A position in a stream, ordered by its provider rather than by value.

    Opaque on purpose. One provider numbers records with integers and another
    with a millisecond-and-sequence pair, so comparing tokens here would be
    right for one and wrong for the other. Hand a cursor back to resume from
    it; ask the reader to compare two.
    """

    token: str

    def __str__(self) -> str:
        """The provider's position token."""
        return self.token


BEGINNING = Cursor("")
"""Read from the oldest record the stream still retains."""


@dataclass(frozen=True)
class Supersession:
    """The body of a :attr:`RecordKind.SUPERSEDED` record."""

    producer: str
    previous_attempt: int
    attempt: int


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """One record as workflow code sees it."""

    value: T
    cursor: Cursor
    kind: RecordKind = RecordKind.DATA
    topic: str = ""
    producer: str = ""
    """Who wrote it, or empty when the owning workflow wrote it itself."""
    attempt: int = 0
    """The producer's attempt, or 0 when it did not declare one."""
    sequence: int = -1
    """The producer's position within its attempt, or -1 when unnumbered."""
