"""Turning a producer's newer attempt into something a reader can act on.

An activity that streams half an answer and then fails leaves those records in
the stream. Its retry calls the model again and writes different words. No
provider can undo the first half, and a workflow that already acted on it has
committed that decision, so the honest thing is to tell the reader that a new
attempt began and let the application decide.

This runs in the reader over records it already observed, so it costs no round
trip and replays without the provider being involved.
"""

from __future__ import annotations

from temporalio.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)

__all__ = ["AttemptTracker"]


class AttemptTracker:
    """Watches producer attempts on one subscription."""

    def __init__(self) -> None:
        self._attempts: dict[str, int] = {}

    def note(
        self, producer: str, attempt: int, cursor: Cursor
    ) -> StreamRecord[Supersession] | None:
        """A supersession record when this record starts a newer attempt.

        A producer that declares no attempt supersedes nothing, because there
        is no generation to compare. That is the same answer as an unnumbered
        record: the interface reports what it was told and invents nothing.
        """
        if not producer or attempt <= 0:
            return None
        previous = self._attempts.get(producer, 0)
        if attempt <= previous:
            return None
        self._attempts[producer] = attempt
        if previous == 0:
            return None
        return StreamRecord(
            value=Supersession(
                producer=producer, previous_attempt=previous, attempt=attempt
            ),
            cursor=cursor,
            kind=RecordKind.SUPERSEDED,
            producer=producer,
            attempt=attempt,
        )
