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

from typing import Any

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
        """Start with no producer seen."""
        self._attempts: dict[str, int] = {}

    def note(
        self, producer_id: str, attempt: int, *, topic: str, previous: Cursor
    ) -> StreamRecord[Any] | None:
        """A supersession record when this record starts a newer attempt.

        ``previous`` is the cursor of the last record delivered before the one
        being noted, or the cursor the read started from. The synthesized
        record carries it, so a consumer that checkpoints the supersession and
        resumes after it is handed the new attempt's first record next rather
        than skipping it.

        A producer that declares no attempt supersedes nothing, because there
        is no generation to compare. That is the same answer as an unnumbered
        record: the interface reports what it was told and invents nothing.
        """
        if not producer_id or attempt <= 0:
            return None
        seen = self._attempts.get(producer_id, 0)
        if attempt <= seen:
            return None
        self._attempts[producer_id] = attempt
        if seen == 0:
            return None
        return StreamRecord(
            kind=RecordKind.SUPERSEDED,
            cursor=previous,
            topic=topic,
            producer_id=producer_id,
            attempt=attempt,
            supersession=Supersession(
                producer_id=producer_id, previous_attempt=seen, attempt=attempt
            ),
        )
