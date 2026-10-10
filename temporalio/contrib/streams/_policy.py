"""Turning a producer's newer attempt into something a reader can act on.

An Activity that streams half an answer and then fails leaves those records
in the stream. Its retry writes different ones. No provider can undo the
first half, so the reader is told that a new attempt began and the
application decides what to do.

This runs in the reader over records it already observed, so it costs no
round trip and every provider reports a retry the same way.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)

__all__ = ["AttemptTracker"]


class AttemptTracker:
    """Watches producer attempts on one read."""

    def __init__(self, warn: Callable[[str], None] | None = None) -> None:
        """Start with no producer seen, reporting anything odd through ``warn``."""
        self._attempts: dict[str, int] = {}
        self._warn = warn

    def behind(self, producer_id: str, attempt: int) -> bool:
        """Whether this read already delivered a newer attempt of ``producer_id``."""
        return bool(producer_id) and 0 < attempt < self._attempts.get(producer_id, 0)

    def note(
        self, producer_id: str, attempt: int, *, topic: str, previous: Cursor
    ) -> StreamRecord[Any] | None:
        """A ``SUPERSEDED`` record when this record starts a newer attempt.

        ``previous`` is the cursor of the last record delivered before the
        one being noted, or the cursor the read started from. The synthesized
        record carries it. A read that resumes there is primed with the
        record at its cursor, so it reports the supersession again before the
        new attempt's first record.

        A producer that declares no attempt supersedes nothing, because there
        is no generation to compare.

        An attempt that goes backwards supersedes nothing either. It is
        reported through ``warn``, and the reader marks its records
        ``stale`` rather than passing them off as ordinary data.
        A lower attempt after a higher one means an older attempt was still
        writing after a newer one started, such as an Activity attempt that
        timed out but kept running. A consumer that reads it as the current
        answer would show a stale one.
        """
        if not producer_id or attempt <= 0:
            return None
        seen = self._attempts.get(producer_id, 0)
        if attempt < seen and self._warn is not None:
            self._warn(
                f"stream record on {topic!r} after {previous} is from attempt "
                f"{attempt} of producer {producer_id!r}, behind attempt {seen}, "
                "which this reader already delivered: an older attempt wrote "
                "after a newer one started"
            )
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
