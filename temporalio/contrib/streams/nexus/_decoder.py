"""Turning the records a stream service read carries into reader records.

Workflow code reads through the stream service, so it can't reach Core's
reader. The records arrive plain, as stored, and this synthesizes
``SUPERSEDED`` from the attempts it observes, the same way Core's reader
does for a read outside Workflow code.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

import temporalio.converter
from temporalio.contrib.streams._errors import StreamRecordError
from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)
from temporalio.contrib.streams._wire import RUN_ID_KEY, WireRecord

__all__ = ["AttemptTracker", "RecordDecoder", "from_wire"]


def from_wire(
    converter: temporalio.converter.PayloadConverter,
    cursor: Cursor,
    wire: WireRecord,
    result_type: type | None,
) -> StreamRecord[Any]:
    """Turn a stored record into the record a reader yields.

    Raises:
        ValueError: The kind is one no store may hold, such as a synthesized
            ``SUPERSEDED`` or a value this SDK does not know.
    """
    kind = RecordKind(wire.kind)
    if kind is RecordKind.SUPERSEDED:
        raise ValueError("a SUPERSEDED record is synthesized by readers, never stored")
    if kind is RecordKind.UNSPECIFIED:
        kind = RecordKind.DATA
    value: Any = None
    if kind is RecordKind.DATA and wire.HasField("body"):
        hints = [result_type] if result_type is not None else None
        value = converter.from_payloads([wire.body], hints)[0]
    return StreamRecord(
        kind=kind,
        cursor=cursor,
        topic=wire.topic,
        producer_id=wire.producer_id,
        attempt=wire.attempt,
        sequence=wire.sequence,
        run_id=wire.metadata[RUN_ID_KEY].data.decode()
        if RUN_ID_KEY in wire.metadata
        else "",
        value=value,
    )


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


class RecordDecoder:
    """Turns stored records into the records a reader yields.

    One per read. It synthesizes ``SUPERSEDED`` from the attempts it
    observes, and positions each synthesized record at the cursor before the
    record that triggered it. A record it cannot decode raises
    :class:`temporalio.contrib.streams.StreamRecordError` with that record's
    cursor. Skipping it silently would lose data the reader never hears of.
    """

    def __init__(
        self,
        converter: temporalio.converter.PayloadConverter,
        result_type: type | None,
        *,
        after: Cursor,
        warn: Callable[[str], None],
    ) -> None:
        """Decode with ``converter`` into ``result_type``, resuming after ``after``."""
        self._converter = converter
        self._result_type = result_type
        self._previous = after
        self._warn = warn
        self._attempts = AttemptTracker(warn)

    def prime(self, wire: WireRecord) -> None:
        """Note the attempt of the record at the resume cursor, which was already delivered.

        A read that resumes would otherwise take the next attempt of that
        producer as its first, and report no ``SUPERSEDED``. Only the cursor
        record's producer is primed. Another producer's earlier attempts are
        not known to a resumed read.
        """
        self._attempts.note(
            wire.producer_id, wire.attempt, topic=wire.topic, previous=self._previous
        )

    def decode(self, cursor: Cursor, wire: WireRecord) -> list[StreamRecord[Any]]:
        """The records to yield for one stored record, in order."""
        try:
            record = from_wire(self._converter, cursor, wire, self._result_type)
        except Exception as error:
            raise StreamRecordError(
                f"stream record at {cursor} could not be decoded: {error}", cursor
            ) from error
        if self._attempts.behind(wire.producer_id, wire.attempt):
            record = dataclasses.replace(record, stale=True)
        out: list[StreamRecord[Any]] = []
        superseded = self._attempts.note(
            wire.producer_id, wire.attempt, topic=wire.topic, previous=self._previous
        )
        if superseded is not None:
            out.append(superseded)
        out.append(record)
        self._previous = cursor
        return out
