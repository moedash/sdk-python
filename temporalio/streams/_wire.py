"""How a record crosses a provider: the proto is the record.

``temporal.api.stream.v1.StreamRecord`` is the wire format on every provider.
A store that keeps bytes keeps ``SerializeToString()`` of it, the native
server stores the proto it is handed, and a reader in any language parses the
same bytes. ``body`` is the user's payload, produced and consumed through the
payload converter, so a codec applies to it like any other payload and a
pre-encoded :class:`temporalio.common.RawValue` passes through untouched.

Cursors are self-describing: a token starts with the name of the provider that
minted it, so a provider can refuse a foreign one at the call rather than
misreading it deep in a generator.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import temporalio.converter
from temporalio.api.stream.v1 import StreamRecord as WireRecord
from temporalio.api.stream.v1 import StreamRecordKind
from temporalio.streams._errors import StreamCursorError
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

__all__ = [
    "RecordDecoder",
    "WireRecord",
    "cursor_position",
    "from_wire",
    "mint_cursor",
    "producer_identity",
    "to_wire",
]


def to_wire(
    converter: temporalio.converter.PayloadConverter,
    *,
    topic: str,
    kind: RecordKind,
    value: Any = None,
    producer_id: str = "",
    attempt: int = 0,
    sequence: int = 0,
) -> WireRecord:
    """Build the record a provider stores or ships.

    Only a ``DATA`` record carries a body; the converter encodes ``value``
    into it, which is where a pre-encoded ``RawValue`` passes through.
    """
    record = WireRecord(
        topic=topic,
        kind=StreamRecordKind.ValueType(int(kind)),
        producer_id=producer_id,
        attempt=attempt,
        sequence=sequence,
    )
    if kind is RecordKind.DATA:
        record.body.CopyFrom(converter.to_payloads([value])[0])
    return record


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
        # The proto defines an unset kind as DATA, so every reader agrees.
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
        value=value,
    )


class RecordDecoder:
    """Turns the records a provider hands over into the records a reader yields.

    One per read. It synthesizes supersession from the attempts it observes,
    positions each synthesized record at the cursor before the record that
    triggered it, and skips a record it cannot decode with a warning rather
    than raising, so a poisoned record cannot pin a workflow on every retry
    while an outside reader of the same stream moves past it.
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
        self._attempts = AttemptTracker()

    def decode(self, cursor: Cursor, wire: WireRecord) -> list[StreamRecord[Any]]:
        """The records to yield for one stored record, in order."""
        try:
            record = from_wire(self._converter, cursor, wire, self._result_type)
        except Exception as error:
            self._warn(f"skipping stream record at {cursor}: {error}")
            # The skipped record still holds its position, so a resume after
            # it moves on rather than tripping over it again.
            self._previous = cursor
            return []
        out: list[StreamRecord[Any]] = []
        superseded = self._attempts.note(
            wire.producer_id, wire.attempt, topic=wire.topic, previous=self._previous
        )
        if superseded is not None:
            out.append(superseded)
        out.append(record)
        self._previous = cursor
        return out


def mint_cursor(provider: str, position: str) -> Cursor:
    """A cursor that names ``position`` and the provider that understands it."""
    return Cursor(f"{provider}:{position}")


def cursor_position(cursor: Cursor, *, provider: str) -> str | None:
    """The position inside a cursor ``provider`` minted, or ``None`` for BEGINNING.

    Raises:
        StreamCursorError: The cursor came from another provider.
    """
    if cursor == BEGINNING:
        return None
    prefix = f"{provider}:"
    if not cursor.token.startswith(prefix):
        raise StreamCursorError(
            f"cursor {cursor.token!r} was not minted by the {provider} stream provider"
        )
    return cursor.token[len(prefix) :]


def producer_identity(producer_id: str, attempt: int) -> tuple[str, int]:
    """Resolve who a producer is, defaulting to the running activity.

    Imported lazily so the module workflow code imports carries no activity
    machinery; the default only means anything inside an activity anyway.
    """
    if producer_id:
        return producer_id, attempt
    import temporalio.activity

    if not temporalio.activity.in_activity():
        raise ValueError(
            "producer_id is required outside an activity; inside one it defaults "
            "to the activity's id and attempt"
        )
    info = temporalio.activity.info()
    return info.activity_id, attempt or info.attempt
