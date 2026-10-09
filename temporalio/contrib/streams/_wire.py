"""How a record crosses a provider: the proto is the record.

``temporal.sdk.streams.v1.StreamRecord`` is the wire format on every
provider. A store that keeps bytes keeps ``SerializeToString()`` of it, and a
reader in any language parses the same bytes. ``body`` is the user's payload,
produced and consumed through the payload converter, so a pre-encoded
:class:`temporalio.common.RawValue` passes through untouched.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.contrib.streams._policy import AttemptTracker
from temporalio.contrib.streams._record import Cursor, RecordKind, StreamRecord
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.contrib.streams.proto.v1 import StreamRecordKind

__all__ = ["RUN_ID_KEY", "RecordDecoder", "WireRecord", "from_wire", "to_wire"]

RUN_ID_KEY = "temporal.io/run-id"
"""The record metadata key the producing run id is stored under.

Every record the owning Workflow publishes carries it, because a stream
follows the Workflow's run chain and a reader needs the run to tell a reset
branch or a successor run apart. Its value is a payload with ``encoding``
``binary/plain`` whose data is the run id.
"""

_RUN_ID_ENCODING = b"binary/plain"


def to_wire(
    converter: temporalio.converter.PayloadConverter,
    *,
    topic: str,
    kind: RecordKind,
    value: Any = None,
    producer_id: str = "",
    attempt: int = 0,
    sequence: int = 0,
    run_id: str = "",
) -> WireRecord:
    """Build the record a provider stores.

    Only a ``DATA`` record carries a body; the converter encodes ``value``
    into it. A ``run_id`` goes into the metadata under :data:`RUN_ID_KEY`.

    Raises:
        ValueError: ``kind`` is ``SUPERSEDED`` or ``UNSPECIFIED``, which no
            writer stores.
    """
    if kind not in (RecordKind.DATA, RecordKind.FINISH):
        raise ValueError(f"a writer stores DATA or FINISH records, not {kind.name}")
    record = WireRecord(
        topic=topic,
        kind=StreamRecordKind.ValueType(int(kind)),
        producer_id=producer_id,
        attempt=attempt,
        sequence=sequence,
    )
    if kind is RecordKind.DATA:
        record.body.CopyFrom(converter.to_payloads([value])[0])
    if run_id:
        record.metadata[RUN_ID_KEY].CopyFrom(
            Payload(metadata={"encoding": _RUN_ID_ENCODING}, data=run_id.encode())
        )
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


class RecordDecoder:
    """Turns stored records into the records a reader yields.

    One per read. It synthesizes ``SUPERSEDED`` from the attempts it
    observes, positions each synthesized record at the cursor before the
    record that triggered it, and skips a record it cannot decode with a
    warning rather than raising, so one bad record cannot stop every reader
    of the stream.
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

    def decode(self, cursor: Cursor, wire: WireRecord) -> list[StreamRecord[Any]]:
        """The records to yield for one stored record, in order."""
        try:
            record = from_wire(self._converter, cursor, wire, self._result_type)
        except Exception as error:
            self._warn(f"skipping stream record at {cursor}: {error}")
            # The skipped record keeps its position, so a resume after it
            # moves on rather than tripping over it again.
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
