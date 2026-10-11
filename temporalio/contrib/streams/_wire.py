"""How a record crosses to Core and back: the proto is the record.

``temporal.sdk.streams.v1.StreamRecord`` is the stored format on every
store, and Core builds and parses it. ``body`` is the user's payload,
produced and consumed through the payload converter, so a pre-encoded
:class:`temporalio.common.RawValue` passes through untouched.
"""

from __future__ import annotations

from typing import Any

import temporalio.converter
from temporalio.bridge.proto.streams import ReadRecord
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.contrib.streams._body import decode_body
from temporalio.contrib.streams._errors import StreamRecordError
from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)

__all__ = ["RUN_ID_KEY", "WireRecord", "from_read", "to_wire"]

RUN_ID_KEY = "temporal.io/run-id"
"""The record metadata key the producing run id is stored under.

Core stamps it on every record the owning Workflow publishes, because a
stream follows the Workflow's run chain and a reader needs the run to tell a
reset branch or a successor run apart. Its value is a payload with
``encoding`` ``binary/plain`` whose data is the run id.
"""


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
    """The record as the converter produced it, before the codec.

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


async def from_read(
    converter: temporalio.converter.DataConverter,
    read: ReadRecord,
    result_type: type | None,
) -> StreamRecord[Any]:
    """The record a reader yields for one record of Core's answer.

    Raises:
        StreamRecordError: The body did not decode or convert, or the record
            holds a kind this SDK does not know.
    """
    cursor = Cursor(read.cursor)
    if read.HasField("superseded"):
        superseded = read.superseded
        return StreamRecord(
            kind=RecordKind.SUPERSEDED,
            cursor=cursor,
            topic=superseded.topic,
            producer_id=superseded.producer_id,
            attempt=superseded.attempt,
            supersession=Supersession(
                superseded.producer_id, superseded.previous_attempt, superseded.attempt
            ),
        )
    wire = read.stored
    try:
        kind = RecordKind(wire.kind)
        if kind is RecordKind.SUPERSEDED:
            raise ValueError("a SUPERSEDED record is synthesized, never stored")
        value: Any = None
        if kind in (RecordKind.DATA, RecordKind.UNSPECIFIED) and wire.HasField("body"):
            body = await decode_body(converter, wire.body)
            hints = [result_type] if result_type is not None else None
            value = converter.payload_converter.from_payloads([body], hints)[0]
    except Exception as error:
        raise StreamRecordError(
            f"stream record at {cursor} could not be decoded: {error}", cursor
        ) from error
    return StreamRecord(
        kind=RecordKind.DATA if kind is RecordKind.UNSPECIFIED else kind,
        cursor=cursor,
        topic=wire.topic,
        producer_id=wire.producer_id,
        attempt=wire.attempt,
        sequence=wire.sequence,
        run_id=wire.metadata[RUN_ID_KEY].data.decode()
        if RUN_ID_KEY in wire.metadata
        else "",
        value=value,
        stale=read.stale,
    )
