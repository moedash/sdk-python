"""The stream record envelope keeps its stored format."""

from __future__ import annotations

from temporalio.api.common.v1 import Payload
from temporalio.contrib.streams.proto.v1 import StreamRecord, StreamRecordKind


def test_the_envelope_keeps_its_field_numbers():
    # The bytes are the stored format; a renumbered field would make every
    # retained record unreadable.
    fields = {f.name: f.number for f in StreamRecord.DESCRIPTOR.fields}
    assert fields == {
        "body": 1,
        "metadata": 2,
        "topic": 3,
        "kind": 4,
        "producer_id": 5,
        "attempt": 6,
        "sequence": 7,
    }
    assert StreamRecord.DESCRIPTOR.full_name == "temporal.sdk.streams.v1.StreamRecord"
    kinds = StreamRecord.DESCRIPTOR.fields_by_name["kind"].enum_type
    assert kinds is not None
    assert {v.name: v.number for v in kinds.values} == {
        "STREAM_RECORD_KIND_UNSPECIFIED": 0,
        "STREAM_RECORD_KIND_DATA": 1,
        "STREAM_RECORD_KIND_FINISH": 2,
    }


def test_the_envelope_round_trips_through_bytes():
    record = StreamRecord(
        body=Payload(metadata={"encoding": b"json/plain"}, data=b"1"),
        metadata={"k": Payload(data=b"v")},
        topic="out",
        kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH,
        producer_id="p",
        attempt=2,
        sequence=3,
    )
    assert StreamRecord.FromString(record.SerializeToString()) == record
