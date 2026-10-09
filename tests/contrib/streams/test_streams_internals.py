"""Unit tests for the shared pieces no provider implements.

Topics, refs, cursors, the body helpers, the wire conversion and the
supersession tracker. None of these needs a server.
"""

from __future__ import annotations

from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord


def test_the_envelope_keeps_its_field_numbers():
    # The bytes are the stored format; a renumbered field would make every
    # retained record unreadable.
    fields = {f.name: f.number for f in WireRecord.DESCRIPTOR.fields}
    assert fields == {
        "body": 1,
        "metadata": 2,
        "topic": 3,
        "kind": 4,
        "producer_id": 5,
        "attempt": 6,
        "sequence": 7,
    }
    assert WireRecord.DESCRIPTOR.full_name == "temporal.sdk.streams.v1.StreamRecord"
    kinds = WireRecord.DESCRIPTOR.fields_by_name["kind"].enum_type
    assert kinds is not None
    assert {v.name: v.number for v in kinds.values} == {
        "STREAM_RECORD_KIND_UNSPECIFIED": 0,
        "STREAM_RECORD_KIND_DATA": 1,
        "STREAM_RECORD_KIND_FINISH": 2,
    }
