"""How a record crosses between the public shape and the stored one.

No server: what this pins is that the two converters carry every field the
record declares, so a field added to ``StreamRecord`` is not dropped in
silence on the way out or the way back.
"""

from __future__ import annotations

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import StreamRecord, StreamRecordKind
from temporalio.client_stream import _copy_by_name, _to_public, _to_service


def test_a_record_crosses_both_ways_without_dropping_a_field() -> None:
    # Copied by descriptor rather than field by field, so a field added to
    # StreamRecord crosses without anybody adding a line to the converters.
    sent = StreamRecord(
        topic="t",
        kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH,
        producer_id="model",
        attempt=3,
        sequence=7,
    )
    sent.body.CopyFrom(Payload(data=b"x", metadata={"encoding": b"binary/plain"}))
    sent.metadata["trace"].CopyFrom(Payload(data=b"abc"))

    entry = _to_public(_to_service(sent))
    assert entry.record == sent
    assert {f.name for f, _ in sent.ListFields()} == {
        f.name for f, _ in entry.record.ListFields()
    }
    # Every field the public record declares is carried, not only the ones
    # the converters happened to name.
    crossed = {f.name for f, _ in _to_service(sent).ListFields()} - {"offset"}
    assert crossed == {f.name for f in StreamRecord.DESCRIPTOR.fields}


def test_a_field_with_no_counterpart_is_refused_rather_than_dropped() -> None:
    # The stored shape carries an offset the public record has no room for.
    # Copying it across has to say so rather than leave it behind, because
    # the same silence is what loses a field somebody adds later.
    stored = _to_service(StreamRecord(topic="t"))
    stored.offset = 12
    with pytest.raises(ValueError, match="offset"):
        _copy_by_name(stored, StreamRecord(), skip=frozenset())
    # Named explicitly, the offset is the store's and rides on the entry.
    assert _to_public(stored).offset == 12
