"""Unit tests for the pieces under ``temporalio.streams`` that no provider owns.

The wire format, the supersession policy, the store key and the cursor prefix
are shared by every provider and implemented once, so they are tested once,
here, against the private modules. What a provider owes
is in ``test_streams_conformance``; keeping the two apart is what makes that
file answerable by a new provider.
"""

from __future__ import annotations

import pytest

from temporalio.converter import DataConverter
from temporalio.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamCursorError,
    Supersession,
    _ids,
    _wire,
)
from temporalio.streams._policy import AttemptTracker


def test_record_roundtrips_through_the_wire():
    converter = DataConverter.default.payload_converter
    wire = _wire.to_wire(
        converter,
        topic="decisions",
        kind=RecordKind.DATA,
        value={"n": 1},
        producer_id="model",
        attempt=3,
        sequence=7,
    )
    parsed = _wire.WireRecord.FromString(wire.SerializeToString())
    record = _wire.from_wire(converter, Cursor("memory:0"), parsed, dict)
    assert (
        record.kind,
        record.topic,
        record.producer_id,
        record.attempt,
        record.sequence,
        record.value,
    ) == (RecordKind.DATA, "decisions", "model", 3, 7, {"n": 1})
    assert record.supersession is None
    finish = _wire.to_wire(converter, topic="decisions", kind=RecordKind.FINISH)
    assert not finish.HasField("body")
    assert _wire.from_wire(converter, Cursor("memory:1"), finish, dict).value is None


def test_a_stored_supersession_is_not_a_record():
    converter = DataConverter.default.payload_converter
    wire = _wire.WireRecord(topic="t", kind=int(RecordKind.SUPERSEDED))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="synthesized"):
        _wire.from_wire(converter, Cursor("memory:0"), wire, None)


def test_an_unset_kind_is_read_as_data():
    converter = DataConverter.default.payload_converter
    wire = _wire.WireRecord(topic="t", body=converter.to_payloads([{"n": 1}])[0])
    record = _wire.from_wire(converter, Cursor("memory:0"), wire, dict)
    assert record.kind is RecordKind.DATA
    assert record.value == {"n": 1}


def test_supersession_is_synthesized_from_observations():
    attempts = AttemptTracker()
    assert attempts.note("model", 1, topic="t", previous=BEGINNING) is None
    superseded = attempts.note("model", 2, topic="t", previous=Cursor("memory:0"))
    assert superseded is not None
    assert superseded.kind is RecordKind.SUPERSEDED
    assert superseded.supersession == Supersession("model", 1, 2)
    assert superseded.value is None
    # Positioned before the triggering record, so a resume after it delivers
    # that record next.
    assert superseded.cursor == Cursor("memory:0")
    # The same attempt again is not a new generation.
    assert attempts.note("model", 2, topic="t", previous=Cursor("memory:1")) is None


def test_topic_keys_cannot_collide():
    # A colon in a workflow id must not make two addresses one key.
    assert _ids.topic_key("a:b", "c") != _ids.topic_key("a", "b:c")
    assert _ids.topic_key("a%3Ab", "c") != _ids.topic_key("a:b", "c")
    assert _ids.topic_key("wf", "inputs") == "wf:inputs"


def test_cursors_name_their_provider():
    assert _wire.cursor_position(BEGINNING, provider="memory") is None
    assert _wire.cursor_position(Cursor("memory:42"), provider="memory") == "42"
    with pytest.raises(StreamCursorError):
        _wire.cursor_position(Cursor("redis:1700000000000-0"), provider="memory")
