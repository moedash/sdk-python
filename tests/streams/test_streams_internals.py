"""Unit tests for the pieces under ``temporalio.streams`` that no provider owns.

The wire format, the supersession policy, the store key, the cursor prefix and
the plugin registration are shared by every provider and implemented once, so
they are tested once, here, against the private modules. What a provider owes
is in ``test_streams_conformance``; keeping the two apart is what makes that
file answerable by a new provider.
"""

from __future__ import annotations

import pytest

from temporalio.client import ClientConfig
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
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.worker import ReplayerConfig, WorkerConfig


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


def test_an_attempt_that_goes_backwards_is_said_rather_than_passed_off():
    # Attempts only rise on one producer, so a lower one means the store handed
    # two generations back out of order. Yielded as data with no signal, a
    # consumer renders the stale generation as the current answer.
    said: list[str] = []
    attempts = AttemptTracker(said.append)
    assert attempts.note("model", 2, topic="t", previous=BEGINNING) is None
    assert attempts.note("model", 1, topic="t", previous=Cursor("memory:3")) is None
    assert len(said) == 1
    assert "attempt 1" in said[0] and "behind attempt 2" in said[0]
    assert "model" in said[0]


def test_a_repeat_of_the_current_attempt_is_not_worth_saying():
    said: list[str] = []
    attempts = AttemptTracker(said.append)
    attempts.note("model", 1, topic="t", previous=BEGINNING)
    assert attempts.note("model", 1, topic="t", previous=Cursor("memory:1")) is None
    assert said == []


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


def test_registering_a_provider_twice_is_refused():
    # There is one slot on each of the three, and a user who passes a provider
    # by hand and a provider plugin, or two provider plugins, meant both.
    first, second = MemoryStreams(), MemoryStreams()
    with pytest.raises(ValueError, match="already registered"):
        second.configure_client(ClientConfig(stream_provider=first))  # type: ignore[typeddict-item]
    with pytest.raises(ValueError, match="already registered"):
        second.configure_worker(WorkerConfig(stream_provider=first))  # type: ignore[typeddict-item]
    with pytest.raises(ValueError, match="already registered"):
        second.configure_replayer(ReplayerConfig(stream_provider=first))  # type: ignore[typeddict-item]


def test_registering_the_same_provider_twice_is_fine():
    # A worker built from a client that already carries the plugin configures
    # it again with the same object, which is not a conflict.
    provider = MemoryStreams()
    config = provider.configure_client(ClientConfig(stream_provider=provider))  # type: ignore[typeddict-item]
    assert config.get("stream_provider") is provider
    assert provider.configure_client(ClientConfig()).get("stream_provider") is provider  # type: ignore[typeddict-item]
