"""Typed topics and the stream reference that crosses process boundaries."""

from __future__ import annotations

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.contrib.streams import (
    DEFAULT_TOPIC,
    StreamRef,
    StreamTopic,
    topic,
)
from temporalio.contrib.streams._topic import (
    resolve_topic,
)
from temporalio.converter import DataConverter

OUT = topic("out", dict)


def test_a_definition_carries_its_name_and_type():
    assert OUT == StreamTopic("out", dict)
    assert resolve_topic(OUT) == ("out", dict)
    assert resolve_topic("free", int) == ("free", int)
    assert resolve_topic(None) == (DEFAULT_TOPIC, None)


def test_topic_mistakes_are_value_errors():
    with pytest.raises(ValueError):
        topic("")
    with pytest.raises(ValueError):
        resolve_topic("")
    with pytest.raises(ValueError, match="already carries its type"):
        resolve_topic(OUT, dict)


def test_a_ref_names_a_workflow_stream():
    ref = StreamRef.for_workflow("wf", topic=OUT)
    assert ref == StreamRef("workflow", "wf", topic="out")
    assert StreamRef.for_workflow("wf").topic == DEFAULT_TOPIC
    assert ref.with_topic(None).topic == DEFAULT_TOPIC
    assert ref.with_topic("other").workflow_id == "wf"


def test_a_ref_round_trips_through_the_default_converter():
    ref = StreamRef.for_workflow("wf", run_id="run", topic=OUT)
    converter = DataConverter.default.payload_converter
    payload = converter.to_payloads([ref])[0]
    assert converter.from_payloads([payload], [StreamRef])[0] == ref


def test_a_ref_refuses_what_it_cannot_name():
    with pytest.raises(ValueError, match="unknown"):
        StreamRef("nexus", "x")
    with pytest.raises(ValueError):
        StreamRef("workflow", "")
    with pytest.raises(ValueError):
        StreamRef("workflow", "wf", topic="")


def test_a_ref_from_a_later_release_decodes_to_the_kind_check():
    # The kind is a plain string, so the converter builds the ref and the ref
    # itself refuses the kind, rather than the converter failing a type check.
    converter = DataConverter.default.payload_converter
    payload = Payload(
        metadata={"encoding": b"json/plain"},
        data=b'{"kind": "activity", "workflow_id": "wf", "topic": "out"}',
    )
    with pytest.raises(ValueError, match="'activity'"):
        converter.from_payloads([payload], [StreamRef])


@pytest.mark.parametrize(
    "name",
    ["x" * 257, "é" * 129, "a\x1fb", "line\nbreak", "\x00"],
)
def test_a_topic_name_that_is_too_long_or_has_a_control_character_is_refused(name: str):
    # Every provider puts the name in keys, cursors and Core's manifest budget.
    with pytest.raises(ValueError):
        topic(name)
    with pytest.raises(ValueError):
        resolve_topic(name)


def test_a_topic_name_of_256_bytes_is_accepted():
    assert topic("x" * 256).name == "x" * 256
    assert resolve_topic("é" * 128)[0] == "é" * 128
