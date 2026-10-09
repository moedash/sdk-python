"""Typed topics and the stream reference that crosses process boundaries."""

from __future__ import annotations

import pytest

from temporalio.contrib.streams import (
    DEFAULT_TOPIC,
    StreamRef,
    StreamTopic,
    resolve_topic,
    topic,
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
        StreamRef("nexus", "x")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        StreamRef("workflow", "")
    with pytest.raises(ValueError):
        StreamRef("workflow", "wf", topic="")
