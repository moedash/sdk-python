"""What the native handles answer without a server.

A ref names the owner as the handle addresses it, a close on an owned stream
is refused, a standalone handle refuses a cursor from another stream, and a
create refuses a policy the server cannot hold before any call is made.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

import temporalio.converter
from temporalio.streams import DEFAULT_TOPIC, StreamCursorError, StreamRef, topic
from temporalio.streams.providers.native import (
    NativeActivityStreamHandle,
    NativeStandaloneStreamHandle,
    NativeStreamHandle,
    NativeStreams,
    _cursor,
)

OUT = topic("out", dict)
_CLIENT: Any = SimpleNamespace(
    data_converter=temporalio.converter.default(), namespace="ns"
)


def test_a_workflow_handle_refs_its_owner_as_it_is_pinned() -> None:
    following = NativeStreamHandle(_CLIENT, "wf", None)
    assert following.ref() == StreamRef.for_workflow("wf")
    assert following.ref(topic=OUT) == StreamRef.for_workflow("wf", topic="out")
    pinned = NativeStreamHandle(_CLIENT, "wf", "run-1")
    assert pinned.ref(topic="a") == StreamRef.for_workflow(
        "wf", run_id="run-1", topic="a"
    )


def test_an_activity_handle_refs_the_activity_and_its_workflow() -> None:
    scheduled = NativeActivityStreamHandle(_CLIENT, "act", "wf", "run-1")
    assert scheduled.ref() == StreamRef.for_activity(
        "act", workflow_id="wf", run_id="run-1"
    )
    standalone = NativeActivityStreamHandle(_CLIENT, "act", None, None)
    assert standalone.ref(topic=OUT) == StreamRef.for_activity("act", topic="out")
    assert standalone.ref().workflow_id is None


def test_a_standalone_handle_refs_its_id() -> None:
    handle = NativeStandaloneStreamHandle(_CLIENT, "s1")
    assert handle.stream_id == "s1"
    assert handle.ref() == StreamRef.for_standalone("s1")
    assert handle.ref().topic == DEFAULT_TOPIC
    assert handle.ref(topic=OUT) == StreamRef.for_standalone("s1", topic="out")


async def test_only_a_standalone_stream_can_be_closed() -> None:
    with pytest.raises(ValueError, match="standalone"):
        await NativeStreamHandle(_CLIENT, "wf", None).close()
    with pytest.raises(ValueError, match="standalone"):
        await NativeActivityStreamHandle(_CLIENT, "act", "wf", None).close()


def test_a_standalone_handle_refuses_another_streams_cursor() -> None:
    handle = NativeStandaloneStreamHandle(_CLIENT, "s1")
    with pytest.raises(StreamCursorError, match="another stream"):
        handle.read(topic=OUT, after=_cursor("s2", 3))
    with pytest.raises(StreamCursorError):
        handle.read(topic=OUT, after=_cursor("", 3))


def test_a_ref_opens_the_handle_it_names_on_the_provider() -> None:
    provider = NativeStreams()
    opened = provider.get_stream_handle(
        _CLIENT, StreamRef.for_standalone("s1", topic="out")
    )
    assert opened.ref() == StreamRef.for_standalone("s1", topic="out")
    assert opened.ref(topic="b") == StreamRef.for_standalone("s1", topic="b")
    workflow = provider.get_stream_handle(
        _CLIENT, StreamRef.for_workflow("wf", run_id="r")
    )
    assert workflow.ref() == StreamRef.for_workflow("wf", run_id="r")
    with pytest.raises(ValueError, match="run_id"):
        provider.get_stream_handle(_CLIENT, StreamRef.for_workflow("wf"), run_id="r")


async def test_a_create_refuses_a_policy_the_server_cannot_hold() -> None:
    provider = NativeStreams()
    for bad in (
        dict(max_records=0),
        dict(max_bytes=-1),
        dict(retention=timedelta(0)),
    ):
        with pytest.raises(ValueError):
            await provider.create_standalone_stream(_CLIENT, "s1", **bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await provider.create_standalone_stream(_CLIENT, "")
    with pytest.raises(ValueError):
        provider.get_standalone_stream_handle(_CLIENT, "")
