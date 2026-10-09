"""The error family and cursors bound to one provider and one stream."""

from __future__ import annotations

import pytest

from temporalio.contrib.streams import (
    BEGINNING,
    END,
    Cursor,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._cursor import (
    cursor_position,
    mint_cursor,
    stream_hash,
)


def test_every_condition_is_a_stream_error():
    for error in (
        StreamClosedError,
        StreamCursorError,
        StreamExpiredError,
        StreamNotFoundError,
        StreamOutcomeUnknownError,
        StreamProducerError,
        StreamUnsupportedError,
    ):
        assert issubclass(error, StreamError)
    # Expired is a kind of invalid cursor, so a caller that only resets on a
    # bad cursor still catches it, and one that cares can tell them apart.
    assert issubclass(StreamExpiredError, StreamCursorError)
    # Refused and unknown are different answers to a retrying caller.
    assert not issubclass(StreamOutcomeUnknownError, StreamProducerError)
    assert not issubclass(StreamProducerError, StreamOutcomeUnknownError)


def test_a_cursor_is_bound_to_its_provider_and_stream():
    one = stream_hash("ns", "workflow", "wf", "a")
    cursor = mint_cursor("memory", one, "7")
    assert cursor_position(cursor, provider="memory", stream=one) == "7"
    assert cursor_position(BEGINNING, provider="memory", stream=one) is None

    with pytest.raises(StreamCursorError, match="minted by the 'memory' provider"):
        cursor_position(cursor, provider="redis", stream=one)
    for other in (
        stream_hash("ns", "workflow", "wf", "b"),
        stream_hash("ns", "workflow", "wf2", "a"),
        stream_hash("ns2", "workflow", "wf", "a"),
        stream_hash("ns", "activity", "wf", "a"),
    ):
        with pytest.raises(StreamCursorError, match="another stream"):
            cursor_position(cursor, provider="memory", stream=other)
    for token in ("garbage", "memory:", f"memory:{one}:"):
        with pytest.raises(StreamCursorError):
            cursor_position(Cursor(token), provider="memory", stream=one)
    with pytest.raises(StreamCursorError, match="END"):
        cursor_position(END, provider="memory", stream=one)


def test_the_stream_hash_is_short_and_length_delimited():
    assert len(stream_hash("ns", "workflow", "wf", "a")) == 8
    assert stream_hash("ns", "workflow", "a:b", "c") != stream_hash(
        "ns", "workflow", "a", "b:c"
    )
