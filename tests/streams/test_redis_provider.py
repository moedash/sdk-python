"""What the Redis provider decides without a store: cursors and the sync publish."""

from __future__ import annotations

import asyncio

import pytest

from temporalio.streams import BEGINNING, Cursor, StreamCursorError, StreamError
from temporalio.streams.providers.redis import (
    _drive,
    _outside_position,
    _workflow_position,
)


def test_outside_cursors_name_output_positions():
    assert _outside_position(BEGINNING) is None
    position = _outside_position(Cursor("redis:1700000000000-3"))
    assert position is not None and position.token == "1700000000000-3"
    # A workflow-side cursor names the input log, which outside code cannot
    # read from, and a token another provider minted is refused the same way.
    with pytest.raises(StreamCursorError):
        _outside_position(Cursor("redis:in:1700000000000-3"))
    with pytest.raises(StreamCursorError):
        _outside_position(Cursor("memory:3"))
    with pytest.raises(StreamCursorError):
        _outside_position(Cursor("redis:not-an-id"))


def test_a_publish_that_completes_at_once_is_driven_to_the_end():
    done = []

    async def publish() -> None:
        done.append(True)

    _drive(publish())
    assert done == [True]


def test_a_publish_that_would_wait_fails_loudly():
    async def publish() -> None:
        await asyncio.get_running_loop().create_future()

    async def run() -> None:
        with pytest.raises(StreamError, match="batch is full"):
            _drive(publish())

    asyncio.run(run())


def test_workflow_cursors_name_input_positions():
    assert _workflow_position(BEGINNING) is None
    position = _workflow_position(Cursor("redis:in:1700000000000-3"))
    assert position is not None and position.token == "1700000000000-3"
    # An outside cursor names the output stream, whose entry ids are not the
    # input stream's, so it cannot seed a workflow reader.
    with pytest.raises(StreamCursorError, match="output stream"):
        _workflow_position(Cursor("redis:1700000000000-3"))
    with pytest.raises(StreamCursorError):
        _workflow_position(Cursor("memory:3"))
    with pytest.raises(StreamCursorError):
        _workflow_position(Cursor("redis:in:not-an-id"))
