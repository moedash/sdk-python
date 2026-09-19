"""In-workflow consumption of a server-side stream.

Ranges arrive on Workflow Tasks and only the offsets they covered are written to
History, so replay is served by the server reading the stream again. These tests
drive the buffering and the workflow-facing read directly, which is the part
this SDK owns; the delivery decision itself lives in the server and sdk-core.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.api.stream.v1 as api_stream
from temporalio.bridge.proto.workflow_commands import WorkflowCommand
from temporalio.worker._workflow_instance import (
    _MAX_STREAM_BATCH_BYTES,
    _MAX_STREAM_MESSAGE_BYTES,
    _StreamBuffer,
    _WorkflowInstanceImpl,
)
from temporalio.workflow import ReadOnlyContextError


def message(body: bytes) -> api_stream.StreamMessage:
    return api_stream.StreamMessage(body=temporalio.api.common.v1.Payload(data=body))


async def test_buffer_hands_over_in_order() -> None:
    buffer = _StreamBuffer()
    buffer.extend([message(b"one"), message(b"two")])

    assert [m.body for m in buffer.take()] == [b"one", b"two"]
    assert len(buffer) == 0


# A reader that arrives before the data must not miss it, and one that arrives
# after must not block: the range is delivered once and never resent.
async def test_buffer_wakes_a_waiting_reader() -> None:
    buffer = _StreamBuffer()
    waiter = buffer.wait_future()
    assert not waiter.done()

    buffer.extend([message(b"late")])
    await asyncio.wait_for(waiter, timeout=1)
    assert [m.body for m in buffer.take()] == [b"late"]


# An empty range is still a delivery the server recorded, but there is nothing
# to hand a reader, so it must not wake one into returning nothing.
async def test_empty_range_does_not_wake_a_reader() -> None:
    buffer = _StreamBuffer()
    waiter = buffer.wait_future()

    buffer.extend([])

    assert not waiter.done()
    assert len(buffer) == 0


async def test_buffer_keeps_data_delivered_before_anyone_reads() -> None:
    buffer = _StreamBuffer()
    buffer.extend([message(b"early")])

    # No waiting: the data is already there.
    assert len(buffer) == 1
    waiter = buffer.wait_future()
    assert not waiter.done(), "a fresh waiter is only resolved by new data"
    assert [m.body for m in buffer.take()] == [b"early"]


class _ReadOnlyStub:
    """Enough of the workflow instance to drive the real read.

    The cap lives inside ``workflow_read_stream_messages``, so a test that
    reimplements it proves nothing about the code that ships. Borrowing the
    method off the real class is what keeps the test on the shipped path.
    """

    workflow_read_stream_messages = _WorkflowInstanceImpl.workflow_read_stream_messages

    def __init__(self, read_only: bool = False) -> None:
        self._stream_buffers: dict[str, _StreamBuffer] = {}
        self._read_only = read_only

    def _assert_not_read_only(self, action: str) -> None:
        if self._read_only:
            raise ReadOnlyContextError(f"cannot {action} in a read-only context")

    async def read(self, stream: str, max_messages: int) -> list[bytes]:
        instance: Any = self
        return await _WorkflowInstanceImpl.workflow_read_stream(
            instance, stream, max_messages
        )


@pytest.mark.parametrize("max_messages", [1, 2, 5])
async def test_read_respects_a_cap_without_losing_the_tail(max_messages: int) -> None:
    stub = _ReadOnlyStub()
    bodies = [b"a", b"b", b"c"]
    stub._stream_buffers["s"] = _StreamBuffer()
    stub._stream_buffers["s"].extend([message(b) for b in bodies])

    got = await stub.read("s", max_messages)

    assert got == bodies[:max_messages]
    # Whatever the cap left behind has to still be there: nothing resends it.
    assert len(stub._stream_buffers["s"]) == max(0, len(bodies) - max_messages)

    if len(stub._stream_buffers["s"]):
        rest = await stub.read("s", 0)
        assert got + rest == bodies
    else:
        assert got == bodies


# A query activation carries no ranges, so a read there would wait on a future
# nothing can resolve and the query would time out saying nothing.
async def test_read_is_refused_in_a_read_only_context() -> None:
    stub = _ReadOnlyStub(read_only=True)
    stub._stream_buffers["s"] = _StreamBuffer()
    stub._stream_buffers["s"].extend([message(b"a")])

    with pytest.raises(ReadOnlyContextError, match="read stream"):
        await stub.read("s", 0)
    # Refused before the buffer was touched, so the range is still there for
    # the task that is allowed to read it.
    assert len(stub._stream_buffers["s"]) == 1


# A range is recorded as consumed once and never resent, so the buffer is the
# only place a repeated, skipped or mis-sized delivery can still be noticed.
async def test_ranges_have_to_abut_the_last_one() -> None:
    buffer = _StreamBuffer("s")
    buffer.extend([message(b"a"), message(b"b")], 0, 2)
    # An empty range moves the expectation too: the server recorded it.
    buffer.extend([], 2, 2)
    buffer.extend([message(b"c")], 2, 3)
    assert [m.offset for m in buffer.take()] == [0, 1, 2]

    with pytest.raises(RuntimeError, match=r"\[2, 3\).*ended at 3"):
        buffer.extend([message(b"c")], 2, 3)
    with pytest.raises(RuntimeError, match=r"\[5, 6\).*ended at 3"):
        buffer.extend([message(b"f")], 5, 6)


async def test_a_range_has_to_carry_as_many_messages_as_it_spans() -> None:
    buffer = _StreamBuffer("s")
    with pytest.raises(RuntimeError, match=r"2 messages for offsets \[0, 1\)"):
        buffer.extend([message(b"a"), message(b"b")], 0, 1)
    assert len(buffer) == 0


class _CommandStub:
    """Drives the real publish path and keeps the commands it issued."""

    workflow_add_stream_messages = _WorkflowInstanceImpl.workflow_add_stream_messages

    def __init__(self) -> None:
        self.commands: list[WorkflowCommand] = []

    def _add_command(self) -> WorkflowCommand:
        command = WorkflowCommand()
        self.commands.append(command)
        return command

    def publish(self, *bodies: bytes) -> None:
        instance: Any = self
        _WorkflowInstanceImpl.workflow_add_stream_messages(instance, "", bodies, "")


# The server refuses an oversized publish, and a refused command is reissued
# on every replay, so the limits have to be applied before the command exists.
def test_a_message_over_the_server_limit_is_refused_before_the_command() -> None:
    stub = _CommandStub()
    stub.publish(b"x" * _MAX_STREAM_MESSAGE_BYTES)
    assert len(stub.commands) == 1

    with pytest.raises(ValueError, match=f"{_MAX_STREAM_MESSAGE_BYTES} bytes"):
        stub.publish(b"x" * (_MAX_STREAM_MESSAGE_BYTES + 1))
    assert len(stub.commands) == 1


def test_a_batch_over_the_server_limit_is_refused_before_the_command() -> None:
    stub = _CommandStub()
    half = b"x" * (_MAX_STREAM_BATCH_BYTES // 2)
    stub.publish(half, half)
    assert len(stub.commands) == 1

    with pytest.raises(ValueError, match=f"{_MAX_STREAM_BATCH_BYTES} bytes"):
        stub.publish(half, half, b"x")
    assert len(stub.commands) == 1
