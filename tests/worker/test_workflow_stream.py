"""In-workflow consumption of a server-side stream.

Ranges arrive on Workflow Tasks and only the offsets they covered are written to
History, so replay is served by the server reading the stream again. These tests
drive the buffering and the workflow-facing read directly, which is the part
this SDK owns; the delivery decision itself lives in the server and sdk-core.
"""

from __future__ import annotations

import asyncio

import pytest

import temporalio.api.common.v1
import temporalio.api.stream.v1 as api_stream
from temporalio.worker._workflow_instance import (
    _StreamBuffer,
    _WorkflowInstanceImpl,
)


def message(body: bytes) -> api_stream.StreamMessage:
    return api_stream.StreamMessage(body=temporalio.api.common.v1.Payload(data=body))


async def test_buffer_hands_over_in_order() -> None:
    buffer = _StreamBuffer()
    buffer.extend([message(b"one"), message(b"two")])

    assert [m.body.data for m in buffer.take()] == [b"one", b"two"]
    assert len(buffer) == 0


# A reader that arrives before the data must not miss it, and one that arrives
# after must not block: the range is delivered once and never resent.
async def test_buffer_wakes_a_waiting_reader() -> None:
    buffer = _StreamBuffer()
    waiter = buffer.wait_future()
    assert not waiter.done()

    buffer.extend([message(b"late")])
    await asyncio.wait_for(waiter, timeout=1)
    assert [m.body.data for m in buffer.take()] == [b"late"]


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
    assert [m.body.data for m in buffer.take()] == [b"early"]


class _ReadOnlyStub:
    """Enough of the workflow instance to drive the real read.

    The cap lives inside ``workflow_read_stream``, so a test that reimplements
    it proves nothing about the code that ships.
    """

    def __init__(self) -> None:
        self._stream_buffers: dict[str, _StreamBuffer] = {}
        self.read_only_calls: list[str] = []

    def _assert_not_read_only(self, action: str) -> None:
        self.read_only_calls.append(action)


@pytest.mark.parametrize("max_messages", [1, 2, 5])
async def test_read_respects_a_cap_without_losing_the_tail(max_messages: int) -> None:
    stub = _ReadOnlyStub()
    bodies = [b"a", b"b", b"c"]
    stub._stream_buffers["s"] = _StreamBuffer()
    stub._stream_buffers["s"].extend([message(b) for b in bodies])

    got = await _WorkflowInstanceImpl.workflow_read_stream(stub, "s", max_messages)

    assert got == bodies[:max_messages]
    # Whatever the cap left behind has to still be there: nothing resends it.
    assert len(stub._stream_buffers["s"]) == max(0, len(bodies) - max_messages)

    if len(stub._stream_buffers["s"]):
        rest = await _WorkflowInstanceImpl.workflow_read_stream(stub, "s", 0)
        assert got + rest == bodies
    else:
        assert got == bodies


# A query activation carries no ranges, so a read there would wait on a future
# nothing can resolve and the query would time out saying nothing.
async def test_read_is_refused_in_a_read_only_context() -> None:
    stub = _ReadOnlyStub()
    stub._stream_buffers["s"] = _StreamBuffer()
    stub._stream_buffers["s"].extend([message(b"a")])

    await _WorkflowInstanceImpl.workflow_read_stream(stub, "s", 0)
    assert stub.read_only_calls == ["read stream"]
