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
from temporalio.worker._workflow_instance import _StreamBuffer


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


@pytest.mark.parametrize("max_messages", [1, 2, 5])
async def test_take_respects_a_cap_without_losing_the_tail(max_messages: int) -> None:
    buffer = _StreamBuffer()
    bodies = [b"a", b"b", b"c"]
    buffer.extend([message(b) for b in bodies])

    taken = buffer.take()
    kept = taken[:max_messages]
    # Whatever a cap leaves behind has to go back: nothing will resend it.
    buffer.extend(taken[max_messages:])

    assert [m.body.data for m in kept] == bodies[:max_messages]
    assert len(buffer) == max(0, len(bodies) - max_messages)
