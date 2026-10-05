"""The reader's rules that a workflow test cannot pin down.

``stream_reader`` promises one subscription per topic per run, that a second
loop on one reader shares its buffer rather than racing the source, and that
closing it and opening the topic again is a new subscription. The memory
provider's source hands back everything it has without ever suspending, so a
workflow test cannot tell a reader that serialises its fetches from one that
does not. These drive the reader directly, with a source that suspends where
a real provider would, and a stand-in runtime on the loop.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from temporalio import workflow
from temporalio.converter import DataConverter
from temporalio.streams import BEGINNING, Cursor, RecordKind, topic
from temporalio.streams._wire import WireRecord, to_wire
from temporalio.workflow._streams import _WorkflowStreams

INPUTS = topic("inputs", dict)

_CONVERTER = DataConverter.default.payload_converter


def _record(n: int) -> WireRecord:
    return to_wire(
        _CONVERTER,
        topic=INPUTS.name,
        kind=RecordKind.DATA,
        value={"n": n},
        producer_id="model",
        attempt=1,
        sequence=n,
    )


class _SlowSource:
    """Hands over one record per batch, suspending first the way a real one does."""

    def __init__(self, count: int) -> None:
        self.opened = True
        self.next_calls = 0
        self.in_flight = 0
        self.overlapped = False
        self._offset = 0
        self._count = count

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        self.next_calls += 1
        self.in_flight += 1
        if self.in_flight > 1:
            self.overlapped = True
        try:
            # Where a provider waits for the worker to deliver. Two fetches
            # that both get here have both left the reader's buffer behind.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            if self._offset >= self._count:
                raise StopAsyncIteration
            self._offset += 1
            return [(Cursor(f"fake:{self._offset - 1}"), _record(self._offset - 1))]
        finally:
            self.in_flight -= 1

    def close(self) -> None:
        self.opened = False


class _Half:
    """A workflow half that hands out one source per topic and counts opens."""

    def __init__(self) -> None:
        self.sources: list[_SlowSource] = []

    def open_reader(self, topic: str, *, after: Cursor) -> Any:
        del topic, after
        source = _SlowSource(4)
        self.sources.append(source)
        return source

    def open_writer(self, topic: str) -> Any:
        del topic
        raise NotImplementedError

    def on_workflow_start(self) -> None:
        pass

    async def on_workflow_finish(self) -> None:
        pass


class _FakeRuntime:
    """Only what the reader reads: the stream state and the payload converter."""

    def __init__(self, half: _Half) -> None:
        self._streams = _WorkflowStreams(half)  # type: ignore[arg-type]

    def workflow_streams(self) -> _WorkflowStreams:
        return self._streams

    def workflow_payload_converter(self) -> Any:
        return _CONVERTER


@pytest.fixture
async def half() -> Any:
    fake = _Half()
    loop = asyncio.get_running_loop()
    workflow._Runtime.set_on_loop(loop, _FakeRuntime(fake))  # type: ignore[arg-type]
    yield fake
    workflow._Runtime.set_on_loop(loop, None)


async def test_two_loops_on_one_reader_split_the_records(half: _Half):
    reader = workflow.stream_reader(INPUTS)
    seen: list[int] = []

    async def pull(count: int) -> None:
        for _ in range(count):
            record = await reader.__anext__()
            assert record.value is not None
            seen.append(record.value["n"])

    await asyncio.wait_for(asyncio.gather(pull(2), pull(2)), 5.0)
    # Every record once and none lost, whichever loop got there first, and
    # the source was never asked for two batches at the same time.
    assert sorted(seen) == [0, 1, 2, 3]
    assert half.sources[0].overlapped is False


@pytest.mark.usefixtures("half")
async def test_a_cancelled_read_does_not_lose_the_record_it_waited_for():
    reader = workflow.stream_reader(INPUTS)
    pending = asyncio.ensure_future(reader.__anext__())
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    # The next read picks up where the cancelled one was, rather than finding
    # the reader wedged behind a lock the cancelled fetch still holds.
    record = await asyncio.wait_for(reader.__anext__(), 5.0)
    assert record.value == {"n": 0}


async def test_closing_a_reader_lets_the_topic_be_opened_again(half: _Half):
    reader = workflow.stream_reader(INPUTS)
    assert workflow.stream_reader(INPUTS) is reader
    reader.close()
    assert half.sources[0].opened is False

    # A new subscription, not the closed one handed back. It is also a new
    # command on a real provider, which is why the docstring says to gate it.
    again = workflow.stream_reader(INPUTS)
    assert again is not reader
    assert len(half.sources) == 2
    assert (await asyncio.wait_for(again.__anext__(), 5.0)).value == {"n": 0}


@pytest.mark.usefixtures("half")
async def test_a_closed_reader_stops_iterating():
    reader = workflow.stream_reader(INPUTS)
    assert (await asyncio.wait_for(reader.__anext__(), 5.0)).value == {"n": 0}
    reader.close()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(reader.__anext__(), 5.0)


@pytest.mark.usefixtures("half")
async def test_a_reader_ends_when_the_source_ends():
    reader = workflow.stream_reader(INPUTS, after=BEGINNING)
    values = [record.value async for record in reader]
    assert values == [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}]
