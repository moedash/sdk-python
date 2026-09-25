"""In-workflow consumption and publication of a server-side stream.

Ranges arrive on Workflow Tasks and only the offsets they covered are written to
History, so replay is served by the server reading the stream again. These tests
drive the buffering, the workflow-facing read and the per-task publish directly,
which is the part this SDK owns; the delivery decision itself lives in the
server and sdk-core.
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
    _MAX_STREAM_RECORD_BYTES,
    _MAX_STREAM_RECORDS_PER_BATCH,
    _StreamBuffer,
    _WorkflowInstanceImpl,
)
from temporalio.workflow import ReadOnlyContextError


def record(body: bytes, topic: str = "") -> api_stream.StreamRecord:
    return api_stream.StreamRecord(
        body=temporalio.api.common.v1.Payload(data=body), topic=topic
    )


def bodies(delivered: list[Any]) -> list[bytes]:
    return [item.record.body.data for item in delivered]


async def test_buffer_hands_over_in_order() -> None:
    buffer = _StreamBuffer()
    buffer.extend([record(b"one"), record(b"two")])

    assert bodies(buffer.take()) == [b"one", b"two"]
    assert len(buffer) == 0


# A reader that arrives before the data must not miss it, and one that arrives
# after must not block: the range is delivered once and never resent.
async def test_buffer_wakes_a_waiting_reader() -> None:
    buffer = _StreamBuffer()
    waiter = buffer.wait_future()
    assert not waiter.done()

    buffer.extend([record(b"late")])
    await asyncio.wait_for(waiter, timeout=1)
    assert bodies(buffer.take()) == [b"late"]


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
    buffer.extend([record(b"early")])

    # No waiting: the data is already there.
    assert len(buffer) == 1
    waiter = buffer.wait_future()
    assert not waiter.done(), "a fresh waiter is only resolved by new data"
    assert bodies(buffer.take()) == [b"early"]


class _ReadOnlyStub:
    """Enough of the workflow instance to drive the real read.

    The cap lives inside ``workflow_read_stream_records``, so a test that
    reimplements it proves nothing about the code that ships. Borrowing the
    method off the real class is what keeps the test on the shipped path.
    """

    workflow_read_stream_records = _WorkflowInstanceImpl.workflow_read_stream_records

    def __init__(self, read_only: bool = False) -> None:
        self._stream_buffers: dict[str, _StreamBuffer] = {}
        self._read_only = read_only

    def _assert_not_read_only(self, action: str) -> None:
        if self._read_only:
            raise ReadOnlyContextError(f"cannot {action} in a read-only context")

    async def read(self, stream: str, max_records: int) -> list[bytes]:
        instance: Any = self
        return bodies(
            await _WorkflowInstanceImpl.workflow_read_stream_records(
                instance, stream, max_records
            )
        )


@pytest.mark.parametrize("max_records", [1, 2, 5])
async def test_read_respects_a_cap_without_losing_the_tail(max_records: int) -> None:
    stub = _ReadOnlyStub()
    expected = [b"a", b"b", b"c"]
    stub._stream_buffers["s"] = _StreamBuffer()
    stub._stream_buffers["s"].extend([record(b) for b in expected])

    got = await stub.read("s", max_records)

    assert got == expected[:max_records]
    # Whatever the cap left behind has to still be there: nothing resends it.
    assert len(stub._stream_buffers["s"]) == max(0, len(expected) - max_records)

    if len(stub._stream_buffers["s"]):
        rest = await stub.read("s", 0)
        assert got + rest == expected
    else:
        assert got == expected


# A query activation carries no ranges, so a read there would wait on a future
# nothing can resolve and the query would time out saying nothing.
async def test_read_is_refused_in_a_read_only_context() -> None:
    stub = _ReadOnlyStub(read_only=True)
    stub._stream_buffers["s"] = _StreamBuffer()
    stub._stream_buffers["s"].extend([record(b"a")])

    with pytest.raises(ReadOnlyContextError, match="read stream"):
        await stub.read("s", 0)
    # Refused before the buffer was touched, so the range is still there for
    # the task that is allowed to read it.
    assert len(stub._stream_buffers["s"]) == 1


# A range is recorded as consumed once and never resent, so the buffer is the
# only place a repeated, skipped or mis-sized delivery can still be noticed.
async def test_ranges_have_to_abut_the_last_one() -> None:
    buffer = _StreamBuffer("s")
    buffer.extend([record(b"a"), record(b"b")], 0, 2)
    # An empty range moves the expectation too: the server recorded it.
    buffer.extend([], 2, 2)
    buffer.extend([record(b"c")], 2, 3)
    assert [item.offset for item in buffer.take()] == [0, 1, 2]

    with pytest.raises(RuntimeError, match=r"\[2, 3\).*ended at 3"):
        buffer.extend([record(b"c")], 2, 3)
    with pytest.raises(RuntimeError, match=r"\[5, 6\).*ended at 3"):
        buffer.extend([record(b"f")], 5, 6)


async def test_a_range_has_to_carry_as_many_records_as_it_spans() -> None:
    buffer = _StreamBuffer("s")
    with pytest.raises(RuntimeError, match=r"2 records for offsets \[0, 1\)"):
        buffer.extend([record(b"a"), record(b"b")], 0, 1)
    assert len(buffer) == 0


class _Completion:
    def __init__(self) -> None:
        self.commands: list[WorkflowCommand] = []


class _Successful:
    def __init__(self) -> None:
        self.successful = _Completion()


class _CommandStub:
    """Drives the real publish path and keeps the commands it issued.

    The completion's command list is a plain list here, which supports the
    same ``insert`` the protobuf container does.
    """

    workflow_append_stream_records = (
        _WorkflowInstanceImpl.workflow_append_stream_records
    )
    _flush_stream_appends = _WorkflowInstanceImpl._flush_stream_appends

    def __init__(self) -> None:
        self._stream_appends: dict[str, list[api_stream.StreamRecord]] = {}
        self._current_completion = _Successful()

    def _assert_not_read_only(self, _action: str) -> None:
        pass

    @property
    def commands(self) -> list[WorkflowCommand]:
        return self._current_completion.successful.commands

    def publish(self, *records: api_stream.StreamRecord, stream_id: str = "") -> None:
        instance: Any = self
        _WorkflowInstanceImpl.workflow_append_stream_records(
            instance, stream_id, list(records)
        )

    def flush(self) -> None:
        instance: Any = self
        _WorkflowInstanceImpl._flush_stream_appends(instance)


# A task's publishes on one stream become one command, whatever their number,
# because the event the command produces is what bounds a workflow's History.
def test_a_tasks_publishes_on_one_stream_become_one_command() -> None:
    stub = _CommandStub()
    stub.publish(record(b"a"), record(b"b"))
    stub.publish(record(b"c"))
    stub.publish(record(b"d"), stream_id="other")
    assert stub.commands == []

    stub.flush()

    by_stream = {
        command.append_stream_records.stream_id: [
            r.body.data for r in command.append_stream_records.records
        ]
        for command in stub.commands
    }
    assert by_stream == {"": [b"a", b"b", b"c"], "other": [b"d"]}
    # Flushed once: a second flush has nothing left to say.
    stub.flush()
    assert len(stub.commands) == 2


# The workflow is the producer of what it publishes, whatever the caller set.
def test_the_workflows_records_carry_no_producer() -> None:
    stub = _CommandStub()
    stub.publish(api_stream.StreamRecord(producer_id="someone", attempt=3))
    stub.flush()
    assert stub.commands[0].append_stream_records.records[0].producer_id == ""


# The server accepts nothing after a command that ends the run, so the
# publishes have to go ahead of it.
def test_publishes_are_flushed_ahead_of_the_completion_command() -> None:
    stub = _CommandStub()
    stub.publish(record(b"a"))
    done = WorkflowCommand()
    done.complete_workflow_execution.SetInParent()
    stub.commands.append(done)

    stub.flush()

    assert [c.WhichOneof("variant") for c in stub.commands] == [
        "append_stream_records",
        "complete_workflow_execution",
    ]


# The server refuses an oversized record, and a refused command is reissued on
# every replay, so the limit has to be applied before the record is buffered.
def test_a_record_over_the_server_limit_is_refused_before_the_command() -> None:
    stub = _CommandStub()
    stub.publish(record(b"x" * (_MAX_STREAM_RECORD_BYTES - 16)))
    with pytest.raises(ValueError, match=f"{_MAX_STREAM_RECORD_BYTES} bytes"):
        stub.publish(record(b"x" * (_MAX_STREAM_RECORD_BYTES + 1)))
    stub.flush()
    assert len(stub.commands) == 1


# A task that publishes more than one batch holds is split into commands the
# server accepts, by count and by bytes, rather than refused.
def test_a_task_over_the_batch_limits_is_split_into_commands() -> None:
    stub = _CommandStub()
    stub.publish(*(record(b"x") for _ in range(_MAX_STREAM_RECORDS_PER_BATCH + 1)))
    stub.flush()
    assert [len(c.append_stream_records.records) for c in stub.commands] == [
        _MAX_STREAM_RECORDS_PER_BATCH,
        1,
    ]

    stub = _CommandStub()
    # Two records that fit one batch together, then one whose framing alone
    # overflows the room they leave.
    big = record(b"x" * (_MAX_STREAM_RECORD_BYTES - 16))
    room = _MAX_STREAM_BATCH_BYTES - 2 * big.ByteSize()
    stub.publish(big, big, record(b"y" * room))
    stub.flush()
    assert [len(c.append_stream_records.records) for c in stub.commands] == [2, 1]


async def test_the_continuity_failure_says_what_to_do_about_it() -> None:
    buffer = _StreamBuffer("s")
    buffer.extend([record(b"one")], 0, 1)
    with pytest.raises(RuntimeError) as failed:
        buffer.extend([record(b"two")], 5, 6)
    # The task fails and keeps failing, so the message has to name the way out.
    assert "Reset the workflow" in str(failed.value)
