"""The native provider inside a real workflow, against a server that has streams.

The two rules the memory provider cannot keep are the measurement here: a
publish commits with its Workflow Task and never lands if the task fails, and
a read is a recorded observation the server re-supplies on replay. Needs a
Temporal server built from the AI-198 branch, because neither the stream
service nor the commands exist on a released one:

    TEMPORAL_STREAM_TARGET=127.0.0.1:7333 uv run pytest tests/worker/test_workflow_stream_e2e.py

Skipped otherwise, rather than passing against a server that has no idea what a
stream is.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.api.stream.v1 import StreamRecord
from temporalio.client import Client
from temporalio.client_stream import StreamClient
from temporalio.streams import RecordKind
from temporalio.streams.providers.native import NativeStreams
from temporalio.worker import Worker
from tests.streams.test_streams_conformance import take

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream commands",
)

INPUTS = "inputs"
DECISIONS = "decisions"

EVENT_STREAM_SUBSCRIBED = EventType.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED
EVENT_STREAM_RECORDS_APPENDED = EventType.EVENT_TYPE_WORKFLOW_STREAM_RECORDS_APPENDED


async def _connect(provider: NativeStreams | None = None) -> Client:
    # Registered once, on the client: the worker inherits it and the tests
    # open handles through client.get_stream_handle. The contrib tests below
    # need no provider.
    return await Client.connect(TARGET or "", plugins=[provider] if provider else [])


async def _event_counts(client: Client, workflow_id: str) -> dict[Any, int]:
    counts = {
        EVENT_STREAM_RECORDS_APPENDED: 0,
        EVENT_STREAM_SUBSCRIBED: 0,
        EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED: 0,
    }
    async for event in client.get_workflow_handle(workflow_id).fetch_history_events():
        if event.event_type in counts:
            counts[event.event_type] += 1
    return counts


@workflow.defn
class ContractLoop:
    """Reads ``inputs``, publishes a decision per value, reports control records."""

    @workflow.run
    async def run(self) -> list[dict[str, Any]]:
        inputs = workflow.stream_reader(INPUTS, result_type=dict)
        decisions = workflow.stream_writer(DECISIONS)
        trace: list[dict[str, Any]] = []
        async for record in inputs:
            if record.kind is RecordKind.SUPERSEDED:
                assert record.supersession is not None
                trace.append(
                    {
                        "kind": "superseded",
                        "replaced": record.supersession.previous_attempt,
                    }
                )
                decisions.publish(
                    {"retracting_attempt": record.supersession.previous_attempt}
                )
                continue
            if record.kind is RecordKind.FINISH:
                trace.append({"kind": "finish", "producer": record.producer_id})
                break
            assert record.value is not None
            decisions.publish({"decided": record.value["n"]})
            trace.append({"kind": "decision", "n": record.value["n"]})
        decisions.finish()
        return trace


async def test_the_interface_loop_runs_on_the_server_with_a_cold_cache() -> None:
    """Rule 2 on the native provider: every task replays from the server.

    With the cache off, each Workflow Task rebuilds the workflow from History
    and the server re-supplies the ranges earlier tasks consumed, so the loop
    completing at all means the same records came back in the same order.
    """
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "loop-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "loop-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ContractLoop],
            max_cached_workflows=0,
        ):
            handle = await client.start_workflow(
                ContractLoop.run, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            first = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
            await first.append({"n": 1}, {"n": 2})
            second = stream.producer(topic=INPUTS, producer_id="model", attempt=2)
            await second.append({"n": 3})
            await second.finish()
            trace = await asyncio.wait_for(handle.result(), 60)

            assert trace == [
                {"kind": "decision", "n": 1},
                {"kind": "decision", "n": 2},
                {"kind": "superseded", "replaced": 1},
                {"kind": "decision", "n": 3},
                {"kind": "finish", "producer": "model"},
            ]

            # The read ends by itself: the workflow is closed and the tail
            # delivered, with the workflow's own records carrying no producer.
            async def read_everything() -> list[Any]:
                return [
                    (r.producer_id, r.kind, r.value)
                    async for r in stream.read(topic=DECISIONS, result_type=dict)
                ]

            assert await asyncio.wait_for(read_everything(), 60) == [
                ("", RecordKind.DATA, {"decided": 1}),
                ("", RecordKind.DATA, {"decided": 2}),
                ("", RecordKind.DATA, {"retracting_attempt": 1}),
                ("", RecordKind.DATA, {"decided": 3}),
                ("", RecordKind.FINISH, None),
            ]
        counts = await _event_counts(client, workflow_id)
        assert counts[EVENT_STREAM_SUBSCRIBED] == 1
        # Several tasks published, one event each; the loop spanned more than
        # one task or the cold cache proved nothing.
        assert counts[EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED] >= 2
        assert (
            1
            <= counts[EVENT_STREAM_RECORDS_APPENDED]
            <= counts[EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED]
        )
    finally:
        await provider.close()


# Run ids whose first workflow task already failed, shared with the workflow
# thread so the retry can tell it is the retry. Outside the sandbox on
# purpose: the sandbox re-imports this module per run and would hide the set.
_failed_once: set[str] = set()


@workflow.defn(sandboxed=False)
class PublishThenFail:
    """Publishes, then fails its first workflow task; the retry publishes again."""

    @workflow.run
    async def run(self) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        run_id = workflow.info().run_id
        committed = run_id in _failed_once
        decisions.publish({"committed": committed})
        if not committed:
            _failed_once.add(run_id)
            raise RuntimeError("the first task fails after publishing")
        decisions.finish()


async def test_a_failed_task_publishes_nothing() -> None:
    """Rule 1 on the native provider: the server applies the command with the task."""
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "fail-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "fail-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[PublishThenFail],
        ):
            handle = await client.start_workflow(
                PublishThenFail.run, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            records = await take(
                stream.read(topic=DECISIONS, result_type=dict), 2, timeout=60
            )
            await handle.result()
        assert [(r.kind, r.value) for r in records] == [
            (RecordKind.DATA, {"committed": True}),
            (RecordKind.FINISH, None),
        ]
    finally:
        await provider.close()


@workflow.defn
class Relay:
    """Publishes one record per run and continues as new once."""

    @workflow.run
    async def run(self, run: int) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        decisions.publish({"run": run})
        if run == 0:
            workflow.continue_as_new(run + 1)
        decisions.finish()


async def test_a_handle_without_a_run_id_reads_across_continue_as_new() -> None:
    provider = NativeStreams()
    client = await _connect(provider)
    task_queue = "relay-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "relay-wf-" + uuid.uuid4().hex[:8]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Relay]):
            handle = await client.start_workflow(
                Relay.run, 0, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)

            async def read_everything() -> list[Any]:
                return [
                    (r.kind, r.value)
                    async for r in stream.read(topic=DECISIONS, result_type=dict)
                ]

            records = await asyncio.wait_for(read_everything(), 60)
            await handle.result()
        # The chain is followed: the successor's record arrives on the same
        # read, each cursor names its run, and the read ends with the chain.
        assert records == [
            (RecordKind.DATA, {"run": 0}),
            (RecordKind.DATA, {"run": 1}),
            (RecordKind.FINISH, None),
        ]
        # Pinned to the last run, a handle sees that run's stream alone.
        last_run = (await client.get_workflow_handle(workflow_id).describe()).run_id
        pinned = client.get_stream_handle(workflow_id, run_id=last_run)
        only_last = [
            r.value async for r in pinned.read(topic=DECISIONS, result_type=dict)
        ]
        assert only_last == [{"run": 1}, None]
    finally:
        await provider.close()


@workflow.defn
class PublishAndRead:
    """Publishes through the low-level surface and reads its own records back."""

    @workflow.run
    async def run(self) -> list[str]:
        workflow._append_stream_records(
            [_record(b"alpha", "progress"), _record(b"beta", "progress")],
            stream_id="output",
        )
        workflow._append_stream_records([_record(b"gamma")], stream_id="output")
        # A name this workflow has not written yet still names a stream it
        # owns, so subscribing creates the one the later publish lands in.
        workflow._subscribe_stream("output", start_offset=0)

        received: list[str] = []
        while len(received) < 3:
            for item in await workflow._read_stream_records("output"):
                received.append(item.record.body.data.decode())
        return received


def _record(body: bytes, topic: str = "") -> StreamRecord:
    return StreamRecord(
        body=Payload(data=body, metadata={"encoding": b"binary/plain"}), topic=topic
    )


async def test_a_tasks_publishes_become_one_event() -> None:
    client = await _connect()
    task_queue = "publish-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "publish-wf-" + uuid.uuid4().hex[:8]

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishAndRead],
        # Every task after the first is a replay, so completing at all means the
        # reissued publish matched the event the first run wrote.
        max_cached_workflows=0,
    ):
        result = await asyncio.wait_for(
            client.execute_workflow(
                PublishAndRead.run, id=workflow_id, task_queue=task_queue
            ),
            timeout=60,
        )
    assert result == ["alpha", "beta", "gamma"]

    counts = await _event_counts(client, workflow_id)
    # Two calls in one task carrying three records, so one event: the event is
    # per task and stream, which is what makes publishing often free.
    assert counts[EVENT_STREAM_RECORDS_APPENDED] == 1
    assert counts[EVENT_STREAM_SUBSCRIBED] == 1


@workflow.defn
class ConsumeAcrossTasks:
    """Reads a standalone stream over several Workflow Tasks, then reports what it saw.

    The turn structure is the point: each read that finds nothing blocks,
    which ends a Workflow Task, so the run spans several. With the cache on,
    every task after the first is sticky.
    """

    @workflow.run
    async def run(self, stream_id: str, expected: int) -> list[str]:
        workflow._subscribe_stream(stream_id, start_offset=0)
        seen: list[str] = []
        while len(seen) < expected:
            for item in await workflow._read_stream_records(stream_id):
                seen.append(item.record.body.data.decode())
        return seen


async def test_a_cached_workflow_consumes_across_sticky_tasks() -> None:
    """The workflow cache stays on, which is what a real worker does.

    The server sends no replay slice for a sticky task, while the sticky
    history still carries the previous task's consumed range, and the two
    together have to agree on every task after the first consumed range.
    """
    client = await _connect()
    streams = StreamClient.connect(TARGET or "")
    task_queue = "sticky-tq-" + uuid.uuid4().hex[:8]
    workflow_id = "sticky-wf-" + uuid.uuid4().hex[:8]
    stream_id = "sticky-src-" + uuid.uuid4().hex[:8]

    batches = [["a1", "a2"], ["b1"], ["c1", "c2", "c3"]]
    expected = [tok for batch in batches for tok in batch]

    try:
        await streams.create(stream_id)
        async with Worker(
            client, task_queue=task_queue, workflows=[ConsumeAcrossTasks]
        ):
            handle = await client.start_workflow(
                ConsumeAcrossTasks.run,
                args=[stream_id, len(expected)],
                id=workflow_id,
                task_queue=task_queue,
            )

            # Spaced out so the workflow drains, blocks and ends a task between
            # them. Without the gap the appends coalesce into one task and the
            # sticky path is never taken.
            for batch in batches:
                await streams.get(stream_id).append(
                    *[_record(t.encode()) for t in batch]
                )
                await asyncio.sleep(0.4)

            assert await asyncio.wait_for(handle.result(), timeout=60) == expected

        counts = await _event_counts(client, workflow_id)
        assert counts[EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED] >= 3, (
            "sticky path not exercised"
        )
    finally:
        await streams.close()
