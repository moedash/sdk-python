"""Workflow-side conformance for the stream contract.

Runs the reader and writer inside a real workflow on the memory provider
with a warm cache, and states the two rules about Workflow Tasks as tests: a
publish commits with its task (rule 1), and reads are recorded observations
that replay re-supplies (rule 2). The memory provider keeps neither and says
so in its docstring, so those two are strict expected failures here. A
storage provider that runs this module turns them into passes; that is the
measurement they exist for.

The rest is what the portable surface promises on every provider: the
lifecycle hooks the worker calls, one subscription per topic per run, a read
that ends when the chain closes, a handle that follows continue-as-new, and
a topic shared by the workflow and an outside producer.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.streams import (
    Cursor,
    ReadSource,
    RecordKind,
    StreamCursorError,
    WriteSink,
    topic,
)
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from tests.helpers import new_worker
from tests.streams.test_streams_conformance import take

INPUTS = topic("inputs", dict)
DECISIONS = topic("decisions", dict)


@pytest.fixture
def provider(env: WorkflowEnvironment):  # pyright: ignore[reportUnusedFunction]
    if env.supports_time_skipping:
        pytest.skip(
            "the memory provider polls on a timer, which time skipping turns into a spin"
        )
    streams = MemoryStreams()
    yield streams
    streams.reset()


@workflow.defn
class ContractLoop:
    """Reads ``inputs``, publishes a decision per value, reports control records."""

    @workflow.run
    async def run(self) -> list[dict[str, Any]]:
        inputs = workflow.stream_reader(INPUTS)
        decisions = workflow.stream_writer(DECISIONS)
        trace: list[dict[str, Any]] = []
        try:
            async for record in inputs:
                if record.kind is RecordKind.SUPERSEDED:
                    assert record.supersession is not None
                    trace.append(
                        {
                            "kind": "superseded",
                            "replaced": record.supersession.previous_attempt,
                            "attempt": record.supersession.attempt,
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
                trace.append(
                    {
                        "kind": "decision",
                        "n": record.value["n"],
                        "attempt": record.attempt,
                    }
                )
        finally:
            inputs.close()
        decisions.finish()
        # Twice on purpose: a finished topic stays finished, with one marker.
        decisions.finish()
        return trace


async def _run_the_loop(
    client: Client, provider: MemoryStreams
) -> tuple[Any, list[dict[str, Any]]]:
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, ContractLoop, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            ContractLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)
        first = stream.producer(topic=INPUTS, producer_id="model", attempt=1)
        await first.append({"n": 1}, {"n": 2})
        second = stream.producer(topic=INPUTS, producer_id="model", attempt=2)
        await second.append({"n": 3})
        await second.finish()
        trace = await handle.result()
    return handle, trace


async def test_workflow_reads_decides_and_publishes(
    client: Client, provider: MemoryStreams
):
    handle, trace = await _run_the_loop(client, provider)
    assert trace == [
        {"kind": "decision", "n": 1, "attempt": 1},
        {"kind": "decision", "n": 2, "attempt": 1},
        {"kind": "superseded", "replaced": 1, "attempt": 2},
        {"kind": "decision", "n": 3, "attempt": 2},
        {"kind": "finish", "producer": "model"},
    ]

    # The outside view of what the workflow published, on its own topic.
    stream = provider.get_stream_handle(client, handle.id)
    records = await take(stream.read(topic=DECISIONS), 5)
    assert [(r.kind, r.value) for r in records] == [
        (RecordKind.DATA, {"decided": 1}),
        (RecordKind.DATA, {"decided": 2}),
        (RecordKind.DATA, {"retracting_attempt": 1}),
        (RecordKind.DATA, {"decided": 3}),
        (RecordKind.FINISH, None),
    ]
    assert all(r.producer_id == "" and r.topic == DECISIONS.name for r in records)
    # The second finish() wrote nothing: the marker is the newest record.
    assert await stream.latest(topic=DECISIONS) == records[-1].cursor


async def test_read_ends_when_the_workflow_closes_and_the_tail_is_delivered(
    client: Client, provider: MemoryStreams
):
    handle, _ = await _run_the_loop(client, provider)
    stream = provider.get_stream_handle(client, handle.id)

    async def read_everything() -> list[Any]:
        return [r.value async for r in stream.read(topic=DECISIONS)]

    # No count and no early break: the read ends by itself once the workflow
    # is closed and everything it retained has been handed over.
    values = await asyncio.wait_for(read_everything(), 30)
    assert values == [
        {"decided": 1},
        {"decided": 2},
        {"retracting_attempt": 1},
        {"decided": 3},
        None,
    ]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "rule 2: reading is a recorded observation, so replaying the history "
        "with the store gone must re-supply the same records; the memory "
        "provider reads live process memory instead"
    ),
)
async def test_replay_without_the_store_resupplies_the_records(
    client: Client, provider: MemoryStreams
):
    handle, _ = await _run_the_loop(client, provider)
    history = await handle.fetch_history()

    provider.reset()
    replayer = Replayer(workflows=[ContractLoop], plugins=[provider])
    await replayer.replay_workflow(history)


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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "rule 1: a publish commits with its workflow task, so no reader sees "
        "a record from a task that failed; the memory provider makes it "
        "visible at publish time"
    ),
)
async def test_a_failed_task_publishes_nothing(client: Client, provider: MemoryStreams):
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, PublishThenFail, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            PublishThenFail.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)
        records = await take(stream.read(topic=DECISIONS), 2, timeout=30)
        await handle.result()
    assert [(r.kind, r.value) for r in records] == [
        (RecordKind.DATA, {"committed": True}),
        (RecordKind.FINISH, None),
    ]


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


class _RecordingHalf:
    """A workflow half that logs the hooks the worker calls, then delegates."""

    def __init__(self, inner: Any, calls: list[tuple[str, str]]) -> None:
        self._inner = inner
        self._calls = calls

    def open_reader(self, topic: str, *, after: Cursor) -> ReadSource:
        return self._inner.open_reader(topic, after=after)

    def open_writer(self, topic: str) -> WriteSink:
        return self._inner.open_writer(topic)

    def on_workflow_start(self) -> None:
        self._calls.append(("start", workflow.info().run_id))

    async def on_workflow_finish(self) -> None:
        self._calls.append(("finish", workflow.info().run_id))


class HookedMemory(MemoryStreams):
    """The memory provider with its lifecycle hooks made visible."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str]] = []

    def workflow_provider(self) -> Any:
        return _RecordingHalf(super().workflow_provider(), self.calls)


@pytest.mark.usefixtures("provider")
async def test_the_worker_calls_the_lifecycle_hooks_around_every_run(client: Client):
    hooked = HookedMemory()
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, Relay, plugins=[hooked]) as worker:
        handle = await client.start_workflow(
            Relay.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    # Start before the function, finish after it, on both runs: the finish
    # hook runs on the continue-as-new exit too, so a provider that parked
    # something against the first run can let go before the successor starts.
    kinds = [kind for kind, _ in hooked.calls]
    assert kinds == ["start", "finish", "start", "finish"]
    runs = [run_id for _, run_id in hooked.calls]
    assert runs[0] == runs[1] and runs[2] == runs[3] and runs[0] != runs[2]


async def test_a_handle_without_a_run_id_reads_across_continue_as_new(
    client: Client, provider: MemoryStreams
):
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, Relay, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            Relay.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)

        async def read_everything() -> list[Any]:
            return [(r.kind, r.value) async for r in stream.read(topic=DECISIONS)]

        records = await asyncio.wait_for(read_everything(), 30)
        await handle.result()
    # The chain is followed: the successor's records arrive on the same read,
    # and the read ends only when the last run of the chain is closed.
    assert records == [
        (RecordKind.DATA, {"run": 0}),
        (RecordKind.DATA, {"run": 1}),
        (RecordKind.FINISH, None),
    ]


@workflow.defn
class SharedReaders:
    """Opens the same topic twice and pulls from both readers in turn."""

    @workflow.run
    async def run(self) -> list[Any]:
        first = workflow.stream_reader(INPUTS)
        second = workflow.stream_reader(INPUTS)
        trace: list[Any] = ["shared" if first is second else "separate"]
        try:
            workflow.stream_reader(INPUTS, after=Cursor("memory:0"))
        except ValueError:
            trace.append("after-rejected")
        try:
            workflow.stream_reader(INPUTS.name, result_type=list)
        except ValueError:
            trace.append("type-rejected")
        trace.append((await first.__anext__()).value)
        trace.append((await second.__anext__()).value)
        first.close()
        return trace


async def test_a_second_reader_on_a_topic_shares_the_subscription(
    client: Client, provider: MemoryStreams
):
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, SharedReaders, plugins=[provider]) as worker:
        handle = await client.start_workflow(
            SharedReaders.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, workflow_id)
        await stream.producer(topic=INPUTS, producer_id="model", attempt=1).append(
            {"n": 1}, {"n": 2}
        )
        assert await handle.result() == [
            "shared",
            "after-rejected",
            "type-rejected",
            {"n": 1},
            {"n": 2},
        ]


@workflow.defn
class ForeignCursor:
    """Resumes from a cursor another provider minted."""

    @workflow.run
    async def run(self) -> str:
        try:
            workflow.stream_reader(INPUTS, after=Cursor("elsewhere:1"))
        except StreamCursorError:
            return "refused"
        return "accepted"


async def test_the_workflow_reader_refuses_a_foreign_cursor(
    client: Client, provider: MemoryStreams
):
    async with new_worker(client, ForeignCursor, plugins=[provider]) as worker:
        result = await client.execute_workflow(
            ForeignCursor.run,
            id=f"streams-wf-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    assert result == "refused"


@workflow.defn
class NoProvider:
    """Opens a stream on a worker that has no provider."""

    @workflow.run
    async def run(self) -> str:
        try:
            workflow.stream_reader(INPUTS)
        except RuntimeError as error:
            return str(error)
        return "opened"


async def test_a_worker_without_a_provider_says_so(client: Client):
    async with new_worker(client, NoProvider) as worker:
        result = await client.execute_workflow(
            NoProvider.run,
            id=f"streams-wf-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    assert "no stream provider is configured" in result


@workflow.defn
class OneLine:
    """Publishes one record and finishes the topic."""

    @workflow.run
    async def run(self) -> None:
        decisions = workflow.stream_writer(DECISIONS)
        decisions.publish({"from": "workflow"})
        decisions.finish()


async def test_an_outside_producer_and_the_workflow_share_a_topic(
    client: Client, provider: MemoryStreams
):
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, OneLine, plugins=[provider]) as worker:
        stream = provider.get_stream_handle(client, workflow_id)
        await stream.producer(topic=DECISIONS, producer_id="tool", attempt=1).append(
            {"from": "producer"}
        )
        handle = await client.start_workflow(
            OneLine.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()

        async def read_everything() -> list[Any]:
            return [r async for r in stream.read(topic=DECISIONS)]

        records = await asyncio.wait_for(read_everything(), 30)
    # Both writers land on one topic in one order, each under its own
    # identity: the producer's records carry its id, the workflow's carry none.
    assert [(r.producer_id, r.kind, r.value) for r in records] == [
        ("tool", RecordKind.DATA, {"from": "producer"}),
        ("", RecordKind.DATA, {"from": "workflow"}),
        ("", RecordKind.FINISH, None),
    ]
