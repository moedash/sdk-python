"""Workflow-side conformance for the stream contract.

Runs the reader and writer handles inside a real workflow on the memory
provider with a warm cache, and states the two rules about workflow tasks as
tests: a publish commits with its task (rule 1), and reads are recorded
observations that replay re-supplies (rule 2). The memory provider keeps
neither and says so in its docstring, so those two are strict expected
failures here. A storage provider that runs this module turns them into
passes; that is the measurement they exist for.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.streams.providers import memory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from tests.helpers import new_worker
from tests.streams.test_streams_conformance import take

INPUTS = "inputs"
DECISIONS = "decisions"


@pytest.fixture(autouse=True)
def _fresh_memory_provider(env: WorkflowEnvironment):  # pyright: ignore[reportUnusedFunction]
    if env.supports_time_skipping:
        pytest.skip(
            "the memory provider polls on a timer, which time skipping turns into a spin"
        )
    memory.reset()
    streams.configure(provider="memory")
    yield
    memory.reset()


@workflow.defn
class ContractLoop:
    """Reads ``inputs``, publishes a decision per value, reports control records."""

    def __init__(self) -> None:
        streams.prepare()

    @workflow.run
    async def run(self) -> list[dict[str, Any]]:
        inputs = streams.reader(INPUTS, type=dict)
        decisions = streams.writer(DECISIONS)
        trace: list[dict[str, Any]] = []
        try:
            async for record in inputs:
                if isinstance(record.value, streams.Supersession):
                    trace.append(
                        {
                            "kind": "superseded",
                            "replaced": record.value.previous_attempt,
                            "attempt": record.value.attempt,
                        }
                    )
                    await decisions.publish(
                        {"retracting_attempt": record.value.previous_attempt}
                    )
                    continue
                if record.kind is RecordKind.FINISH:
                    trace.append({"kind": "finish", "producer": record.producer})
                    break
                assert isinstance(record.value, dict)
                await decisions.publish({"decided": record.value["n"]})
                trace.append(
                    {
                        "kind": "decision",
                        "n": record.value["n"],
                        "attempt": record.attempt,
                    }
                )
        finally:
            inputs.close()
        await decisions.finish()
        # Twice on purpose: a finished topic stays finished, with one marker.
        await decisions.finish()
        streams.drain()
        return trace


async def _run_the_loop(client: Client) -> tuple[Any, list[dict[str, Any]]]:
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(client, ContractLoop, **streams.worker_options()) as worker:
        handle = await client.start_workflow(
            ContractLoop.run, id=workflow_id, task_queue=worker.task_queue
        )
        first = await streams.producer(
            client,
            workflow_id=workflow_id,
            stream=INPUTS,
            producer_id="model",
            attempt=1,
        )
        await first.append({"n": 1}, {"n": 2})
        second = await streams.producer(
            client,
            workflow_id=workflow_id,
            stream=INPUTS,
            producer_id="model",
            attempt=2,
        )
        await second.append({"n": 3})
        await second.finish()
        trace = await handle.result()
    return handle, trace


async def test_workflow_reads_decides_and_publishes(client: Client):
    handle, trace = await _run_the_loop(client)
    assert trace == [
        {"kind": "decision", "n": 1, "attempt": 1},
        {"kind": "decision", "n": 2, "attempt": 1},
        {"kind": "superseded", "replaced": 1, "attempt": 2},
        {"kind": "decision", "n": 3, "attempt": 2},
        {"kind": "finish", "producer": "model"},
    ]

    # The outside view of what the workflow published, on its own topic.
    consumer = await streams.consumer(client, workflow_id=handle.id)
    records = await take(consumer.read(type=dict, topic=DECISIONS), 5)
    assert [(r.kind, r.value) for r in records] == [
        (RecordKind.DATA, {"decided": 1}),
        (RecordKind.DATA, {"decided": 2}),
        (RecordKind.DATA, {"retracting_attempt": 1}),
        (RecordKind.DATA, {"decided": 3}),
        (RecordKind.FINISH, None),
    ]
    assert all(r.producer == "" and r.topic == DECISIONS for r in records)
    # The second finish() wrote nothing: the marker is the newest record.
    assert await consumer.latest() == records[-1].cursor


@pytest.mark.xfail(
    strict=True,
    reason=(
        "rule 2: reading is a recorded observation, so replaying the history "
        "with the store gone must re-supply the same records; the memory "
        "provider reads live process memory instead"
    ),
)
async def test_replay_without_the_store_resupplies_the_records(client: Client):
    handle, _ = await _run_the_loop(client)
    history = await handle.fetch_history()

    memory.reset()
    replayer = Replayer(workflows=[ContractLoop], **streams.worker_options())
    await replayer.replay_workflow(history)


# Run ids whose first workflow task already failed, shared with the workflow
# thread so the retry can tell it is the retry. Outside the sandbox on
# purpose: the sandbox re-imports this module per run and would hide the set.
_failed_once: set[str] = set()


@workflow.defn(sandboxed=False)
class PublishThenFail:
    """Publishes, then fails its first workflow task; the retry publishes again."""

    def __init__(self) -> None:
        streams.prepare()

    @workflow.run
    async def run(self) -> None:
        decisions = streams.writer(DECISIONS)
        run_id = workflow.info().run_id
        committed = run_id in _failed_once
        await decisions.publish({"committed": committed})
        if not committed:
            _failed_once.add(run_id)
            raise RuntimeError("the first task fails after publishing")
        await decisions.finish()
        streams.drain()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "rule 1: a publish commits with its workflow task, so no reader sees "
        "a record from a task that failed; the memory provider makes it "
        "visible at publish time"
    ),
)
async def test_a_failed_task_publishes_nothing(client: Client):
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(
        client, PublishThenFail, **streams.worker_options()
    ) as worker:
        handle = await client.start_workflow(
            PublishThenFail.run, id=workflow_id, task_queue=worker.task_queue
        )
        consumer = await streams.consumer(client, workflow_id=workflow_id)
        records = await take(consumer.read(type=dict, topic=DECISIONS), 2, timeout=30)
        await handle.result()
    assert [(r.kind, r.value) for r in records] == [
        (RecordKind.DATA, {"committed": True}),
        (RecordKind.FINISH, None),
    ]
