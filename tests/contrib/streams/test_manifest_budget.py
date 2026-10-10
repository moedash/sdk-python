"""A completion's stream manifest stays under Core's limit.

Core refuses a manifest over 64 KiB by failing the Workflow Task on every
retry, and takes one commit per completion. So the publish that would take a
completion's manifest past the budget is refused at the call.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import (
    StreamError,
    StreamRef,
    workflow_writer,
)
from temporalio.contrib.streams._output import (
    MANIFEST_BUDGET_BYTES,
    _RunOutput,
    build_manifest,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from tests.contrib.streams.test_output_commit import output_markers
from tests.helpers import new_worker

# Long names make the budget bite after a couple of hundred topics.
_NAME = "t" * 200


def topic_name(index: int) -> str:
    return f"{_NAME}-{index:04d}"


@workflow.defn
class PublishToManyTopics:
    @workflow.run
    async def run(self, count: int) -> int:
        published = 0
        for index in range(count):
            try:
                workflow_writer(topic_name(index)).publish({"i": index})
            except StreamError:
                break
            published += 1
        return published


def client_with(client: Client, provider: MemoryStreams) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


async def test_the_publish_that_crosses_the_budget_is_refused(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = f"streams-budget-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishToManyTopics) as worker:
        handle = await streams_client.start_workflow(
            PublishToManyTopics.run, 1000, id=workflow_id, task_queue=worker.task_queue
        )
        published = await handle.result()
    # Refused well before 1000 topics, and the task still completed.
    assert 0 < published < 1000
    (marker,) = await output_markers(handle)
    assert len(marker.output.topics) == published
    assert marker.output.ByteSize() <= MANIFEST_BUDGET_BYTES
    # The refused topic was never staged; the ones before it were committed.
    assert provider._stages == {}
    refused = provider._topic(client.namespace, workflow_id, topic_name(published))
    assert refused.records == []
    last = provider.get_stream_handle(
        client, StreamRef.for_workflow(workflow_id, topic=topic_name(published - 1))
    )
    records = last.read()
    assert (await asyncio.wait_for(records.__anext__(), 5.0)).value == {
        "i": published - 1
    }
    await records.aclose()


def test_the_bound_is_never_below_the_real_manifest():
    run = _RunOutput("wf", "run-" + "x" * 30, "first")
    records: list[Any] = []
    index = 0
    while True:
        record = WireRecord(
            topic=topic_name(index),
            body=Payload(metadata={"encoding": b"json/plain"}, data=b"1"),
        )
        try:
            run.publish([record])
        except StreamError:
            break
        records.append(record)
        index += 1
    manifest = build_manifest(
        records,
        history_floor_event_id=2**40,
        run_id=run.run_id,
        provider_id="p" * 100,
    )
    manifest.stage_token = uuid.uuid4().hex
    assert manifest.ByteSize() <= run.manifest_bound <= MANIFEST_BUDGET_BYTES
    # Publishing again to a topic already in the activation costs nothing.
    run.publish([records[0]])
    # A drained buffer starts over.
    run.take_pending()
    run.publish([records[0]])


def test_a_refused_publish_buffers_nothing():
    run = _RunOutput("wf", "run", "first")
    with pytest.raises(StreamError):
        run.publish(
            [WireRecord(topic=topic_name(i)) for i in range(MANIFEST_BUDGET_BYTES)]
        )
    assert run.pending == []


@workflow.defn
class FinishAfterTheBudget:
    @workflow.run
    async def run(self) -> str:
        index = 0
        while True:
            try:
                workflow_writer(topic_name(index)).publish({"i": index})
            except StreamError:
                break
            index += 1
        refused = workflow_writer(topic_name(index))
        try:
            refused.finish()
            return "finish was not refused"
        except StreamError:
            pass
        # The next Workflow Task has a fresh budget, and the topic is still
        # open because its FINISH never went out.
        await workflow.sleep(0.01)
        refused.publish({"i": index})
        refused.finish()
        return "finished"


async def test_a_refused_finish_leaves_the_topic_open(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    async with new_worker(streams_client, FinishAfterTheBudget) as worker:
        result = await streams_client.execute_workflow(
            FinishAfterTheBudget.run,
            id=f"streams-budget-finish-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    assert result == "finished"


@workflow.defn
class PublishToManyTopicsUncaught:
    @workflow.run
    async def run(self, count: int) -> None:
        for index in range(count):
            workflow_writer(topic_name(index)).publish({"i": index})


async def test_an_uncaught_budget_error_fails_the_task_on_every_retry(client: Client):
    # The error is not a Temporal failure, so the Workflow does not fail; the
    # task retries the same code and fails the same way until it is fixed.
    streams_client = client_with(client, MemoryStreams())
    async with new_worker(streams_client, PublishToManyTopicsUncaught) as worker:
        handle = await streams_client.start_workflow(
            PublishToManyTopicsUncaught.run,
            1000,
            id=f"streams-budget-uncaught-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
        failures: list[str] = []
        for _ in range(100):
            failures = [
                event.workflow_task_failed_event_attributes.failure.message
                async for event in handle.fetch_history_events()
                if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
            ]
            if failures:
                break
            await asyncio.sleep(0.1)
        assert failures and "manifest past" in failures[0]
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        await handle.terminate()
