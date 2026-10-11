"""A completion's stream manifest stays under Core's limit.

Core refuses a manifest over 64 KiB by failing the Workflow Task on every
retry, and takes one commit per completion. So the publish that would take a
completion's manifest past the budget is refused at the call.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import (
    BEGINNING,
    StreamError,
    get_stream_handle,
    workflow_writer,
)
from temporalio.contrib.streams._workflow import (
    _MANIFEST_FIXED_BYTES,
    _MANIFEST_TOPIC_BYTES,
    MANIFEST_BUDGET_BYTES,
)
from temporalio.contrib.streams.memory import MemoryStreams
from tests.contrib.streams._support import connect_with, new_workflow_id
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
            # Another record on a topic already in the activation costs nothing.
            workflow_writer(topic_name(0)).publish({"again": index})
            published += 1
        return published


async def test_the_publish_that_crosses_the_budget_is_refused(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishToManyTopics) as worker:
        handle = await streams_client.start_workflow(
            PublishToManyTopics.run, 1000, id=workflow_id, task_queue=worker.task_queue
        )
        published = await handle.result()
    # Refused well before 1000 topics, and the task still completed.
    assert 0 < published < 1000
    (marker,) = await output_markers(handle)
    assert len(marker.output.topics) == published
    # The bound the writer kept is never below the manifest Core built.
    bound = _MANIFEST_FIXED_BYTES + sum(
        _MANIFEST_TOPIC_BYTES + len(topic_name(i)) for i in range(published)
    )
    assert marker.output.ByteSize() <= bound <= MANIFEST_BUDGET_BYTES
    # The refused topic was never committed; the ones before it were.
    refused = get_stream_handle(
        streams_client, workflow_id, topic=topic_name(published)
    )
    assert await refused.latest() == BEGINNING
    last = get_stream_handle(
        streams_client, workflow_id, topic=topic_name(published - 1)
    ).read()
    assert (await asyncio.wait_for(anext(last), 5.0)).value == {"i": published - 1}
    await last.aclose()


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
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, FinishAfterTheBudget) as worker:
        result = await streams_client.execute_workflow(
            FinishAfterTheBudget.run,
            id=new_workflow_id(),
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
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, PublishToManyTopicsUncaught) as worker:
        handle = await streams_client.start_workflow(
            PublishToManyTopicsUncaught.run,
            1000,
            id=new_workflow_id(),
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
