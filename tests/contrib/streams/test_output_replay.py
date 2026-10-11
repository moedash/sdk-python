"""Replaying a publishing Workflow sends its records again, without bodies.

Core checks the replayed records against the marker History holds, so a
Workflow that publishes something else on replay fails as nondeterministic.
Nothing is stored again, so replaying needs no store.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload, WorkflowExecution
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import Client
from temporalio.contrib.streams import get_stream_handle, topic, workflow_writer
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import Replayer
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.helpers import new_worker

EVENTS = topic("events", dict)


@activity.defn
async def nothing() -> None:
    pass


@workflow.defn(name="Publisher")
class Publisher:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_writer(EVENTS).publish({"n": "after"})
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": "last"})
        workflow_writer(EVENTS).finish()


@workflow.defn(name="Publisher")
class PublisherWithDifferentData:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_writer(EVENTS).publish({"n": "changed"})
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": "last"})
        workflow_writer(EVENTS).finish()


@workflow.defn(name="Publisher")
class PublisherWithFewerCommits:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": "last"})
        workflow_writer(EVENTS).finish()


async def publisher_history(client: Client) -> Any:
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Publisher, activities=[nothing]) as worker:
        handle = await streams_client.start_workflow(
            Publisher.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
        await handle.result()
    return await handle.fetch_history()


class RecordingCodec(PayloadCodec):
    """Keeps what it encodes, so a test sees which payloads crossed it."""

    def __init__(self) -> None:
        self.encoded: list[Payload] = []

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.encoded.extend(payloads)
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(payloads)


async def test_a_replay_needs_no_store_and_sends_no_body(client: Client):
    history = await publisher_history(client)
    codec = RecordingCodec()
    # Two commits share the first Workflow Task, around the Local Activity,
    # and Core pairs them with the recorded markers in order.
    await Replayer(
        workflows=[Publisher], data_converter=DataConverter(payload_codec=codec)
    ).replay_workflow(history)
    published = [p for p in codec.encoded if b'"n"' in p.data]
    assert published == []


async def test_a_replay_that_publishes_different_data_is_nondeterministic(
    client: Client,
):
    history = await publisher_history(client)
    with pytest.raises(Exception, match="differs from the manifest recorded"):
        await Replayer(workflows=[PublisherWithDifferentData]).replay_workflow(history)


async def test_a_replay_that_commits_less_than_history_is_nondeterministic(
    client: Client,
):
    history = await publisher_history(client)
    # The first Workflow Task recorded two commits; this replay makes one.
    with pytest.raises(Exception, match="(?i)nondetermin"):
        await Replayer(workflows=[PublisherWithFewerCommits]).replay_workflow(history)


@workflow.defn
class PublishAroundSignals:
    def __init__(self) -> None:
        self.next = False
        self.done = False

    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        await workflow.wait_condition(lambda: self.next)
        workflow_writer(EVENTS).publish({"n": 2})
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def go_on(self) -> None:
        self.next = True

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def take(records: Any, count: int, timeout: float = 10.0) -> list:
    out: list[Any] = []
    try:
        while len(out) < count:
            out.append(await asyncio.wait_for(anext(records), timeout))
    finally:
        await records.aclose()
    return out


async def reset_to_last_completed_task(client: Client, workflow_id: str) -> str:
    handle = client.get_workflow_handle(workflow_id)
    completed = [
        event.event_id
        async for event in handle.fetch_history_events()
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    ]
    response = await client.workflow_service.reset_workflow_execution(
        ResetWorkflowExecutionRequest(
            namespace=client.namespace,
            workflow_execution=WorkflowExecution(workflow_id=workflow_id),
            reason="test a reset of a publishing Workflow",
            workflow_task_finish_event_id=completed[-1],
            request_id=str(uuid.uuid4()),
        )
    )
    return response.run_id


async def test_a_reset_of_a_publishing_workflow_replays_its_output(client: Client):
    # The reset run replays the base run's markers, whose records carry the
    # base run's id. The run id must not be part of what replay compares.
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishAroundSignals) as worker:
        handle = await streams_client.start_workflow(
            PublishAroundSignals.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = get_stream_handle(streams_client, workflow_id)
        # Two publishing tasks, so the reset run replays a marker of the base run.
        await take(stream.read(topic=EVENTS), 1)
        await handle.signal(PublishAroundSignals.go_on)
        await take(stream.read(topic=EVENTS), 2)
        reset_run = await reset_to_last_completed_task(client, workflow_id)
        reset = streams_client.get_workflow_handle(workflow_id, run_id=reset_run)
        await reset.signal(PublishAroundSignals.finish)
        await asyncio.wait_for(reset.result(), 30)
    failed = [
        event
        async for event in reset.fetch_history_events()
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
        and event.workflow_task_failed_event_attributes.cause
        != WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW
    ]
    assert failed == []
