"""Publishing from Workflow code, against the memory provider and a dev server.

A Workflow publishes to its own stream with ``workflow_writer``. A query or
an update validator commits nothing, so a publish there is refused at the
call.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import (
    Client,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.contrib.streams import (
    BEGINNING,
    DEFAULT_TOPIC,
    RecordKind,
    StreamHandle,
    StreamRef,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer
from tests.helpers import new_worker

EVENTS = topic("events", dict)


def client_with(client: Client, provider: MemoryStreams | None) -> Client:
    config = client.config()
    config["plugins"] = [provider] if provider is not None else []
    return Client(**config)


def stream_of(
    client: Client, provider: MemoryStreams, workflow_id: str
) -> StreamHandle:
    return provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))


def new_workflow_id() -> str:
    return f"streams-publish-{uuid.uuid4().hex}"


async def read_all(records: Any, timeout: float = 10.0) -> list:
    """Read until the stream ends, which is when its owner closes."""
    out = []

    async def pull() -> None:
        async for record in records:
            out.append(record)

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    return out


@workflow.defn
class Publisher:
    def __init__(self) -> None:
        workflow_writer(EVENTS).publish({"step": "init"})

    @workflow.run
    async def run(self, count: int) -> None:
        writer = workflow_writer(EVENTS)
        for step in range(count):
            writer.publish({"step": step})
            await workflow.sleep(timedelta(milliseconds=10))
        writer.finish()
        # Finish belongs to the topic, so another writer finds it written.
        workflow_writer(EVENTS).finish()
        try:
            workflow_writer(EVENTS).publish({"step": "late"})
            raise ApplicationError("a finished topic took a record")
        except ValueError:
            pass
        workflow_writer().publish("on the default topic")


async def test_a_workflow_publishes_and_a_client_reads(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, Publisher) as worker:
        await streams_client.execute_workflow(
            Publisher.run, 3, id=workflow_id, task_queue=worker.task_queue
        )
    stream = stream_of(streams_client, provider, workflow_id)
    records = await read_all(stream.read(topic=EVENTS))
    assert [r.kind for r in records] == [RecordKind.DATA] * 4 + [RecordKind.FINISH]
    assert [r.value for r in records[:4]] == [
        {"step": "init"},
        {"step": 0},
        {"step": 1},
        {"step": 2},
    ]
    # The owning Workflow writes with no producer identity: its task is the
    # boundary, not a producer attempt.
    assert {(r.producer_id, r.attempt, r.sequence) for r in records} == {("", 0, 0)}
    (default,) = await read_all(stream.read(result_type=str))
    assert default.value == "on the default topic"
    assert default.topic == DEFAULT_TOPIC


async def test_an_evicted_workflow_does_not_publish_twice(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    # With no cache every task replays the run from the start, so a provider
    # that stored on replay would store each record again.
    async with new_worker(streams_client, Publisher, max_cached_workflows=0) as worker:
        await streams_client.execute_workflow(
            Publisher.run, 3, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(
        stream_of(streams_client, provider, workflow_id).read(topic=EVENTS)
    )
    assert len(records) == 5


async def test_replaying_a_publishing_workflow_stores_nothing(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, Publisher) as worker:
        handle = await streams_client.start_workflow(
            Publisher.run, 2, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    history = await handle.fetch_history()

    fresh = MemoryStreams()
    await Replayer(workflows=[Publisher], plugins=[fresh]).replay_workflow(history)
    stream = fresh.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    assert await stream.latest(topic=EVENTS) == BEGINNING


@workflow.defn
class ReadOnlyPublisher:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.query
    def peek(self) -> str:
        workflow_writer(EVENTS).publish({"from": "query"})
        return "unreachable"

    @workflow.query
    def finish_from_query(self) -> str:
        workflow_writer(EVENTS).finish()
        return "unreachable"

    @workflow.update
    async def poke(self) -> None:
        self.done = True

    @poke.validator
    def validate_poke(self) -> None:
        # Uncaught, a read-only violation fails the Workflow Task, as it does
        # for any command from a validator; caught, it rejects the Update.
        try:
            workflow_writer(EVENTS).publish({"from": "validator"})
        except workflow.ReadOnlyContextError as error:
            raise ApplicationError(str(error)) from None


async def test_a_query_or_validator_cannot_publish(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, ReadOnlyPublisher) as worker:
        handle = await streams_client.start_workflow(
            ReadOnlyPublisher.run, id=workflow_id, task_queue=worker.task_queue
        )
        with pytest.raises(WorkflowQueryFailedError, match="publish to a stream"):
            await handle.query(ReadOnlyPublisher.peek)
        with pytest.raises(WorkflowQueryFailedError, match="finish a stream topic"):
            await handle.query(ReadOnlyPublisher.finish_from_query)
        with pytest.raises(WorkflowUpdateFailedError) as failed:
            await handle.execute_update(ReadOnlyPublisher.poke)
        assert "publish to a stream" in str(failed.value.cause)
        stream = stream_of(streams_client, provider, workflow_id)
        assert await stream.latest(topic=EVENTS) == BEGINNING
        await handle.terminate()


@workflow.defn
class PublishesWithoutProvider:
    @workflow.run
    async def run(self) -> str:
        try:
            workflow_writer(EVENTS)
        except ValueError as error:
            return str(error)
        return "published"


async def test_a_workflow_without_a_provider_is_told(client: Client):
    async with new_worker(client, PublishesWithoutProvider) as worker:
        said = await client.execute_workflow(
            PublishesWithoutProvider.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert "no stream provider is registered" in said
