"""The publish accessors, against the memory provider and a dev server.

A Workflow publishes to its own stream with ``workflow_writer``, an Activity
writes to its scheduling Workflow's stream with ``activity_handle``, and a
client reads with ``get_stream_handle``. What a Workflow cannot do in this
release (read, publish from a query or an update validator) is refused at
the call.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import (
    Client,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    BEGINNING,
    DEFAULT_TOPIC,
    RecordKind,
    StreamRef,
    StreamUnsupportedError,
    Supersession,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_reader,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer
from tests.helpers import new_worker

EVENTS = topic("events", dict)
TOKENS = topic("tokens", str)


def client_with(client: Client, provider: MemoryStreams | None) -> Client:
    config = client.config()
    config["plugins"] = [provider] if provider is not None else []
    return Client(**config)


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
    stream = get_stream_handle(streams_client, workflow_id)
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
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
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
        stream = get_stream_handle(streams_client, workflow_id)
        assert await stream.latest(topic=EVENTS) == BEGINNING
        await handle.terminate()


@workflow.defn
class TriesToRead:
    @workflow.run
    async def run(self) -> str:
        try:
            workflow_reader(EVENTS)
        except StreamUnsupportedError as error:
            return str(error)
        return "read"


async def test_a_workflow_cannot_read_a_stream(client: Client):
    streams_client = client_with(client, MemoryStreams())
    async with new_worker(streams_client, TriesToRead) as worker:
        said = await streams_client.execute_workflow(
            TriesToRead.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
    assert "not supported in this release" in said


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


@activity.defn
async def stream_tokens(tokens: list[str]) -> bool:
    stream = activity_handle()
    producer = stream.producer(topic=TOKENS)
    attempt = activity.info().attempt
    # The first attempt writes part of its answer and fails; the retry
    # writes the whole answer, which readers must see as superseding it.
    await producer.append(*tokens[:attempt])
    if attempt == 1:
        raise ApplicationError("retry me")
    await producer.append(*tokens[attempt:])
    await producer.finish()
    return stream.ref.run_id == activity.info().workflow_run_id


@workflow.defn
class RunsStreamingActivity:
    @workflow.run
    async def run(self, tokens: list[str]) -> bool:
        return await workflow.execute_activity(
            stream_tokens,
            tokens,
            activity_id="tokens",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50), maximum_attempts=2
            ),
        )


async def test_an_activity_retry_supersedes_its_first_attempt(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(
        streams_client, RunsStreamingActivity, activities=[stream_tokens]
    ) as worker:
        pinned = await streams_client.execute_workflow(
            RunsStreamingActivity.run,
            ["a", "b", "c"],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
    assert pinned
    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=TOKENS)
    )
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert records[1].supersession == Supersession("tokens", 1, 2)
    assert [r.value for r in records if r.kind is RecordKind.DATA] == [
        "a",
        "a",
        "b",
        "c",
    ]
    assert [(r.producer_id, r.attempt, r.sequence) for r in records[2:]] == [
        ("tokens", 2, 1),
        ("tokens", 2, 2),
        ("tokens", 2, 3),
        ("tokens", 2, 4),
    ]


@activity.defn
async def stream_without_provider() -> str:
    try:
        activity_handle()
    except ValueError as error:
        return str(error)
    return "opened"


@workflow.defn
class RunsActivityWithoutProvider:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            stream_without_provider, start_to_close_timeout=timedelta(seconds=10)
        )


async def test_an_activity_without_a_provider_is_told(client: Client):
    async with new_worker(
        client, RunsActivityWithoutProvider, activities=[stream_without_provider]
    ) as worker:
        said = await client.execute_workflow(
            RunsActivityWithoutProvider.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert "no stream provider is registered" in said


async def test_an_activity_with_no_workflow_has_no_stream():
    env = ActivityEnvironment()
    env.info = dataclasses.replace(
        ActivityEnvironment.default_info(), workflow_id=None, workflow_run_id=None
    )

    async def standalone() -> None:
        activity_handle()

    with pytest.raises(StreamUnsupportedError, match="owned by an Activity"):
        await env.run(standalone)


async def test_a_client_handle_follows_the_chain_or_pins_a_run(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    follower = get_stream_handle(streams_client, "wf")
    assert follower.ref == StreamRef.for_workflow("wf")
    pinned = get_stream_handle(streams_client, "wf", run_id="run", topic=EVENTS)
    assert pinned.ref == StreamRef.for_workflow("wf", run_id="run", topic=EVENTS)
    assert get_stream_handle(streams_client, pinned.ref).ref == pinned.ref
    with pytest.raises(ValueError, match="carries its own"):
        get_stream_handle(streams_client, pinned.ref, run_id="other")
    with pytest.raises(ValueError, match="no stream provider is registered"):
        get_stream_handle(client_with(client, None), "wf")


async def test_a_client_producer_and_the_workflow_share_a_topic(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    stream = get_stream_handle(streams_client, workflow_id)
    outside = stream.producer(topic=EVENTS, producer_id="backend", attempt=1)
    await outside.append({"from": "backend"})
    async with new_worker(streams_client, Publisher) as worker:
        await streams_client.execute_workflow(
            Publisher.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(stream.read(topic=EVENTS))
    assert [(r.producer_id, r.value) for r in records] == [
        ("backend", {"from": "backend"}),
        ("", {"step": "init"}),
        ("", None),
    ]
