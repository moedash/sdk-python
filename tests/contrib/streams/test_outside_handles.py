"""Reaching a Workflow's stream from an Activity or a client.

An Activity writes to the stream of the Workflow that scheduled it as
itself, so its retry is reported to readers as ``SUPERSEDED``. A client
reaches a Workflow's stream by Workflow id through its provider.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    Supersession,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.exceptions import ApplicationError
from tests.contrib.streams.test_workflow_writer import EVENTS, Publisher
from tests.helpers import new_worker

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
    producer_id = f"tokens@{(await streams_client.get_workflow_handle(workflow_id).describe()).run_id}"
    assert records[1].supersession == Supersession(producer_id, 1, 2)
    assert [r.value for r in records if r.kind is RecordKind.DATA] == [
        "a",
        "a",
        "b",
        "c",
    ]
    assert [(r.producer_id, r.attempt, r.sequence) for r in records[2:]] == [
        (producer_id, 2, 1),
        (producer_id, 2, 2),
        (producer_id, 2, 3),
        (producer_id, 2, 4),
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


@activity.defn
async def count_twice() -> str:
    producer = activity_handle().producer(topic=TOKENS)
    await producer.append("one")
    await producer.append("two")
    return producer.producer_id


@workflow.defn
class CountThenContinue:
    @workflow.run
    async def run(self, runs_left: int) -> list[str]:
        # The same Activity id in every run, as a counter-based id would be.
        producer_id = await workflow.execute_activity(
            count_twice,
            activity_id="count",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        if runs_left:
            workflow.continue_as_new(runs_left - 1)
        return [producer_id]


async def test_an_activity_producer_writes_in_every_run_of_a_chain(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(
        streams_client, CountThenContinue, activities=[count_twice]
    ) as worker:
        handle = await streams_client.start_workflow(
            CountThenContinue.run, 1, id=workflow_id, task_queue=worker.task_queue
        )
        (last_producer,) = await handle.result()
    stream = get_stream_handle(streams_client, workflow_id, topic=TOKENS)
    records = await read_all(stream.read())
    # Both runs' records, under two producers: the run is part of the id.
    assert [r.value for r in records] == ["one", "two", "one", "two"]
    producers = [r.producer_id for r in records]
    assert producers[0] == producers[1] != producers[2] == producers[3]
    assert producers[3] == last_producer
    assert last_producer.endswith("@" + (await handle.describe()).run_id)
