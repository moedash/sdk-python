"""Reaching a Workflow's stream from an Activity or a client.

An Activity writes to the stream of the Workflow that scheduled it as
itself, so its retry is reported to readers as ``SUPERSEDED``. A client
reaches a Workflow's stream by Workflow id through its store.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Sequence
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamClosedError,
    StreamCursorError,
    StreamNotFoundError,
    StreamProducerError,
    StreamRef,
    Supersession,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.contrib.streams.test_workflow_writer import EVENTS, Publisher
from tests.helpers import new_worker

TOKENS = topic("tokens", str)


@workflow.defn
class Waits:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def running_owner(client: Client, task_queue: str) -> str:
    # Core asks the server about the owner, so the stream's Workflow must exist.
    workflow_id = new_workflow_id()
    await client.start_workflow(Waits.run, id=workflow_id, task_queue=task_queue)
    return workflow_id


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
    streams_client = await connect_with(client, MemoryStreams())
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
    described = await streams_client.get_workflow_handle(workflow_id).describe()
    # Core derives the same producer id the handle reports.
    producer_id = f"tokens@{described.run_id}"
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
async def stream_without_store() -> str:
    try:
        activity_handle()
    except ValueError as error:
        return str(error)
    return "opened"


@workflow.defn
class RunsActivityWithoutStore:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            stream_without_store, start_to_close_timeout=timedelta(seconds=10)
        )


async def test_an_activity_without_a_store_is_told(client: Client):
    async with new_worker(
        client, RunsActivityWithoutStore, activities=[stream_without_store]
    ) as worker:
        said = await client.execute_workflow(
            RunsActivityWithoutStore.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert "no stream store is registered" in said


async def test_a_client_handle_follows_the_chain_or_pins_a_run(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    follower = get_stream_handle(streams_client, "wf")
    assert follower.ref == StreamRef.for_workflow("wf")
    pinned = get_stream_handle(streams_client, "wf", run_id="run", topic=EVENTS)
    assert pinned.ref == StreamRef.for_workflow("wf", run_id="run", topic=EVENTS)
    assert get_stream_handle(streams_client, pinned.ref).ref == pinned.ref
    with pytest.raises(ValueError, match="carries its own"):
        get_stream_handle(streams_client, pinned.ref, run_id="other")
    with pytest.raises(ValueError, match="no stream store is registered"):
        get_stream_handle(client, "wf")


async def test_a_client_producer_and_the_workflow_share_a_topic(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, Publisher) as worker:
        handle = await streams_client.start_workflow(
            Publisher.run, 0, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    stream = get_stream_handle(streams_client, workflow_id)
    records = await read_all(stream.read(topic=EVENTS))
    assert [(r.producer_id, r.value) for r in records] == [
        ("", {"step": "init"}),
        ("", None),
    ]
    # The chain closed, so an outside producer is refused there.
    outside = stream.producer(topic=EVENTS, producer_id="backend", attempt=1)
    with pytest.raises(StreamClosedError):
        await outside.append({"from": "backend"})


async def test_a_producer_retry_dedupes_and_a_divergent_one_is_refused(
    client: Client,
):
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Waits) as worker:
        workflow_id = await running_owner(streams_client, worker.task_queue)
        stream = get_stream_handle(streams_client, workflow_id, topic=TOKENS)
        assert await stream.latest() == BEGINNING
        producer = stream.producer(producer_id="p", attempt=1)
        assert await producer.append() == BEGINNING
        first = await producer.append("a", "b")
        assert await producer.append() == first
        assert await stream.latest() == first
        # A second object for the same session starts at one again: a repeat
        # of the newest batch dedupes to the original position.
        again = stream.producer(producer_id="p", attempt=1)
        assert await again.append("a", "b") == first
        divergent = stream.producer(producer_id="p", attempt=1)
        with pytest.raises(StreamProducerError):
            await divergent.append("x", "y")
        owner = streams_client.get_workflow_handle(workflow_id)
        await owner.signal(Waits.finish)
        await owner.result()
    records = await read_all(stream.read())
    assert [r.value for r in records] == ["a", "b"]


async def test_a_resumed_read_continues_after_its_cursor(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Waits) as worker:
        workflow_id = await running_owner(streams_client, worker.task_queue)
        stream = get_stream_handle(streams_client, workflow_id, topic=TOKENS)
        producer = stream.producer(producer_id="p", attempt=1)
        middle = await producer.append("a", "b")
        await producer.append("c")
        # A read from END starts at its first iteration, so a reader that must
        # not miss the next record positions itself with latest() first.
        live = stream.read(after=await stream.latest())
        await producer.append("d")
        assert (await anext(live)).value == "d"
        await live.aclose()
        with pytest.raises(StreamCursorError):
            await anext(stream.read(after=Cursor("not a cursor")))
        owner = streams_client.get_workflow_handle(workflow_id)
        await owner.signal(Waits.finish)
        await owner.result()
    assert [r.value for r in await read_all(stream.read(after=middle))] == ["c", "d"]


async def test_a_stream_without_an_owner_is_not_found(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    stream = get_stream_handle(streams_client, f"no-such-owner-{uuid.uuid4()}")
    with pytest.raises(StreamNotFoundError):
        await stream.producer(producer_id="p", attempt=1).append("a")


async def test_a_producer_refuses_a_bad_identity(client: Client):
    stream = get_stream_handle(await connect_with(client, MemoryStreams()), "wf")
    with pytest.raises(ValueError, match="producer_id"):
        stream.producer(producer_id="", attempt=1)
    for attempt in (0, True, 1.5):
        with pytest.raises(ValueError, match="attempt"):
            stream.producer(producer_id="p", attempt=attempt)  # type: ignore[arg-type]


class NonceCodec(PayloadCodec):
    """Encrypts with a fresh nonce per call, so two encodings never match."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=uuid.uuid4().bytes + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[16:]) for p in payloads]


async def test_a_retry_through_a_nonce_codec_still_dedupes(client: Client):
    coded = await connect_with(
        client, MemoryStreams(), DataConverter(payload_codec=NonceCodec())
    )
    async with new_worker(coded, Waits) as worker:
        workflow_id = await running_owner(coded, worker.task_queue)
        stream = get_stream_handle(coded, workflow_id, topic=TOKENS)
        first = await stream.producer(producer_id="p", attempt=1).append("a")
        retry = stream.producer(producer_id="p", attempt=1)
        # The ciphertext differs, and the digest lang took before the codec does not.
        assert await retry.append("a") == first
        owner = coded.get_workflow_handle(workflow_id)
        await owner.signal(Waits.finish)
        await owner.result()
    assert [r.value for r in await read_all(stream.read())] == ["a"]


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
    streams_client = await connect_with(client, MemoryStreams())
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
