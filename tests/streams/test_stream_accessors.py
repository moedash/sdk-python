"""The accessors each context asks for its stream, with one registration.

The provider is registered on the client alone. A worker built from that
client inherits it, an activity on that worker reaches its own workflow's
stream through ``activity.stream_handle()``, and any code holding the client
reaches a stream through ``client.get_stream_handle()``. Without a
registration, both say so with the same error.
"""

from __future__ import annotations

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.streams import (
    BEGINNING,
    DEFAULT_TOPIC,
    END,
    RecordKind,
    StreamClosedError,
    StreamRef,
    StreamUnsupportedError,
    topic,
)
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from tests.helpers import new_worker

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


def _with_provider(client: Client, provider: MemoryStreams) -> Client:
    # The same connection, with the provider registered the way an
    # application registers it: once, on the client.
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


@activity.defn
async def emit(count: int) -> str:
    # No workflow id and no run id: the handle is this activity's own
    # workflow, pinned to its run, and the producer's identity is the
    # activity's.
    model = activity.stream_handle().producer(topic=INPUTS)
    for n in range(count):
        await model.append({"n": n})
    await model.finish()
    return activity.info().activity_id


@workflow.defn
class Echo:
    """Runs the emitting activity and echoes what arrives on ``inputs``."""

    @workflow.run
    async def run(self, count: int) -> list[Any]:
        inputs = workflow.stream_reader(INPUTS)
        decisions = workflow.stream_writer(DECISIONS)
        emitting = workflow.start_activity(
            emit, count, start_to_close_timeout=timedelta(seconds=30)
        )
        seen: list[Any] = []
        async for record in inputs:
            if record.kind is RecordKind.FINISH:
                seen.append(("finish", record.producer_id))
                break
            assert record.value is not None
            seen.append(record.value["n"])
            decisions.publish({"echo": record.value["n"]})
        decisions.finish()
        producer_id = await emitting
        return [*seen, ("activity", producer_id)]


async def test_one_registration_on_the_client_serves_every_context(
    client: Client, provider: MemoryStreams
):
    registered = _with_provider(client, provider)
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    # No plugins on the worker: it inherits the client's provider.
    async with new_worker(registered, Echo, activities=[emit]) as worker:
        handle = await registered.start_workflow(
            Echo.run, 2, id=workflow_id, task_queue=worker.task_queue
        )
        result = await handle.result()
        # The activity wrote as itself onto its own workflow's topic.
        assert result[:2] == [0, 1]
        assert result[2] == ["finish", result[3][1]]

        stream = registered.get_stream_handle(workflow_id)

        async def read_everything() -> list[Any]:
            return [(r.kind, r.value) async for r in stream.read(topic=DECISIONS)]

        assert await asyncio.wait_for(read_everything(), 30) == [
            (RecordKind.DATA, {"echo": 0}),
            (RecordKind.DATA, {"echo": 1}),
            (RecordKind.FINISH, None),
        ]


@activity.defn
async def emit_on_the_default_topic() -> None:
    await activity.stream_handle().producer().append({"from": "activity"})


@workflow.defn
class Answers:
    """Waits for the activity's record on the default topic and answers there."""

    @workflow.run
    async def run(self) -> None:
        reader = workflow.stream_reader(result_type=dict)
        await workflow.execute_activity(
            emit_on_the_default_topic, start_to_close_timeout=timedelta(seconds=30)
        )
        async for value in reader.values():
            workflow.stream_writer().publish({"answer": value["from"]})
            reader.close()


async def test_every_accessor_defaults_to_the_same_topic(
    client: Client, provider: MemoryStreams
):
    registered = _with_provider(client, provider)
    workflow_id = f"streams-wf-{uuid.uuid4().hex}"
    async with new_worker(
        registered, Answers, activities=[emit_on_the_default_topic]
    ) as worker:
        stream = registered.get_stream_handle(workflow_id)
        assert await stream.latest() == BEGINNING
        await registered.execute_workflow(
            Answers.run, id=workflow_id, task_queue=worker.task_queue
        )

        async def read_everything() -> list[Any]:
            return [(r.topic, r.value) async for r in stream.read()]

        assert await asyncio.wait_for(read_everything(), 30) == [
            (DEFAULT_TOPIC, {"from": "activity"}),
            (DEFAULT_TOPIC, {"answer": "activity"}),
        ]


async def test_get_stream_handle_needs_a_registered_provider(client: Client):
    with pytest.raises(StreamUnsupportedError, match="plugins="):
        client.get_stream_handle("wf")


async def test_stream_handle_needs_a_provider_on_the_worker():
    async def ask() -> None:
        activity.stream_handle()

    with pytest.raises(StreamUnsupportedError, match="plugins="):
        await ActivityEnvironment().run(ask)


@activity.defn
def ask_from_a_sync_activity() -> str:
    try:
        activity.stream_handle()
    except RuntimeError as error:
        return str(error)
    return "opened"


@workflow.defn
class RunsASyncActivity:
    """Runs the `def` activity that reaches for a handle."""

    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            ask_from_a_sync_activity, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_a_sync_activity_is_told_it_cannot_have_a_handle(
    client: Client, provider: MemoryStreams
):
    # The worker has a provider. What a `def` activity is missing is the
    # client, so that is what the error has to say, rather than sending the
    # reader to register a provider that is already there.
    registered = _with_provider(client, provider)
    with ThreadPoolExecutor(max_workers=1) as executor:
        async with new_worker(
            registered,
            RunsASyncActivity,
            activities=[ask_from_a_sync_activity],
            activity_executor=executor,
        ) as worker:
            result = await registered.execute_workflow(
                RunsASyncActivity.run,
                id=f"streams-wf-{uuid.uuid4().hex}",
                task_queue=worker.task_queue,
            )
    assert "only available in `async def` activities" in result
    assert "plugins=" not in result


async def test_an_activity_owned_stream_takes_every_read_start(
    client: Client, provider: MemoryStreams
):
    registered = _with_provider(client, provider)
    stream = registered.get_stream_handle(activity_id=f"act-{uuid.uuid4().hex}")
    producer = stream.producer(topic=INPUTS, producer_id="tool", attempt=1)
    await producer.append({"n": 1}, {"n": 2}, {"n": 3})

    async def first(records: Any) -> Any:
        async for record in records:
            await records.aclose()
            return record.value
        return None

    assert await first(stream.read(topic=INPUTS, after=BEGINNING)) == {"n": 1}
    assert await first(stream.read(topic=INPUTS, last=1)) == {"n": 3}
    at_end = stream.read(topic=INPUTS, after=END)
    await producer.append({"n": 4})
    assert await asyncio.wait_for(first(at_end), 10) == {"n": 4}


NOTES = topic("notes", dict)


@activity.defn
async def append_to_ref(ref: StreamRef) -> str:
    # The ref arrived as an argument and names the stream in full, so it
    # takes no scope; the handle it opens writes to the ref's topic.
    try:
        activity.stream_handle(ref, scope="activity")
    except ValueError as error:
        refused = str(error)
    else:
        refused = "accepted"
    await activity.stream_handle(ref).producer().append({"via": "activity"})
    return refused


@workflow.defn
class PublishToRef:
    """Hands a stream ref to an activity and returns it, both as plain data."""

    @workflow.run
    async def run(self, ref: StreamRef) -> tuple[StreamRef, str]:
        refused = await workflow.execute_activity(
            append_to_ref, ref, start_to_close_timeout=timedelta(seconds=30)
        )
        return ref, refused


async def test_a_ref_travels_as_data_and_opens_from_every_context(
    client: Client, provider: MemoryStreams
):
    registered = _with_provider(client, provider)
    shared = await registered.create_stream(f"shared-{uuid.uuid4().hex}")
    ref = shared.ref(topic=NOTES)
    async with new_worker(
        registered, PublishToRef, activities=[append_to_ref]
    ) as worker:
        returned, refused = await registered.execute_workflow(
            PublishToRef.run,
            ref,
            id=f"streams-wf-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    # The workflow argument and result went through the data converter.
    assert returned == ref
    assert "takes no run_id or scope" in refused
    # The activity wrote to the referenced topic, and the client reads it
    # from the ref without naming the topic. Sealing the stream is what ends
    # a full read of it.
    await shared.close()
    records = [
        (r.topic, r.value) async for r in registered.get_stream_handle(ref).read(last=1)
    ]
    assert records == [("notes", {"via": "activity"})]
    with pytest.raises(ValueError, match="takes no run_id"):
        registered.get_stream_handle(ref, run_id="r")


async def test_the_client_creates_reaches_and_closes_a_standalone_stream(
    client: Client, provider: MemoryStreams
):
    registered = _with_provider(client, provider)
    stream_id = f"shared-{uuid.uuid4().hex}"
    created = await registered.create_stream(stream_id, max_records=3)
    producer = created.producer(topic=NOTES, producer_id="writer", attempt=1)
    await producer.append({"n": 1})

    reached = registered.get_stream_handle(stream_id=stream_id)
    assert await reached.latest(topic=NOTES) != BEGINNING
    await reached.close()
    with pytest.raises(StreamClosedError):
        await producer.append({"n": 2})
    assert [r.value async for r in reached.read(topic=NOTES)] == [{"n": 1}]

    with pytest.raises(ValueError, match="no workflow_id"):
        registered.get_stream_handle(stream_id=stream_id, workflow_id="wf")
    with pytest.raises(ValueError, match="empty"):
        await registered.create_stream("")
    with pytest.raises(StreamUnsupportedError, match="plugins="):
        await client.create_stream(stream_id)
    with pytest.raises(StreamUnsupportedError, match="plugins="):
        client.get_stream_handle(stream_id=stream_id)
