"""Streams owned by activities, and which stream an activity's accessor reaches.

``activity.stream_handle()`` resolves by a static rule, never by what exists:
an activity a workflow scheduled reaches its workflow's stream, a standalone
activity reaches its own, and ``scope="activity"`` gives an activity a
workflow scheduled its own streams. An activity's own streams are one per
activity execution, so a retry writes to the same stream and a reader sees
the attempt change as ``SUPERSEDED``.

The memory provider always runs. A storage provider adds itself to
``SETUPS`` behind its own ``STREAMS_LIVE`` gate: its setup receives the
environment's client and hands back the provider and a client with it
registered, which the workers and the reads in these cases share.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.streams import (
    BEGINNING,
    RecordKind,
    StreamProvider,
    topic,
)
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.streams.providers.native import NativeStreams
from temporalio.testing import WorkflowEnvironment
from tests.helpers import new_worker

TOKENS = topic("tokens", dict)


@dataclass
class ActivitySetup:
    """A provider under test, registered on the client the cases use."""

    name: str
    provider: StreamProvider
    client: Client


async def _memory_setup(client: Client) -> AsyncIterator[ActivitySetup]:
    provider = MemoryStreams(poll_interval=timedelta(milliseconds=50))
    config = client.config()
    config["plugins"] = [provider]
    yield ActivitySetup("memory", provider, Client(**config))
    provider.reset()


async def _native_setup(client: Client) -> AsyncIterator[ActivitySetup]:
    # The store is a server built from the stream-carrying branch, which the
    # test environment's own server is not; TEMPORAL_ADDRESS names it.
    address = os.environ.get("TEMPORAL_ADDRESS")
    if address:
        client = await Client.connect(
            address, namespace=os.environ.get("TEMPORAL_NAMESPACE", "default")
        )
    provider = NativeStreams()
    config = client.config()
    config["plugins"] = [provider]
    yield ActivitySetup("native", provider, Client(**config))
    await provider.close()


SETUPS: dict[str, Callable[[Client], AsyncIterator[ActivitySetup]]] = {
    "memory": _memory_setup
}
if os.environ.get("STREAMS_LIVE") == "native":
    SETUPS["native"] = _native_setup


@pytest.fixture(params=sorted(SETUPS))
async def setup(
    request: pytest.FixtureRequest, client: Client, env: WorkflowEnvironment
) -> AsyncIterator[ActivitySetup]:
    if env.supports_time_skipping:
        pytest.skip("the time-skipping test server has no standalone activities")
    async for found in SETUPS[request.param](client):
        yield found


async def read_all(records: Any, timeout: float = 30.0) -> list:
    """Read until the stream ends, which is when its owner does."""

    async def _collect() -> list:
        return [record async for record in records]

    return await asyncio.wait_for(_collect(), timeout)


def summary(records: list) -> list[tuple[Any, ...]]:
    return [
        (r.kind, r.attempt, r.value["token"] if r.kind is RecordKind.DATA else None)
        for r in records
    ]


@activity.defn
async def write_by_default(label: str) -> str:
    # No arguments: where this lands depends only on where the activity runs.
    producer = activity.stream_handle().producer(topic=TOKENS)
    await producer.append({"token": label})
    await producer.finish()
    return activity.info().activity_id


@activity.defn
async def write_to_own_streams(label: str) -> str:
    producer = activity.stream_handle(scope="activity").producer(topic=TOKENS)
    await producer.append({"token": label})
    await producer.finish()
    return activity.info().activity_id


@workflow.defn
class RunsOneActivity:
    """Runs one activity by name and returns what it returned."""

    @workflow.run
    async def run(self, name: str, label: str) -> str:
        return await workflow.execute_activity(
            name,
            label,
            activity_id="streamer",
            start_to_close_timeout=timedelta(seconds=30),
        )


async def test_workflow_activity_defaults_to_its_workflow(setup: ActivitySetup):
    client = setup.client
    workflow_id = f"streams-wfa-{uuid.uuid4().hex}"
    async with new_worker(
        client, RunsOneActivity, activities=[write_by_default]
    ) as worker:
        await client.execute_workflow(
            RunsOneActivity.run,
            args=["write_by_default", "to the workflow"],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        records = await read_all(
            client.get_stream_handle(workflow_id).read(topic=TOKENS)
        )
        assert summary(records) == [
            (RecordKind.DATA, 1, "to the workflow"),
            (RecordKind.FINISH, 1, None),
        ]
        assert all(r.producer_id == "streamer" for r in records)
        own = client.get_stream_handle(workflow_id, activity_id="streamer")
        assert await own.latest(topic=TOKENS) == BEGINNING


async def test_scope_activity_gives_a_workflow_activity_its_own_streams(
    setup: ActivitySetup,
):
    client = setup.client
    workflow_id = f"streams-wfa-own-{uuid.uuid4().hex}"
    async with new_worker(
        client, RunsOneActivity, activities=[write_to_own_streams]
    ) as worker:
        await client.execute_workflow(
            RunsOneActivity.run,
            args=["write_to_own_streams", "to the activity"],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        own = client.get_stream_handle(workflow_id, activity_id="streamer")
        assert summary(await read_all(own.read(topic=TOKENS))) == [
            (RecordKind.DATA, 1, "to the activity"),
            (RecordKind.FINISH, 1, None),
        ]
        # The workflow's topic of the same name is a different stream.
        workflow_stream = client.get_stream_handle(workflow_id)
        assert await workflow_stream.latest(topic=TOKENS) == BEGINNING


async def test_standalone_activity_defaults_to_its_own_stream(setup: ActivitySetup):
    client = setup.client
    activity_id = f"streams-saa-{uuid.uuid4().hex}"
    async with new_worker(client, activities=[write_by_default]) as worker:
        handle = await client.start_activity(
            write_by_default,
            "standalone",
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
        assert await handle.result() == activity_id
        stream = client.get_stream_handle(activity_id=activity_id)
        # The read ends by itself: the activity reached a terminal status.
        records = await read_all(stream.read(topic=TOKENS))
        assert summary(records) == [
            (RecordKind.DATA, 1, "standalone"),
            (RecordKind.FINISH, 1, None),
        ]
        assert all(r.producer_id == activity_id for r in records)


@activity.defn
async def fail_once_after_writing() -> None:
    attempt = activity.info().attempt
    producer = activity.stream_handle().producer(topic=TOKENS)
    await producer.append({"token": f"attempt {attempt}"})
    if attempt == 1:
        raise RuntimeError("the first attempt fails after writing")
    await producer.finish()


async def test_a_retry_inherits_the_stream_and_supersedes(setup: ActivitySetup):
    client = setup.client
    activity_id = f"streams-saa-retry-{uuid.uuid4().hex}"
    async with new_worker(client, activities=[fail_once_after_writing]) as worker:
        handle = await client.start_activity(
            fail_once_after_writing,
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=200), maximum_attempts=2
            ),
        )
        await handle.result()
        records = await read_all(
            client.get_stream_handle(activity_id=activity_id).read(topic=TOKENS)
        )
        # One stream across both attempts, with the takeover reported between.
        assert summary(records) == [
            (RecordKind.DATA, 1, "attempt 1"),
            (RecordKind.SUPERSEDED, 2, None),
            (RecordKind.DATA, 2, "attempt 2"),
            (RecordKind.FINISH, 2, None),
        ]
        superseded = records[1].supersession
        assert superseded is not None
        assert (superseded.previous_attempt, superseded.attempt) == (1, 2)


@activity.defn
async def ask_for_misaddressed_handles() -> list[str]:
    errors: list[str] = []
    try:
        activity.stream_handle(scope="workflow")
    except RuntimeError as error:
        errors.append(f"RuntimeError: {error}")
    try:
        activity.stream_handle("some-workflow", scope="activity")
    except ValueError as error:
        errors.append(f"ValueError: {error}")
    return errors


async def test_a_standalone_activity_has_no_workflow_to_address(setup: ActivitySetup):
    client = setup.client
    async with new_worker(client, activities=[ask_for_misaddressed_handles]) as worker:
        errors = await client.execute_activity(
            ask_for_misaddressed_handles,
            id=f"streams-saa-errors-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
    assert len(errors) == 2
    assert errors[0].startswith("RuntimeError: this activity belongs to no workflow")
    assert errors[1].startswith("ValueError: scope='activity'")


async def test_get_stream_handle_needs_an_owner(setup: ActivitySetup):
    with pytest.raises(ValueError, match="workflow_id or the activity_id"):
        setup.client.get_stream_handle()
