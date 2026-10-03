"""Live checks for what the Redis provider decides about an activity's streams.

``test_activity_streams`` runs the shared cases on this provider behind
``STREAMS_LIVE=redis``. This module covers what only this store does: how a
read learns that a workflow's activity is terminal without the server saying
so, that an activity that never wrote is read until its workflow closes,
that retention trims an activity's stream like any other, that nothing
gates an append once the owner is terminal, and that an activity's streams
belong to the run its execution is in, so an id started again in a new run
starts new ones. All need a dev server (``TEMPORAL_ADDRESS`` or the test
environment's own) and a Redis (``TEMPORAL_TEST_REDIS_URL`` or
``AI198_REDIS_URL``).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.streams import BEGINNING, RecordKind, StreamCursorError
from temporalio.streams.providers.redis import RedisStreams
from temporalio.testing import WorkflowEnvironment
from tests.helpers import new_worker
from tests.streams.test_activity_streams import (
    TOKENS,
    RunsOneActivity,
    read_all,
    summary,
    write_by_default,
    write_to_own_streams,
)
from tests.streams.test_streams_conformance import StreamHost, take

pytestmark = pytest.mark.skipif(
    os.environ.get("STREAMS_LIVE") != "redis",
    reason="needs a live server and redis; run with STREAMS_LIVE=redis",
)


def redis_url() -> str:
    return os.environ.get("TEMPORAL_TEST_REDIS_URL") or os.environ.get(
        "AI198_REDIS_URL", "redis://127.0.0.1:6379"
    )


@pytest.fixture
async def live(client: Client, env: WorkflowEnvironment) -> AsyncIterator[Client]:
    """A client with a fresh provider registered, on a server with standalone activities."""
    if env.supports_time_skipping:
        pytest.skip("the time-skipping test server has no standalone activities")
    address = os.environ.get("TEMPORAL_ADDRESS")
    if address:
        client = await Client.connect(
            address, namespace=os.environ.get("TEMPORAL_NAMESPACE", "default")
        )
    provider = RedisStreams(
        url=redis_url(),
        # A prefix per case, because the store keeps what earlier cases wrote.
        key_prefix=f"streams-redis-activity-{uuid.uuid4().hex}",
        poll_interval=timedelta(milliseconds=100),
    )
    config = client.config()
    config["plugins"] = [provider]
    try:
        yield Client(**config)
    finally:
        await provider.close()


@activity.defn
async def write_nothing(label: str) -> str:
    return label


@workflow.defn
class RunsOneActivityThenWaits:
    """Runs one activity by name, then stays open until released."""

    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self, name: str, label: str) -> str:
        result = await workflow.execute_activity(
            name,
            label,
            activity_id="streamer",
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.wait_condition(lambda: self._released)
        return result


async def test_a_read_opened_after_the_activity_finished_ends_by_itself(
    live: Client,
):
    # The server does not describe a workflow's activity, so the provider ends
    # the read once the activity is no longer pending and its stream exists.
    workflow_id = f"streams-redis-wfa-{uuid.uuid4().hex}"
    async with new_worker(
        live, RunsOneActivityThenWaits, activities=[write_to_own_streams]
    ) as worker:
        handle = await live.start_workflow(
            RunsOneActivityThenWaits.run,
            args=["write_to_own_streams", "to the activity"],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        own = live.get_stream_handle(workflow_id, activity_id="streamer")
        # The first read follows the activity to its FINISH while it runs.
        await take(own.read(topic=TOKENS), 2, 30)
        # A read opened afterwards ends on its own, with the workflow still open.
        records = await read_all(own.read(topic=TOKENS), timeout=10)
        assert summary(records) == [
            (RecordKind.DATA, 1, "to the activity"),
            (RecordKind.FINISH, 1, None),
        ]
        assert (await handle.describe()).status is not None
        await handle.signal(RunsOneActivityThenWaits.release)
        await handle.result()


async def test_an_activity_that_never_wrote_is_read_until_its_workflow_closes(
    live: Client,
):
    workflow_id = f"streams-redis-wfa-silent-{uuid.uuid4().hex}"
    async with new_worker(
        live, RunsOneActivityThenWaits, activities=[write_nothing]
    ) as worker:
        handle = await live.start_workflow(
            RunsOneActivityThenWaits.run,
            args=["write_nothing", "quiet"],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        assert await handle.query("__temporal_workflow_metadata") is not None
        own = live.get_stream_handle(workflow_id, activity_id="streamer")
        reading = asyncio.ensure_future(read_all(own.read(topic=TOKENS), timeout=30))
        # No stream exists for an activity that wrote nothing, so the read has
        # nothing to say the activity is over and waits for the workflow.
        await asyncio.sleep(1.0)
        assert not reading.done()
        await handle.signal(RunsOneActivityThenWaits.release)
        await handle.result()
        assert await reading == []


async def test_retention_trims_an_activity_stream_too(client: Client):
    address = os.environ.get("TEMPORAL_ADDRESS")
    if address:
        client = await Client.connect(address)
    provider = RedisStreams(
        url=redis_url(),
        key_prefix=f"streams-redis-activity-{uuid.uuid4().hex}",
        poll_interval=timedelta(milliseconds=100),
        max_len=2,
    )
    config = client.config()
    config["plugins"] = [provider]
    live = Client(**config)
    workflow_id = f"streams-redis-wfa-trim-{uuid.uuid4().hex}"
    try:
        async with new_worker(live, StreamHost) as worker:
            host = await live.start_workflow(
                StreamHost.run, id=workflow_id, task_queue=worker.task_queue
            )
            stream = live.get_stream_handle(workflow_id, activity_id="tool")
            producer = stream.producer(topic=TOKENS, producer_id="tool", attempt=1)
            first = await producer.append({"token": "one"})
            assert first is not None
            await producer.append({"token": "two"}, {"token": "three"})
            # The window holds two entries, so the first record is gone.
            records = await take(stream.read(topic=TOKENS), 2, 10)
            assert [r.value["token"] for r in records] == ["two", "three"]
            with pytest.raises(StreamCursorError, match="retention has trimmed"):
                await take(stream.read(topic=TOKENS, after=first), 1, 10)
            await host.terminate()
    finally:
        await provider.close()


async def test_an_append_after_the_owner_is_terminal_still_lands(live: Client):
    # The store has no gate the server would have: a late attempt writes, and
    # a reader that already ended is not told. The handle names no run, so it
    # describes the finished activity and lands on that run's stream.
    activity_id = f"streams-redis-saa-late-{uuid.uuid4().hex}"
    async with new_worker(live, activities=[write_by_default]) as worker:
        handle = await live.start_activity(
            write_by_default,
            "standalone",
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
        assert await handle.result() == activity_id
    stream = live.get_stream_handle(activity_id=activity_id)
    before = await stream.latest(topic=TOKENS)
    assert before != BEGINNING
    late = stream.producer(topic=TOKENS, producer_id=activity_id, attempt=2)
    after = await late.append({"token": "late"})
    assert after != before
    records = await read_all(stream.read(topic=TOKENS), timeout=10)
    assert summary(records) == [
        (RecordKind.DATA, 1, "standalone"),
        (RecordKind.FINISH, 1, None),
        (RecordKind.SUPERSEDED, 2, None),
        (RecordKind.DATA, 2, "late"),
    ]
    # Pinned to the run, the same stream reads the same way.
    pinned = live.get_stream_handle(activity_id=activity_id, run_id=handle.run_id)
    assert summary(await read_all(pinned.read(topic=TOKENS), timeout=10)) == summary(
        records
    )


async def test_a_standalone_activity_id_started_again_starts_a_new_stream(
    live: Client,
):
    # The stream belongs to the activity execution, which is its run, not to
    # the id: the second execution under the same id writes to a new stream,
    # a handle without a run reads the current one, and a run pins the other.
    activity_id = f"streams-redis-saa-again-{uuid.uuid4().hex}"
    async with new_worker(live, activities=[write_by_default]) as worker:
        runs = []
        for label in ("first", "second"):
            handle = await live.start_activity(
                write_by_default,
                label,
                id=activity_id,
                task_queue=worker.task_queue,
                start_to_close_timeout=timedelta(seconds=30),
            )
            assert await handle.result() == activity_id
            runs.append(handle.run_id)
    assert runs[0] != runs[1]
    current = live.get_stream_handle(activity_id=activity_id)
    assert summary(await read_all(current.read(topic=TOKENS), timeout=10)) == [
        (RecordKind.DATA, 1, "second"),
        (RecordKind.FINISH, 1, None),
    ]
    earlier = live.get_stream_handle(activity_id=activity_id, run_id=runs[0])
    assert summary(await read_all(earlier.read(topic=TOKENS), timeout=10)) == [
        (RecordKind.DATA, 1, "first"),
        (RecordKind.FINISH, 1, None),
    ]


async def test_a_workflow_activity_in_a_new_run_starts_a_new_stream(live: Client):
    # The same workflow id run again schedules the same activity id; keyed by
    # the workflow's run, the two executions keep their streams apart.
    workflow_id = f"streams-redis-wfa-again-{uuid.uuid4().hex}"
    async with new_worker(
        live, RunsOneActivity, activities=[write_to_own_streams]
    ) as worker:
        runs = []
        for label in ("first", "second"):
            handle = await live.start_workflow(
                RunsOneActivity.run,
                args=["write_to_own_streams", label],
                id=workflow_id,
                task_queue=worker.task_queue,
            )
            await handle.result()
            runs.append(handle.result_run_id)
    assert runs[0] != runs[1]
    current = live.get_stream_handle(workflow_id, activity_id="streamer")
    assert summary(await read_all(current.read(topic=TOKENS), timeout=10)) == [
        (RecordKind.DATA, 1, "second"),
        (RecordKind.FINISH, 1, None),
    ]
    earlier = live.get_stream_handle(
        workflow_id, activity_id="streamer", run_id=runs[0]
    )
    assert summary(await read_all(earlier.read(topic=TOKENS), timeout=10)) == [
        (RecordKind.DATA, 1, "first"),
        (RecordKind.FINISH, 1, None),
    ]
