"""What the Redis provider does that the conformance suite cannot see.

Runs against the Redis at ``STREAMS_REDIS_URL`` and the dev server from the
test fixtures. Each test gets its own key prefix.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from typing import Any

import pytest
import redis.asyncio
import redis.exceptions

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHandle
from temporalio.contrib.streams import (
    CONTENT_HASH_KEY,
    RUN_ID_KEY,
    StreamClosedError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRef,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._output import StagedBatch, StageRef
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.converter import DataConverter, PayloadCodec
from tests.helpers import new_worker

EVENTS = topic("events", dict)
OTHER = topic("other", dict)

pytestmark = pytest.mark.skipif(
    not os.environ.get("STREAMS_REDIS_URL"),
    reason="set STREAMS_REDIS_URL to run the Redis provider tests",
)


@workflow.defn
class Owner:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@pytest.fixture
async def raw() -> AsyncIterator[redis.asyncio.Redis]:
    client = redis.asyncio.Redis.from_url(os.environ["STREAMS_REDIS_URL"])
    yield client
    await client.aclose()


@pytest.fixture
async def provider() -> AsyncIterator[RedisStreams]:
    streams = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"test-{uuid.uuid4().hex}"
    )
    yield streams
    await streams.close()


@pytest.fixture
async def owner(client: Client) -> AsyncIterator[WorkflowHandle]:
    async with new_worker(client, Owner) as worker:
        handle = await client.start_workflow(
            Owner.run,
            id=f"redis-owner-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
        yield handle
        if (await handle.describe()).status == WorkflowExecutionStatus.RUNNING:
            await handle.terminate()


def client_with(
    client: Client, provider: RedisStreams, converter: DataConverter | None = None
) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    if converter is not None:
        config["data_converter"] = converter
    return Client(**config)


async def log_entries(
    raw: redis.asyncio.Redis, handle: Any, name: str
) -> list[WireRecord]:
    keys = await handle._keys()
    entries = await raw.xrange(keys.log(name))
    return [WireRecord.FromString(fields[b"r"]) for _, fields in entries]


async def test_a_batch_is_one_script_and_keeps_one_high_water_field(
    client: Client, provider: RedisStreams, owner: WorkflowHandle, raw: Any
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    for batch in range(5):
        await producer.append({"batch": batch, "i": 0}, {"batch": batch, "i": 1})
    keys = await stream._keys()
    assert [r.sequence for r in await log_entries(raw, stream, "events")] == [
        *range(1, 11)
    ]
    held = {
        field: value
        for field, value in (await raw.hgetall(keys.meta("events"))).items()
        if field.startswith(b"hw:")
    }
    # Bounded state: one field per producer attempt, however long it writes.
    assert len(held) == 1
    ((field, value),) = held.items()
    assert field == b"hw:1:p:1"
    assert value.split(b"|")[0] == b"9"


async def test_a_retry_of_an_older_batch_is_refused_even_with_its_content(
    client: Client, provider: RedisStreams, owner: WorkflowHandle, raw: Any
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    first = await producer.append({"n": 1})
    await producer.append({"n": 2})
    restarted = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    with pytest.raises(StreamProducerError, match="below the newest"):
        await restarted.append({"n": 1})
    assert first != await stream.latest(topic=EVENTS)
    assert len(await log_entries(raw, stream, "events")) == 2


class NonceCodec(PayloadCodec):
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


async def test_the_digest_is_taken_before_the_codec(
    client: Client, provider: RedisStreams, owner: WorkflowHandle, raw: Any
):
    coded = client_with(client, provider, DataConverter(payload_codec=NonceCodec()))
    stream = provider.get_stream_handle(coded, StreamRef.for_workflow(owner.id))
    first = await stream.producer(topic=EVENTS, producer_id="p", attempt=1).append(
        {"secret": 1}
    )
    retry = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    assert await retry.append({"secret": 1}) == first
    (stored,) = await log_entries(raw, stream, "events")
    assert stored.body.metadata["encoding"] == b"binary/nonce"
    assert CONTENT_HASH_KEY in stored.metadata


async def test_a_lost_connection_is_an_unknown_outcome_and_the_retry_dedupes(
    client: Client,
    provider: RedisStreams,
    owner: WorkflowHandle,
    raw: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    real = provider._append

    async def lost_after_write(*args: Any, **kwargs: Any) -> Any:
        await real(*args, **kwargs)
        raise redis.exceptions.ConnectionError("connection reset")

    monkeypatch.setattr(provider, "_append", lost_after_write)
    with pytest.raises(StreamOutcomeUnknownError):
        await producer.append({"n": 1})
    monkeypatch.setattr(provider, "_append", real)
    # The producer kept its sequence, so the retry matches what landed.
    cursor = await producer.append({"n": 1})
    assert cursor == await stream.latest(topic=EVENTS)
    assert len(await log_entries(raw, stream, "events")) == 1


async def test_ids_with_separators_and_braces_keep_their_own_keys():
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"p:{{x}}-{uuid.uuid4().hex}"
    )
    keys = provider._chain_keys("ns", "a:{b}", "run")
    assert keys.log("t:1").startswith("p%3A%7Bx%7D-")
    assert "{ns:a%3A%7Bb%7D:run}" in keys.log("t:1")
    assert keys.log("t:1").endswith(":t:t%3A1")
    assert provider._chain_keys("ns", "a:b", "c").log("t") != provider._chain_keys(
        "ns", "a", "b:c"
    ).log("t")
    await provider.close()


async def test_an_append_sends_nothing_to_temporal(
    client: Client, provider: RedisStreams, owner: WorkflowHandle
):
    # The owner's first Workflow Task must be done, or its completion would
    # count as a change.
    while (
        before := (await owner.describe()).raw_description.workflow_execution_info
    ).history_length < 4:
        await asyncio.sleep(0.05)
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    await stream.producer(topic=EVENTS, producer_id="p", attempt=1).append({"n": 1})
    after = (await owner.describe()).raw_description.workflow_execution_info
    assert after.history_length == before.history_length


async def test_a_stream_of_a_missing_workflow_is_not_found(
    client: Client, provider: RedisStreams
):
    stream = provider.get_stream_handle(
        client, StreamRef.for_workflow(f"missing-{uuid.uuid4().hex}")
    )
    with pytest.raises(StreamNotFoundError):
        await stream.producer(topic=EVENTS, producer_id="p", attempt=1).append(1)


@workflow.defn
class PublishThenContinue:
    @workflow.run
    async def run(self, runs_left: int) -> None:
        workflow_writer(EVENTS).publish({"runs_left": runs_left})
        workflow_writer(OTHER).publish({"runs_left": runs_left})
        if runs_left:
            workflow.continue_as_new(runs_left - 1)


async def test_a_workflow_publish_lands_in_the_chain_log_once_committed(
    client: Client, provider: RedisStreams, raw: Any
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-publish-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishThenContinue) as worker:
        handle = await streams_client.start_workflow(
            PublishThenContinue.run, 1, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    # Both runs publish into one log, keyed by the chain's first run.
    events = await log_entries(raw, stream, "events")
    first_run = handle.first_execution_run_id
    last_run = (await handle.describe()).run_id
    assert [r.metadata[RUN_ID_KEY].data.decode() for r in events] == [
        first_run,
        last_run,
    ]
    assert len(await log_entries(raw, stream, "other")) == 2
    keys = await stream._keys()
    assert handle.first_execution_run_id is not None
    assert keys.base.endswith(f":{handle.first_execution_run_id}}}")
    # Every stage was promoted, so none is left behind.
    assert [
        key async for key in raw.scan_iter(match=f"{keys.base[:20]}*:stage:*")
    ] == []


async def test_appends_trim_to_retention_and_slide_the_expiry(
    client: Client, owner: WorkflowHandle, raw: Any
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        retention=timedelta(milliseconds=500),
    )
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    await producer.append({"n": 1}, {"n": 2})
    keys = await stream._keys()
    assert 0 < await raw.pttl(keys.log("events")) <= 500
    # The meta outlives the log by the tombstone grace.
    assert await raw.pttl(keys.meta("events")) > 29 * 24 * 3600 * 1000
    await asyncio.sleep(0.6)
    await producer.append({"n": 3})
    assert [r.sequence for r in await log_entries(raw, stream, "events")] == [3]
    meta = await raw.hgetall(keys.meta("events"))
    assert meta[b"added"] == b"3"
    assert meta[b"last"] == (await raw.xrange(keys.log("events")))[-1][0]
    await provider.close()


async def test_a_stage_expires_with_retention_and_a_promotion_keeps_the_log(
    raw: Any,
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        retention=timedelta(seconds=30),
    )
    record = WireRecord(topic="events")
    batch = StagedBatch("ns", "wf", "first", "run", [record, record])
    token = await provider._stage(batch)
    keys = provider._chain_keys("ns", "wf", "first")
    assert 0 < await raw.pttl(keys.stage(token)) <= 30_000
    await provider._promote(StageRef("ns", "wf", "first", token, ("events",)))
    assert await raw.exists(keys.stage(token)) == 0
    assert await raw.xlen(keys.log("events")) == 2
    assert 0 < await raw.pttl(keys.log("events")) <= 30_000
    assert (await raw.hgetall(keys.meta("events")))[b"added"] == b"2"
    # Promoting again does nothing: the stage is gone.
    await provider._promote(StageRef("ns", "wf", "first", token, ("events",)))
    assert await raw.xlen(keys.log("events")) == 2
    await provider.close()


def test_retention_must_be_positive():
    with pytest.raises(ValueError, match="retention"):
        RedisStreams("redis://localhost:1", retention=timedelta(0))


@workflow.defn
class PublishUntilTold:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self, continue_once: bool) -> None:
        workflow_writer(EVENTS).publish({"run": "started"})
        if continue_once:
            workflow.continue_as_new(False)
        await workflow.wait_condition(lambda: self.done)
        workflow_writer(EVENTS).publish({"run": "last"})

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def wait_closed(raw: Any, keys: Any) -> None:
    for _ in range(100):
        if await raw.hget(keys.chain(), "closed"):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("the chain was never marked closed")


async def test_the_worker_closes_the_streams_of_an_ended_chain(
    client: Client, provider: RedisStreams, raw: Any
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-close-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishUntilTold) as worker:
        handle = await streams_client.start_workflow(
            PublishUntilTold.run, False, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
        producer = stream.producer(topic=OTHER, producer_id="backend", attempt=1)
        landed = await producer.append({"n": 1})
        await handle.signal(PublishUntilTold.finish)
        await handle.result()
        keys = await stream._keys()
        await wait_closed(raw, keys)

    with pytest.raises(StreamClosedError):
        await producer.append({"n": 2})
    # A retry of a batch that landed before the close still finds it.
    retry = stream.producer(topic=OTHER, producer_id="backend", attempt=1)
    retry._checked_owner = True
    assert await retry.append({"n": 1}) == landed
    # The final Workflow Task's own publish is committed output, never refused.
    events = await log_entries(raw, stream, "events")
    assert [r.body.data for r in events][-1] == b'{"run":"last"}'


async def test_a_new_producer_closes_an_ended_chain_the_worker_missed(
    client: Client, provider: RedisStreams, owner: WorkflowHandle, raw: Any
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    await stream.producer(topic=EVENTS, producer_id="p", attempt=1).append(1)
    await owner.terminate()
    keys = await stream._keys()
    assert not await raw.hget(keys.chain(), "closed")
    late = stream.producer(topic=EVENTS, producer_id="q", attempt=1)
    with pytest.raises(StreamClosedError):
        await late.append(2)
    assert await raw.hget(keys.chain(), "closed") == b"1"
    assert 0 < await raw.pttl(keys.chain())


async def test_continue_as_new_does_not_close_the_chain(
    client: Client, provider: RedisStreams, raw: Any
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-can-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishUntilTold) as worker:
        handle = await streams_client.start_workflow(
            PublishUntilTold.run, True, id=workflow_id, task_queue=worker.task_queue
        )
        stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
        for _ in range(100):
            if (await handle.describe()).run_id != handle.first_execution_run_id:
                break
            await asyncio.sleep(0.05)
        await stream.producer(topic=OTHER, producer_id="p", attempt=1).append(1)
        keys = await stream._keys()
        assert not await raw.hget(keys.chain(), "closed")
        await handle.signal(PublishUntilTold.finish)
        await handle.result()


class StopsBeforePromoting(RedisStreams):
    """Stands in for a Worker that died between its commit and the promotion."""

    async def _promote(self, stage: StageRef) -> None:
        pass


@workflow.defn
class PublishThenWait:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def test_a_replay_promotes_output_a_stopped_worker_committed(
    client: Client, raw: Any
):
    prefix = f"test-{uuid.uuid4().hex}"
    url = os.environ["STREAMS_REDIS_URL"]
    first = StopsBeforePromoting(url, key_prefix=prefix)
    workflow_id = f"redis-repair-{uuid.uuid4().hex}"
    task_queue = f"redis-repair-{uuid.uuid4().hex}"
    first_client = client_with(client, first)
    # No cache, so nothing sticks to this Worker once it is gone.
    async with new_worker(
        first_client, PublishThenWait, task_queue=task_queue, max_cached_workflows=0
    ):
        handle = await first_client.start_workflow(
            PublishThenWait.run, id=workflow_id, task_queue=task_queue
        )
        for _ in range(100):
            if (
                await handle.describe()
            ).raw_description.workflow_execution_info.history_length >= 5:
                break
            await asyncio.sleep(0.05)
    stream = first.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    keys = await stream._keys()
    assert await raw.exists(keys.log("events")) == 0
    assert len([key async for key in raw.scan_iter(match=f"{prefix}*:stage:*")]) == 1

    second = RedisStreams(url, key_prefix=prefix)
    second_client = client_with(client, second)
    async with new_worker(
        second_client, PublishThenWait, task_queue=task_queue, max_cached_workflows=0
    ):
        await handle.signal(PublishThenWait.finish)
        await handle.result()

    # The replay found the committed stage and promoted it, exactly once.
    assert [
        r.metadata[RUN_ID_KEY].data.decode()
        for r in await log_entries(raw, stream, "events")
    ] == [handle.first_execution_run_id]
    assert [key async for key in raw.scan_iter(match=f"{prefix}*:stage:*")] == []
    await first.close()
    await second.close()
