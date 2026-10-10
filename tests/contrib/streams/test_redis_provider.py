"""What the Redis provider does that the conformance suite cannot see.

Runs against the Redis at ``STREAMS_REDIS_URL`` and the dev server from the
test fixtures. Each test gets its own key prefix.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
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
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    END,
    RUN_ID_KEY,
    StreamClosedError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRef,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._body import (
    CONTENT_HASH_KEY,
)
from temporalio.contrib.streams._output import StagedBatch, StageRef
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
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
    retry._owner_checked_at = time.monotonic()
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


async def read_until_end(records: Any, timeout: float = 15.0) -> list:
    out: list = []

    async def pull() -> None:
        async for record in records:
            out.append(record)

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    return out


async def test_a_read_follows_the_chain_and_marks_it_closed(
    client: Client, provider: RedisStreams, raw: Any
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-follow-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishUntilTold) as worker:
        handle = await streams_client.start_workflow(
            PublishUntilTold.run, True, id=workflow_id, task_queue=worker.task_queue
        )
        stream = get_stream_handle(streams_client, workflow_id, topic=EVENTS)
        records = stream.read()
        first = await asyncio.wait_for(records.__anext__(), 10.0)
        await handle.signal(PublishUntilTold.finish)
        rest = await read_until_end(records)
        keys = await stream._keys()  # type: ignore[attr-defined]
    values = [r.value for r in [first, *rest]]
    assert values == [{"run": "started"}, {"run": "started"}, {"run": "last"}]
    runs = [r.run_id for r in [first, *rest]]
    assert runs[0] == handle.first_execution_run_id
    assert runs[1] == runs[2] != runs[0]
    # The read saw the chain end, and says so to later producers.
    assert await raw.hget(keys.chain(), "closed") == b"1"


async def test_a_read_pinned_to_a_run_ends_when_that_run_continues(
    client: Client, provider: RedisStreams
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-pinned-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishUntilTold) as worker:
        handle = await streams_client.start_workflow(
            PublishUntilTold.run, True, id=workflow_id, task_queue=worker.task_queue
        )
        pinned = provider.get_stream_handle(
            client,
            StreamRef.for_workflow(
                workflow_id, run_id=handle.first_execution_run_id, topic=EVENTS
            ),
        )
        records = await read_until_end(pinned.read())
        # It stopped with the first run, though the chain is still running.
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        assert len(records) >= 1
        await handle.signal(PublishUntilTold.finish)
        await handle.result()


async def nothing_arrives(records: Any, wait: float = 0.5) -> bool:
    try:
        await asyncio.wait_for(records.__anext__(), wait)
        return False
    except asyncio.TimeoutError:
        return True
    finally:
        await records.aclose()


async def test_retention_expires_a_cursor_only_past_the_newest_trimmed_record(
    client: Client, owner: WorkflowHandle
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        retention=timedelta(milliseconds=300),
        poll_interval=timedelta(milliseconds=100),
    )
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    first = await producer.append({"n": 1})
    second = await producer.append({"n": 2})
    await asyncio.sleep(0.4)
    await producer.append({"n": 3})
    with pytest.raises(StreamExpiredError, match="dropped by retention"):
        await stream.read(topic=EVENTS, after=first).__anext__()
    # Nothing after the newest trimmed record was lost, so it still resumes.
    records = stream.read(topic=EVENTS, after=second)
    assert (await asyncio.wait_for(records.__anext__(), 5.0)).value == {"n": 3}
    await records.aclose()
    await provider.close()


async def test_an_expired_log_leaves_a_tombstone_that_tells_expired_from_empty(
    client: Client, owner: WorkflowHandle, raw: Any
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        retention=timedelta(milliseconds=300),
        poll_interval=timedelta(milliseconds=100),
    )
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    first = await producer.append({"n": 1})
    last = await producer.append({"n": 2})
    await asyncio.sleep(0.5)
    keys = await stream._keys()
    assert await raw.exists(keys.log("events")) == 0
    assert await raw.exists(keys.meta("events")) == 1
    with pytest.raises(StreamExpiredError, match="expired"):
        await stream.read(topic=EVENTS, after=first).__anext__()
    # Nothing came after the last record, so the stream is empty after it.
    assert await nothing_arrives(stream.read(topic=EVENTS, after=last))
    await raw.delete(keys.meta("events"))
    with pytest.raises(StreamNotFoundError, match="no tombstone"):
        await stream.read(topic=EVENTS, after=last).__anext__()
    # A read from the beginning of a missing log just waits for records.
    assert await nothing_arrives(stream.read(topic=EVENTS))
    await provider.close()


async def test_a_server_older_than_redis_7_is_refused(
    client: Client, provider: RedisStreams, monkeypatch: pytest.MonkeyPatch
):
    async def old_server(*_: Any) -> dict[str, str]:
        return {"redis_version": "6.2.14"}

    monkeypatch.setattr(provider._redis, "info", old_server)
    stream = provider.get_stream_handle(client, StreamRef.for_workflow("any"))
    with pytest.raises(StreamUnsupportedError, match="Redis 7.0 or later"):
        await stream.latest()


async def test_a_read_that_falls_behind_retention_raises_instead_of_skipping(
    client: Client, owner: WorkflowHandle
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        retention=timedelta(milliseconds=300),
        poll_interval=timedelta(milliseconds=100),
    )
    # One record per read, so the trim lands between two reads of one batch.
    provider._read_count = 1
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    await producer.append({"n": 1}, {"n": 2})
    records = stream.read(topic=EVENTS)
    assert (await records.__anext__()).value == {"n": 1}
    await asyncio.sleep(0.35)
    # This append trims records 1 and 2; record 2 was never delivered.
    await producer.append({"n": 3})
    with pytest.raises(StreamExpiredError, match="while this read was behind"):
        await records.__anext__()
    await records.aclose()

    # A read that starts after the trim begins at the oldest record left.
    fresh = stream.read(topic=EVENTS)
    assert (await asyncio.wait_for(fresh.__anext__(), 5.0)).value == {"n": 3}
    await fresh.aclose()
    await provider.close()


async def test_an_idle_read_backs_off_its_owner_checks(
    client: Client, owner: WorkflowHandle, monkeypatch: pytest.MonkeyPatch
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        poll_interval=timedelta(milliseconds=50),
    )
    provider._owner_check_min, provider._owner_check_max = 0.1, 0.4
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    await producer.append({"n": 0})
    checks: list[float] = []
    real = stream._owner_ended

    async def counted(keys: Any) -> bool:
        checks.append(asyncio.get_running_loop().time())
        return await real(keys)

    monkeypatch.setattr(stream, "_owner_ended", counted)
    records = stream.read(topic=EVENTS)
    await records.__anext__()
    pending = asyncio.ensure_future(records.__anext__())
    await asyncio.sleep(2.0)
    # Fixed checks every 0.1 s would be about 20; doubling to 0.4 s is 6.
    assert 4 <= len(checks) <= 8
    gaps = [later - earlier for earlier, later in zip(checks, checks[1:])]
    assert gaps[-1] >= 0.35
    # A record restarts the interval at its minimum.
    await producer.append({"n": 1})
    assert (await asyncio.wait_for(pending, 5.0)).value == {"n": 1}
    before = len(checks)
    pending = asyncio.ensure_future(records.__anext__())
    await asyncio.sleep(0.25)
    assert len(checks) - before >= 1
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await records.aclose()
    await provider.close()


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


@workflow.defn
class PublishAndFinish:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        workflow_writer(EVENTS).publish({"n": 2})


async def test_a_reader_promotes_output_a_stopped_worker_committed(
    client: Client, raw: Any
):
    # The run finished, so no Worker will ever replay it; only a reader can
    # find the stage the stopped Worker left.
    prefix = f"test-{uuid.uuid4().hex}"
    url = os.environ["STREAMS_REDIS_URL"]
    stopped = StopsBeforePromoting(url, key_prefix=prefix)
    workflow_id = f"redis-reader-repair-{uuid.uuid4().hex}"
    stopped_client = client_with(client, stopped)
    async with new_worker(stopped_client, PublishAndFinish) as worker:
        await stopped_client.execute_workflow(
            PublishAndFinish.run, id=workflow_id, task_queue=worker.task_queue
        )
    reader = RedisStreams(url, key_prefix=prefix)
    stream = reader.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    keys = await stream._keys()
    assert await raw.exists(keys.log("events")) == 0
    assert len(await raw.hgetall(keys.pending())) == 1

    records = await read_until_end(stream.read(topic=EVENTS))
    assert [r.value for r in records] == [{"n": 1}, {"n": 2}]
    assert await raw.hgetall(keys.pending()) == {}
    # A second reader finds nothing left to repair and sees each record once.
    again = await read_until_end(stream.read(topic=EVENTS))
    assert [r.value for r in again] == [{"n": 1}, {"n": 2}]
    await stopped.close()
    await reader.close()


@workflow.defn
class PublishInThreeTasks:
    @workflow.run
    async def run(self) -> None:
        for n in range(1, 4):
            workflow_writer(EVENTS).publish({"n": n})
            await workflow.sleep(0.01)


async def test_a_reader_repairs_pending_stages_in_commit_order(
    client: Client, raw: Any
):
    prefix = f"test-{uuid.uuid4().hex}"
    url = os.environ["STREAMS_REDIS_URL"]
    stopped = StopsBeforePromoting(url, key_prefix=prefix)
    workflow_id = f"redis-reader-repair-order-{uuid.uuid4().hex}"
    stopped_client = client_with(client, stopped)
    async with new_worker(stopped_client, PublishInThreeTasks) as worker:
        await stopped_client.execute_workflow(
            PublishInThreeTasks.run, id=workflow_id, task_queue=worker.task_queue
        )
    reader = RedisStreams(url, key_prefix=prefix)
    stream = reader.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    keys = await stream._keys()
    pending = await raw.hgetall(keys.pending())
    assert len(pending) == 3
    # A hash keeps no order a reader can rely on; put the stages in reverse.
    floors = {token: int(d.split(b"\x1f")[1]) for token, d in pending.items()}
    await raw.delete(keys.pending())
    for token in sorted(pending, key=lambda t: floors[t], reverse=True):
        await raw.hset(keys.pending(), token, pending[token])

    records = await read_until_end(stream.read(topic=EVENTS))
    assert [r.value for r in records] == [{"n": 1}, {"n": 2}, {"n": 3}]
    await stopped.close()
    await reader.close()


async def test_a_promote_that_finds_its_stage_gone_warns(
    provider: RedisStreams, raw: Any, caplog: pytest.LogCaptureFixture
):
    batch = StagedBatch("ns", "wf", "first", "run", [WireRecord(topic="events")])
    token = await provider._stage(batch)
    keys = provider._chain_keys("ns", "wf", "first")
    # Retention dropped the stage while its run still held the commit.
    await raw.delete(keys.stage(token))
    with caplog.at_level(logging.WARNING):
        await provider._promote(StageRef("ns", "wf", "first", token, ("events",)))
    assert any(token in r.getMessage() for r in caplog.records)
    assert await raw.hgetall(keys.pending()) == {}


async def test_the_pending_stages_outlive_every_stage_they_name(raw: Any):
    prefix = f"test-{uuid.uuid4().hex}"
    url = os.environ["STREAMS_REDIS_URL"]
    # Two Workers of one chain configured with different retentions.
    long = RedisStreams(url, key_prefix=prefix, retention=timedelta(seconds=30))
    short = RedisStreams(url, key_prefix=prefix, retention=timedelta(milliseconds=300))
    batch = StagedBatch("ns", "wf", "first", "run", [WireRecord(topic="events")])
    token = await long._stage(batch)
    await short._stage(batch)
    await asyncio.sleep(0.6)
    keys = long._chain_keys("ns", "wf", "first")
    assert await raw.exists(keys.stage(token)) == 1
    assert token.encode() in await raw.hgetall(keys.pending())
    await long.close()
    await short.close()


class YieldingCodec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.01)
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(payloads)


async def test_concurrent_appends_on_one_producer_take_consecutive_sequences(
    client: Client, provider: RedisStreams, owner: WorkflowHandle, raw: Any
):
    coded = client_with(client, provider, DataConverter(payload_codec=YieldingCodec()))
    stream = provider.get_stream_handle(coded, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    await asyncio.gather(*(producer.append({"n": n}) for n in range(10)))
    records = await log_entries(raw, stream, "events")
    assert [r.sequence for r in records] == list(range(1, 11))


async def test_store_errors_on_writes_arrive_as_stream_errors(
    client: Client,
    provider: RedisStreams,
    owner: WorkflowHandle,
    monkeypatch: pytest.MonkeyPatch,
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    await producer.append({"n": 0})

    async def out_of_memory(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise redis.exceptions.ResponseError(
            "OOM command not allowed when used memory > 'maxmemory'."
        )

    monkeypatch.setattr(provider, "_append", out_of_memory)
    with pytest.raises(StreamRefusedError, match="OOM"):
        await producer.append({"n": 1})

    batch = StagedBatch("ns", "wf", "first", "run", [WireRecord(topic="events")])
    # A real call first, so any first-contact checks are already done.
    await provider._stage(batch)

    async def lost(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise redis.exceptions.ConnectionError("connection reset")

    monkeypatch.setattr(provider._redis, "execute_command", lost)
    with pytest.raises(StreamOutcomeUnknownError):
        await provider._stage(batch)

    async def wrong_type(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise redis.exceptions.ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )

    monkeypatch.setattr(provider._redis, "execute_command", wrong_type)
    with pytest.raises(StreamRefusedError, match="WRONGTYPE"):
        await provider._promote(StageRef("ns", "wf", "first", "token", ("events",)))


async def test_store_errors_on_reads_arrive_as_stream_errors(
    client: Client,
    provider: RedisStreams,
    owner: WorkflowHandle,
    monkeypatch: pytest.MonkeyPatch,
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))

    async def lost(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise redis.exceptions.ConnectionError("connection reset")

    monkeypatch.setattr(provider._redis, "xrevrange", lost)
    with pytest.raises(StreamStorageError, match="connection reset"):
        await stream.latest(topic=EVENTS)
    with pytest.raises(StreamStorageError, match="connection reset"):
        await anext(stream.read(topic=EVENTS, after=END))
    monkeypatch.setattr(type(provider._redis.pipeline()), "execute", lost)
    with pytest.raises(StreamStorageError, match="connection reset"):
        await anext(stream.read(topic=EVENTS))


async def test_store_errors_on_read_checks_arrive_as_stream_errors(
    client: Client,
    provider: RedisStreams,
    owner: WorkflowHandle,
    monkeypatch: pytest.MonkeyPatch,
):
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    cursor = await producer.append({"n": 1})

    async def lost(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise redis.exceptions.ConnectionError("connection reset")

    pipeline_type = type(provider._redis.pipeline())
    monkeypatch.setattr(pipeline_type, "execute", lost)
    with pytest.raises(StreamStorageError, match="connection reset"):
        await anext(stream.read(topic=EVENTS, after=cursor))
    monkeypatch.undo()

    fresh = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"test-{uuid.uuid4().hex}"
    )
    monkeypatch.setattr(fresh._redis, "info", lost)
    with pytest.raises(StreamStorageError, match="connection reset"):
        await fresh.get_stream_handle(client, StreamRef.for_workflow(owner.id)).latest(
            topic=EVENTS
        )
    await fresh.close()


async def test_a_large_stage_lands_whole(raw: Any):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"test-{uuid.uuid4().hex}"
    )
    records = [WireRecord(topic="events", sequence=i) for i in range(10_000)]
    token = await provider._stage(StagedBatch("ns", "wf", "first", "run", records))
    await provider._promote(StageRef("ns", "wf", "first", token, ("events",)))
    keys = provider._chain_keys("ns", "wf", "first")
    assert await raw.xlen(keys.log("events")) == 10_000
    await provider.close()


@workflow.defn
class PublishAndFailTheFirstAttempt:
    @workflow.run
    async def run(self) -> str:
        workflow_writer(EVENTS).publish({"attempt": workflow.info().attempt})
        if workflow.info().attempt == 1:
            raise ApplicationError("fail this run so the retry policy starts another")
        return "done"


async def wait_for_next_run(handle: WorkflowHandle, first_run_id: str) -> None:
    for _ in range(200):
        if (await handle.describe()).run_id != first_run_id:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("no next run started")


async def test_a_retried_run_keeps_the_chain_open(
    client: Client, provider: RedisStreams, raw: Any
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-retry-{uuid.uuid4().hex}"
    async with new_worker(streams_client, PublishAndFailTheFirstAttempt) as worker:
        handle = await streams_client.start_workflow(
            PublishAndFailTheFirstAttempt.run,
            id=workflow_id,
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                maximum_attempts=2, initial_interval=timedelta(seconds=3)
            ),
        )
        assert handle.first_execution_run_id is not None
        await wait_for_next_run(handle, handle.first_execution_run_id)
        # Give the Worker's close check, which runs after the failed run's
        # final task, time to act.
        await asyncio.sleep(1.0)
        stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
        keys = await stream._keys()
        assert not await raw.hget(keys.chain(), "closed")
        await stream.producer(topic=OTHER, producer_id="p", attempt=1).append(1)
        assert await handle.result() == "done"


@workflow.defn
class CronPublisher:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"run": workflow.info().run_id})


@pytest.mark.timeout(150)
async def test_a_cron_run_keeps_the_chain_open(
    client: Client, provider: RedisStreams, raw: Any
):
    streams_client = client_with(client, provider)
    workflow_id = f"redis-cron-{uuid.uuid4().hex}"
    async with new_worker(streams_client, CronPublisher) as worker:
        handle = await streams_client.start_workflow(
            CronPublisher.run,
            id=workflow_id,
            task_queue=worker.task_queue,
            cron_schedule="* * * * *",
        )
        assert handle.first_execution_run_id is not None
        await wait_for_next_run_after_completion(streams_client, handle)
        await asyncio.sleep(1.0)
        stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
        keys = await stream._keys()
        assert not await raw.hget(keys.chain(), "closed")
        await stream.producer(topic=OTHER, producer_id="p", attempt=1).append(1)
        await handle.terminate()


async def wait_for_next_run_after_completion(
    client: Client, handle: WorkflowHandle
) -> None:
    first = handle.first_execution_run_id
    assert first is not None
    for _ in range(1400):
        first_run = await client.get_workflow_handle(handle.id, run_id=first).describe()
        if first_run.status == WorkflowExecutionStatus.COMPLETED:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("the first cron run never completed")


async def test_a_producer_rechecks_its_owner_once_retention_passed(
    client: Client, owner: WorkflowHandle, raw: Any
):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"],
        key_prefix=f"test-{uuid.uuid4().hex}",
        retention=timedelta(milliseconds=300),
    )
    # The closed flag outlives retention by the tombstone grace; with none,
    # it expires with the stream.
    provider._grace_ms = 0
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(owner.id))
    producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
    await producer.append(1)
    await owner.terminate()
    keys = await stream._keys()
    await provider._mark_closed(keys)
    await asyncio.sleep(0.5)
    assert not await raw.exists(keys.chain())
    with pytest.raises(StreamClosedError):
        await producer.append(2)
    await provider.close()
