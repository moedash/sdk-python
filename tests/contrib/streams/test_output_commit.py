"""A Workflow's own publish commits with its Workflow Task.

The Worker stages what an activation published, Core records the manifest
in a marker, and the batch is promoted once History shows that marker.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.bridge.proto.external_data import ExternalStreamMarkerData
from temporalio.client import Client, WorkflowHandle, WorkflowUpdateFailedError
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._body import (
    CONTENT_HASH_KEY,
)
from temporalio.contrib.streams._output import MARKER_NAME
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import Replayer
from tests.helpers import new_worker

EVENTS = topic("events", dict)
OTHER = topic("other", dict)


def client_with(
    client: Client, provider: MemoryStreams, converter: DataConverter | None = None
) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    if converter is not None:
        config["data_converter"] = converter
    return Client(**config)


def new_workflow_id() -> str:
    return f"streams-commit-{uuid.uuid4().hex}"


async def read_all(records: Any, timeout: float = 10.0) -> list:
    out = []

    async def pull() -> None:
        async for record in records:
            out.append(record)

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    return out


async def output_markers(handle: WorkflowHandle) -> list[ExternalStreamMarkerData]:
    markers = []
    async for event in handle.fetch_history_events():
        if event.event_type != EventType.EVENT_TYPE_MARKER_RECORDED:
            continue
        attributes = event.marker_recorded_event_attributes
        if attributes.marker_name == MARKER_NAME:
            markers.append(
                ExternalStreamMarkerData.FromString(
                    attributes.details["external_stream"].payloads[0].data
                )
            )
    return markers


@workflow.defn
class PublishTwoTopics:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        workflow_writer(OTHER).publish({"n": 2})
        workflow_writer(EVENTS).publish({"n": 3})
        workflow_writer(EVENTS).finish()


async def test_an_accepted_task_commits_its_output_in_a_marker(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishTwoTopics) as worker:
        handle = await streams_client.start_workflow(
            PublishTwoTopics.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()

    (marker,) = await output_markers(handle)
    manifest = marker.output
    assert manifest.run_id == handle.result_run_id
    assert manifest.stage_token
    assert manifest.provider_id == provider.name()
    assert [(t.topic, t.record_count, t.finished) for t in manifest.topics] == [
        ("events", 3, True),
        ("other", 1, False),
    ]
    assert [list(s.record_counts_by_topic) for s in manifest.segments] == [[3, 1]]

    stream = provider.get_stream_handle(
        streams_client, StreamRef.for_workflow(workflow_id)
    )
    events = await read_all(stream.read(topic=EVENTS))
    assert [r.value for r in events if r.kind is RecordKind.DATA] == [
        {"n": 1},
        {"n": 3},
    ]
    assert events[-1].kind is RecordKind.FINISH
    assert [r.value for r in await read_all(stream.read(topic=OTHER))] == [{"n": 2}]
    # Promotion empties the stage.
    assert provider._stages == {}


_task_attempts: dict[str, int] = {}


# Unsandboxed so the counter survives the failed task: a sandboxed run
# re-imports this module and would start it at zero every time.
@workflow.defn(sandboxed=False)
class FailsItsFirstTask:
    @workflow.run
    async def run(self, key: str) -> None:
        _task_attempts[key] = _task_attempts.get(key, 0) + 1
        workflow_writer(EVENTS).publish({"attempt": _task_attempts[key]})
        if _task_attempts[key] == 1:
            raise RuntimeError("fail this Workflow Task once")


async def test_a_failed_task_publishes_nothing(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, FailsItsFirstTask) as worker:
        handle = await streams_client.start_workflow(
            FailsItsFirstTask.run,
            workflow_id,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        await handle.result()

    failed = [
        event
        async for event in handle.fetch_history_events()
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
    ]
    assert len(failed) == 1
    assert len(await output_markers(handle)) == 1
    stream = provider.get_stream_handle(
        streams_client, StreamRef.for_workflow(workflow_id)
    )
    records = await read_all(stream.read(topic=EVENTS))
    # Only the retry's record: the failed task never staged its own.
    assert [r.value for r in records] == [{"attempt": 2}]
    assert provider._stages == {}


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


@workflow.defn
class PublishOnce:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"secret": "value"})


async def test_bodies_go_through_the_workflow_codec(client: Client):
    provider = MemoryStreams()
    coded = client_with(client, provider, DataConverter(payload_codec=NonceCodec()))
    workflow_id = new_workflow_id()
    async with new_worker(coded, PublishOnce) as worker:
        await coded.execute_workflow(
            PublishOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    store = provider._topic(client.namespace, workflow_id, EVENTS.name)
    (stored,) = [WireRecord.FromString(raw) for raw in store.records]
    assert stored.body.metadata["encoding"] == b"binary/nonce"
    assert CONTENT_HASH_KEY in stored.metadata
    stream = provider.get_stream_handle(coded, StreamRef.for_workflow(workflow_id))
    (record,) = await read_all(stream.read(topic=EVENTS))
    assert record.value == {"secret": "value"}


@activity.defn
async def nothing() -> None:
    pass


@workflow.defn
class PublishAroundALocalActivity:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_writer(EVENTS).publish({"n": "after"})


async def test_two_publishing_completions_in_one_task_commit_in_order(
    client: Client,
):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(
        streams_client, PublishAroundALocalActivity, activities=[nothing]
    ) as worker:
        handle = await streams_client.start_workflow(
            PublishAroundALocalActivity.run,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        await handle.result()

    markers = await output_markers(handle)
    assert len(markers) == 2
    assert markers[0].output.stage_token != markers[1].output.stage_token
    stream = provider.get_stream_handle(
        streams_client, StreamRef.for_workflow(workflow_id)
    )
    records = await read_all(stream.read(topic=EVENTS))
    assert [r.value for r in records] == [{"n": "before"}, {"n": "after"}]


async def test_a_replayer_without_the_stream_plugin_says_so(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    async with new_worker(streams_client, PublishOnce) as worker:
        handle = await streams_client.start_workflow(
            PublishOnce.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
        await handle.result()
    history = await handle.fetch_history()
    with pytest.raises(Exception, match="stream provider plugin"):
        await Replayer(workflows=[PublishOnce]).replay_workflow(history)


@workflow.defn
class PublishAfterARejectedUpdate:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.go)
        workflow_writer(EVENTS).publish({"after": "rejected update"})

    @workflow.update
    def set_go(self, value: bool) -> None:
        self.go = value

    @set_go.validator
    def check_go(self, value: bool) -> None:
        if not value:
            raise ValueError("only True starts the publish")

    @workflow.signal
    def start(self) -> None:
        self.go = True


async def test_a_publish_right_after_a_rejected_update_commits(client: Client):
    # Core drops the speculative task of a rejected Update, and with it the
    # task's history floor, so the next publishing task must find its own.
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishAfterARejectedUpdate) as worker:
        handle = await streams_client.start_workflow(
            PublishAfterARejectedUpdate.run,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update(PublishAfterARejectedUpdate.set_go, False)
        await handle.signal(PublishAfterARejectedUpdate.start)
        await handle.result()
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    (record,) = await read_all(stream.read(topic=EVENTS))
    assert record.value == {"after": "rejected update"}
