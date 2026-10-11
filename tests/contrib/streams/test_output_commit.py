"""A Workflow's own publish rides the completion of the activation that made it.

The writer adds the records to the completion's commit, the Worker's
completion encoder runs each body through the codec, and Core records a
marker of them and makes them visible once the Workflow Task is accepted.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.bridge.proto.external_data import ExternalStreamMarkerData
from temporalio.bridge.proto.streams import (
    ReadRequest,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.client import Client, WorkflowHandle, WorkflowUpdateFailedError
from temporalio.contrib.streams import (
    RecordKind,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter, PayloadCodec
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.helpers import new_worker

EVENTS = topic("events", dict)
OTHER = topic("other", dict)

MARKER_NAME = "core_external_stream"


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
    streams_client = await connect_with(client, MemoryStreams())
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
    assert [(t.topic, t.record_count, t.finished) for t in manifest.topics] == [
        ("events", 3, True),
        ("other", 1, False),
    ]

    stream = get_stream_handle(streams_client, workflow_id)
    events = await read_all(stream.read(topic=EVENTS))
    assert [r.value for r in events if r.kind is RecordKind.DATA] == [
        {"n": 1},
        {"n": 3},
    ]
    assert events[-1].kind is RecordKind.FINISH
    assert [r.value for r in await read_all(stream.read(topic=OTHER))] == [{"n": 2}]


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
    streams_client = await connect_with(client, MemoryStreams())
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
    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    # Only the retry's record: the failed task's completion never reached Core.
    assert [r.value for r in records] == [{"attempt": 2}]


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
    store = MemoryStreams()
    coded = await connect_with(client, store, DataConverter(payload_codec=NonceCodec()))
    workflow_id = new_workflow_id()
    async with new_worker(coded, PublishOnce) as worker:
        await coded.execute_workflow(
            PublishOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    service = await store._service_for(coded)
    raw = await service.read(
        ReadRequest(
            stream=StreamAddress(
                namespace=client.namespace,
                owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                workflow_id=workflow_id,
                topic=EVENTS.name,
            )
        )
    )
    (stored,) = [record.stored for record in raw.records]
    # The store holds ciphertext and the plaintext hash Core stamped from lang's.
    assert stored.body.metadata["encoding"] == b"binary/nonce"
    assert len(stored.metadata["temporal.io/content-hash"].data) == 64
    (record,) = await read_all(get_stream_handle(coded, workflow_id).read(topic=EVENTS))
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
    streams_client = await connect_with(client, MemoryStreams())
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
    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    assert [r.value for r in records] == [{"n": "before"}, {"n": "after"}]


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
    streams_client = await connect_with(client, MemoryStreams())
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
    (record,) = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    assert record.value == {"after": "rejected update"}


@workflow.defn
class PublishThenContinue:
    @workflow.run
    async def run(self, runs_left: int) -> None:
        workflow_writer(EVENTS).publish({"runs_left": runs_left})
        if runs_left:
            workflow.continue_as_new(runs_left - 1)
        workflow_writer(EVENTS).finish()


async def test_each_run_stamps_its_own_id_across_continue_as_new(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishThenContinue) as worker:
        handle = await streams_client.start_workflow(
            PublishThenContinue.run, 1, id=workflow_id, task_queue=worker.task_queue
        )
        first_run = handle.first_execution_run_id
        await handle.result()
        last_run = (await handle.describe()).run_id

    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    assert [(r.kind, r.value, r.run_id) for r in records] == [
        (RecordKind.DATA, {"runs_left": 1}, first_run),
        (RecordKind.DATA, {"runs_left": 0}, last_run),
        (RecordKind.FINISH, None, last_run),
    ]
    assert first_run != last_run
