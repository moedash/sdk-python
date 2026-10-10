"""Promotion, abort and replay of a Workflow's committed stream output.

A stage is promoted when History shows its marker and aborted when History
shows its task failed. On replay nothing is staged: the recorded manifests
come back in order and the replayed commits are checked against them.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

import temporalio.contrib.streams._output as output_module
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.workflowservice.v1 import GetWorkflowExecutionHistoryReverseResponse
from temporalio.bridge.proto.external_data import (
    ExternalOutputStreamManifest,
    ExternalStreamMarkerData,
)
from temporalio.bridge.proto.workflow_activation import WorkflowActivation
from temporalio.bridge.proto.workflow_completion import WorkflowActivationCompletion
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams._output import (
    MARKER_NAME,
    OutputCoordinator,
    StagedBatch,
    StageRef,
    _Stage,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError
from tests.helpers import new_worker

EVENTS = topic("events", dict)


class CountingStreams(MemoryStreams):
    """The memory provider, counting every call on the Workflow output seam."""

    def __init__(self) -> None:
        super().__init__()
        self.staged: list[str] = []
        self.promoted: list[str] = []
        self.aborted: list[str] = []

    async def _stage(self, batch: StagedBatch) -> str:
        token = await super()._stage(batch)
        self.staged.append(token)
        return token

    async def _promote(self, stage: StageRef) -> None:
        self.promoted.append(stage.token)
        # A store round trip yields, which is where two settles interleave.
        await asyncio.sleep(0.01)
        await super()._promote(stage)

    async def _abort(self, stage: StageRef) -> None:
        self.aborted.append(stage.token)
        await super()._abort(stage)


def client_with(client: Client, provider: MemoryStreams) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


def new_workflow_id() -> str:
    return f"streams-replay-{uuid.uuid4().hex}"


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


@activity.defn
async def nothing() -> None:
    pass


@workflow.defn(name="Publisher")
class Publisher:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_writer(EVENTS).publish({"n": "after"})
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": "last"})


@workflow.defn(name="Publisher")
class PublisherWithDifferentData:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_writer(EVENTS).publish({"n": "changed"})
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": "last"})


async def run_publisher(client: Client, provider: MemoryStreams) -> Any:
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, Publisher, activities=[nothing]) as worker:
        handle = await streams_client.start_workflow(
            Publisher.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    return handle


async def test_an_accepted_task_is_promoted_once(client: Client):
    provider = CountingStreams()
    handle = await run_publisher(client, provider)
    assert len(provider.staged) == 3
    assert sorted(provider.promoted) == sorted(provider.staged)
    assert provider.aborted == []
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(handle.id))
    records = await read_all(stream.read(topic=EVENTS))
    assert [r.value for r in records] == [
        {"n": "before"},
        {"n": "after"},
        {"n": "last"},
    ]


async def test_replay_touches_no_store_and_stays_deterministic(client: Client):
    handle = await run_publisher(client, CountingStreams())
    history = await handle.fetch_history()

    fresh = CountingStreams()
    # Two commits share the first Workflow Task, around the Local Activity,
    # and replay pairs them with the recorded manifests in order.
    await Replayer(workflows=[Publisher], plugins=[fresh]).replay_workflow(history)
    assert (fresh.staged, fresh.promoted, fresh.aborted) == ([], [], [])
    assert fresh._topics == {} and fresh._stages == {}


async def test_a_replay_that_publishes_different_data_is_nondeterministic(
    client: Client,
):
    handle = await run_publisher(client, CountingStreams())
    history = await handle.fetch_history()
    with pytest.raises(
        Exception, match="External output committed while replaying differs"
    ):
        await Replayer(
            workflows=[PublisherWithDifferentData], plugins=[CountingStreams()]
        ).replay_workflow(history)


@workflow.defn(name="Publisher")
class PublisherWithFewerCommits:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": "last"})


async def test_a_replay_that_commits_less_than_history_is_nondeterministic(
    client: Client,
):
    handle = await run_publisher(client, CountingStreams())
    history = await handle.fetch_history()
    # The first Workflow Task recorded two commits; this replay makes one.
    with pytest.raises(Exception, match="did not commit"):
        await Replayer(
            workflows=[PublisherWithFewerCommits], plugins=[CountingStreams()]
        ).replay_workflow(history)


async def test_an_evicted_run_replays_without_publishing_twice(client: Client):
    provider = CountingStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(
        streams_client, Publisher, activities=[nothing], max_cached_workflows=0
    ) as worker:
        await streams_client.execute_workflow(
            Publisher.run, id=workflow_id, task_queue=worker.task_queue
        )
    assert len(provider.staged) == 3
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    assert len(await read_all(stream.read(topic=EVENTS))) == 3


@workflow.defn
class PublishOnce:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})


async def test_a_rejected_commit_is_aborted(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    # Core refuses a manifest whose history floor is not the task's, which
    # fails the Workflow Task after the batch was staged. Only the first
    # attempt is broken, so the retry commits.
    real = output_module.build_manifest
    broken = {"left": 1}

    def wrong_floor_once(*args: Any, **kwargs: Any) -> ExternalOutputStreamManifest:
        manifest = real(*args, **kwargs)
        if broken["left"]:
            broken["left"] -= 1
            manifest.history_floor_event_id += 1000
        return manifest

    monkeypatch.setattr(output_module, "build_manifest", wrong_floor_once)
    provider = CountingStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishOnce) as worker:
        await streams_client.execute_workflow(
            PublishOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
        # The dead stage is settled once History shows the failed task, at
        # eviction or after the run's next completion.
        for _ in range(50):
            if provider.aborted:
                break
            await asyncio.sleep(0.1)

    assert len(provider.staged) == 2
    assert provider.aborted == provider.staged[:1]
    assert provider.promoted == provider.staged[1:]
    assert provider._stages == {}
    stream = provider.get_stream_handle(client, StreamRef.for_workflow(workflow_id))
    assert [r.value for r in await read_all(stream.read(topic=EVENTS))] == [{"n": 1}]


async def test_output_history_recorded_but_not_republished_is_nondeterministic():
    coordinator = OutputCoordinator(MemoryStreams(), None, "default")
    replayed = WorkflowActivation(run_id="run", is_replaying=True)
    replayed.jobs.add().replay_external_streams.output.stage_token = "recorded"
    coordinator.take_jobs(replayed)
    assert list(replayed.jobs) == []

    live = WorkflowActivation(run_id="run", is_replaying=False)
    completion = WorkflowActivationCompletion(run_id="run")
    completion.successful.SetInParent()
    with pytest.raises(NondeterminismError, match="did not publish"):
        await coordinator.before_completion(live, completion, DataConverter.default)


class _HistoryWithMarker:
    """A client whose reverse History holds one output marker, after a pause."""

    def __init__(self, token: str) -> None:
        self.namespace = "default"
        self.workflow_service = self
        self._token = token

    async def get_workflow_execution_history_reverse(
        self, request: Any
    ) -> GetWorkflowExecutionHistoryReverseResponse:
        del request
        await asyncio.sleep(0.05)
        marker = ExternalStreamMarkerData()
        marker.output.stage_token = self._token
        event = HistoryEvent(
            event_id=5, event_type=EventType.EVENT_TYPE_MARKER_RECORDED
        )
        event.marker_recorded_event_attributes.marker_name = MARKER_NAME
        event.marker_recorded_event_attributes.details["external_stream"].payloads.add(
            data=marker.SerializeToString()
        )
        response = GetWorkflowExecutionHistoryReverseResponse()
        response.history.events.append(event)
        return response


async def test_a_completion_and_an_eviction_settle_a_stage_once():
    provider = CountingStreams()
    token = await provider._stage(StagedBatch("default", "wf", "run", "run", []))
    history: Any = _HistoryWithMarker(token)
    coordinator = OutputCoordinator(provider, history, "default")
    run = coordinator.open_run("wf", "run")
    run.staged.append(_Stage(StageRef("default", "wf", "run", token, ()), 1))
    await asyncio.gather(
        coordinator.after_completion("run"), coordinator.on_eviction("run")
    )
    assert provider.promoted == [token]


async def test_a_stage_from_a_failed_transient_attempt_is_aborted(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    # Attempts 1 and 2 of the first Workflow Task are rejected after staging;
    # attempt 3 commits. Attempt 2 is transient: History never records its
    # failure, so only the later attempt's commit at the same floor, or the
    # run's close, can settle it.
    real = output_module.build_manifest
    broken = {"left": 2}

    def wrong_floor_twice(*args: Any, **kwargs: Any) -> ExternalOutputStreamManifest:
        manifest = real(*args, **kwargs)
        if broken["left"]:
            broken["left"] -= 1
            manifest.history_floor_event_id += 1000
        return manifest

    monkeypatch.setattr(output_module, "build_manifest", wrong_floor_twice)
    provider = CountingStreams()
    streams_client = client_with(client, provider)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishOnce) as worker:
        await streams_client.execute_workflow(
            PublishOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
        for _ in range(100):
            if len(provider.aborted) == 2:
                break
            await asyncio.sleep(0.1)

    assert len(provider.staged) == 3
    assert sorted(provider.aborted) == sorted(provider.staged[:2])
    assert provider.promoted == provider.staged[2:]
    # Nothing is left to read History for.
    assert provider._stages == {}


class _FailingPromote(CountingStreams):
    async def _promote(self, stage: StageRef) -> None:
        raise RuntimeError("the store is unavailable")


async def test_an_eviction_keeps_stages_it_could_not_settle():
    provider = _FailingPromote()
    token = await provider._stage(StagedBatch("default", "wf", "run", "run", []))
    history: Any = _HistoryWithMarker(token)
    coordinator = OutputCoordinator(provider, history, "default")
    run = coordinator.open_run("wf", "run")
    run.staged.append(_Stage(StageRef("default", "wf", "run", token, ()), 1))
    await coordinator.on_eviction("run")
    # The stage waits for the run's return instead of being forgotten.
    returned = coordinator.open_run("wf", "run")
    assert [stage.token for stage in returned.staged] == [token]


async def test_settling_a_proven_stage_updates_the_list_others_hold():
    provider = CountingStreams()
    token = await provider._stage(StagedBatch("default", "wf", "run", "run", []))
    coordinator = OutputCoordinator(provider, None, "default")
    run = coordinator.open_run("wf", "run")
    stage = _Stage(StageRef("default", "wf", "run", token, ()), 1)
    run.staged.append(stage)
    # An eviction hands this same list to the run's next incarnation.
    shared = run.staged
    run.proven.append(stage)
    await coordinator.after_completion("run")
    assert provider.promoted == [token]
    assert shared == []
