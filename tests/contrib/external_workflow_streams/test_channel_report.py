"""The channel report: what a run tells Core its readers listen on.

Driven with activations directly, the way Core drives the instance, over a
real stream runtime on the memory backend, so the report is built from the
readers the runtime holds open exactly as the Worker's would be. Core keeps
the server's subscriptions in step with the report, so what is pinned down
here is the report alone: when it goes out, what it carries and in which
order, and that the external runtime no longer issues the subscribe command
itself.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.api.notification.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.common
import temporalio.converter
from temporalio import workflow
from temporalio.contrib.external_workflow_streams import external_stream
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._runtime import WorkflowStreamRuntime
from temporalio.contrib.external_workflow_streams._wake import channel_for
from temporalio.worker._workflow import _WorkflowWorker
from temporalio.worker._workflow_instance import (
    UnsandboxedWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
    _WorkflowLogicFlag,
)
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

WorkflowActivation = temporalio.bridge.proto.workflow_activation.WorkflowActivation
WorkflowActivationJob = (
    temporalio.bridge.proto.workflow_activation.WorkflowActivationJob
)
WorkflowActivationCompletion = (
    temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion
)
Notification = temporalio.api.notification.v1.Notification
INDEPENDENT = _WorkflowLogicFlag.SUBSCRIBE_NOTIFICATION_CHANNELS
LINKED = _WorkflowLogicFlag.LINKED_NOTIFICATION_CHANNELS
RUN_ID = "run"
WORKFLOW_ID = "wf"


@workflow.defn
class OpenThenWait:
    """Opens a reader without blocking on it, waits, closes it, waits again.

    The reader is open across the first task and gone across the second,
    while neither task blocks on a stream, so both completions leave with a
    timer and no quiescent snapshot.
    """

    @workflow.run
    async def run(self) -> None:
        reader = external_stream.topic("tokens", type=str).subscribe()
        await workflow.sleep(60)
        reader.close()
        await workflow.sleep(60)


@workflow.defn
class ReadOne:
    """Blocks on a reader until its first value arrives."""

    @workflow.run
    async def run(self) -> str:
        async for value in external_stream.topic("tokens", type=str).subscribe():
            return value
        return ""


@workflow.defn
class SleepThenRead:
    """Waits on a timer before opening any reader."""

    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(60)
        async for value in external_stream.topic("tokens", type=str).subscribe():
            return value
        return ""


@workflow.defn
class ThreeReaders:
    """Opens readers on two streams, the first of them twice, then waits."""

    @workflow.run
    async def run(self) -> None:
        external_stream.topic("b", type=str).subscribe()
        external_stream.topic("a", type=str).subscribe()
        external_stream.topic("b", type=str).subscribe()
        await workflow.sleep(60)


async def _accept(run_id: str, wait_id: int, generation: int) -> str:
    del run_id, wait_id, generation
    return ReadinessResult.ACCEPTED


@pytest.fixture
async def manager() -> AsyncIterator[StreamSubscriptionManager]:
    manager = StreamSubscriptionManager(
        backend=MemoryStreamBackend(),
        notify_ready=_accept,
        watch_block=timedelta(milliseconds=10),
    )
    try:
        yield manager
    finally:
        await manager.shutdown()


def _runtime(manager: StreamSubscriptionManager) -> WorkflowStreamRuntime:
    return WorkflowStreamRuntime(
        manager=manager,
        backend=manager._backend,
        run_id=RUN_ID,
        namespace="default",
        workflow_id=WORKFLOW_ID,
        first_execution_run_id=RUN_ID,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )


def _instance(
    workflow_class: type,
    runtime: WorkflowStreamRuntime,
    flags: Iterable[_WorkflowLogicFlag] = (),
) -> WorkflowInstance:
    """Build an instance the way the worker does, with its stream runtime.

    Needs a running event loop, since the constructor puts the runtime on it.
    ``flags`` are the lang flags the Worker would default on from its probe.
    """
    defn = workflow._Definition.must_from_class(workflow_class)
    now = datetime.now(timezone.utc)
    info = workflow.Info(
        attempt=1,
        continued_run_id=None,
        cron_schedule=None,
        execution_timeout=None,
        first_execution_run_id=RUN_ID,
        headers={},
        namespace="default",
        original_execution_run_id=RUN_ID,
        parent=None,
        root=None,
        priority=temporalio.common.Priority.default,
        raw_memo={},
        retry_policy=None,
        run_id=RUN_ID,
        run_timeout=None,
        search_attributes={},
        start_time=now,
        task_queue="tq",
        task_timeout=timedelta(seconds=10),
        typed_search_attributes=temporalio.common.TypedSearchAttributes.empty,
        workflow_id=WORKFLOW_ID,
        workflow_start_time=now,
        workflow_type=defn.name or "",
    )
    converter = temporalio.converter.DataConverter.default
    return UnsandboxedWorkflowRunner().create_instance(
        WorkflowInstanceDetails(
            payload_converter_factory=converter._new_internal_payload_converter,
            failure_converter_class=converter.failure_converter_class,
            interceptor_classes=[],
            defn=defn,
            info=info,
            randomness_seed=0,
            extern_functions={},
            disable_eager_activity_execution=False,
            worker_level_failure_exception_types=[],
            patch_activation_callback=None,
            last_completion_result=temporalio.api.common.v1.Payloads(),
            last_failure=None,
            default_workflow_logic_flags=frozenset(flags),
            external_stream_runtime=runtime,
            external_streams_configured=True,
        )
    )


def _start(
    workflow_class: type, recorded_flags: Iterable[_WorkflowLogicFlag] = ()
) -> WorkflowActivation:
    """The run's first activation, a replay when ``recorded_flags`` is given."""
    job = WorkflowActivationJob()
    init = job.initialize_workflow
    init.workflow_type = workflow._Definition.must_from_class(workflow_class).name or ""
    init.workflow_id = WORKFLOW_ID
    init.first_execution_run_id = RUN_ID
    recorded = [int(flag) for flag in recorded_flags]
    return WorkflowActivation(
        run_id=RUN_ID,
        jobs=[job],
        is_replaying=bool(recorded),
        available_internal_flags=recorded,
    )


def _timer_fired(seq: int) -> WorkflowActivation:
    job = WorkflowActivationJob()
    job.fire_timer.seq = seq
    return WorkflowActivation(run_id=RUN_ID, jobs=[job])


def _notified(*notifications: Notification) -> WorkflowActivation:
    job = WorkflowActivationJob()
    job.notifications_received.notifications.extend(notifications)
    return WorkflowActivation(run_id=RUN_ID, jobs=[job])


def _variants(completion: WorkflowActivationCompletion) -> list[str]:
    assert completion.HasField("successful"), completion.failed.failure.message
    return [
        command.WhichOneof("variant") or ""
        for command in completion.successful.commands
    ]


def _reported(completion: WorkflowActivationCompletion) -> list[str] | None:
    """The channel report, or ``None`` when the completion carries none."""
    assert completion.HasField("successful"), completion.failed.failure.message
    reports = [
        list(command.workflow_stream_channels.channels)
        for command in completion.successful.commands
        if command.HasField("workflow_stream_channels")
    ]
    assert len(reports) <= 1, "one report per completion"
    return reports[0] if reports else None


def _subscribed(completion: WorkflowActivationCompletion) -> list[str]:
    return [
        command.subscribe_notification_channel.channel
        for command in completion.successful.commands
        if command.HasField("subscribe_notification_channel")
    ]


def _channel(runtime: WorkflowStreamRuntime, stream: str) -> str:
    return channel_for(runtime.stream_key(stream)).channel


async def test_the_report_goes_out_on_a_completion_without_a_snapshot(
    manager: StreamSubscriptionManager,
) -> None:
    """A reader that is open but not blocked on is still listened for."""
    runtime = _runtime(manager)
    instance = _instance(OpenThenWait, runtime, flags=[INDEPENDENT])
    completion = instance.activate(_start(OpenThenWait))
    assert "start_timer" in _variants(completion)
    assert "workflow_stream_quiescent" not in _variants(completion)
    assert _reported(completion) == [_channel(runtime, "tokens")]
    assert _subscribed(completion) == []


async def test_the_report_goes_out_with_a_snapshot(
    manager: StreamSubscriptionManager,
) -> None:
    runtime = _runtime(manager)
    instance = _instance(ReadOne, runtime, flags=[INDEPENDENT])
    completion = instance.activate(_start(ReadOne))
    assert "workflow_stream_quiescent" in _variants(completion)
    assert _reported(completion) == [_channel(runtime, "tokens")]
    assert _subscribed(completion) == []


async def test_the_report_is_absent_before_any_reader_opened(
    manager: StreamSubscriptionManager,
) -> None:
    """Absent means unchanged to Core, and nothing has changed yet."""
    runtime = _runtime(manager)
    instance = _instance(SleepThenRead, runtime, flags=[INDEPENDENT])
    completion = instance.activate(_start(SleepThenRead))
    assert _variants(completion) == ["start_timer"]
    assert _reported(completion) is None
    completion = instance.activate(_timer_fired(1))
    assert _reported(completion) == [_channel(runtime, "tokens")]


async def test_the_report_carries_the_empty_set_after_the_last_reader_closes(
    manager: StreamSubscriptionManager,
) -> None:
    """Empty is a report, not an absence: it is what Core unsubscribes from."""
    runtime = _runtime(manager)
    instance = _instance(OpenThenWait, runtime, flags=[INDEPENDENT])
    instance.activate(_start(OpenThenWait))
    completion = instance.activate(_timer_fired(1))
    assert "start_timer" in _variants(completion)
    assert _reported(completion) == []


async def test_the_report_orders_channels_by_reader_and_names_each_once(
    manager: StreamSubscriptionManager,
) -> None:
    runtime = _runtime(manager)
    instance = _instance(ThreeReaders, runtime, flags=[INDEPENDENT])
    completion = instance.activate(_start(ThreeReaders))
    assert _reported(completion) == [_channel(runtime, "b"), _channel(runtime, "a")]


async def test_a_linked_server_reports_nothing_for_the_runs_own_streams(
    manager: StreamSubscriptionManager,
) -> None:
    """The owner is the listener by construction, so there is nothing to subscribe."""
    runtime = _runtime(manager)
    instance = _instance(ReadOne, runtime, flags=[INDEPENDENT, LINKED])
    completion = instance.activate(_start(ReadOne))
    assert "workflow_stream_quiescent" in _variants(completion)
    assert _reported(completion) == []
    assert _subscribed(completion) == []


async def test_a_server_without_channels_gets_no_report(
    manager: StreamSubscriptionManager,
) -> None:
    runtime = _runtime(manager)
    instance = _instance(ReadOne, runtime)
    completion = instance.activate(_start(ReadOne))
    assert "workflow_stream_quiescent" in _variants(completion)
    assert _reported(completion) is None
    assert _subscribed(completion) == []


async def test_a_replay_reports_what_the_live_run_did(
    manager: StreamSubscriptionManager,
) -> None:
    """Core matches the recorded subscribed event from the replayed report."""
    runtime = _runtime(manager)
    instance = _instance(ReadOne, runtime)
    completion = instance.activate(_start(ReadOne, recorded_flags=[INDEPENDENT]))
    assert _reported(completion) == [_channel(runtime, "tokens")]
    assert _subscribed(completion) == []


class _BareWorker:
    """Only the two things ``_handle_external_stream_jobs`` reaches for."""

    def __init__(self, runtime: WorkflowStreamRuntime, manager: object) -> None:
        self._external_stream_runtimes = {RUN_ID: runtime}
        self._manager = manager

    def _stream_manager(self) -> object:
        return self._manager


def _park_job(quiescence_generation: int, *wait_ids: int) -> WorkflowActivation:
    activation = WorkflowActivation(run_id=RUN_ID)
    job = activation.jobs.add().prepare_external_stream_park
    job.quiescence_generation = quiescence_generation
    for wait_id in wait_ids:
        job.waits.add().wait_id = wait_id
    return activation


def _finalize_job(quiescence_generation: int) -> WorkflowActivation:
    activation = WorkflowActivation(run_id=RUN_ID)
    job = activation.jobs.add().finalize_external_streams
    job.quiescence_generation = quiescence_generation
    return activation


async def test_the_park_and_finalize_answers_carry_no_report(
    manager: StreamSubscriptionManager,
) -> None:
    """Those answers run no Workflow code, so Core keeps the last report."""
    runtime = _runtime(manager)
    instance = _instance(ReadOne, runtime, flags=[INDEPENDENT])
    assert _reported(instance.activate(_start(ReadOne))) is not None
    worker: Any = _BareWorker(runtime, manager)

    parked = await _WorkflowWorker._handle_external_stream_jobs(
        worker,
        _park_job(1, 1),
        None,  # type: ignore[arg-type]
    )
    assert parked is not None
    assert _variants(parked) == ["external_stream_park_result"]

    finalized = await _WorkflowWorker._handle_external_stream_jobs(
        worker,
        _finalize_job(1),
        None,  # type: ignore[arg-type]
    )
    assert finalized is not None
    assert _variants(finalized) == ["external_stream_finalized"]


async def test_a_notification_on_a_reported_channel_is_the_runs_concern(
    manager: StreamSubscriptionManager, caplog: pytest.LogCaptureFixture
) -> None:
    """The report, not a command, says which channels the run listens on."""
    runtime = _runtime(manager)
    instance = _instance(ReadOne, runtime, flags=[INDEPENDENT])
    instance.activate(_start(ReadOne))
    with caplog.at_level(logging.DEBUG, logger="temporalio.worker._workflow_instance"):
        instance.activate(
            _notified(
                Notification(channel=_channel(runtime, "tokens"), counter=1),
                Notification(channel="other", counter=1),
            )
        )
    dropped = [
        record.getMessage()
        for record in caplog.records
        if "Dropping a notification" in record.getMessage()
    ]
    assert len(dropped) == 1 and "'other'" in dropped[0], dropped
