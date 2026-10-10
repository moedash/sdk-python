"""Progress of a Workflow's Nexus operations.

The server folds an operation's progress onto the scheduled event of the
caller's next Workflow Task, and Core hands it to the Workflow as a job. These
tests replay histories built by hand, since History is the record: a replay
must see the same progress at the same points as the run that wrote it.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import nexusrpc
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from nexusrpc.handler import (
    CancelOperationContext,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    operation_handler,
    service_handler,
)

from temporalio import nexus, workflow
from temporalio.api.common.v1 import Callback, Payload, Payloads, WorkflowType
from temporalio.api.enums.v1 import EventType, StreamOwnerKind
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.history.v1 import (
    HistoryEvent,
    NexusOperationCompletedEventAttributes,
    NexusOperationFailedEventAttributes,
    NexusOperationScheduledEventAttributes,
    NexusOperationStartedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes,
)
from temporalio.api.nexus.v1 import NexusOperationProgress as ProgressProto
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    AttachStreamCallbackRequest,
    DescribeStreamNotifierRequest,
    NotifyStreamRequest,
)
from temporalio.client import Client, WorkflowHistory
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from tests.helpers.nexus import make_nexus_endpoint_name

ENDPOINT = "progress-endpoint"
TASK_QUEUE = "progress-task-queue"


@nexusrpc.service
class ProgressService:
    produce: nexusrpc.Operation[str, str]


@dataclass
class Observed:
    first: workflow.NexusOperationProgress | None = None
    second: workflow.NexusOperationProgress | None = None
    after_last: workflow.NexusOperationProgress | None = None
    latest_at_end: workflow.NexusOperationProgress | None = None
    result: str | None = None


observed: list[Observed] = []


@workflow.defn(name="ProgressCaller")
class ProgressCaller:
    @workflow.run
    async def run(self) -> None:
        client = workflow.create_nexus_client(
            service=ProgressService, endpoint=ENDPOINT
        )
        handle = await client.start_operation(ProgressService.produce, "input")
        seen = Observed()
        seen.first = await handle.progress()
        assert seen.first
        seen.second = await handle.progress(after_counter=seen.first.counter)
        assert seen.second
        # No newer progress comes, so the wait ends when the operation resolves.
        seen.after_last = await handle.progress(after_counter=seen.second.counter)
        seen.result = await handle
        seen.latest_at_end = handle.latest_progress
        observed.append(seen)


class History:
    """Builds a caller Workflow's History event by event."""

    def __init__(self) -> None:
        self.events: list[HistoryEvent] = []
        self._time = datetime(2026, 10, 10, tzinfo=timezone.utc)
        self._scheduled = 0
        self._started = 0
        self._completed = 0

    def add(self, event_type: EventType.ValueType, **attributes: object) -> int:
        self._time += timedelta(milliseconds=10)
        event_time = Timestamp()
        event_time.FromDatetime(self._time)
        event = HistoryEvent(
            event_id=len(self.events) + 1,
            event_type=event_type,
            event_time=event_time,
            **attributes,  # type: ignore[arg-type]
        )
        self.events.append(event)
        return event.event_id

    def started(self) -> None:
        self.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                workflow_type=WorkflowType(name="ProgressCaller"),
                task_queue=TaskQueue(name=TASK_QUEUE),
                original_execution_run_id="run-id",
                first_execution_run_id="run-id",
                attempt=1,
            ),
        )

    def task(self, *progress: ProgressProto, complete: bool = True) -> None:
        self._scheduled = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
            workflow_task_scheduled_event_attributes=WorkflowTaskScheduledEventAttributes(
                task_queue=TaskQueue(name=TASK_QUEUE),
                start_to_close_timeout=Duration(seconds=10),
                attempt=1,
                nexus_operation_progress=list(progress),
            ),
        )
        self._started = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
            workflow_task_started_event_attributes=WorkflowTaskStartedEventAttributes(
                scheduled_event_id=self._scheduled
            ),
        )
        if complete:
            self._completed = self.add(
                EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
                workflow_task_completed_event_attributes=WorkflowTaskCompletedEventAttributes(
                    scheduled_event_id=self._scheduled, started_event_id=self._started
                ),
            )

    def operation_scheduled(self) -> int:
        return self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
            nexus_operation_scheduled_event_attributes=NexusOperationScheduledEventAttributes(
                endpoint=ENDPOINT,
                service="ProgressService",
                operation="produce",
                input=DataConverter.default.payload_converter.to_payload("input"),
                workflow_task_completed_event_id=self._completed,
                request_id="request-id",
            ),
        )

    def operation_started(self, scheduled_event_id: int) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED,
            nexus_operation_started_event_attributes=NexusOperationStartedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                operation_token="operation-token",
                request_id="request-id",
            ),
        )

    def operation_completed(self, scheduled_event_id: int, result: str) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
            nexus_operation_completed_event_attributes=NexusOperationCompletedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                result=DataConverter.default.payload_converter.to_payload(result),
            ),
        )

    def workflow_completed(self) -> None:
        self.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
                result=Payloads(payloads=[Payload()]),
                workflow_task_completed_event_id=self._completed,
            ),
        )


def progress(scheduled_event_id: int, counter: int) -> ProgressProto:
    return ProgressProto(
        scheduled_event_id=scheduled_event_id,
        position=f"cursor-{counter}",
        counter=counter,
        metadata={"records": str(counter)},
    )


def progress_history() -> WorkflowHistory:
    """The operation starts and reports progress 1 in the same task, then 3 in a
    task of its own, and completes in the last task."""
    h = History()
    h.started()
    h.task()
    scheduled = h.operation_scheduled()
    h.operation_started(scheduled)
    h.task(progress(scheduled, 1))
    h.task(progress(scheduled, 3))
    h.operation_completed(scheduled, "done")
    h.task()
    h.workflow_completed()
    return WorkflowHistory(workflow_id="progress-caller", events=h.events)


async def replay(history: WorkflowHistory) -> None:
    await Replayer(
        workflows=[ProgressCaller],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ).replay_workflow(history)


@pytest.fixture(autouse=True)
def clear_observed() -> None:
    observed.clear()


async def test_progress_reaches_the_operation_handle_on_replay() -> None:
    await replay(progress_history())

    [seen] = observed
    assert seen.first == workflow.NexusOperationProgress(
        position="cursor-1", counter=1, metadata={"records": "1"}
    )
    assert seen.second == workflow.NexusOperationProgress(
        position="cursor-3", counter=3, metadata={"records": "3"}
    )
    assert seen.after_last is None
    assert seen.result == "done"
    assert seen.latest_at_end == seen.second


async def test_progress_is_the_same_on_every_replay() -> None:
    history = progress_history()
    await replay(history)
    await replay(history)

    first, second = observed
    assert first == second


failed_waits: list[tuple[workflow.NexusOperationProgress | None, str]] = []


@workflow.defn(name="ProgressCallerSeesFailure")
class ProgressCallerSeesFailure:
    @workflow.run
    async def run(self) -> None:
        client = workflow.create_nexus_client(
            service=ProgressService, endpoint=ENDPOINT
        )
        handle = await client.start_operation(ProgressService.produce, "input")
        waited = await handle.progress()
        try:
            await handle
            outcome = "completed"
        except Exception as err:
            outcome = type(err).__name__
        failed_waits.append((waited, outcome))


async def test_a_progress_wait_ends_when_the_operation_fails() -> None:
    failed_waits.clear()
    h = History()
    h.events.clear()
    h.add(
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
            workflow_type=WorkflowType(name="ProgressCallerSeesFailure"),
            task_queue=TaskQueue(name=TASK_QUEUE),
            original_execution_run_id="run-id",
            first_execution_run_id="run-id",
            attempt=1,
        ),
    )
    h.task()
    scheduled = h.operation_scheduled()
    h.operation_started(scheduled)
    h.task()
    h.add(
        EventType.EVENT_TYPE_NEXUS_OPERATION_FAILED,
        nexus_operation_failed_event_attributes=NexusOperationFailedEventAttributes(
            scheduled_event_id=scheduled,
            failure=Failure(
                message="handler gave up",
                application_failure_info=ApplicationFailureInfo(type="GaveUp"),
            ),
        ),
    )
    h.task()
    h.workflow_completed()

    await Replayer(
        workflows=[ProgressCallerSeesFailure],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ).replay_workflow(WorkflowHistory(workflow_id="progress-failure", events=h.events))

    assert failed_waits == [(None, "ApplicationError")]


# Live progress, end to end on a server with Nexus progress and the stream
# notifier: the handler attaches the caller's callback to a stream notifier,
# and the producer's NotifyStream calls reach the caller as progress. A server
# without them makes the test skip.

STREAM_TOPIC = "output"


@nexusrpc.service
class LiveProgressService:
    produce: nexusrpc.Operation[str, str]


def _stream_ref(stream_workflow_id: str) -> StreamReference:
    # The server keys a notifier by the owner's run chain, so a reference
    # names the chain's first run. No owner runs here, so any id serves.
    return StreamReference(
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id=stream_workflow_id,
        run_id=f"{stream_workflow_id}-first-run",
        topic=STREAM_TOPIC,
    )


class _LiveProgressOperation(OperationHandler[str, str]):
    """Returns a stream: attaches the caller's callback to the stream's notifier
    and completes asynchronously when the stream closes."""

    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def start(
        self, ctx: StartOperationContext, input: str
    ) -> StartOperationResultAsync:
        assert ctx.callback_url
        client = nexus.client()
        await client.workflow_service.attach_stream_callback(
            AttachStreamCallbackRequest(
                namespace=client.namespace,
                stream_ref=_stream_ref(input),
                request_id=ctx.request_id,
                callback=Callback.Nexus(
                    url=ctx.callback_url, header=dict(ctx.callback_headers)
                ),
            )
        )
        self.started.set()
        return StartOperationResultAsync(token=f"stream-{input}")

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise NotImplementedError


@service_handler(service=LiveProgressService)
class LiveProgressServiceHandler:
    def __init__(self, operation: _LiveProgressOperation) -> None:
        self._operation = operation

    @operation_handler
    def produce(self) -> OperationHandler[str, str]:
        return self._operation


@dataclass
class LiveObserved:
    counters: list[int]
    positions: list[str]
    result: str


live_replayed: list[LiveObserved] = []


@workflow.defn(name="LiveProgressCaller")
class LiveProgressCaller:
    def __init__(self) -> None:
        self._counters: list[int] = []

    @workflow.run
    async def run(self, endpoint: str, stream_workflow_id: str) -> LiveObserved:
        client = workflow.create_nexus_client(
            service=LiveProgressService, endpoint=endpoint
        )
        handle = await client.start_operation(
            LiveProgressService.produce, stream_workflow_id
        )
        positions: list[str] = []
        last = 0
        while (progress := await handle.progress(after_counter=last)) is not None:
            self._counters.append(progress.counter)
            positions.append(progress.position)
            last = progress.counter
        observed = LiveObserved(
            counters=list(self._counters), positions=positions, result=await handle
        )
        if workflow.unsafe.is_replaying():
            live_replayed.append(observed)
        return observed

    @workflow.query
    def counters(self) -> list[int]:
        return list(self._counters)


async def _notify(
    client: Client,
    stream_workflow_id: str,
    counter: int,
    *,
    close_result: str | None = None,
) -> None:
    request = NotifyStreamRequest(
        namespace=client.namespace,
        stream_ref=_stream_ref(stream_workflow_id),
        position=f"cursor-{counter}",
        counter=counter,
        metadata={"n": str(counter)},
    )
    if close_result is not None:
        request.close = True
        request.close_result.CopyFrom(
            DataConverter.default.payload_converter.to_payload(close_result)
        )
    await client.workflow_service.notify_stream(request)


async def _wait_for_event(
    client: Client, workflow_id: str, event_type: EventType.ValueType
) -> None:
    for _ in range(100):
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        if any(e.event_type == event_type for e in history.events):
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"event {EventType.Name(event_type)} never recorded")


async def _wait_for_counters(
    client: Client, workflow_id: str, done: Callable[[list[int]], bool]
) -> list[int] | None:
    handle = client.get_workflow_handle(workflow_id)
    for _ in range(100):
        counters = await handle.query(LiveProgressCaller.counters)
        if done(counters):
            return counters
        await asyncio.sleep(0.1)
    return None


async def _progress_disabled(client: Client, stream_workflow_id: str) -> bool:
    described = await client.workflow_service.describe_stream_notifier(
        DescribeStreamNotifierRequest(
            namespace=client.namespace, stream_ref=_stream_ref(stream_workflow_id)
        )
    )
    return any(c.progress_disabled for c in described.callbacks)


async def test_live_progress_is_ordered_folded_ended_and_replayed(
    client: Client, env: WorkflowEnvironment
) -> None:
    stream_workflow_id = f"live-stream-{uuid.uuid4()}"
    try:
        await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace, stream_ref=_stream_ref(stream_workflow_id)
            )
        )
    except RPCError as err:
        if err.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip(f"server has no stream notifier: {err.message}")
        if err.status != RPCStatusCode.NOT_FOUND:
            raise

    task_queue = f"live-progress-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    operation = _LiveProgressOperation()
    workflow_id = f"live-progress-{uuid.uuid4()}"

    def worker() -> Worker:
        # No cache, so the restarted worker takes the run's next task at once
        # instead of waiting for the sticky queue to time out.
        return Worker(
            client,
            task_queue=task_queue,
            workflows=[LiveProgressCaller],
            nexus_service_handlers=[LiveProgressServiceHandler(operation)],
            max_cached_workflows=0,
        )

    async with worker():
        handle = await client.start_workflow(
            LiveProgressCaller.run,
            args=[endpoint, stream_workflow_id],
            id=workflow_id,
            task_queue=task_queue,
        )
        await asyncio.wait_for(operation.started.wait(), 10)
        # Progress that reaches the caller before the start response is dropped,
        # so the first notification waits for the started event.
        await _wait_for_event(
            client, workflow_id, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED
        )
        await _notify(client, stream_workflow_id, 1)
        first = await _wait_for_counters(client, workflow_id, lambda c: 1 in c)
        if first is None and await _progress_disabled(client, stream_workflow_id):
            await handle.terminate("server does not accept Nexus progress")
            pytest.skip("the caller refused Nexus progress, so the flag is off")
        assert first == [1]

    # With no worker polling, the burst lands while the caller's next task is
    # scheduled but not started. The first notification that reaches the caller
    # rides that task, and the server schedules at most one more task for the
    # newer progress that folded in after the task's scheduled event was written.
    for counter in (2, 3, 4):
        await _notify(client, stream_workflow_id, counter)
    # The notifier delivers in order, so by now the caller has every counter.
    described = await client.workflow_service.describe_stream_notifier(
        DescribeStreamNotifierRequest(
            namespace=client.namespace, stream_ref=_stream_ref(stream_workflow_id)
        )
    )
    for _ in range(100):
        if described.callbacks and described.callbacks[0].delivered_counter == 4:
            break
        await asyncio.sleep(0.1)
        described = await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=_stream_ref(stream_workflow_id),
            )
        )
    assert described.callbacks[0].delivered_counter == 4

    async with worker():
        burst = await _wait_for_counters(client, workflow_id, lambda c: 4 in c)
        assert burst is not None
        # Closing the stream completes the operation, which ends the wait.
        await _notify(client, stream_workflow_id, 5, close_result="done")
        observed = await handle.result()

    assert observed.result == "done"
    assert observed.counters == burst
    assert observed.counters[0] == 1
    assert observed.counters[-1] == 4, "a burst folds to its highest counter"
    assert observed.counters == sorted(set(observed.counters))
    assert len(observed.counters) <= 3, "the burst costs at most two tasks"
    assert observed.positions == [f"cursor-{c}" for c in observed.counters]

    live_replayed.clear()
    await Replayer(
        workflows=[LiveProgressCaller], workflow_runner=UnsandboxedWorkflowRunner()
    ).replay_workflow(await handle.fetch_history())
    assert live_replayed == [observed]
