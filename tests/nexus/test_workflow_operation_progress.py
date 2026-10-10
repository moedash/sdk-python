"""Progress of a Workflow's Nexus operations.

The server folds an operation's progress onto the scheduled event of the
caller's next Workflow Task, and Core hands it to the Workflow as a job. These
tests replay histories built by hand, since History is the record: a replay
must see the same progress at the same points as the run that wrote it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import nexusrpc
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from temporalio import workflow
from temporalio.api.common.v1 import Payload, Payloads, WorkflowType
from temporalio.api.enums.v1 import EventType
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
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.client import WorkflowHistory
from temporalio.converter import DataConverter
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner

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
