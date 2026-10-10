"""A Worker that gets a Nexus operation progress job it doesn't act on.

The server folds an operation's progress onto the caller's next Workflow
Task, and Core hands it to the Workflow as a job. A Worker that doesn't act on
progress must drop the job, not fail the task, so a server that sends progress
can't break it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import nexusrpc
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from temporalio import workflow
from temporalio.api.common.v1 import Payloads, WorkflowType
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import (
    HistoryEvent,
    NexusOperationCompletedEventAttributes,
    NexusOperationScheduledEventAttributes,
    NexusOperationStartedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes,
)
from temporalio.api.nexus.v1 import NexusOperationProgress
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.client import WorkflowHistory
from temporalio.converter import DataConverter
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner

ENDPOINT = "progress-endpoint"
TASK_QUEUE = "progress-task-queue"
CONVERTER = DataConverter.default.payload_converter


@nexusrpc.service
class ProduceService:
    produce: nexusrpc.Operation[str, str]


@workflow.defn(name="ResultOnlyCaller")
class ResultOnlyCaller:
    @workflow.run
    async def run(self) -> str:
        client = workflow.create_nexus_client(service=ProduceService, endpoint=ENDPOINT)
        handle = await client.start_operation(ProduceService.produce, "input")
        return await handle


def history_with_progress() -> WorkflowHistory:
    events: list[HistoryEvent] = []
    clock = datetime(2026, 10, 10, tzinfo=timezone.utc)

    def add(event_type: EventType.ValueType, **attributes: object) -> int:
        nonlocal clock
        clock += timedelta(milliseconds=10)
        event_time = Timestamp()
        event_time.FromDatetime(clock)
        events.append(
            HistoryEvent(
                event_id=len(events) + 1,
                event_type=event_type,
                event_time=event_time,
                **attributes,  # type: ignore[arg-type]
            )
        )
        return len(events)

    def task(*progress: NexusOperationProgress) -> int:
        scheduled = add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
            workflow_task_scheduled_event_attributes=WorkflowTaskScheduledEventAttributes(
                task_queue=TaskQueue(name=TASK_QUEUE),
                start_to_close_timeout=Duration(seconds=10),
                attempt=1,
                nexus_operation_progress=list(progress),
            ),
        )
        started = add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
            workflow_task_started_event_attributes=WorkflowTaskStartedEventAttributes(
                scheduled_event_id=scheduled
            ),
        )
        return add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
            workflow_task_completed_event_attributes=WorkflowTaskCompletedEventAttributes(
                scheduled_event_id=scheduled, started_event_id=started
            ),
        )

    add(
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
            workflow_type=WorkflowType(name="ResultOnlyCaller"),
            task_queue=TaskQueue(name=TASK_QUEUE),
            original_execution_run_id="run-id",
            first_execution_run_id="run-id",
            attempt=1,
        ),
    )
    completed = task()
    scheduled = add(
        EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
        nexus_operation_scheduled_event_attributes=NexusOperationScheduledEventAttributes(
            endpoint=ENDPOINT,
            service="ProduceService",
            operation="produce",
            input=CONVERTER.to_payload("input"),
            workflow_task_completed_event_id=completed,
            request_id="request-id",
        ),
    )
    add(
        EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED,
        nexus_operation_started_event_attributes=NexusOperationStartedEventAttributes(
            scheduled_event_id=scheduled,
            operation_token="operation-token",
            request_id="request-id",
        ),
    )
    task(
        NexusOperationProgress(
            scheduled_event_id=scheduled, position="cursor-1", counter=1
        )
    )
    add(
        EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
        nexus_operation_completed_event_attributes=NexusOperationCompletedEventAttributes(
            scheduled_event_id=scheduled, result=CONVERTER.to_payload("done")
        ),
    )
    completed = task()
    add(
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
        workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
            result=Payloads(payloads=[CONVERTER.to_payload("done")]),
            workflow_task_completed_event_id=completed,
        ),
    )
    return WorkflowHistory(workflow_id="result-only-caller", events=events)


async def test_a_progress_job_the_workflow_does_not_act_on_is_dropped() -> None:
    result = await Replayer(
        workflows=[ResultOnlyCaller], workflow_runner=UnsandboxedWorkflowRunner()
    ).replay_workflow(history_with_progress(), raise_on_replay_failure=False)
    assert result.replay_failure is None, result.replay_failure
