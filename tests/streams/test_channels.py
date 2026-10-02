"""The notification channel surface: the command, the delivery and the client calls.

The workflow instance is driven with activations directly, the way Core
drives it, because the dev server this chain tests against does not accept
the subscribe command.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import temporalio.api.common.v1
import temporalio.api.notification.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.common
import temporalio.converter
from temporalio import workflow
from temporalio.worker._workflow_instance import (
    UnsandboxedWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
)

WorkflowActivation = temporalio.bridge.proto.workflow_activation.WorkflowActivation
WorkflowActivationJob = (
    temporalio.bridge.proto.workflow_activation.WorkflowActivationJob
)
WorkflowActivationCompletion = (
    temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion
)
Notification = temporalio.api.notification.v1.Notification


@workflow.defn
class ReceiveOne:
    """Subscribes to one channel and reports the first notification."""

    @workflow.run
    async def run(self, channel: str) -> dict[str, Any]:
        notification = await workflow.subscribe_channel(channel).receive()
        return {
            "channel": notification.channel,
            "counter": notification.counter,
            "position": notification.position.decode(),
            "topic": (
                workflow.payload_converter().from_payload(
                    notification.metadata["topic"], str
                )
                if "topic" in notification.metadata
                else None
            ),
        }


@workflow.defn
class CountToTwo:
    """Subscribes twice to one channel and counts notifications up to counter two."""

    @workflow.run
    async def run(self, channel: str) -> int:
        first = workflow.subscribe_channel(channel)
        second = workflow.subscribe_channel(channel)
        assert first is second
        seen = 0
        async for notification in second:
            seen += 1
            if notification.counter >= 2:
                break
        return seen


@workflow.defn
class EmptyChannel:
    """Asks for a channel with no name."""

    @workflow.run
    async def run(self) -> None:
        workflow.subscribe_channel("")


def _instance(workflow_class: type) -> WorkflowInstance:
    """Build an instance the way the worker does, without a worker.

    Needs a running event loop, since the constructor puts the runtime on it.
    """
    defn = workflow._Definition.must_from_class(workflow_class)
    now = datetime.now(timezone.utc)
    info = workflow.Info(
        attempt=1,
        continued_run_id=None,
        cron_schedule=None,
        execution_timeout=None,
        first_execution_run_id="run",
        headers={},
        namespace="default",
        original_execution_run_id="run",
        parent=None,
        root=None,
        priority=temporalio.common.Priority.default,
        raw_memo={},
        retry_policy=None,
        run_id="run",
        run_timeout=None,
        search_attributes={},
        start_time=now,
        task_queue="tq",
        task_timeout=timedelta(seconds=10),
        typed_search_attributes=temporalio.common.TypedSearchAttributes.empty,
        workflow_id="wf",
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
        )
    )


def _start(workflow_class: type, *args: Any) -> WorkflowActivation:
    job = WorkflowActivationJob()
    init = job.initialize_workflow
    init.workflow_type = workflow._Definition.must_from_class(workflow_class).name or ""
    init.workflow_id = "wf"
    init.arguments.extend(
        temporalio.converter.PayloadConverter.default.to_payloads(args)
    )
    return WorkflowActivation(run_id="run", jobs=[job])


def _notified(*notifications: Notification) -> WorkflowActivation:
    job = WorkflowActivationJob()
    job.notifications_received.notifications.extend(notifications)
    return WorkflowActivation(run_id="run", jobs=[job])


def _subscribed(completion: WorkflowActivationCompletion) -> list[str]:
    assert completion.HasField("successful"), completion.failed.failure.message
    return [
        command.subscribe_notification_channel.channel
        for command in completion.successful.commands
        if command.HasField("subscribe_notification_channel")
    ]


def _completed(completion: WorkflowActivationCompletion) -> bool:
    assert completion.HasField("successful"), completion.failed.failure.message
    return any(
        command.HasField("complete_workflow_execution")
        for command in completion.successful.commands
    )


def _result(completion: WorkflowActivationCompletion) -> Any:
    assert completion.HasField("successful"), completion.failed.failure.message
    [done] = [
        command
        for command in completion.successful.commands
        if command.HasField("complete_workflow_execution")
    ]
    return temporalio.converter.PayloadConverter.default.from_payload(
        done.complete_workflow_execution.result
    )


async def test_the_first_subscription_is_a_command_and_the_second_shares_it():
    instance = _instance(CountToTwo)
    completion = instance.activate(_start(CountToTwo, "orders"))
    assert _subscribed(completion) == ["orders"]
    assert not _completed(completion)


async def test_a_notifications_received_job_wakes_the_receiver():
    instance = _instance(ReceiveOne)
    assert _subscribed(instance.activate(_start(ReceiveOne, "orders"))) == ["orders"]
    [topic] = temporalio.converter.PayloadConverter.default.to_payloads(["inputs"])
    completion = instance.activate(
        _notified(
            Notification(
                channel="orders", position=b"7-0", counter=7, metadata={"topic": topic}
            )
        )
    )
    assert _result(completion) == {
        "channel": "orders",
        "counter": 7,
        "position": "7-0",
        "topic": "inputs",
    }


async def test_notifications_arrive_in_order_and_other_channels_are_dropped():
    instance = _instance(CountToTwo)
    instance.activate(_start(CountToTwo, "orders"))
    # A channel this run never subscribed to is not the workflow's concern.
    completion = instance.activate(_notified(Notification(channel="other", counter=9)))
    assert not _completed(completion)
    completion = instance.activate(
        _notified(
            Notification(channel="orders", counter=1),
            Notification(channel="orders", counter=2),
        )
    )
    assert _result(completion) == 2


async def test_an_empty_channel_name_is_refused():
    completion = _instance(EmptyChannel).activate(_start(EmptyChannel))
    assert completion.HasField("failed")
    assert "channel must not be empty" in completion.failed.failure.message
