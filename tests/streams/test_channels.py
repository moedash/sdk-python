"""The notification channel surface: the command, the delivery and the client calls.

Both kinds of channel are covered: the independent one a workflow subscribes
to by command, and the one linked to the workflow, which needs none. The
workflow instance is driven with activations directly, the way Core drives
it, because the dev server this chain tests against does not accept the
subscribe command. The live cases at the end need a server that does, or one
with the linked kind, one whose describe lists the subscriptions, or one that
accepts the unsubscribe, and skip otherwise.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.notification.v1
import temporalio.api.workflow.v1
import temporalio.api.workflowservice.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.common
import temporalio.converter
from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    Callback,
    ChannelKind,
    ChannelSubscriptionInfo,
    Client,
    WorkflowExecutionDescription,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker._workflow_instance import (
    UnsandboxedWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
)
from tests.helpers import assert_eventually, new_worker

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


@workflow.defn
class EmptyLinkedChannel:
    """Asks for a linked channel with no name."""

    @workflow.run
    async def run(self) -> None:
        workflow.linked_channel("")


def _describe(notification: workflow.Notification) -> dict[str, Any]:
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
        "owner": (
            notification.linked_to.business_id if notification.linked_to else None
        ),
        "owner_run": notification.linked_to.run_id if notification.linked_to else None,
    }


@workflow.defn
class ReceiveLinked:
    """Listens on its linked channel and reports the first notification."""

    @workflow.run
    async def run(self, channel: str) -> dict[str, Any]:
        handle = workflow.linked_channel(channel)
        assert handle is workflow.linked_channel(channel)
        assert handle.linked
        return _describe(await handle.receive())


@workflow.defn
class BothKinds:
    """Holds both kinds of handle on one name and keeps what each receives.

    Ends once the linked handle has seen counter two.
    """

    @workflow.run
    async def run(self, channel: str) -> dict[str, list[int]]:
        independent = workflow.subscribe_channel(channel)
        linked = workflow.linked_channel(channel)
        assert not independent.linked and linked.linked
        seen: dict[str, list[int]] = {"independent": [], "linked": []}

        async def collect_independent() -> None:
            async for notification in independent:
                assert notification.linked_to is None
                seen["independent"].append(notification.counter)

        collector = asyncio.create_task(collect_independent())
        async for notification in linked:
            assert notification.linked_to is not None
            seen["linked"].append(notification.counter)
            if notification.counter >= 2:
                break
        collector.cancel()
        return seen


@workflow.defn
class CountLinked:
    """Counts the notifications on its linked channel up to counter two."""

    @workflow.run
    async def run(self, channel: str) -> int:
        seen = 0
        async for notification in workflow.linked_channel(channel):
            seen += 1
            if notification.counter >= 2:
                break
        return seen


async def _drain(handle: workflow.ChannelSubscription) -> dict[str, Any]:
    """What a closed handle still gives: the queue, then the end, then the refusal."""
    drained = [notification.counter async for notification in handle]
    try:
        await handle.receive()
    except RuntimeError as err:
        refused: str | None = str(err)
    else:
        refused = None
    return {"closed": handle.closed, "drained": drained, "refused": refused}


@workflow.defn
class ReceiveThenUnsubscribe:
    """Takes the first notification, unsubscribes twice, then waits to be finished.

    The wait keeps the run open so a late notification can be aimed at it.
    """

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, channel: str) -> dict[str, Any]:
        handle = workflow.subscribe_channel(channel)
        first = await handle.receive()
        assert not handle.closed
        handle.unsubscribe()
        handle.unsubscribe()
        drained = await _drain(handle)
        await workflow.wait_condition(lambda: self._done)
        return {"first": first.counter, **drained}

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class UnsubscribeWithOneQueued:
    """Unsubscribes with a notification still queued and reads it afterwards."""

    @workflow.run
    async def run(self, channel: str) -> dict[str, Any]:
        handle = workflow.subscribe_channel(channel)
        first = await handle.receive()
        handle.unsubscribe()
        return {"first": first.counter, **(await _drain(handle))}


@workflow.defn
class UnsubscribeLinked:
    """Tries to unsubscribe from its linked channel."""

    @workflow.run
    async def run(self, channel: str) -> None:
        workflow.linked_channel(channel).unsubscribe()


@workflow.defn
class Resubscribe:
    """Subscribes, unsubscribes and subscribes again, then receives on the new handle."""

    @workflow.run
    async def run(self, channel: str) -> dict[str, Any]:
        first = workflow.subscribe_channel(channel)
        first.unsubscribe()
        second = workflow.subscribe_channel(channel)
        assert second is not first
        assert first.closed and not second.closed
        notification = await second.receive()
        return {"counter": notification.counter, **(await _drain(first))}


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


def _linked(channel: str, counter: int, position: bytes = b"") -> Notification:
    """A notification the way a linked channel's owner receives it."""
    return Notification(
        channel=channel,
        counter=counter,
        position=position,
        linked_to=temporalio.common.Execution.workflow("wf", "run").to_proto(),
    )


def _signalled(name: str) -> WorkflowActivation:
    job = WorkflowActivationJob()
    job.signal_workflow.signal_name = name
    return WorkflowActivation(run_id="run", jobs=[job])


def _subscribed(completion: WorkflowActivationCompletion) -> list[str]:
    assert completion.HasField("successful"), completion.failed.failure.message
    return [
        command.subscribe_notification_channel.channel
        for command in completion.successful.commands
        if command.HasField("subscribe_notification_channel")
    ]


def _channel_commands(
    completion: WorkflowActivationCompletion,
) -> list[tuple[str, str]]:
    """The channel commands of a completion in order, as (verb, channel) pairs."""
    assert completion.HasField("successful"), completion.failed.failure.message
    commands: list[tuple[str, str]] = []
    for command in completion.successful.commands:
        if command.HasField("subscribe_notification_channel"):
            commands.append(
                ("subscribe", command.subscribe_notification_channel.channel)
            )
        elif command.HasField("unsubscribe_notification_channel"):
            commands.append(
                ("unsubscribe", command.unsubscribe_notification_channel.channel)
            )
    return commands


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


async def test_an_empty_linked_channel_name_is_refused():
    completion = _instance(EmptyLinkedChannel).activate(_start(EmptyLinkedChannel))
    assert completion.HasField("failed")
    assert "channel must not be empty" in completion.failed.failure.message


async def test_a_linked_channel_issues_no_command_and_gets_its_own_notifications():
    instance = _instance(ReceiveLinked)
    completion = instance.activate(_start(ReceiveLinked, "orders"))
    assert completion.HasField("successful"), completion.failed.failure.message
    assert list(completion.successful.commands) == []
    # Without an owner the notification is the independent channel's, which
    # this run never subscribed to.
    completion = instance.activate(_notified(Notification(channel="orders", counter=1)))
    assert not _completed(completion)
    [topic] = temporalio.converter.PayloadConverter.default.to_payloads(["inputs"])
    notification = _linked("orders", 7, b"7-0")
    notification.metadata["topic"].CopyFrom(topic)
    assert _result(instance.activate(_notified(notification))) == {
        "channel": "orders",
        "counter": 7,
        "position": "7-0",
        "topic": "inputs",
        "owner": "wf",
        "owner_run": "run",
    }


async def test_the_owner_on_a_notification_picks_the_handle_of_its_kind():
    instance = _instance(BothKinds)
    completion = instance.activate(_start(BothKinds, "orders"))
    # Only the independent handle costs a command.
    assert _subscribed(completion) == ["orders"]
    assert len(completion.successful.commands) == 1
    assert not _completed(instance.activate(_notified(_linked("orders", 1))))
    assert not _completed(
        instance.activate(_notified(Notification(channel="orders", counter=5)))
    )
    completion = instance.activate(_notified(_linked("orders", 2)))
    assert _result(completion) == {"independent": [5], "linked": [1, 2]}


_CLOSED = "channel subscription closed"


async def test_an_unsubscribe_is_one_command_and_a_late_notification_is_dropped():
    instance = _instance(ReceiveThenUnsubscribe)
    assert _channel_commands(
        instance.activate(_start(ReceiveThenUnsubscribe, "orders"))
    ) == [("subscribe", "orders")]
    # The first notification is taken, then the two unsubscribe calls cost one
    # command between them.
    completion = instance.activate(_notified(Notification(channel="orders", counter=1)))
    assert _channel_commands(completion) == [("unsubscribe", "orders")]
    assert not _completed(completion)
    # The server may still hand the run a notification it folded onto a task
    # before the command landed. Nothing listens, so it changes nothing.
    completion = instance.activate(_notified(Notification(channel="orders", counter=2)))
    assert _channel_commands(completion) == []
    assert not _completed(completion)
    assert _result(instance.activate(_signalled("finish"))) == {
        "first": 1,
        "closed": True,
        "drained": [],
        "refused": _CLOSED,
    }


async def test_a_queued_notification_survives_the_unsubscribe_then_the_iteration_ends():
    instance = _instance(UnsubscribeWithOneQueued)
    instance.activate(_start(UnsubscribeWithOneQueued, "orders"))
    completion = instance.activate(
        _notified(
            Notification(channel="orders", counter=1),
            Notification(channel="orders", counter=2),
        )
    )
    assert _channel_commands(completion) == [("unsubscribe", "orders")]
    assert _result(completion) == {
        "first": 1,
        "closed": True,
        "drained": [2],
        "refused": _CLOSED,
    }


async def test_a_linked_handle_has_no_subscription_to_end():
    completion = _instance(UnsubscribeLinked).activate(
        _start(UnsubscribeLinked, "orders")
    )
    assert completion.HasField("failed")
    assert "a linked channel has no subscription" in completion.failed.failure.message


async def test_a_subscription_after_an_unsubscribe_is_a_new_one():
    instance = _instance(Resubscribe)
    completion = instance.activate(_start(Resubscribe, "orders"))
    assert _channel_commands(completion) == [
        ("subscribe", "orders"),
        ("unsubscribe", "orders"),
        ("subscribe", "orders"),
    ]
    assert not _completed(completion)
    # The notification reaches the open handle, and the closed one stays closed.
    assert _result(
        instance.activate(_notified(Notification(channel="orders", counter=3)))
    ) == {
        "counter": 3,
        "closed": True,
        "drained": [],
        "refused": _CLOSED,
    }


async def test_the_description_maps_every_channel_subscription_field():
    [topic] = temporalio.converter.PayloadConverter.default.to_payloads(["inputs"])
    pending = Notification(channel="orders", position=b"4-0", counter=4)
    pending.metadata["topic"].CopyFrom(topic)
    raw = temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse(
        workflow_execution_info=temporalio.api.workflow.v1.WorkflowExecutionInfo(
            execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id="wf", run_id="run"
            ),
            type=temporalio.api.common.v1.WorkflowType(name="ReceiveOne"),
            status=temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
            task_queue="tq",
        ),
        channel_subscriptions=[
            temporalio.api.workflow.v1.ChannelSubscriptionInfo(
                channel="orders",
                kind=temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_INDEPENDENT,
                subscribed_event_id=5,
                last_counter=3,
                pending_notification=pending,
                scheduled_counter=4,
            ),
            temporalio.api.workflow.v1.ChannelSubscriptionInfo(
                channel="orders",
                kind=temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_LINKED,
                last_counter=2,
                listener_count=1,
                retained_count=2,
                accepted_count=7,
            ),
        ],
    )
    description = await WorkflowExecutionDescription._from_raw_description(
        raw, "default", temporalio.converter.DataConverter.default
    )
    assert description.id == "wf"
    assert description.channel_subscriptions == (
        ChannelSubscriptionInfo(
            channel="orders",
            kind=ChannelKind.INDEPENDENT,
            subscribed_event_id=5,
            last_counter=3,
            pending_notification=workflow.Notification(
                channel="orders", position=b"4-0", counter=4, metadata={"topic": topic}
            ),
            scheduled_counter=4,
            listener_count=0,
            retained_count=0,
            accepted_count=0,
        ),
        ChannelSubscriptionInfo(
            channel="orders",
            kind=ChannelKind.LINKED,
            subscribed_event_id=0,
            last_counter=2,
            pending_notification=None,
            scheduled_counter=0,
            listener_count=1,
            retained_count=2,
            accepted_count=7,
        ),
    )
    raw.ClearField("channel_subscriptions")
    description = await WorkflowExecutionDescription._from_raw_description(
        raw, "default", temporalio.converter.DataConverter.default
    )
    assert description.channel_subscriptions == ()


async def test_the_client_addresses_a_linked_channel_by_execution(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    """Every channel call carries the owner it was given, and only then."""
    requests: list[Any] = []
    owner = temporalio.common.Execution.workflow("wf", "run")
    describe_response = temporalio.api.workflowservice.v1.DescribeChannelResponse(
        kind=temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_LINKED,
        linked_to=owner.to_proto(),
        latest=_linked("orders", 3, b"3-0"),
    )
    responses = {
        "notify_channel": temporalio.api.workflowservice.v1.NotifyChannelResponse(),
        "poll_channel": temporalio.api.workflowservice.v1.PollChannelResponse(
            notifications=[_linked("orders", 3, b"3-0")]
        ),
        "describe_channel": describe_response,
        "register_channel_listener": (
            temporalio.api.workflowservice.v1.RegisterChannelListenerResponse(
                listener_id="listener"
            )
        ),
        "unregister_channel_listener": (
            temporalio.api.workflowservice.v1.UnregisterChannelListenerResponse()
        ),
    }
    for name, response in responses.items():

        async def call(req: Any, *, _response: Any = response, **_: Any) -> Any:
            requests.append(req)
            return _response

        monkeypatch.setattr(client.workflow_service, name, call)

    callback = Callback(url="http://localhost:1/never-called", headers={})
    await client.notify_channel("orders", counter=1, workflow_id="wf", run_id="run")
    [polled] = await client.poll_channel("orders", workflow_id="wf", wait=False)
    description = await client.describe_channel("orders", workflow_id="wf")
    await client.register_channel_listener("orders", callback, workflow_id="wf")
    await client.unregister_channel_listener("orders", "listener", workflow_id="wf")
    # The workflow id is shorthand for a workflow execution, run id and all.
    by_workflow_id = temporalio.common.Execution.workflow("wf").to_proto()
    assert [req.execution for req in requests] == [
        owner.to_proto(),
        by_workflow_id,
        by_workflow_id,
        by_workflow_id,
        by_workflow_id,
    ]
    assert polled.linked_to == owner
    assert description.kind == ChannelKind.LINKED
    assert description.linked_to == owner
    assert description.latest is not None and description.latest.linked_to == owner

    # An execution names any owner, a standalone activity included.
    requests.clear()
    activity = temporalio.common.Execution.activity("act", "run")
    await client.notify_channel("orders", counter=1, execution=activity)
    await client.poll_channel("orders", execution=activity, wait=False)
    await client.describe_channel("orders", execution=activity)
    await client.register_channel_listener("orders", callback, execution=activity)
    await client.unregister_channel_listener("orders", "listener", execution=activity)
    assert [req.execution for req in requests] == [activity.to_proto()] * 5
    assert activity.to_proto().type == (
        temporalio.api.enums.v1.ExecutionType.EXECUTION_TYPE_ACTIVITY
    )

    # Without an owner the calls address the independent channel.
    requests.clear()
    await client.notify_channel("orders", counter=1)
    await client.poll_channel("orders", wait=False)
    await client.describe_channel("orders")
    await client.register_channel_listener("orders", callback)
    await client.unregister_channel_listener("orders", "listener")
    assert [req.HasField("execution") for req in requests] == [False] * 5

    with pytest.raises(ValueError, match="run_id needs workflow_id"):
        await client.notify_channel("orders", counter=1, run_id="run")
    # The shorthand and the execution are two ways to say one thing.
    with pytest.raises(ValueError, match="not both"):
        await client.notify_channel(
            "orders", counter=1, execution=activity, workflow_id="wf"
        )
    with pytest.raises(ValueError, match="not both"):
        await client.poll_channel("orders", execution=activity, run_id="run")


@pytest.mark.needs_channel_server
async def test_a_workflow_receives_a_client_notification(client: Client):
    channel = f"orders-{uuid.uuid4()}"
    worker = new_worker(client, ReceiveOne)
    running = asyncio.create_task(worker.run())
    handle = await client.start_workflow(
        ReceiveOne.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=worker.task_queue,
    )
    try:

        async def subscribed() -> None:
            # The worker's Core has to carry the subscribe command to the
            # server. One that refuses it fails every completion, and the task
            # times out instead; say so rather than wait on the result forever.
            events = [event.event_type async for event in handle.fetch_history_events()]
            if EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT in events:
                pytest.fail(
                    "the worker could not complete the task that subscribes; the Core "
                    "the bridge pins must carry the subscribe command"
                )
            assert (
                EventType.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED in events
            )

        await assert_eventually(subscribed, timeout=timedelta(seconds=30))

        async def listening() -> None:
            # The channel exists once the subscribe lands, so a describe that
            # races it is answered with not found.
            try:
                description = await client.describe_channel(channel)
            except RPCError as err:
                assert err.status != RPCStatusCode.NOT_FOUND, "channel not created yet"
                raise
            assert [listener.workflow_id for listener in description.listeners] == [
                handle.id
            ]

        await assert_eventually(listening)
        listeners = await client.notify_channel(
            channel, position=b"1-0", counter=1, metadata={"topic": "inputs"}
        )
        assert listeners == 1
        assert await asyncio.wait_for(handle.result(), 30) == {
            "channel": channel,
            "counter": 1,
            "position": "1-0",
            "topic": "inputs",
        }
        polled = await client.poll_channel(channel, wait=False)
        assert [n.counter for n in polled] == [1]
        description = await client.describe_channel(channel)
        assert description.latest is not None and description.latest.counter == 1
    finally:
        # A Core that refuses the subscribe command leaves the task in a
        # timeout loop and the worker's shutdown waiting on it, so end the run
        # first and give the shutdown a bound.
        with contextlib.suppress(RPCError):
            await handle.terminate()
        try:
            await asyncio.wait_for(worker.shutdown(), 15)
        except asyncio.TimeoutError:
            running.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await running


@pytest.mark.needs_channel_server
async def test_a_callback_listener_registers_and_unregisters(client: Client):
    channel = f"orders-{uuid.uuid4()}"
    callback = Callback(url="http://localhost:1/never-called", headers={})
    listener_id = await client.register_channel_listener(channel, callback)
    description = await client.describe_channel(channel)
    assert [listener.listener_id for listener in description.listeners] == [listener_id]
    assert description.listeners[0].callback == callback
    await client.unregister_channel_listener(channel, listener_id)
    description = await client.describe_channel(channel)
    assert description.listeners == []


@pytest.mark.needs_channel_server
async def test_a_channel_retains_notifications_for_pollers(client: Client):
    channel = f"orders-{uuid.uuid4()}"
    # A channel nobody has touched does not exist.
    with pytest.raises(RPCError) as untouched:
        await client.describe_channel(f"untouched-{uuid.uuid4()}")
    assert untouched.value.status == RPCStatusCode.NOT_FOUND
    # Nobody listens yet: the notification is kept for pollers and the count
    # says zero.
    assert await client.notify_channel(channel, position=b"2-0", counter=2) == 0
    description = await client.describe_channel(channel)
    assert description.listeners == []
    assert description.latest is not None
    assert (description.latest.position, description.latest.counter) == (b"2-0", 2)
    assert description.retained_count == 1
    polled = await client.poll_channel(channel, wait=False)
    assert [(n.position, n.counter) for n in polled] == [(b"2-0", 2)]
    # At or below the latest counter a notify changes nothing and is not kept.
    assert await client.notify_channel(channel, position=b"1-0", counter=1) == 0
    assert await client.notify_channel(channel, position=b"2-0", counter=2) == 0
    description = await client.describe_channel(channel)
    assert description.latest is not None and description.latest.counter == 2
    assert description.retained_count == 1
    # Above it the notification is kept, metadata and all, and a poll after
    # the earlier counter sees only the new one.
    assert (
        await client.notify_channel(
            channel, position=b"3-0", counter=3, metadata={"topic": "inputs"}
        )
        == 0
    )
    [newest] = await client.poll_channel(channel, after_counter=2, wait=False)
    assert newest.counter == 3
    assert (
        client.data_converter.payload_converter.from_payload(
            newest.metadata["topic"], str
        )
        == "inputs"
    )
    polled = await client.poll_channel(channel, wait=False)
    assert [n.counter for n in polled] == [2, 3]
    # A poll above the latest waits its bound out and comes back empty.
    polled = await client.poll_channel(
        channel, after_counter=3, wait=timedelta(seconds=1)
    )
    assert polled == []


async def _stop(handle: Any, worker: Any, running: asyncio.Task[None]) -> None:
    """End the run and the worker, with a bound on the shutdown."""
    with contextlib.suppress(RPCError):
        await handle.terminate()
    try:
        await asyncio.wait_for(worker.shutdown(), 15)
    except asyncio.TimeoutError:
        running.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await running


@pytest.mark.needs_linked_server
async def test_a_workflow_receives_a_notification_on_its_linked_channel(
    client: Client,
):
    channel = f"orders-{uuid.uuid4()}"
    worker = new_worker(client, ReceiveLinked)
    running = asyncio.create_task(worker.run())
    handle = await client.start_workflow(
        ReceiveLinked.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=worker.task_queue,
    )
    try:
        # The channel exists with the run: no listener registers, nothing is
        # retained yet, and the owner is named.
        description = await client.describe_channel(channel, workflow_id=handle.id)
        assert description.kind == ChannelKind.LINKED
        assert description.listeners == []
        assert description.latest is None
        assert description.retained_count == 0
        assert description.linked_to is not None
        assert description.linked_to.business_id == handle.id
        # The owner is the one listener.
        listeners = await client.notify_channel(
            channel,
            position=b"1-0",
            counter=1,
            metadata={"topic": "inputs"},
            workflow_id=handle.id,
        )
        assert listeners == 1
        assert await asyncio.wait_for(handle.result(), 30) == {
            "channel": channel,
            "counter": 1,
            "position": "1-0",
            "topic": "inputs",
            "owner": handle.id,
            "owner_run": handle.first_execution_run_id,
        }
        # The owner listens by construction, so History holds no subscribe
        # event; the notification rode a scheduled event.
        events = [event.event_type async for event in handle.fetch_history_events()]
        assert (
            EventType.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED not in events
        )
    finally:
        await _stop(handle, worker, running)


@pytest.mark.needs_linked_server
async def test_a_linked_channel_lives_and_dies_with_its_workflow(client: Client):
    """The client side alone: no worker polls, so the run stays open until ended."""
    channel = f"orders-{uuid.uuid4()}"
    handle = await client.start_workflow(
        CountLinked.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=f"nobody-polls-{uuid.uuid4()}",
    )
    run_id = handle.first_execution_run_id
    assert run_id is not None
    # A name nobody has notified exists all the same, with nothing in it.
    description = await client.describe_channel(channel, workflow_id=handle.id)
    assert description.kind == ChannelKind.LINKED
    assert (description.listeners, description.latest) == ([], None)
    assert description.retained_count == 0
    assert description.linked_to is not None
    assert (description.linked_to.business_id, description.linked_to.run_id) == (
        handle.id,
        run_id,
    )
    # The independent channel of that name is a different thing and does not
    # exist.
    with pytest.raises(RPCError) as independent:
        await client.describe_channel(channel)
    assert independent.value.status == RPCStatusCode.NOT_FOUND
    # A run id names that run; one that is not the chain's is not found, and
    # neither is a workflow that never ran.
    description = await client.describe_channel(
        channel, workflow_id=handle.id, run_id=run_id
    )
    assert description.kind == ChannelKind.LINKED
    for wrong in (
        client.describe_channel(
            channel, workflow_id=handle.id, run_id=str(uuid.uuid4())
        ),
        client.notify_channel(
            channel, counter=1, workflow_id=handle.id, run_id=str(uuid.uuid4())
        ),
        client.notify_channel(channel, counter=1, workflow_id=f"never-{uuid.uuid4()}"),
    ):
        with pytest.raises(RPCError) as missing:
            await wrong
        assert missing.value.status == RPCStatusCode.NOT_FOUND
    # The channel ends with the run.
    await handle.terminate()
    with pytest.raises(RPCError) as closed:
        await client.notify_channel(channel, counter=1, workflow_id=handle.id)
    assert closed.value.status == RPCStatusCode.NOT_FOUND


@pytest.mark.needs_linked_server
@pytest.mark.needs_execution_server
async def test_a_linked_channel_names_its_owner_as_an_execution(client: Client):
    """The client side alone: the owner comes back typed, by either spelling."""
    channel = f"orders-{uuid.uuid4()}"
    handle = await client.start_workflow(
        CountLinked.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=f"nobody-polls-{uuid.uuid4()}",
    )
    run_id = handle.first_execution_run_id
    assert run_id is not None
    by_id = temporalio.common.Execution.workflow(handle.id)
    by_run = temporalio.common.Execution.workflow(handle.id, run_id)
    try:
        description = await client.describe_channel(channel, execution=by_id)
        assert description.kind == ChannelKind.LINKED
        assert description.linked_to is not None
        assert description.linked_to == by_run
        assert description.linked_to.type is temporalio.common.ExecutionType.WORKFLOW
        # The execution and the workflow id shorthand reach one channel.
        assert (
            await client.notify_channel(
                channel, position=b"1-0", counter=1, execution=by_id
            )
            == 1
        )
        [polled] = await client.poll_channel(channel, workflow_id=handle.id, wait=False)
        assert (polled.counter, polled.linked_to) == (1, by_run)
        [polled] = await client.poll_channel(channel, execution=by_run, wait=False)
        assert polled.counter == 1
        # The same id as an activity names an execution that never ran.
        with pytest.raises(RPCError) as missing:
            await client.describe_channel(
                channel, execution=temporalio.common.Execution.activity(handle.id)
            )
        assert missing.value.status == RPCStatusCode.NOT_FOUND
    finally:
        with contextlib.suppress(RPCError):
            await handle.terminate()


@pytest.mark.needs_linked_server
async def test_a_linked_channel_is_polled_by_workflow_id(client: Client):
    channel = f"orders-{uuid.uuid4()}"
    worker = new_worker(client, CountLinked)
    running = asyncio.create_task(worker.run())
    handle = await client.start_workflow(
        CountLinked.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=worker.task_queue,
    )
    try:
        assert (
            await client.notify_channel(
                channel, position=b"1-0", counter=1, workflow_id=handle.id
            )
            == 1
        )
        polled = await client.poll_channel(channel, workflow_id=handle.id, wait=False)
        assert [(n.position, n.counter) for n in polled] == [(b"1-0", 1)]
        assert polled[0].linked_to is not None
        assert polled[0].linked_to.business_id == handle.id
        # The run id reaches the same channel.
        assert [
            n.counter
            for n in await client.poll_channel(
                channel,
                workflow_id=handle.id,
                run_id=handle.first_execution_run_id,
                wait=False,
            )
        ] == [1]
        description = await client.describe_channel(channel, workflow_id=handle.id)
        assert description.kind == ChannelKind.LINKED
        assert description.latest is not None and description.latest.counter == 1
        assert description.retained_count == 1
        # A poll above the latest waits its bound out and comes back empty.
        polled = await client.poll_channel(
            channel, workflow_id=handle.id, after_counter=1, wait=timedelta(seconds=1)
        )
        assert polled == []
        assert (
            await client.notify_channel(
                channel, position=b"2-0", counter=2, workflow_id=handle.id
            )
            == 1
        )
        assert await asyncio.wait_for(handle.result(), 30) == 2
        # The ring went with the run, so a poll after the close finds nothing
        # to read.
        with pytest.raises(RPCError) as closed:
            await client.poll_channel(
                channel, workflow_id=handle.id, after_counter=1, wait=False
            )
        assert closed.value.status == RPCStatusCode.NOT_FOUND
    finally:
        await _stop(handle, worker, running)


_SUBSCRIBED = EventType.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED
_UNSUBSCRIBED = EventType.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_UNSUBSCRIBED


async def _event_ids(handle: Any, event_type: Any) -> list[int]:
    """The ids of the events of ``event_type`` in the run's History so far."""
    return [
        event.event_id
        async for event in handle.fetch_history_events()
        if event.event_type == event_type
    ]


async def _one_event(handle: Any, event_type: Any) -> int:
    """The id of the one event of ``event_type``, failing until it is there."""
    ids = await _event_ids(handle, event_type)
    assert len(ids) == 1, ids
    return ids[0]


@pytest.mark.needs_describe_server
async def test_a_description_lists_an_independent_subscription(client: Client):
    channel = f"orders-{uuid.uuid4()}"
    worker = new_worker(client, CountToTwo)
    running = asyncio.create_task(worker.run())
    handle = await client.start_workflow(
        CountToTwo.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=worker.task_queue,
    )
    try:
        event_id = await assert_eventually(
            lambda: _one_event(handle, _SUBSCRIBED), timeout=timedelta(seconds=30)
        )
        # Listed from the subscribe event on, with nothing accepted yet. The
        # counts belong to the channel execution and stay zero here.
        description = await handle.describe()
        assert description.channel_subscriptions == (
            ChannelSubscriptionInfo(
                channel=channel,
                kind=ChannelKind.INDEPENDENT,
                subscribed_event_id=event_id,
                last_counter=0,
                pending_notification=None,
                scheduled_counter=0,
                listener_count=0,
                retained_count=0,
                accepted_count=0,
            ),
        )
        assert await client.notify_channel(channel, position=b"1-0", counter=1) == 1

        async def accepted() -> None:
            # Once the task that carried it completes, the counter is the
            # run's and nothing is pending or scheduled any more.
            [info] = (await handle.describe()).channel_subscriptions
            assert info.last_counter == 1
            assert info.pending_notification is None
            assert info.scheduled_counter == 0

        await assert_eventually(accepted)
        assert await client.notify_channel(channel, position=b"2-0", counter=2) == 1
        assert await asyncio.wait_for(handle.result(), 30) == 2
        # A closed run keeps listing what it stood on.
        [info] = (await handle.describe()).channel_subscriptions
        assert (info.kind, info.last_counter) == (ChannelKind.INDEPENDENT, 2)
    finally:
        await _stop(handle, worker, running)


@pytest.mark.needs_describe_server
async def test_a_description_lists_a_linked_channel_once_it_holds_state(
    client: Client,
):
    """The client side alone: no worker polls, so the run stays open until ended."""
    channel = f"orders-{uuid.uuid4()}"
    handle = await client.start_workflow(
        CountLinked.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=f"nobody-polls-{uuid.uuid4()}",
    )
    try:
        # An untouched linked name exists by construction and holds nothing,
        # so it is not listed.
        assert (await handle.describe()).channel_subscriptions == ()
        assert (
            await client.notify_channel(
                channel, position=b"1-0", counter=1, workflow_id=handle.id
            )
            == 1
        )
        [info] = (await handle.describe()).channel_subscriptions
        assert (info.channel, info.kind) == (channel, ChannelKind.LINKED)
        # The owner's state took the notification in the write that accepted
        # it, so the counter is the run's at once. Nobody polls, and the first
        # task was scheduled without a counter when the run started, so the
        # notification waits behind it as the pending entry.
        assert (info.subscribed_event_id, info.last_counter) == (0, 1)
        assert info.pending_notification is not None
        assert info.pending_notification.counter == 1
        assert info.pending_notification.linked_to is not None
        assert info.pending_notification.linked_to.business_id == handle.id
        assert info.scheduled_counter == 0
        assert (info.listener_count, info.retained_count, info.accepted_count) == (
            0,
            1,
            1,
        )
        # A callback on the linked channel shows up in the owner's count.
        callback = Callback(url="http://localhost:1/never-called", headers={})
        listener_id = await client.register_channel_listener(
            channel, callback, workflow_id=handle.id
        )
        [info] = (await handle.describe()).channel_subscriptions
        assert info.listener_count == 1
        await client.unregister_channel_listener(
            channel, listener_id, workflow_id=handle.id
        )
        [info] = (await handle.describe()).channel_subscriptions
        assert info.listener_count == 0
        # A closed run keeps listing what it stood on.
        await handle.terminate()
        [info] = (await handle.describe()).channel_subscriptions
        assert (info.kind, info.last_counter) == (ChannelKind.LINKED, 1)
    finally:
        with contextlib.suppress(RPCError):
            await handle.terminate()


@pytest.mark.needs_unsubscribe_server
async def test_a_workflow_unsubscribes_and_a_later_notify_wakes_nothing(
    client: Client,
):
    channel = f"orders-{uuid.uuid4()}"
    worker = new_worker(client, ReceiveThenUnsubscribe)
    running = asyncio.create_task(worker.run())
    handle = await client.start_workflow(
        ReceiveThenUnsubscribe.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=worker.task_queue,
    )
    try:
        subscribed_id = await assert_eventually(
            lambda: _one_event(handle, _SUBSCRIBED), timeout=timedelta(seconds=30)
        )
        assert await client.notify_channel(channel, position=b"1-0", counter=1) == 1
        unsubscribed_id = await assert_eventually(
            lambda: _one_event(handle, _UNSUBSCRIBED), timeout=timedelta(seconds=30)
        )
        assert unsubscribed_id > subscribed_id
        # The event names the subscription it ended.
        [event] = [
            event
            async for event in handle.fetch_history_events()
            if event.event_id == unsubscribed_id
        ]
        attrs = event.workflow_notification_channel_unsubscribed_event_attributes
        assert (attrs.channel, attrs.subscribed_event_id) == (channel, subscribed_id)
        # Gone from both sides: the channel's listeners and the run's standing.
        description = await client.describe_channel(channel)
        assert [listener.workflow_id for listener in description.listeners] == []
        assert (await handle.describe()).channel_subscriptions == ()
        # Nothing listens any more, so a notify wakes nobody and is only
        # retained for pollers.
        assert await client.notify_channel(channel, position=b"2-0", counter=2) == 0
        await handle.signal(ReceiveThenUnsubscribe.finish)
        assert await asyncio.wait_for(handle.result(), 30) == {
            "first": 1,
            "closed": True,
            "drained": [],
            "refused": _CLOSED,
        }
        assert await _event_ids(handle, _SUBSCRIBED) == [subscribed_id]
        assert await _event_ids(handle, _UNSUBSCRIBED) == [unsubscribed_id]
    finally:
        await _stop(handle, worker, running)
