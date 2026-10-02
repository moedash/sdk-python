"""The notification channel surface: the command, the delivery and the client calls.

The workflow instance is driven with activations directly, the way Core
drives it, because the dev server this chain tests against does not accept
the subscribe command. The live cases at the end need a server that does and
skip otherwise; the ones on the linked kind need a server with it. The
external-stream runtime's own subscriptions go through the same instance
behind a gate, which is driven here too, and are covered beside the stream
tests as well.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.api.notification.v1
import temporalio.api.workflowservice.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.common
import temporalio.converter
from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Callback, ChannelKind, Client
from temporalio.client._impl import _address_channel
from temporalio.contrib.external_workflow_streams._wake import ChannelAddress
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker._workflow_instance import (
    UnsandboxedWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
    _WorkflowLogicFlag,
)
from temporalio.workflow._context import _Runtime
from tests.contrib.external_workflow_streams.conftest import server_channel_support
from tests.helpers import assert_eventually, new_worker

WorkflowActivation = temporalio.bridge.proto.workflow_activation.WorkflowActivation
WorkflowActivationJob = (
    temporalio.bridge.proto.workflow_activation.WorkflowActivationJob
)
WorkflowActivationCompletion = (
    temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion
)
Notification = temporalio.api.notification.v1.Notification
WorkflowExecution = temporalio.api.common.v1.WorkflowExecution
INDEPENDENT = _WorkflowLogicFlag.SUBSCRIBE_NOTIFICATION_CHANNELS
LINKED = _WorkflowLogicFlag.LINKED_NOTIFICATION_CHANNELS


def _linked_to(notification: workflow.Notification) -> dict[str, str] | None:
    if notification.linked_to is None:
        return None
    return {
        "workflow_id": notification.linked_to.workflow_id,
        "run_id": notification.linked_to.run_id,
    }


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
class ReceiveLinked:
    """Reads its own linked channel and reports the first notification."""

    @workflow.run
    async def run(self, channel: str) -> dict[str, Any]:
        subscription = workflow.linked_channel(channel)
        assert subscription is workflow.linked_channel(channel)
        assert subscription.linked
        notification = await subscription.receive()
        return {
            "channel": notification.channel,
            "counter": notification.counter,
            "linked_to": _linked_to(notification),
        }


@workflow.defn
class BothKinds:
    """Listens on the independent and the linked channel of one name."""

    @workflow.run
    async def run(self, channel: str) -> list[str]:
        independent = workflow.subscribe_channel(channel)
        linked = workflow.linked_channel(channel)
        seen: list[str] = []

        async def take(kind: str, subscription: workflow.ChannelSubscription) -> None:
            notification = await subscription.receive()
            assert (notification.linked_to is not None) == (kind == "linked")
            seen.append(kind)

        await asyncio.gather(take("independent", independent), take("linked", linked))
        return seen


@workflow.defn
class ListenOnStream:
    """Asks the run to listen for a stream the way the stream runtime does."""

    @workflow.run
    async def run(self, channel: str, owner: str) -> bool:
        instance: Any = _Runtime.current()
        return instance.subscribe_stream_channel(
            ChannelAddress(channel=channel, workflow_id=owner)
        )


def _instance(
    workflow_class: type, flags: Iterable[_WorkflowLogicFlag] = ()
) -> WorkflowInstance:
    """Build an instance the way the worker does, without a worker.

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
        first_execution_run_id="run",
        headers={},
        namespace="default",
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
            payload_converter_factory=converter._new_payload_converter,
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
        )
    )


def _start(
    workflow_class: type,
    *args: Any,
    recorded_flags: Iterable[_WorkflowLogicFlag] = (),
) -> WorkflowActivation:
    """The run's first activation.

    With ``recorded_flags`` it is a replay, and those are the flags History
    says the live run used.
    """
    job = WorkflowActivationJob()
    init = job.initialize_workflow
    init.workflow_type = workflow._Definition.must_from_class(workflow_class).name or ""
    init.workflow_id = "wf"
    init.arguments.extend(
        temporalio.converter.PayloadConverter.default.to_payloads(args)
    )
    recorded = [int(flag) for flag in recorded_flags]
    return WorkflowActivation(
        run_id="run",
        jobs=[job],
        is_replaying=bool(recorded),
        available_internal_flags=recorded,
    )


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


# --- the linked kind ----------------------------------------------------------


def _linked(channel: str, counter: int, run_id: str = "run") -> Notification:
    return Notification(
        channel=channel,
        counter=counter,
        linked_to=WorkflowExecution(workflow_id="wf", run_id=run_id),
    )


async def test_a_linked_channel_issues_no_command_and_gets_its_notification():
    instance = _instance(ReceiveLinked)
    completion = instance.activate(_start(ReceiveLinked, "orders"))
    assert _subscribed(completion) == []
    assert not _completed(completion)
    completion = instance.activate(_notified(_linked("orders", 7)))
    assert _result(completion) == {
        "channel": "orders",
        "counter": 7,
        "linked_to": {"workflow_id": "wf", "run_id": "run"},
    }


async def test_a_notification_without_an_owner_does_not_reach_the_linked_channel():
    instance = _instance(ReceiveLinked)
    instance.activate(_start(ReceiveLinked, "orders"))
    # The independent channel of the same name is somebody else's.
    completion = instance.activate(_notified(Notification(channel="orders", counter=1)))
    assert not _completed(completion)
    assert _result(instance.activate(_notified(_linked("orders", 2))))["counter"] == 2


async def test_the_two_kinds_under_one_name_are_told_apart_by_the_owner():
    instance = _instance(BothKinds)
    completion = instance.activate(_start(BothKinds, "orders"))
    assert _subscribed(completion) == ["orders"]
    completion = instance.activate(
        _notified(Notification(channel="orders", counter=1), _linked("orders", 1))
    )
    assert sorted(_result(completion)) == ["independent", "linked"]


async def test_a_linked_notification_nobody_asked_for_is_dropped():
    instance = _instance(ReceiveLinked)
    instance.activate(_start(ReceiveLinked, "orders"))
    completion = instance.activate(_notified(_linked("other", 9)))
    assert completion.HasField("successful")
    assert not _completed(completion)


def _used_flags(completion: WorkflowActivationCompletion) -> set[int]:
    assert completion.HasField("successful"), completion.failed.failure.message
    return set(completion.successful.used_internal_flags)


async def test_a_stream_the_run_owns_listens_on_its_linked_channel_without_a_command():
    """The three-way probe's answer, recorded on the first task as two flags."""
    instance = _instance(ListenOnStream, flags=[INDEPENDENT, LINKED])
    completion = instance.activate(_start(ListenOnStream, "external-stream/x", "wf"))
    assert _result(completion) is True
    assert _subscribed(completion) == []
    assert _used_flags(completion) == {int(INDEPENDENT), int(LINKED)}


async def test_a_stream_with_another_owner_subscribes_by_command_on_a_linked_server():
    # Both built before either runs: a completed run leaves no loop to build on.
    another_owner = _instance(ListenOnStream, flags=[INDEPENDENT, LINKED])
    no_owner = _instance(ListenOnStream, flags=[INDEPENDENT, LINKED])
    completion = another_owner.activate(
        _start(ListenOnStream, "external-stream/x", "other")
    )
    assert _result(completion) is True
    assert _subscribed(completion) == ["external-stream/x"]
    # A stream nobody owns does the same.
    completion = no_owner.activate(_start(ListenOnStream, "standalone/x", ""))
    assert _subscribed(completion) == ["standalone/x"]


async def test_a_server_with_only_independent_channels_takes_the_command():
    instance = _instance(ListenOnStream, flags=[INDEPENDENT])
    completion = instance.activate(_start(ListenOnStream, "external-stream/x", "wf"))
    assert _result(completion) is True
    assert _subscribed(completion) == ["external-stream/x"]
    assert _used_flags(completion) == {int(INDEPENDENT)}


async def test_a_server_without_channels_leaves_the_run_to_the_signal():
    instance = _instance(ListenOnStream)
    completion = instance.activate(_start(ListenOnStream, "external-stream/x", "wf"))
    assert _result(completion) is False
    assert _subscribed(completion) == []
    assert _used_flags(completion) == set()


async def test_a_replay_takes_the_path_the_live_run_recorded():
    without_channels = _instance(ListenOnStream)
    with_linked = _instance(ListenOnStream, flags=[INDEPENDENT, LINKED])
    # Replayed on a Worker that found no channels: the flags come from History.
    completion = without_channels.activate(
        _start(
            ListenOnStream,
            "external-stream/x",
            "wf",
            recorded_flags=[INDEPENDENT, LINKED],
        )
    )
    assert _result(completion) is True
    assert _subscribed(completion) == []
    # And a live run on a linked server replays the independent path it took.
    completion = with_linked.activate(
        _start(ListenOnStream, "external-stream/x", "wf", recorded_flags=[INDEPENDENT])
    )
    assert _result(completion) is True
    assert _subscribed(completion) == ["external-stream/x"]


def test_a_channel_call_names_the_workflow_it_is_linked_to():
    request = temporalio.api.workflowservice.v1.DescribeChannelRequest(channel="c")
    _address_channel(request, None, None)
    assert not request.HasField("workflow_execution")
    _address_channel(request, "wf", None)
    assert (
        request.workflow_execution.workflow_id,
        request.workflow_execution.run_id,
    ) == (
        "wf",
        "",
    )
    _address_channel(request, "wf", "run")
    assert request.workflow_execution.run_id == "run"
    with pytest.raises(ValueError, match="workflow_id"):
        _address_channel(request, None, "run")


async def test_the_client_describes_a_linked_channel_by_its_owner(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    described: list[temporalio.api.workflowservice.v1.DescribeChannelRequest] = []

    async def describe(request, **kwargs):  # type: ignore[no-untyped-def]
        described.append(request)
        return temporalio.api.workflowservice.v1.DescribeChannelResponse(
            kind=temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_LINKED,
            linked_to=WorkflowExecution(workflow_id="wf", run_id="run"),
            latest=_linked("orders", 3),
        )

    monkeypatch.setattr(client.workflow_service, "describe_channel", describe)
    description = await client.describe_channel(
        "orders", workflow_id="wf", run_id="run"
    )
    [request] = described
    assert request.channel == "orders"
    assert request.workflow_execution.workflow_id == "wf"
    assert request.workflow_execution.run_id == "run"
    assert description.kind is ChannelKind.LINKED
    assert description.linked_to is not None
    assert description.linked_to.workflow_id == "wf"
    assert description.latest is not None
    assert _linked_to(description.latest) == {"workflow_id": "wf", "run_id": "run"}
    # A server that predates the kinds reports none.
    monkeypatch.setattr(
        client.workflow_service,
        "describe_channel",
        lambda request, **kwargs: _answer(  # type: ignore[no-untyped-def]
            temporalio.api.workflowservice.v1.DescribeChannelResponse()
        ),
    )
    description = await client.describe_channel("orders")
    assert description.kind is None and description.linked_to is None


async def _answer(response: Any) -> Any:
    return response


async def _require_linked(client: Client) -> None:
    from temporalio.contrib.external_workflow_streams._wake import ChannelSupport

    if await server_channel_support(client) is not ChannelSupport.LINKED:
        pytest.skip("the server does not serve channels linked to a workflow")


@pytest.mark.needs_linked_server
async def test_a_workflow_receives_a_notification_on_its_linked_channel(client: Client):
    await _require_linked(client)
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
        # The channel exists with the run: no listener to register, nothing
        # retained yet, and the owner named.
        description = await client.describe_channel(channel, workflow_id=handle.id)
        assert description.kind is ChannelKind.LINKED
        assert description.linked_to is not None
        assert description.linked_to.workflow_id == handle.id
        assert description.listeners == []
        assert description.latest is None
        await client.notify_channel(
            channel, position=b"1-0", counter=1, workflow_id=handle.id
        )
        result = await asyncio.wait_for(handle.result(), 30)
        assert (result["channel"], result["counter"]) == (channel, 1)
        # The owner and the run that received it, so a listener holding both
        # kinds under one name can route it.
        assert result["linked_to"]["workflow_id"] == handle.id
        assert result["linked_to"]["run_id"]
        events = [event.event_type async for event in handle.fetch_history_events()]
        assert (
            EventType.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED not in events
        )
        # The channel's state dies with the run.
        with pytest.raises(RPCError) as closed:
            await client.notify_channel(channel, counter=2, workflow_id=handle.id)
        assert closed.value.status == RPCStatusCode.NOT_FOUND
    finally:
        with contextlib.suppress(RPCError):
            await handle.terminate()
        await asyncio.wait_for(worker.shutdown(), 15)
        await running


@pytest.mark.needs_linked_server
async def test_a_linked_channel_retains_for_pollers_and_takes_callbacks(client: Client):
    await _require_linked(client)
    channel = f"orders-{uuid.uuid4()}"
    # Nobody polls this queue, so the run stays open for the calls below.
    handle = await client.start_workflow(
        ReceiveLinked.run,
        channel,
        id=f"wf-{uuid.uuid4()}",
        task_queue=f"nobody-polls-{uuid.uuid4()}",
    )
    try:
        callback = Callback(url="http://localhost:1/never-called", headers={})
        listener_id = await client.register_channel_listener(
            channel, callback, workflow_id=handle.id
        )
        description = await client.describe_channel(channel, workflow_id=handle.id)
        assert [listener.listener_id for listener in description.listeners] == [
            listener_id
        ]
        assert description.listeners[0].callback == callback
        await client.unregister_channel_listener(
            channel, listener_id, workflow_id=handle.id
        )
        description = await client.describe_channel(channel, workflow_id=handle.id)
        assert description.listeners == []
        # The owner is woken, so a notify counts no registered listener but
        # still retains for pollers.
        await client.notify_channel(
            channel, position=b"2-0", counter=2, workflow_id=handle.id
        )
        polled = await client.poll_channel(channel, workflow_id=handle.id, wait=False)
        assert [(n.counter, _linked_to(n) is not None) for n in polled] == [(2, True)]
        description = await client.describe_channel(channel, workflow_id=handle.id)
        assert description.latest is not None and description.latest.counter == 2
        # The independent channel of the same name is untouched.
        with pytest.raises(RPCError) as untouched:
            await client.describe_channel(channel)
        assert untouched.value.status == RPCStatusCode.NOT_FOUND
    finally:
        with contextlib.suppress(RPCError):
            await handle.terminate()


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
