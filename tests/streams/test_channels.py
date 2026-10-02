"""The notification channel surface: the command, the delivery and the client calls.

Both kinds of channel are covered: the independent one a workflow subscribes
to by command, and the one linked to the workflow, which needs none. The
workflow instance is driven with activations directly, the way Core drives
it, because the dev server this chain tests against does not accept the
subscribe command. The live cases at the end need a server that does, or one
with the linked kind, and skip otherwise.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
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
            notification.linked_to.workflow_id if notification.linked_to else None
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
        linked_to=temporalio.api.common.v1.WorkflowExecution(
            workflow_id="wf", run_id="run"
        ),
    )


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


async def test_the_client_addresses_a_linked_channel_by_workflow(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    """Every channel call carries the owner it was given, and only then."""
    requests: list[Any] = []
    owner = temporalio.api.common.v1.WorkflowExecution(workflow_id="wf", run_id="run")
    describe_response = temporalio.api.workflowservice.v1.DescribeChannelResponse(
        kind=temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_LINKED,
        linked_to=owner,
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
    assert [req.workflow_execution for req in requests] == [
        owner,
        temporalio.api.common.v1.WorkflowExecution(workflow_id="wf"),
        temporalio.api.common.v1.WorkflowExecution(workflow_id="wf"),
        temporalio.api.common.v1.WorkflowExecution(workflow_id="wf"),
        temporalio.api.common.v1.WorkflowExecution(workflow_id="wf"),
    ]
    assert polled.linked_to == owner
    assert description.kind == ChannelKind.LINKED
    assert description.linked_to == owner
    assert description.latest is not None and description.latest.linked_to == owner

    # Without an owner the calls address the independent channel.
    requests.clear()
    await client.notify_channel("orders", counter=1)
    await client.poll_channel("orders", wait=False)
    await client.describe_channel("orders")
    await client.register_channel_listener("orders", callback)
    await client.unregister_channel_listener("orders", "listener")
    assert [req.HasField("workflow_execution") for req in requests] == [False] * 5

    with pytest.raises(ValueError, match="run_id needs workflow_id"):
        await client.notify_channel("orders", counter=1, run_id="run")


@pytest.mark.needs_channel_server
@pytest.mark.needs_channel_core
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
@pytest.mark.needs_linked_core
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
        assert description.linked_to.workflow_id == handle.id
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
    assert (description.linked_to.workflow_id, description.linked_to.run_id) == (
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
@pytest.mark.needs_linked_core
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
        assert polled[0].linked_to.workflow_id == handle.id
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
