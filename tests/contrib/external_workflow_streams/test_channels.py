"""The notification channel calls on the client.

The unit cases fake the service. The live cases need a server that serves
channels, named with -E, and skip otherwise.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.notification.v1
import temporalio.api.workflowservice.v1
from temporalio import workflow
from temporalio.client import Callback, ChannelKind, Client
from temporalio.client._client import _channel_execution
from temporalio.client._impl import _channel_owner
from temporalio.common import Execution, ExecutionType
from temporalio.service import RPCError, RPCStatusCode

Notification = temporalio.api.notification.v1.Notification
ExecutionProto = temporalio.api.common.v1.Execution
WORKFLOW_TYPE = temporalio.api.enums.v1.ExecutionType.EXECUTION_TYPE_WORKFLOW


def _linked_to(notification: workflow.Notification) -> dict[str, Any] | None:
    if notification.linked_to is None:
        return None
    return {
        "type": notification.linked_to.type.name,
        "business_id": notification.linked_to.business_id,
        "run_id": notification.linked_to.run_id,
    }


def _linked(channel: str, counter: int, run_id: str = "run") -> Notification:
    return Notification(
        channel=channel,
        counter=counter,
        linked_to=ExecutionProto(type=WORKFLOW_TYPE, business_id="wf", run_id=run_id),
    )


def test_a_channel_call_names_the_execution_it_is_linked_to():
    # The short form names a workflow. The long form names any execution.
    assert _channel_execution(None, None, None) is None
    assert _channel_execution(None, "wf", None) == Execution.workflow("wf")
    assert _channel_execution(None, "wf", "run") == Execution.workflow("wf", "run")
    activity = Execution.activity("act")
    assert _channel_execution(activity, None, None) is activity
    with pytest.raises(ValueError, match="workflow_id"):
        _channel_execution(None, None, "run")
    with pytest.raises(ValueError, match="not both"):
        _channel_execution(activity, "wf", None)
    with pytest.raises(ValueError, match="not both"):
        _channel_execution(activity, None, "run")
    # Unset, the request addresses the independent channel of that name.
    request = temporalio.api.workflowservice.v1.DescribeChannelRequest(
        channel="c", execution=_channel_owner(None)
    )
    assert not request.HasField("execution")
    owner = _channel_owner(Execution.workflow("wf"))
    assert owner is not None
    assert (owner.type, owner.business_id, owner.run_id) == (WORKFLOW_TYPE, "wf", "")
    owner = _channel_owner(Execution.workflow("wf", "run"))
    assert owner is not None
    assert (owner.type, owner.business_id, owner.run_id) == (
        WORKFLOW_TYPE,
        "wf",
        "run",
    )
    assert Execution.from_proto(owner) == Execution.workflow("wf", "run")
    assert Execution.from_proto(_channel_owner(activity)) == activity  # type: ignore[arg-type]
    assert Execution.from_proto(ExecutionProto(business_id="x")) == Execution(
        ExecutionType.UNSPECIFIED, "x"
    )


async def test_the_client_describes_a_linked_channel_by_its_owner(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    described: list[temporalio.api.workflowservice.v1.DescribeChannelRequest] = []

    async def describe(request, **kwargs):  # type: ignore[no-untyped-def]
        described.append(request)
        return temporalio.api.workflowservice.v1.DescribeChannelResponse(
            kind=temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_LINKED,
            linked_to=ExecutionProto(
                type=WORKFLOW_TYPE, business_id="wf", run_id="run"
            ),
            latest=_linked("orders", 3),
        )

    monkeypatch.setattr(client.workflow_service, "describe_channel", describe)
    description = await client.describe_channel(
        "orders", workflow_id="wf", run_id="run"
    )
    [request] = described
    assert request.channel == "orders"
    assert request.execution.type == WORKFLOW_TYPE
    assert request.execution.business_id == "wf"
    assert request.execution.run_id == "run"
    assert description.kind is ChannelKind.LINKED
    assert description.linked_to == Execution.workflow("wf", "run")
    assert description.latest is not None
    assert _linked_to(description.latest) == {
        "type": "WORKFLOW",
        "business_id": "wf",
        "run_id": "run",
    }
    # The long form carries any execution as given.
    activity = Execution.activity("act", "run-2")
    await client.describe_channel("orders", execution=activity)
    assert Execution.from_proto(described[-1].execution) == activity
    with pytest.raises(ValueError, match="not both"):
        await client.describe_channel("orders", execution=activity, workflow_id="wf")
    # A server that predates the kinds reports none.
    monkeypatch.setattr(
        client.workflow_service,
        "describe_channel",
        lambda request, **kwargs: _answer(  # type: ignore[no-untyped-def]
            temporalio.api.workflowservice.v1.DescribeChannelResponse()
        ),
    )
    description = await client.describe_channel("orders")
    assert description.kind is ChannelKind.UNSPECIFIED
    assert description.linked_to is None


async def _answer(response: Any) -> Any:
    return response


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
