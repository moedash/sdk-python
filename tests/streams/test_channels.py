"""The notification channel calls on the client.

The unit case fakes the service. The live cases need a server that serves
channels, named with -E, and skip otherwise.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import pytest

import temporalio.api.enums.v1
import temporalio.api.notification.v1
import temporalio.api.workflowservice.v1
import temporalio.common
from temporalio.client import (
    Callback,
    ChannelKind,
    Client,
)
from temporalio.service import RPCError, RPCStatusCode

Notification = temporalio.api.notification.v1.Notification


def _linked(channel: str, counter: int, position: bytes = b"") -> Notification:
    """A notification the way a linked channel's owner receives it."""
    return Notification(
        channel=channel,
        counter=counter,
        position=position,
        linked_to=temporalio.common.Execution.workflow("wf", "run").to_proto(),
    )


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
