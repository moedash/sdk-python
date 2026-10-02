"""Notification channel descriptions as the client reports them."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum

import temporalio.api.common.v1
import temporalio.api.notification.v1
import temporalio.api.workflow.v1
from temporalio.streams._ref import StreamRef
from temporalio.workflow import Notification

from ._callback import Callback

__all__ = [
    "ChannelAddress",
    "ChannelDescription",
    "ChannelKind",
    "ChannelListener",
    "ChannelSubscriptionInfo",
    "stream_channel",
]

STREAM_CHANNEL_PREFIX = "stream/"
"""The first segment of the channel a native stream notifies."""


class ChannelKind(IntEnum):
    """Where a channel lives, which decides how a call addresses it.

    .. warning::
       This API is experimental and unstable.
    """

    UNSPECIFIED = int(
        temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_UNSPECIFIED
    )
    """The server did not say; an older server answers this."""

    INDEPENDENT = int(
        temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_INDEPENDENT
    )
    """Its own execution, keyed by namespace and channel name.

    Any number of workflows subscribe to it and callbacks register on it.
    """

    LINKED = int(temporalio.api.notification.v1.ChannelKind.CHANNEL_KIND_LINKED)
    """Kept in one workflow's state, keyed by namespace, workflow id and name.

    The owning workflow is its listener by construction; a call reaches it
    with the ``workflow_id`` argument.
    """


@dataclass(frozen=True)
class ChannelListener:
    """One listener of a channel: a workflow or a callback.

    .. warning::
       This API is experimental and unstable.
    """

    listener_id: str
    """Assigned by the server when the listener registered."""

    workflow_id: str | None
    """The subscribed workflow, when the listener is one."""

    run_id: str | None
    """The run that subscribed. Delivery follows the chain's current run."""

    callback: Callback | None
    """The callback the server invokes, when the listener is one."""

    registered_time: datetime | None
    """When the listener registered."""

    @staticmethod
    def _from_proto(
        proto: temporalio.api.notification.v1.ChannelListener,
    ) -> ChannelListener:
        callback: Callback | None = None
        if proto.HasField("callback") and proto.callback.HasField("nexus"):
            callback = Callback(
                url=proto.callback.nexus.url, headers=dict(proto.callback.nexus.header)
            )
        workflow = proto.workflow if proto.HasField("workflow") else None
        return ChannelListener(
            listener_id=proto.listener_id,
            workflow_id=workflow.workflow_id if workflow else None,
            run_id=workflow.run_id if workflow else None,
            callback=callback,
            registered_time=(
                proto.registered_time.ToDatetime(tzinfo=timezone.utc)
                if proto.HasField("registered_time")
                else None
            ),
        )


@dataclass(frozen=True)
class ChannelDescription:
    """What the server knows about a channel.

    .. warning::
       This API is experimental and unstable.
    """

    listeners: Sequence[ChannelListener]
    """Who is listening, workflows and callbacks alike."""

    latest: Notification | None
    """The notification with the highest counter the channel retains."""

    retained_count: int
    """How many notifications the channel keeps for pollers."""

    kind: ChannelKind = ChannelKind.UNSPECIFIED
    """Which kind of channel this is.

    A linked channel of a running workflow exists by construction, so a
    describe with ``workflow_id`` answers :attr:`ChannelKind.LINKED` with no
    listeners and nothing retained for a name nobody has notified yet.
    """

    linked_to: temporalio.api.common.v1.WorkflowExecution | None = None
    """The owner of a linked channel and the run that holds it.

    ``None`` for an independent channel.
    """


@dataclass(frozen=True)
class ChannelSubscriptionInfo:
    """A workflow's standing on one channel, as its description reports it.

    An independent channel is listed from the subscribe event until the run
    unsubscribes or closes. A linked channel is listed once it holds state. A
    closed run keeps listing what it stood on, and a continue-as-new
    successor starts with nothing.

    .. warning::
       This API is experimental and unstable.
    """

    channel: str
    """Channel name."""

    kind: ChannelKind
    """:attr:`ChannelKind.INDEPENDENT` for a subscription the workflow made by
    command, :attr:`ChannelKind.LINKED` for a channel linked to it."""

    subscribed_event_id: int
    """Id of the event that recorded the subscription. Zero for the linked kind."""

    last_counter: int
    """Highest counter the workflow has accepted from the channel. Zero when
    none has arrived."""

    pending_notification: Notification | None
    """The notification held for the workflow's next Workflow Task, when one
    is pending."""

    scheduled_counter: int
    """Counter carried by the scheduled event of a Workflow Task that has not
    started yet. Zero otherwise."""

    listener_count: int
    """Linked kind: callback listeners registered on the channel."""

    retained_count: int
    """Linked kind: notifications retained for pollers."""

    accepted_count: int
    """Linked kind: notifications the channel has accepted over its life."""

    @staticmethod
    def _from_proto(
        proto: temporalio.api.workflow.v1.ChannelSubscriptionInfo,
    ) -> ChannelSubscriptionInfo:
        return ChannelSubscriptionInfo(
            channel=proto.channel,
            kind=ChannelKind(proto.kind),
            subscribed_event_id=proto.subscribed_event_id,
            last_counter=proto.last_counter,
            pending_notification=(
                Notification._from_proto(proto.pending_notification)
                if proto.HasField("pending_notification")
                else None
            ),
            scheduled_counter=proto.scheduled_counter,
            listener_count=proto.listener_count,
            retained_count=proto.retained_count,
            accepted_count=proto.accepted_count,
        )


@dataclass(frozen=True)
class ChannelAddress:
    """Where a channel call reaches a channel: its name and, when linked, its owner.

    .. warning::
       This API is experimental and unstable.
    """

    channel: str
    """The channel name."""

    workflow_id: str | None
    """The workflow the channel is linked to, or ``None`` for an independent one.

    Pass both to :py:meth:`temporalio.client.Client.poll_channel` and the
    other channel calls as ``channel`` and ``workflow_id``.
    """


def stream_channel(ref: StreamRef) -> ChannelAddress:
    """The channel a native stream notifies on every append and on its close.

    The server derives the name from the stream's identity, and this helper
    derives the same one, so a client polls or registers a callback without
    asking. A stream a workflow owns notifies ``stream/<topic>`` linked to the
    owning workflow. A stream an activity owns notifies
    ``stream/<activity id>/<topic>``, linked to the workflow that scheduled
    the activity, or independent for a standalone activity, which has no
    linked channels of its own. A standalone stream notifies the independent
    channel ``stream/<stream id>``, whatever the topic, since its topics share
    one stream on the server.

    Each change arrives as one notification: the stream's change sequence as
    the counter, the head after the change as the position, and ``closed``
    set in the metadata on the close.
    """
    if ref.kind == "workflow":
        assert ref.workflow_id is not None
        return ChannelAddress(STREAM_CHANNEL_PREFIX + ref.topic, ref.workflow_id)
    if ref.kind == "activity":
        assert ref.activity_id is not None
        return ChannelAddress(
            f"{STREAM_CHANNEL_PREFIX}{ref.activity_id}/{ref.topic}", ref.workflow_id
        )
    assert ref.stream_id is not None
    return ChannelAddress(STREAM_CHANNEL_PREFIX + ref.stream_id, None)
