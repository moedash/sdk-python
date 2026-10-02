"""Notification channel descriptions as the client reports them."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum

import temporalio.api.common.v1
import temporalio.api.notification.v1
from temporalio.workflow import Notification

from ._callback import Callback

__all__ = ["ChannelDescription", "ChannelKind", "ChannelListener"]


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
