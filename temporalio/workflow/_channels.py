"""Notification channels from inside workflow code.

.. warning::
    This module is experimental and may change in future versions.

A channel carries notifications, not data. A writer tells the channel's
listeners that a source they consume has moved, and each listener reads the
source itself. A workflow that subscribes gets the notifications the server
folded for it with each Workflow Task. They travel in History, so a replay
sees the same ones at the same points.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import temporalio.api.common.v1
import temporalio.api.notification.v1
import temporalio.common

__all__ = [
    "Notification",
]


@dataclass(frozen=True)
class Notification:
    """One notification from a channel.

    The server folds notifications per listener while one is pending and no
    task has been scheduled for it, keeping the one with the highest counter.
    A listener therefore sees where a burst of writes ended, not every write.
    """

    channel: str
    """The channel the writer notified."""

    position: bytes
    """Where the source stands after the write, in the writer's terms.

    Opaque to the server and handed over as sent.
    """

    counter: int
    """Orders notifications from one channel's writers. Higher is later."""

    metadata: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Details for the listener, such as which topic moved, as payloads.

    A codec has been applied. Convert a value with
    :py:meth:`temporalio.converter.PayloadConverter.from_payload` on the
    converter :py:func:`temporalio.workflow.payload_converter` returns.
    """

    linked_to: temporalio.common.Execution | None = None
    """The execution a linked channel belongs to, and the run that received this.

    ``None`` for a notification from an independent channel. A workflow that
    holds both kinds of handle on one name gets a notification on the handle
    its kind names: :func:`temporalio.workflow.linked_channel` when set,
    :func:`temporalio.workflow.subscribe_channel` otherwise.
    """

    @staticmethod
    def _from_proto(
        proto: temporalio.api.notification.v1.Notification,
    ) -> Notification:
        return Notification(
            channel=proto.channel,
            position=proto.position,
            counter=proto.counter,
            metadata=dict(proto.metadata.items()),
            linked_to=(
                temporalio.common.Execution.from_proto(proto.linked_to)
                if proto.HasField("linked_to")
                else None
            ),
        )
