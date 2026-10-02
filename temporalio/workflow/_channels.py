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

import asyncio
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field

import temporalio.api.common.v1
import temporalio.api.notification.v1
from temporalio.workflow._context import _Runtime

__all__ = ["ChannelSubscription", "Notification", "subscribe_channel"]


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

    @staticmethod
    def _from_proto(
        proto: temporalio.api.notification.v1.Notification,
    ) -> Notification:
        return Notification(
            channel=proto.channel,
            position=proto.position,
            counter=proto.counter,
            metadata=dict(proto.metadata.items()),
        )


class ChannelSubscription:
    """A workflow's subscription to one channel.

    Prefer :func:`temporalio.workflow.subscribe_channel`. The subscription is
    an async iterator over the notifications as they arrive, and
    :meth:`receive` takes them one at a time. Notifications wait in arrival
    order until taken. Two loops on one subscription share its buffer and
    interleave.
    """

    def __init__(self, channel: str) -> None:
        """Prefer :func:`temporalio.workflow.subscribe_channel`."""
        self._channel = channel
        self._pending: deque[Notification] = deque()
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def channel(self) -> str:
        """The channel this subscription is on."""
        return self._channel

    async def receive(self) -> Notification:
        """The next notification on this channel, waiting for one to arrive.

        The wait is a future the delivery resolves, so it adds no command and
        replays the same way.
        """
        while not self._pending:
            waiter: asyncio.Future[None] = asyncio.Future()
            self._waiters.append(waiter)
            try:
                await waiter
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
        return self._pending.popleft()

    def __aiter__(self) -> ChannelSubscription:
        """The subscription is its own iterator."""
        return self

    async def __anext__(self) -> Notification:
        """The next notification; the iteration never ends on its own."""
        return await self.receive()

    def _deliver(self, notification: Notification) -> None:
        self._pending.append(notification)
        # Every waiter wakes; the ones that find the buffer empty again wait
        # once more.
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)


def subscribe_channel(channel: str) -> ChannelSubscription:
    """Subscribe this workflow to ``channel``.

    The first call for a channel in a run issues a command, so gate a new
    channel with :func:`temporalio.workflow.patched` as you would a timer. A
    second call for the same channel returns the subscription already open,
    and the two share its buffer. From then on each Workflow Task carries the
    notifications the server folded for this workflow on the channel, and
    they arrive here. A successor run after continue-as-new starts with no
    subscriptions.

    Args:
        channel: Name of the channel, scoped to the namespace.

    Raises:
        ValueError: ``channel`` is empty.
    """
    if not channel:
        raise ValueError("channel must not be empty")
    return _Runtime.current().workflow_subscribe_channel(channel)
