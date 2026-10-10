"""Telling a stream's notifier on the server that the stream moved or closed.

The notifier holds the callbacks of every caller a stream-returning Nexus
operation attached (see ``StreamOperationHandler`` in
:mod:`temporalio.contrib.streams.nexus`). A
notification is a hint that there is something new to read; the records
themselves stay on the read path.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import NotifyStreamRequest
from temporalio.contrib.streams._ref import StreamRef

if TYPE_CHECKING:
    from temporalio.client import Client

__all__ = ["StreamNotifier"]

logger = logging.getLogger(__name__)


def stream_reference(ref: StreamRef, topic: str) -> StreamReference:
    """The notifier's key for ``ref``'s stream on ``topic``.

    The notifier is keyed without the run: one notifier per stream across
    Continue-as-New, so a pinned ref and a following one reach the same one.
    """
    return StreamReference(
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id=ref.workflow_id,
        topic=topic,
    )


class StreamNotifier:
    """Notifies one stream's notifier, folded, with one call in flight.

    :meth:`notify` never waits: it records the newest position and returns.
    While a call is out, later notifications fold into the one that goes
    next, so a burst of appends costs at most two calls. A failed call is
    logged and dropped, because a notification is only a hint: the next one
    tells the reader again, and the reader reads from its own cursor.

    Each notification carries a counter from the store's position (see
    :func:`temporalio.contrib.streams._cursor.progress_counter`). The notifier
    on the server keeps the highest counter and a caller drops a lower one, so
    the counters of producers in different processes stay in the stream's own
    order whatever their clocks say.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, client: Client, ref: StreamRef, *, topic: str | None = None
    ) -> None:
        """Notify the notifier of ``ref``'s stream, on ``topic`` or the ref's own."""
        self._client = client
        self._reference = stream_reference(ref, ref.topic if topic is None else topic)
        self._pending: tuple[str, int, dict[str, str]] | None = None
        self._last_position = ""
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    def notify(
        self, position: str, counter: int, metadata: Mapping[str, str] | None = None
    ) -> None:
        """Say the stream moved to ``position``, whose counter is ``counter``.

        Does nothing after :meth:`close`. Of the notifications that fold,
        the one with the highest counter goes.
        """
        if self._closed:
            return
        if self._pending is None or counter >= self._pending[1]:
            self._pending = (position, counter, dict(metadata or {}))
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._send_pending())

    async def flush(self) -> None:
        """Wait until no notification is out or waiting."""
        while self._task is not None and not self._task.done():
            await asyncio.shield(self._task)

    async def close(self, result: Any, counter: int) -> None:
        """Close the stream on the server, completing every attached operation.

        ``result`` becomes the operations' result, encoded with the client's
        data converter. ``counter`` must rank above the stream's last
        notification. Waits for the notification in flight first, so the
        close is the last thing the notifier hears from this process.

        Raises:
            temporalio.service.RPCError: The server refused the close.
        """
        self._closed = True
        await self.flush()
        [payload] = await self._client.data_converter.encode([result])
        request = NotifyStreamRequest(
            namespace=self._client.namespace,
            stream_ref=self._reference,
            position=self._last_position,
            counter=counter,
            close=True,
        )
        request.close_result.CopyFrom(payload)
        await self._client.workflow_service.notify_stream(request)

    async def _send_pending(self) -> None:
        while self._pending is not None:
            position, counter, metadata = self._pending
            self._pending = None
            request = NotifyStreamRequest(
                namespace=self._client.namespace,
                stream_ref=self._reference,
                position=position,
                counter=counter,
                metadata=metadata,
            )
            try:
                await self._client.workflow_service.notify_stream(request)
                self._last_position = position
            except Exception:
                logger.warning(
                    "Stream notification for Workflow %r topic %r failed; the next "
                    "one tells the reader again",
                    self._reference.workflow_id,
                    self._reference.topic,
                    exc_info=True,
                )
