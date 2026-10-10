"""Closing a Workflow's own stream from Workflow code."""

from __future__ import annotations

from typing import Any

import temporalio.workflow
from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.nexus.system.workflow_service import notify_stream
from temporalio.nexus.system.workflow_service.models import StreamReference

__all__ = ["close_workflow_stream"]

_HIGHEST_COUNTER = (1 << 63) - 1


async def close_workflow_stream(
    result: Any = None, *, topic: str | StreamTopic[Any] | None = None
) -> None:
    """Close the running Workflow's stream on the server's notifier.

    Every Nexus operation that handed out this stream completes with
    ``result``. Call it from Workflow code once the Workflow has published its
    last record on ``topic``. The call goes through System Nexus, so it is
    recorded in History and replays like any other operation.

    Raises:
        temporalio.exceptions.NexusOperationError: The server refused the
            close.

    .. warning::
        This API is experimental.
    """
    name, _ = resolve_topic(topic)
    info = temporalio.workflow.info()
    # The server takes the namespace from this Workflow. Workflow code cannot
    # read the store's position, so the close takes the highest counter there
    # is, which outranks every notification before it.
    handle = await notify_stream(
        stream_ref=StreamReference(
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=info.workflow_id,
            # The notifier is keyed by the run chain, through its first run.
            run_id=info.first_execution_run_id,
            topic=name,
        ),
        position="",
        counter=_HIGHEST_COUNTER,
        close=True,
        close_result=result,
    )
    await handle
