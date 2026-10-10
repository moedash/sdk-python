"""Closing a Workflow's own stream from Workflow code."""

from __future__ import annotations

from typing import Any

import temporalio.workflow
from temporalio.api.common.v1 import Payload
from temporalio.contrib.streams._record import RecordKind
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import to_wire
from temporalio.contrib.streams._workflow import _refuse_read_only, _run_streams

__all__ = ["close_workflow_stream"]

CLOSE_KEY = "temporal.io/stream-close"
"""The metadata key that marks the owner's ``FINISH`` record as the close of
its topic. The Worker closes the topic once that record is visible."""

_CLOSE_ENCODING = b"binary/plain"


def close_workflow_stream(
    result: Any = None, *, topic: str | StreamTopic[Any] | None = None
) -> None:
    """Close the running Workflow's stream on ``topic``.

    Every Nexus operation that handed out this stream completes with
    ``result``. The close commits with this Workflow Task, like a publish:
    once the Worker has made the task's records visible, it closes the
    stream in the store, so every reader ends after the last record, and
    then completes the operations. A record published in the same task is
    never overtaken by the close. The Worker retries a close that fails,
    with backoff, until it lands or the Worker stops. A Worker that stops
    first leaves it to a Worker that replays the run, if one does.

    Closing a topic finishes it for this Workflow. Closing it again does
    nothing.

    Raises:
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator, which commit nothing.
        temporalio.contrib.streams.StreamError: The close record would take
            the Workflow Task's manifest past its budget.

    .. warning::
        This API is experimental.
    """
    _refuse_read_only("close a stream topic")
    name, _ = resolve_topic(topic)
    state = _run_streams()
    if name in state.closed:
        return
    record = to_wire(
        temporalio.workflow.payload_converter(),
        topic=name,
        kind=RecordKind.FINISH,
        run_id=temporalio.workflow.info().run_id,
    )
    record.metadata[CLOSE_KEY].CopyFrom(
        Payload(metadata={"encoding": _CLOSE_ENCODING}, data=b"1")
    )
    state.publish([record])
    [state.closes[name]] = temporalio.workflow.payload_converter().to_payloads([result])
    state.closed.add(name)
    state.finished.add(name)
