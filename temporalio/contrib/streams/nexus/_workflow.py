"""Closing a Workflow's own stream from Workflow code."""

from __future__ import annotations

import weakref
from typing import Any

import temporalio.workflow
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.bridge.proto.workflow_commands import OutputRecord
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._workflow import _publish, _run_streams

__all__ = ["close_workflow_stream"]

# The topics each run closed, keyed by the run's runtime like the writer's
# state, so a second close in the run does nothing.
_closed: weakref.WeakKeyDictionary[Any, set[str]] = weakref.WeakKeyDictionary()


def close_workflow_stream(
    result: Any = None, *, topic: str | StreamTopic[Any] | None = None
) -> None:
    """Close the running Workflow's stream on ``topic``.

    Every Nexus operation that handed out this stream completes with
    ``result``. The close commits with this Workflow Task, like a publish.
    Once Core has made the task's records visible, it closes the topic in the
    store, so every reader ends after the last record, and then the stream's
    notifier, which completes the operations. A record published in the same
    task is never overtaken by the close. Core retries a close that fails.

    Closing a topic finishes it for this Workflow. Closing it again does
    nothing.

    Raises:
        ValueError: The Worker has no stream store.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator, which commit nothing.
        temporalio.contrib.streams.StreamError: The close record would take
            the Workflow Task's manifest past its budget.

    .. warning::
        This API is experimental.
    """
    name, _ = resolve_topic(topic)
    runtime = temporalio.workflow._Runtime.current()
    closed = _closed.setdefault(runtime, set())
    if name in closed:
        return
    # Core takes a close only with its topic's FINISH in the same commit, which
    # is what proves the close in History.
    _publish(
        OutputRecord(topic=name, kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH),
        "close a stream topic",
    )
    [payload] = temporalio.workflow.payload_converter().to_payloads([result])
    # Sent again while replaying, since a close History proves may still need
    # sending, and the codec covers it through the completion encoder.
    runtime.workflow_stream_commit().closes.add(topic=name, result=payload)
    _run_streams().finished.add(name)
    closed.add(name)
