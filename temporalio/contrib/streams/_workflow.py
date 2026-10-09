"""Publishing to a Workflow's own stream from Workflow code.

The writer converts values on the Workflow thread and hands the records to
the provider's output for the run. Nothing here does I/O.
"""

from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar, overload

from temporalio import workflow
from temporalio.contrib.streams._plugin import _WorkflowOutput, provider_for_workflow
from temporalio.contrib.streams._record import RecordKind
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import to_wire

__all__ = ["WorkflowStreamWriter", "workflow_writer"]

T = TypeVar("T")

_RUN_STATE = "__temporal_contrib_streams_run"


class _RunStreams:
    """The stream state one Workflow run carries."""

    def __init__(self, output: _WorkflowOutput) -> None:
        self.output = output
        # FINISH is a statement about the topic, not about the writer object,
        # and every workflow_writer() call returns a new writer. Rebuilt in
        # the same order on replay, so it stays deterministic.
        self.finished: set[str] = set()


def _run_streams() -> _RunStreams:
    # The running loop is the Workflow instance, so state kept on it lives
    # and dies with the run, and an evicted run rebuilds it on replay.
    loop = asyncio.get_running_loop()
    state: _RunStreams | None = getattr(loop, _RUN_STATE, None)
    if state is None:
        state = _RunStreams(provider_for_workflow()._workflow_output(workflow.info()))
        setattr(loop, _RUN_STATE, state)
    return state


def _refuse_read_only(action: str) -> None:
    # A query or an update validator commits nothing, so a record published
    # there could only be dropped. Refused at the call instead.
    if workflow.unsafe.is_read_only():
        raise workflow.ReadOnlyContextError(
            f"While in read-only function, action attempted: {action}"
        )


class WorkflowStreamWriter(Generic[T]):
    """Publishes to one topic of the running Workflow's own stream.

    A Workflow publishes only to its own stream. Writing to another stream
    is I/O, which belongs in an Activity.
    """

    def __init__(self, state: _RunStreams, topic: str) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_writer`."""
        self._state = state
        self._topic = topic

    @property
    def topic(self) -> str:
        """The name of the topic this writer is bound to."""
        return self._topic

    def publish(self, value: T) -> None:
        """Append ``value`` to this topic.

        Synchronous: the value is converted with the Workflow's payload
        converter here, and the record goes to the provider. A
        :class:`temporalio.common.RawValue` passes through pre-encoded.

        When the record becomes visible to readers is the provider's
        contract. A provider for production makes it visible when the
        Workflow Task that published it is accepted, and never if the task
        fails. The memory provider makes it visible at once.

        Raises:
            ValueError: The topic was already finished in this run.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator, which commit nothing.
        """
        _refuse_read_only("publish to a stream")
        if self._topic in self._state.finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        self._state.output.publish(
            [
                to_wire(
                    workflow.payload_converter(),
                    topic=self._topic,
                    kind=RecordKind.DATA,
                    value=value,
                )
            ]
        )

    def finish(self) -> None:
        """Write ``FINISH`` for this Workflow on this topic. Idempotent.

        Says this Workflow publishes nothing more on the topic. It does not
        say the Workflow succeeded, and it does not end anyone's read.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator, which commit nothing.
        """
        _refuse_read_only("finish a stream topic")
        if self._topic in self._state.finished:
            return
        self._state.finished.add(self._topic)
        self._state.output.publish(
            [
                to_wire(
                    workflow.payload_converter(),
                    topic=self._topic,
                    kind=RecordKind.FINISH,
                )
            ]
        )


@overload
def workflow_writer(topic: StreamTopic[T]) -> WorkflowStreamWriter[T]: ...


@overload
def workflow_writer(topic: str | None = None) -> WorkflowStreamWriter[Any]: ...


def workflow_writer(
    topic: str | StreamTopic[Any] | None = None,
) -> WorkflowStreamWriter[Any]:
    """A writer on ``topic`` of the running Workflow's own stream.

    Call it from Workflow code, including the Workflow's ``__init__``. Every
    call returns a new writer, and all writers in a run share which topics
    were finished.

    Args:
        topic: A :func:`temporalio.contrib.streams.topic` definition, whose
            value type the writer's ``publish`` takes, or a plain string.
            Without one, the writer is on
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.

    Raises:
        ValueError: ``topic`` is empty, or no stream provider is registered
            on the Worker.
        StreamUnsupportedError: The provider does not accept a Workflow's own
            publish.
    """
    name, _ = resolve_topic(topic)
    return WorkflowStreamWriter(_run_streams(), name)
