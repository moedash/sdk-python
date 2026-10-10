"""Publishing to a Workflow's own stream from Workflow code.

The writer converts values on the Workflow thread and buffers the records
for the run. Nothing here does I/O: the Worker stages the buffered records
when the activation completes and commits them with its Workflow Task. Reading a stream
inside a Workflow is not part of this release.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar, overload

from temporalio import workflow
from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._output import _RunOutput
from temporalio.contrib.streams._plugin import output_for_workflow
from temporalio.contrib.streams._record import RecordKind
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import to_wire

__all__ = ["WorkflowStreamWriter", "workflow_reader", "workflow_writer"]

T = TypeVar("T")


def _run_streams() -> _RunOutput:
    # Kept by the Worker's coordinator, keyed by run, so it is dropped with
    # the run at eviction and rebuilt in order on replay.
    info = workflow.info()
    return output_for_workflow().open_run(info.workflow_id, info.run_id)


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

    def __init__(self, state: _RunOutput, topic: str) -> None:
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
        converter here and buffered for this Workflow Task. A
        :class:`temporalio.common.RawValue` passes through pre-encoded.

        The record becomes visible to readers when the Workflow Task that
        published it is accepted, and never if the task fails. Its body goes
        through the Workflow's payload codec before it is stored.

        Raises:
            ValueError: The topic was already finished in this run.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator, which commit nothing.
        """
        _refuse_read_only("publish to a stream")
        if self._topic in self._state.finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        self._state.publish(
            [
                to_wire(
                    workflow.payload_converter(),
                    topic=self._topic,
                    kind=RecordKind.DATA,
                    value=value,
                    run_id=workflow.info().run_id,
                )
            ]
        )

    def finish(self) -> None:
        """Write ``FINISH`` for this Workflow on this topic. Idempotent.

        Says this run publishes nothing more on the topic. A run that
        follows by Continue-as-New can publish on it again. It does not say
        the Workflow succeeded, and it does not end anyone's read.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator, which commit nothing.
        """
        _refuse_read_only("finish a stream topic")
        if self._topic in self._state.finished:
            return
        self._state.finished.add(self._topic)
        self._state.publish(
            [
                to_wire(
                    workflow.payload_converter(),
                    topic=self._topic,
                    kind=RecordKind.FINISH,
                    run_id=workflow.info().run_id,
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


def workflow_reader(*_args: Any, **_kwargs: Any) -> Any:
    """Reading a stream inside a Workflow is not supported in this release.

    Read from an Activity or a client with
    :func:`temporalio.contrib.streams.activity_handle` or
    :func:`temporalio.contrib.streams.get_stream_handle`, and send the
    Workflow what it needs, for example as a Signal.

    Raises:
        StreamUnsupportedError: Always.
    """
    raise StreamUnsupportedError(
        "reading a stream inside a Workflow is not supported in this release; "
        "read it from an Activity or a client and send the Workflow what it "
        "needs, for example as a Signal"
    )
