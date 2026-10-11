"""Publishing to a Workflow's own stream from Workflow code.

The writer converts and hashes values on the Workflow thread and adds the
records to the activation's ``WorkflowOutputStreamCommit``. Nothing here does
I/O. The Worker's completion encoder runs each body through the payload
codec, and Core stages the records, records a marker of them, and makes them
visible once the Workflow Task is accepted. While replaying, the same
records go without bodies, and Core checks them against the marker. Reading
a stream inside a Workflow is not part of this release.
"""

from __future__ import annotations

import weakref
from typing import Any, Generic, TypeVar, overload

from temporalio import workflow
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.bridge.proto.workflow_commands import (
    OutputRecord,
    WorkflowOutputStreamCommit,
)
from temporalio.contrib.streams._body import content_hash
from temporalio.contrib.streams._errors import StreamError, StreamUnsupportedError
from temporalio.contrib.streams._plugin import worker_has_store
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic

__all__ = ["WorkflowStreamWriter", "workflow_reader", "workflow_writer"]

T = TypeVar("T")

MANIFEST_BUDGET_BYTES = 48 * 1024
"""The largest manifest one publishing completion may produce.

Core refuses a manifest over 64 KiB by failing the Workflow Task, again on
every retry, and it takes one commit per completion. A publish that would
take the completion's manifest past this budget is refused at the call
instead, which leaves room under Core's limit for the fields this bound
estimates.
"""

# Upper bounds, in bytes, on what the manifest spends: once per completion
# (versions, stage token, floor, run id, store name, the segment's header),
# and per topic (its entry, counts, fingerprint and segment count) on top of
# the topic name itself.
_MANIFEST_FIXED_BYTES = 512
_MANIFEST_TOPIC_BYTES = 72


class _RunStreams:
    """One run's stream state across its activations."""

    def __init__(self) -> None:
        # FINISH is a statement about the topic, not about a writer object,
        # and every workflow_writer() call returns a new writer.
        self.finished: set[str] = set()
        self.commit: WorkflowOutputStreamCommit | None = None
        self.topics: set[str] = set()
        self.manifest_bound = _MANIFEST_FIXED_BYTES

    def add(self, record: OutputRecord) -> None:
        """Add ``record`` to this activation's commit.

        Raises:
            StreamError: The record would take this completion's manifest
                past :data:`MANIFEST_BUDGET_BYTES`. Nothing is added.
        """
        commit = workflow._Runtime.current().workflow_stream_commit()
        if commit is not self.commit:
            self.commit, self.topics = commit, set()
            self.manifest_bound = _MANIFEST_FIXED_BYTES
        if record.topic not in self.topics:
            bound = (
                self.manifest_bound + _MANIFEST_TOPIC_BYTES + len(record.topic.encode())
            )
            if bound > MANIFEST_BUDGET_BYTES:
                raise StreamError(
                    f"publishing to {record.topic!r} would take this Workflow Task's "
                    f"stream manifest past {MANIFEST_BUDGET_BYTES} bytes, with "
                    f"{len(self.topics)} topics already published in this "
                    "activation; spread the topics across Workflow Tasks"
                )
            self.topics.add(record.topic)
            self.manifest_bound = bound
        commit.records.append(record)


# Keyed by the run's runtime, so the state goes with the run at eviction and
# is rebuilt in order on replay.
_runs: weakref.WeakKeyDictionary[Any, _RunStreams] = weakref.WeakKeyDictionary()


def _run_streams() -> _RunStreams:
    return _runs.setdefault(workflow._Runtime.current(), _RunStreams())


def _publish(record: OutputRecord, action: str) -> None:
    # A query or an update validator commits nothing, so a record published
    # there could only be dropped. Refused at the call instead.
    if workflow.unsafe.is_read_only():
        raise workflow.ReadOnlyContextError(
            f"While in read-only function, action attempted: {action}"
        )
    # Replaying needs no store, since nothing is stored again.
    if not workflow.unsafe.is_replaying() and not worker_has_store():
        raise ValueError(
            "no stream store is registered on this Workflow's Worker; register "
            "one on its client, for example Client.connect(..., plugins=[store])"
        )
    _run_streams().add(record)


class WorkflowStreamWriter(Generic[T]):
    """Publishes to one topic of the running Workflow's own stream.

    A Workflow publishes only to its own stream. Writing to another stream
    is I/O, which belongs in an Activity.
    """

    def __init__(self, topic: str) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_writer`."""
        self._topic = topic

    @property
    def topic(self) -> str:
        """The name of the topic this writer is bound to."""
        return self._topic

    def publish(self, value: T) -> None:
        """Append ``value`` to this topic.

        Synchronous: the value is converted with the Workflow's payload
        converter here and committed with this Workflow Task. A
        :class:`temporalio.common.RawValue` passes through pre-encoded.

        The record becomes visible to readers when the Workflow Task that
        published it is accepted, and never if the task fails. Its body goes
        through the Workflow's payload codec before it is stored.

        Raises:
            ValueError: The topic was already finished in this run, or the
                Worker has no stream store.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator, which commit nothing.
            temporalio.contrib.streams.StreamError: This activation already
                published to so many topics that one more would take the
                Workflow Task's manifest past its budget. Nothing is
                published; publish the rest after the Workflow next waits.
                Catch it: uncaught, it fails the Workflow Task, and every
                retry runs the same code and fails the same way.
        """
        if self._topic in _run_streams().finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        payload = workflow.payload_converter().to_payloads([value])[0]
        record = OutputRecord(
            topic=self._topic,
            kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
            content_hash=content_hash(payload),
            logical_size=payload.ByteSize(),
        )
        if not workflow.unsafe.is_replaying():
            record.body.CopyFrom(payload)
        _publish(record, "publish to a stream")

    def finish(self) -> None:
        """Write ``FINISH`` for this Workflow on this topic. Idempotent.

        Says this run publishes nothing more on the topic. A run that
        follows by Continue-as-New can publish on it again. It does not say
        the Workflow succeeded, and it does not end anyone's read.

        Raises:
            ValueError: The Worker has no stream store.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator, which commit nothing.
            temporalio.contrib.streams.StreamError: The ``FINISH`` record would
                take the Workflow Task's manifest past its budget; the topic
                stays open.
        """
        state = _run_streams()
        if self._topic in state.finished:
            return
        record = OutputRecord(
            topic=self._topic, kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH
        )
        _publish(record, "finish a stream topic")
        state.finished.add(self._topic)


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
        ValueError: ``topic`` is empty.
    """
    name, _ = resolve_topic(topic)
    return WorkflowStreamWriter(name)


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
