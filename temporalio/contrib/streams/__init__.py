"""Streams: an ordered log a Workflow owns, written from many places.

.. warning::
    This package is experimental and may change in future versions.

A Workflow owns a stream. Producers outside Workflow code, such as the
Workflow's Activities and clients, append to it, and outside readers consume
it from a cursor. Records live in a store the application runs, which
Core reaches for every SDK, and never pass through Temporal or its History.

The contract:

1. **Producers write with an identity.** A producer has an id, an attempt
   and a sequence that starts at one for each attempt. A retry of the
   newest batch with the same content is written once and returns the
   original position. A retry with different content, or with a lower
   sequence, is refused with :class:`StreamProducerError`. Content is
   compared over the payloads the converter produced, before the codec, so
   a codec with a fresh nonce per call still deduplicates a retry.
2. **A new attempt supersedes the old one.** When a reader sees the first
   record of a producer's newer attempt, it yields a
   :attr:`RecordKind.SUPERSEDED` record first. No store holds that record,
   so every store reports a retry the same way.
3. **A cursor belongs to one stream.** Hand it back to resume strictly after
   the record it names. A cursor from another store or another stream is
   refused with :class:`StreamCursorError`, and one whose record
   retention dropped with :class:`StreamExpiredError`. A read with no cursor
   starts at :data:`BEGINNING` or :data:`END`.
4. **Topics are defined once.** :func:`topic` defines a topic with the type
   its records decode to, and every party shares that definition. A call
   that names no topic addresses :data:`DEFAULT_TOPIC`.

A store is configured as a plugin:
``Client.connect(..., plugins=[RedisStreams(...)])`` registers it on the
client and on every Worker built from that client. Each
context then reaches a stream the same way:

- Workflow code publishes to its own stream with :func:`workflow_writer`.
- An Activity reaches the stream of the Workflow that scheduled it with
  :func:`activity_handle`, and writes as itself: its Activity id with the
  scheduling run id, and its Temporal attempt. The handle is pinned to that
  run, so a read on it ends when the run closes, Continue-as-New included.
- Any process holding a client reaches a Workflow's stream with
  :func:`get_stream_handle`, and writes with a producer id and an attempt of
  its own.

In this release only a Workflow owns a stream, and only Activities and
clients read one. Reading inside a Workflow (:func:`workflow_reader`) and
the other owner kinds raise :class:`StreamUnsupportedError`.

The stored record is ``temporal.sdk.streams.v1.StreamRecord``, in
:mod:`temporalio.bridge.proto.streams.v1`, with the user's value in ``body``
as an ordinary payload.
"""

from temporalio.contrib.streams._cursor import BEGINNING, END
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRecordError,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._handles import (
    ActivityStreamHandle,
    StreamHandle,
    StreamProducer,
    activity_handle,
    get_stream_handle,
)
from temporalio.contrib.streams._plugin import StreamStorePlugin
from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)
from temporalio.contrib.streams._ref import StreamOwnerKind, StreamRef
from temporalio.contrib.streams._topic import DEFAULT_TOPIC, StreamTopic, topic
from temporalio.contrib.streams._workflow import (
    WorkflowStreamWriter,
    workflow_reader,
    workflow_writer,
)

__all__ = [
    "ActivityStreamHandle",
    "BEGINNING",
    "Cursor",
    "DEFAULT_TOPIC",
    "END",
    "RecordKind",
    "StreamClosedError",
    "StreamCursorError",
    "StreamError",
    "StreamExpiredError",
    "StreamHandle",
    "StreamNotFoundError",
    "StreamOutcomeUnknownError",
    "StreamOwnerKind",
    "StreamProducer",
    "StreamProducerError",
    "StreamRecord",
    "StreamRecordError",
    "StreamRef",
    "StreamRefusedError",
    "StreamStorageError",
    "StreamStorePlugin",
    "StreamTopic",
    "StreamUnsupportedError",
    "Supersession",
    "WorkflowStreamWriter",
    "activity_handle",
    "get_stream_handle",
    "topic",
    "workflow_reader",
    "workflow_writer",
]
