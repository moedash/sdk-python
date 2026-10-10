"""Streams: an ordered log a Workflow owns, written from many places.

.. warning::
    This package is experimental and may change in future versions.

A Workflow owns a stream. Producers outside Workflow code, such as the
Workflow's Activities and clients, append to it, and outside readers consume
it from a cursor. Records live in a store the application runs, reached
through a provider, and never pass through Temporal or its History.

The contract:

1. **A new attempt supersedes the old one.** When a reader sees the first
   record of a producer's newer attempt, it yields a
   :attr:`RecordKind.SUPERSEDED` record first. No store holds that record,
   so every provider reports a retry the same way.
2. **A cursor belongs to one stream.** Hand it back to resume strictly after
   the record it names. A provider refuses a cursor from another provider
   or another stream with :class:`StreamCursorError`, and one whose record
   retention dropped with :class:`StreamExpiredError`. A read with no cursor
   starts at :data:`BEGINNING` or :data:`END`.

The record on the wire is ``temporal.sdk.streams.v1.StreamRecord``, in
:mod:`temporalio.contrib.streams.proto.v1`, with the user's value in ``body``
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
from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)

__all__ = [
    "BEGINNING",
    "Cursor",
    "END",
    "RecordKind",
    "StreamClosedError",
    "StreamCursorError",
    "StreamError",
    "StreamExpiredError",
    "StreamNotFoundError",
    "StreamOutcomeUnknownError",
    "StreamProducerError",
    "StreamRecordError",
    "StreamRecord",
    "StreamRefusedError",
    "StreamStorageError",
    "StreamUnsupportedError",
    "Supersession",
]
