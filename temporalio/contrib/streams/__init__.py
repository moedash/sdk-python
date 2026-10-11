"""Streams: an ordered log a Workflow owns, written from many places.

.. warning::
    This package is experimental and may change in future versions.

A Workflow owns a stream. Producers outside Workflow code, such as the
Workflow's Activities and clients, append to it, and outside readers consume
it from a cursor. Records live in a store the application runs, which
Core reaches for every SDK, and never pass through Temporal or its History.

The contract:

1. **A new attempt supersedes the old one.** When a reader sees the first
   record of a producer's newer attempt, it yields a
   :attr:`RecordKind.SUPERSEDED` record first. No store holds that record,
   so every store reports a retry the same way.

The stored record is ``temporal.sdk.streams.v1.StreamRecord``, in
:mod:`temporalio.bridge.proto.streams.v1`, with the user's value in ``body``
as an ordinary payload.
"""

from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)

__all__ = [
    "Cursor",
    "RecordKind",
    "StreamRecord",
    "Supersession",
]
