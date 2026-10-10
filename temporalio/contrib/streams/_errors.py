"""The errors a stream call raises.

Every stream condition is a :class:`StreamError`, so a caller can catch by
meaning the way it catches other :class:`temporalio.exceptions.TemporalError`
subclasses. Argument mistakes stay ``ValueError``.

Two distinctions matter to a caller that retries. A write the store refused
(:class:`StreamRefusedError`, which includes :class:`StreamProducerError`
and :class:`StreamClosedError`) did not happen, and repeating it gets the
same answer until something changes. A failure of the store or of the way
to it (:class:`StreamStorageError`) may hide a write that landed: for an
append that is :class:`StreamOutcomeUnknownError`, and repeating it on the
same producer is safe because the store deduplicates the retry. Errors from
the store's client library never escape: they arrive as one of these. A cursor
that names a record the store no longer keeps
(:class:`StreamExpiredError`) is told apart from one that is not valid for
the stream at all (:class:`StreamCursorError`). A stored record that the
reader can't decode (:class:`StreamRecordError`) carries its cursor, so the
caller can resume past it on purpose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import temporalio.exceptions

if TYPE_CHECKING:
    from temporalio.contrib.streams._record import Cursor

__all__ = [
    "StreamClosedError",
    "StreamCursorError",
    "StreamError",
    "StreamExpiredError",
    "StreamNotFoundError",
    "StreamOutcomeUnknownError",
    "StreamProducerError",
    "StreamRecordError",
    "StreamRefusedError",
    "StreamStorageError",
    "StreamUnsupportedError",
]


class StreamError(temporalio.exceptions.TemporalError):
    """Base for stream conditions."""


class StreamNotFoundError(StreamError):
    """The stream's owner does not exist, or nothing is known about the stream."""


class StreamCursorError(StreamError):
    """The cursor is not valid for this stream.

    It was minted by another provider or for another stream, or it is not a
    token the provider can parse.
    """


class StreamExpiredError(StreamCursorError):
    """The cursor names a record the store no longer retains.

    The cursor was valid once, but retention dropped the records after it,
    so resuming from it would skip records silently. Start again from
    :data:`temporalio.contrib.streams.BEGINNING` or
    :data:`temporalio.contrib.streams.END` and decide what to do about the
    gap.
    """


class StreamRefusedError(StreamError):
    """The store refused the write, so nothing was written.

    For example the store is out of memory, or a key holds a value of the
    wrong type. The subclasses name the refusals the contract defines.
    """


class StreamStorageError(StreamError):
    """The store failed, or could not be reached.

    For a read, retry later. For an append, the outcome is unknown and
    :class:`StreamOutcomeUnknownError` is raised.
    """


class StreamClosedError(StreamRefusedError):
    """The stream is closed and refuses appends.

    The store refused the write, so nothing was written. Records the stream
    retains stay readable.
    """


class StreamProducerError(StreamRefusedError):
    """The store refused the append because of the producer's identity.

    The sequence was already used with different content, or it is below the
    newest sequence the store holds for this producer and attempt. Nothing
    was written, and repeating the call gets the same answer.
    """


class StreamOutcomeUnknownError(StreamStorageError):
    """The append may or may not have landed.

    The connection failed or timed out after the request left. Retrying the
    same values on the same producer is safe: the producer keeps its
    sequence until an append succeeds, and the store returns the original
    position for a retry that repeats a batch it already holds.
    """


class StreamRecordError(StreamError):
    """A stored record could not be turned into a record for this reader.

    Its body did not convert to the topic's type, or the codec or the store
    could not decode it. Reading on from :attr:`cursor` skips it, which is a
    choice the caller makes, since skipping loses the record.
    """

    def __init__(self, message: str, cursor: Cursor) -> None:
        """Name the record that failed by its cursor."""
        super().__init__(message)
        self.cursor = cursor


class StreamUnsupportedError(StreamError):
    """The capability is not offered by this release or by this provider."""
