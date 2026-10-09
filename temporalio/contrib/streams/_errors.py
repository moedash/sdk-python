"""The errors a stream call raises.

Every stream condition is a :class:`StreamError`, so a caller can catch by
meaning the way it catches other :class:`temporalio.exceptions.TemporalError`
subclasses. Argument mistakes stay ``ValueError``.

Two distinctions matter to a caller that retries. A write the store refused
(:class:`StreamProducerError`, :class:`StreamClosedError`) did not happen,
and repeating it gets the same answer. A write whose outcome is unknown
(:class:`StreamOutcomeUnknownError`) may have landed, and repeating it on
the same producer is safe because the store deduplicates the retry. A cursor
that names a record the store no longer keeps
(:class:`StreamExpiredError`) is told apart from one that is not valid for
the stream at all (:class:`StreamCursorError`).
"""

from __future__ import annotations

import temporalio.exceptions

__all__ = [
    "StreamClosedError",
    "StreamCursorError",
    "StreamError",
    "StreamExpiredError",
    "StreamNotFoundError",
    "StreamOutcomeUnknownError",
    "StreamProducerError",
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


class StreamClosedError(StreamError):
    """The stream is closed and refuses appends.

    The store refused the write, so nothing was written. Records the stream
    retains stay readable.
    """


class StreamProducerError(StreamError):
    """The store refused the append because of the producer's identity.

    The sequence was already used with different content, or it is below the
    newest sequence the store holds for this producer and attempt. Nothing
    was written, and repeating the call gets the same answer.
    """


class StreamOutcomeUnknownError(StreamError):
    """The append may or may not have landed.

    The connection failed or timed out after the request left. Retrying the
    same values on the same producer is safe: the producer keeps its
    sequence until an append succeeds, and the store returns the original
    position for a retry that repeats a batch it already holds.
    """


class StreamUnsupportedError(StreamError):
    """The capability is not offered by this release or by this provider."""
