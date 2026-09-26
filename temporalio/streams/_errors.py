"""The errors a stream call raises.

Every stream condition is a :class:`StreamError`, so a caller can catch by
meaning the way it catches other :class:`temporalio.exceptions.TemporalError`
subclasses. Argument mistakes stay ``ValueError``. A provider's transport
failure surfaces as :class:`temporalio.service.RPCError`, never as the
transport's own exception type.
"""

from __future__ import annotations

import temporalio.exceptions

__all__ = [
    "StreamCursorError",
    "StreamError",
    "StreamNotFoundError",
    "StreamProducerError",
    "StreamUnsupportedError",
]


class StreamError(temporalio.exceptions.TemporalError):
    """Base for stream conditions."""


class StreamNotFoundError(StreamError):
    """The workflow, chain or topic does not exist or is past retention."""


class StreamCursorError(StreamError):
    """The cursor was minted by another provider or names a record no longer retained."""


class StreamProducerError(StreamError):
    """The producer attempt or sequence conflicts with what the store holds."""


class StreamUnsupportedError(StreamError):
    """This provider does not offer the requested capability."""
