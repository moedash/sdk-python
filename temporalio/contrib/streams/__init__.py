"""Streams: an ordered log a Workflow owns, written from many places.

.. warning::
    This package is experimental and may change in future versions.

A Workflow owns a stream. Producers outside Workflow code, such as the
Workflow's Activities and clients, append to it, and outside readers consume
it from a cursor. Records live in a store the application runs, reached
through a provider, and never pass through Temporal or its History.

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
   so every provider reports a retry the same way.
3. **A cursor belongs to one stream.** Hand it back to resume strictly after
   the record it names. A provider refuses a cursor from another provider
   or another stream with :class:`StreamCursorError`, and one whose record
   retention dropped with :class:`StreamExpiredError`. A read with no cursor
   starts at :data:`BEGINNING` or :data:`END`.
4. **Topics are defined once.** :func:`topic` defines a topic with the type
   its records decode to, and every party shares that definition. A call
   that names no topic addresses :data:`DEFAULT_TOPIC`.

A provider is a plugin: ``Client.connect(..., plugins=[provider])``
registers it on the client and on every Worker built from that client.

The record on the wire is ``temporal.sdk.streams.v1.StreamRecord``, in
:mod:`temporalio.contrib.streams.proto.v1`, with the user's value in ``body``
as an ordinary payload.
"""

from temporalio.contrib.streams._body import (
    CONTENT_HASH_KEY,
    content_fingerprint,
    content_hash,
    decode_body,
    encode_body,
)
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._plugin import StreamProviderPlugin
from temporalio.contrib.streams._provider import (
    StreamHandle,
    StreamProducer,
    StreamProvider,
)
from temporalio.contrib.streams._record import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)
from temporalio.contrib.streams._ref import StreamOwnerKind, StreamRef
from temporalio.contrib.streams._topic import (
    DEFAULT_TOPIC,
    StreamTopic,
    resolve_topic,
    topic,
)

__all__ = [
    "BEGINNING",
    "CONTENT_HASH_KEY",
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
    "StreamProvider",
    "StreamProviderPlugin",
    "StreamRecord",
    "StreamRef",
    "StreamTopic",
    "StreamUnsupportedError",
    "Supersession",
    "content_fingerprint",
    "content_hash",
    "decode_body",
    "encode_body",
    "resolve_topic",
    "topic",
]
