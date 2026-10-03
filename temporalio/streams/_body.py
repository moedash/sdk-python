"""What a provider owes a record's body between the converter and its store.

:func:`temporalio.streams._wire.to_wire` converts a value into the body with
the payload converter and stops there. Every other payload the SDK sends then
passes through the payload codec and external storage, and a stream body owes
the same, or a codec-protected deployment would leak plaintext through its
streams and a claim-check deployment would push oversized bodies at its store.
A provider runs the body through :func:`encode_body` before it stores or ships
a record and through :func:`decode_body` after it reads one back, off the
workflow thread in both directions.

The order inside :func:`encode_body` is the point. The plaintext hash is taken
first and stamped on the record, and :func:`content_fingerprint` is taken over
the converted records too, before the codec runs, because a codec that
encrypts with a fresh nonce makes every retry's bytes differ, and a store that
fingerprinted those bytes would refuse the retry as a divergent write. The
store keeps the plaintext hash under :data:`CONTENT_HASH_KEY` and can compare
retries by it without ever seeing the plaintext.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import StreamRecord as WireRecord
from temporalio.converter import DataConverter

__all__ = [
    "CONTENT_HASH_KEY",
    "content_fingerprint",
    "content_hash",
    "decode_body",
    "encode_body",
]

CONTENT_HASH_KEY = "temporal.io/content-hash"
"""The record metadata key the plaintext hash of the body is stored under.

Its value is a payload with ``encoding`` ``binary/plain`` whose data is the
hex digest :func:`content_hash` returns. A ``FINISH`` record carries no body
and no hash.
"""

_HASH_ENCODING = b"binary/plain"


def content_hash(payload: Payload) -> str:
    """The hex SHA-256 of ``payload`` as the converter produced it.

    Taken over the deterministic serialization of the whole payload, metadata
    included, so two payloads that differ only in their encoding hash apart.
    """
    return hashlib.sha256(payload.SerializeToString(deterministic=True)).hexdigest()


def content_fingerprint(records: Sequence[WireRecord]) -> bytes:
    """The identity of one append, taken over its converted records.

    Length-delimited, so a batch split differently cannot collide with this
    one. Take it before :func:`encode_body`, while the bodies are still what
    the converter produced; that is what makes a retry through a
    nondeterministic codec match its original.
    """
    digest = hashlib.sha256()
    for record in records:
        body = record.SerializeToString(deterministic=True)
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    return digest.digest()


async def encode_body(converter: DataConverter, record: WireRecord) -> WireRecord:
    """Stamp the plaintext hash on ``record`` and encode its body for the store.

    In place, and returned for convenience. The hash goes under
    :data:`CONTENT_HASH_KEY` first; then the body passes through
    ``converter``'s payload codec and external storage in the order
    :meth:`temporalio.converter.DataConverter.encode` uses, so a body above
    the external storage threshold is replaced by a claim and the claim is
    what the store holds. A record without a body is returned untouched.
    """
    if not record.HasField("body"):
        return record
    record.metadata[CONTENT_HASH_KEY].CopyFrom(
        Payload(
            metadata={"encoding": _HASH_ENCODING},
            data=content_hash(record.body).encode(),
        )
    )
    encoded = await converter._encode_payload_sequence([record.body])
    stored = await converter._external_store_payload_sequence(encoded)
    record.body.CopyFrom(stored[0])
    return record


async def decode_body(converter: DataConverter, record: WireRecord) -> WireRecord:
    """Undo :func:`encode_body` on a record read back from the store.

    In place, and returned for convenience. The body is retrieved from
    external storage when it is a claim and then run through the payload
    codec, in the order :meth:`temporalio.converter.DataConverter.decode`
    uses, leaving the payload the converter can turn back into a value. The
    hash stays on the record.

    Raises:
        RuntimeError: The body is a claim and ``converter`` has no external
            storage to redeem it with.
    """
    if not record.HasField("body"):
        return record
    retrieved = await converter._external_retrieve_payload_sequence([record.body])
    decoded = await converter._decode_payload_sequence(retrieved)
    record.body.CopyFrom(decoded[0])
    return record
