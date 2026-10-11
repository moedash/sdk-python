"""What lang owes a record's body between the converter and Core.

The payload converter turns a value into the body and stops there. Every
other payload the SDK sends then passes through the payload codec and
external storage, and a stream body owes the same, or a codec-protected
deployment would leak plaintext through its streams. Core never sees a
plaintext body, so both hashes that need one are taken here, before the
codec. A codec that encrypts with a fresh nonce makes every retry's bytes
differ, so a store that compared encoded bytes would refuse a retry as a
divergent write.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.converter import DataConverter

__all__ = ["batch_digest", "content_hash", "decode_body", "encode_bodies"]


def content_hash(payload: Payload) -> bytes:
    """The SHA-256 of ``payload`` as the converter produced it.

    Taken over the deterministic serialization of the whole payload, metadata
    included, so two payloads that differ only in their encoding hash apart.
    Core stores it on the record as hex under ``temporal.io/content-hash``.
    """
    return hashlib.sha256(payload.SerializeToString(deterministic=True)).digest()


def batch_digest(records: Sequence[WireRecord]) -> bytes:
    """The identity of one append, taken over its converted records.

    Each record is its length in 8 bytes big-endian, then its deterministic
    serialization, so a batch split differently can't collide with this one.
    Every SDK takes it the same way, and the store compares it when a
    producer repeats its newest batch.
    """
    digest = hashlib.sha256()
    for record in records:
        body = record.SerializeToString(deterministic=True)
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    return digest.digest()


async def encode_bodies(
    converter: DataConverter, payloads: Sequence[Payload]
) -> list[Payload]:
    """``payloads`` as the store keeps them.

    Through the payload codec and then external storage, in the order
    :meth:`temporalio.converter.DataConverter.encode` uses, so a body above
    the external storage threshold is replaced by a claim.
    """
    encoded = await converter._encode_payload_sequence(payloads)
    return await converter._external_store_payload_sequence(encoded)


async def decode_body(converter: DataConverter, payload: Payload) -> Payload:
    """Undo :func:`encode_bodies` on a body read back from the store.

    Raises:
        RuntimeError: The body is a claim and ``converter`` has no external
            storage to redeem it with.
    """
    retrieved = await converter._external_retrieve_payload_sequence([payload])
    return (await converter._decode_payload_sequence(retrieved))[0]
