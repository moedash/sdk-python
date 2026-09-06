"""The record envelope both bindings put on the wire.

A provider stores an opaque body. The contract needs more than a body: which
topic a record belongs to, whether it is data or a terminal marker, and which
producer attempt wrote it. Neither prototype carries all of that in a channel
of its own, so the interface carries it here and each binding stores the result
as an ordinary body.

Encoding is synchronous and deterministic because a workflow publishes on the
workflow thread. The header is JSON with sorted keys for that reason, and
because a stored record that a person can read is worth more during an
incident than a few saved bytes.
"""

from __future__ import annotations

import json
from typing import Any

from temporalio.streams._record import RecordKind

__all__ = ["decode", "encode"]

_MAGIC = b"tsf1"


def encode(
    *,
    topic: str,
    kind: RecordKind,
    producer: str,
    attempt: int,
    sequence: int,
    body: bytes,
) -> bytes:
    """Wrap one already-encoded body with the contract's metadata."""
    header: dict[str, Any] = {"k": int(kind), "t": topic}
    # Omitted rather than written as a default, so a workflow's own publish
    # does not carry three fields that only mean something for a producer.
    if producer:
        header["p"] = producer
    if attempt:
        header["a"] = attempt
    if sequence >= 0:
        header["s"] = sequence
    raw = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
    return _MAGIC + len(raw).to_bytes(4, "big") + raw + body


def decode(frame: bytes) -> tuple[RecordKind, str, str, int, int, bytes]:
    """Split a frame into ``(kind, topic, producer, attempt, sequence, body)``.

    Raises:
        ValueError: The bytes are not a frame this interface wrote. Reading a
            stream someone else populated is a real case, so it fails with a
            name rather than an index error.
    """
    if len(frame) < 8 or frame[:4] != _MAGIC:
        raise ValueError(
            "this record was not written through the stream interface, so its "
            "topic and producer are unknown"
        )
    size = int.from_bytes(frame[4:8], "big")
    header = json.loads(frame[8 : 8 + size])
    return (
        RecordKind(header.get("k", int(RecordKind.DATA))),
        header.get("t", ""),
        header.get("p", ""),
        int(header.get("a", 0)),
        int(header.get("s", -1)),
        frame[8 + size :],
    )
