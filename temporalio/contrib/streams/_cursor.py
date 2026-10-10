"""Cursor tokens bound to the provider and the stream that minted them.

A token reads ``<provider>:<stream hash>:<position>``. The stream hash is a
short digest of the stream's identity, so a provider refuses a cursor from
another stream at the call instead of resuming at an unrelated position
that happens to exist. The position is the provider's own and nothing else
reads it.
"""

from __future__ import annotations

import hashlib

from temporalio.contrib.streams._errors import StreamCursorError
from temporalio.contrib.streams._record import Cursor

__all__ = [
    "BEGINNING",
    "END",
    "cursor_position",
    "mint_cursor",
    "progress_counter",
    "stream_hash",
]

BEGINNING = Cursor("")
"""Read from the oldest record the stream still retains."""

END = Cursor("$end")
"""Read only what is appended after the read starts.

It is resolved when the read starts. To position a reader before the
reader's process writes something, use the handle's ``latest`` instead.
"""

_HASH_LENGTH = 8


def stream_hash(namespace: str, owner_kind: str, owner_id: str, topic: str) -> str:
    """The short digest that binds a cursor to one stream.

    Taken over the namespace, the owner and the topic. A run id is never
    part of it, because a stream follows its owner's run chain and a cursor
    stays valid across Continue-as-New.
    """
    digest = hashlib.sha256()
    for part in (namespace, owner_kind, owner_id, topic):
        encoded = part.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()[:_HASH_LENGTH]


def mint_cursor(provider: str, stream: str, position: str) -> Cursor:
    """A cursor that names ``position`` on the stream whose hash is ``stream``."""
    return Cursor(f"{provider}:{stream}:{position}")


def cursor_position(cursor: Cursor, *, provider: str, stream: str) -> str | None:
    """The position inside ``cursor``, or ``None`` for ``BEGINNING``.

    Raises:
        StreamCursorError: The cursor is ``END``, which has no position, or it
            was minted by another provider or for another stream, or it is
            not a token at all.
    """
    if cursor == BEGINNING:
        return None
    if cursor == END:
        raise StreamCursorError("END has no position; resolve it at the read")
    parts = cursor.token.split(":", 2)
    if len(parts) != 3 or not parts[2]:
        raise StreamCursorError(f"cursor {cursor.token!r} is not a stream cursor")
    minted_by, minted_for, position = parts
    if minted_by != provider:
        raise StreamCursorError(
            f"cursor {cursor.token!r} was minted by the {minted_by!r} provider, "
            f"not the {provider!r} provider"
        )
    if minted_for != stream:
        raise StreamCursorError(
            f"cursor {cursor.token!r} belongs to another stream; a cursor resumes "
            "only the stream it was read from"
        )
    return position


# Redis entry ids are ``<ms>-<seq>``. Twenty bits hold the sequence, which is
# far more entries than one stream takes in a millisecond, and the
# milliseconds stay below 2**43 until the year 2248, so the packed value fits
# a positive int64.
_REDIS_SEQUENCE_BITS = 20


def progress_counter(cursor: Cursor) -> int:
    """The notifier counter for the record ``cursor`` names.

    A counter must grow with the stream (a receiver keeps the highest and
    drops a lower one), and producers in different processes must agree on
    it. So it comes from the store's own position, not from a clock:

    * the memory provider's position, an offset in the topic, gives
      ``offset + 1``;
    * a Redis entry id ``<ms>-<seq>`` gives ``ms << 20 | seq``, with a
      sequence beyond 20 bits held at the largest, which keeps the order
      non-decreasing;
    * ``BEGINNING`` gives 0.

    Both grow with the records because the store assigns the positions in
    order. Two records in one Redis millisecond past the 20-bit sequence get
    one counter, and the notifier drops the second as a repeat; the next
    notification tells the reader again. See ``docs/wire-contract.md``.

    Raises:
        ValueError: The cursor's position is in neither form.
    """
    if cursor == BEGINNING:
        return 0
    position = cursor.token.rsplit(":", 1)[-1]
    milliseconds, separator, sequence = position.partition("-")
    try:
        if separator:
            return (int(milliseconds) << _REDIS_SEQUENCE_BITS) | min(
                int(sequence), (1 << _REDIS_SEQUENCE_BITS) - 1
            )
        return int(position) + 1
    except ValueError:
        raise ValueError(
            f"cursor {cursor.token!r} names no position a counter can come from"
        ) from None
