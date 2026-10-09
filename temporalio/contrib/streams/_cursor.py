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
from temporalio.contrib.streams._record import BEGINNING, END, Cursor
from temporalio.contrib.streams._ref import StreamRef

__all__ = ["cursor_position", "mint_cursor", "stream_hash"]

_HASH_LENGTH = 8


def stream_hash(namespace: str, ref: StreamRef) -> str:
    """The short digest that binds a cursor to one stream.

    Taken over the namespace, the owner and the topic. The run id is left
    out, because a stream follows its owner's run chain and a cursor stays
    valid across Continue-as-New.
    """
    digest = hashlib.sha256()
    for part in (namespace, ref.kind, ref.workflow_id, ref.topic):
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
