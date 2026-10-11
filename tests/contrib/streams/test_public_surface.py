"""The package exports only what applications use."""

from __future__ import annotations

import temporalio.contrib.streams as streams

# The helpers behind the handles stay importable from their private modules,
# but they are not part of the public surface.
_PRIVATE = {
    "batch_digest",
    "content_hash",
    "decode_body",
    "encode_bodies",
    "error_from_failure",
}


def test_internal_helpers_are_not_public():
    assert _PRIVATE.isdisjoint(streams.__all__)
    for name in _PRIVATE:
        assert not hasattr(streams, name), name
