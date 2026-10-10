"""The package exports only what applications use."""

from __future__ import annotations

import temporalio.contrib.streams as streams

# Provider-author helpers stay importable from their private modules, but
# they are not part of the public surface.
_PRIVATE = {
    "CONTENT_HASH_KEY",
    "content_fingerprint",
    "content_hash",
    "decode_body",
    "encode_body",
    "resolve_topic",
}


def test_provider_helpers_are_not_public():
    assert _PRIVATE.isdisjoint(streams.__all__)
    for name in _PRIVATE:
        assert not hasattr(streams, name), name
