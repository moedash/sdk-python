"""Who owns a shared channel, and who may close it.

One channel is shared per loop, target and namespace, so two providers in one
process can be using the same one. Closing a provider has to leave the other's
channels alone.
"""

from __future__ import annotations

import asyncio

from temporalio import client_stream
from temporalio.streams.providers.native import NativeStreams


class _FakeClient:
    """Stands in for a StreamClient, which would want a real channel."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _put(target: str, namespace: str) -> _FakeClient:
    fake = _FakeClient()
    per_loop = client_stream._shared.setdefault(asyncio.get_running_loop(), {})
    per_loop[(target, namespace)] = fake  # type: ignore[assignment]
    return fake


async def test_closing_named_clients_leaves_the_others_open() -> None:
    mine = _put("host-a:7233", "ns")
    theirs = _put("host-b:7233", "ns")
    await client_stream.close_shared_clients(("host-a:7233", "ns"))
    assert mine.closed
    assert not theirs.closed, "another provider is still reading through it"
    await client_stream.close_shared_clients()
    assert theirs.closed


async def test_a_provider_closes_only_what_its_own_handles_opened() -> None:
    mine = _put("host-a:7233", "ns")
    theirs = _put("host-b:7233", "ns")

    provider = NativeStreams()
    provider._opened.add(("host-a:7233", "ns"))
    await provider.close()
    assert mine.closed
    assert not theirs.closed
    await client_stream.close_shared_clients()
