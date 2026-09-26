"""Pick the provider for a demo run from the environment.

``STREAMS_PROVIDER`` names a provider; this tree carries ``memory``,
``workflow_streams``, ``native`` and ``redis``. The demo needs a Temporal
server to run the workflow either way; ``TEMPORAL_ADDRESS`` points at it, the
native demo needs one built with the stream service, and the Redis demo reads
its store from ``AI198_REDIS_URL`` and ``AI198_REDIS_PREFIX``.
"""

from __future__ import annotations

import os

from temporalio.streams.providers import ProviderPlugin
from temporalio.streams.providers.memory import MemoryStreams

NAME = os.environ.get("STREAMS_PROVIDER", "memory")

# The memory provider is not replay-safe, so its demo keeps the cache warm.
# Storage providers run with the smallest cache they support instead.
WORKFLOW_CACHE = int(os.environ.get("STREAMS_WORKFLOW_CACHE", "512"))


async def open() -> tuple[str, ProviderPlugin]:
    """The server to connect to and the provider the worker and the client share."""
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    if NAME == "memory":
        return address, MemoryStreams()
    if NAME == "workflow_streams":
        from temporalio.streams.providers.workflow_streams import (
            WorkflowStreamsProvider,
        )

        return address, WorkflowStreamsProvider()
    if NAME == "native":
        from temporalio.streams.providers.native import NativeStreams

        return address, NativeStreams()
    if NAME == "redis":
        from temporalio.streams.providers.redis import RedisStreams

        return address, RedisStreams(
            url=os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6379"),
            key_prefix=os.environ.get("AI198_REDIS_PREFIX", "ai198-contract"),
        )
    raise SystemExit(f"this tree carries no stream provider named {NAME!r}")


async def close(provider: ProviderPlugin) -> None:
    """Let go of whatever :func:`open` acquired.

    Every provider releases what it opened through its own ``close()``, so
    the demo's teardown reads the same on all of them.
    """
    await provider.close()
