"""Pick the provider for a demo run from the environment.

``STREAMS_PROVIDER`` names a provider; this base tree carries only ``memory``,
and each provider branch adds its own name here. The demo needs a Temporal
server to run the workflow either way; ``TEMPORAL_ADDRESS`` points at it.
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
    if NAME != "memory":
        raise SystemExit(f"this tree carries no stream provider named {NAME!r}")
    return os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"), MemoryStreams()


async def close(provider: ProviderPlugin) -> None:
    """Let go of whatever :func:`open` acquired.

    The memory provider holds no connection, so this is its ``close()`` and
    nothing more. A provider branch that opens one closes it the same way, so
    the demo's teardown reads the same on every provider.
    """
    await provider.close()
