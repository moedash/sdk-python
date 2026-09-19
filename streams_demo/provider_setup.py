"""Pick the provider for a demo run from the environment.

``STREAMS_PROVIDER`` names any registered provider; this tree carries
``memory`` and ``redis``. The demo needs a Temporal server to run the
workflow either way; ``TEMPORAL_ADDRESS`` points at it.
"""

from __future__ import annotations

import os

NAME = os.environ.get("STREAMS_PROVIDER", "memory")

# The memory provider is not replay-safe, so its demo keeps the cache warm.
# Storage providers run with the smallest cache they support instead.
WORKFLOW_CACHE = int(os.environ.get("STREAMS_WORKFLOW_CACHE", "512"))


async def open() -> tuple[str, dict]:
    """The server to connect to and the options :func:`configure` takes."""
    return os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"), {"provider": NAME}


async def close() -> None:
    """Let go of whatever :func:`open` acquired.

    Nothing here: the memory provider holds no connection. A provider branch
    that opens one closes it here, so the demo's teardown reads the same on
    every provider.
    """
