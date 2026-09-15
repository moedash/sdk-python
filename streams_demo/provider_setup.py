"""Pick the provider for a demo run from the environment.

``STREAMS_PROVIDER`` names any registered provider; this base tree carries
only ``memory``, and each provider branch adds its own name. The demo needs a
Temporal server to run the workflow either way; ``TEMPORAL_ADDRESS`` points
at it.
"""

from __future__ import annotations

import os

NAME = os.environ.get("STREAMS_PROVIDER", "memory")

# The memory provider is not replay-safe, so its demo keeps the cache warm.
# Storage providers run with the smallest cache they support instead.
WORKFLOW_CACHE = int(os.environ.get("STREAMS_WORKFLOW_CACHE", "512"))


async def open() -> tuple[str, dict]:
    return os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"), {"provider": NAME}
