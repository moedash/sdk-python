"""Choose the client-side provider.

The store is a Redis the customer runs, so this process constructs the backend
and is responsible for closing it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import redis.asyncio
from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend

NAME = "external"

WORKFLOW_CACHE = 2
"""The smallest cache this provider runs on.

It holds the workflow task open between records, which a run that can be
evicted cannot do. Below two, a consuming workflow stops with no error and no
failed task. Recovery has to be tested by killing the worker rather than by
shrinking the cache.
"""

_backend: RedisStreamBackend | None = None


async def open() -> tuple[str, dict[str, Any]]:
    """The server to connect to, and the provider options."""
    global _backend
    ready = Path(__file__).resolve().parents[2] / "verify/ready.json"
    url = os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6399")
    # redis-py defaults to a socket timeout equal to this provider's own
    # blocking read, so an idle read would abandon a healthy socket.
    _backend = RedisStreamBackend(
        client=redis.asyncio.from_url(url, decode_responses=False, socket_timeout=30),
        key_prefix=os.environ.get("AI198_REDIS_PREFIX", "ai198-contract"),
    )
    return json.loads(ready.read_text())["target"], {"backend": _backend}


async def close() -> None:
    """Release the store this process opened."""
    global _backend
    if _backend is not None:
        await _backend.aclose()
        _backend = None
