"""Choose the server-side provider.

The streams are on the server this process is already connected to, so there
is nothing to construct and nothing to shut down.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

NAME = "native"

WORKFLOW_CACHE = 0
"""No cache at all.

Delivery arrives on the workflow task the server dispatches, so nothing is
held between tasks and every task after the first is rebuilt from History.
"""


async def open() -> tuple[str, dict[str, Any]]:
    """The server to connect to, and the provider options."""
    ready = Path(__file__).resolve().parents[2] / "verify/ready.json"
    return json.loads(ready.read_text())["target"], {}


async def close() -> None:
    """Nothing to release."""
