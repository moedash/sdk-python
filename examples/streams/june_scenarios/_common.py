"""What every scenario shares: its flags, its ids and one way to print.

The store is still named in one place, ``examples.streams._setup``. The
scenarios add the memory provider to the choices because two of them need an
activity-owned stream or truncation, and memory is the one in-process store
that has both.
"""

from __future__ import annotations

import argparse
import uuid

from examples.streams import _setup

PROVIDERS = (*_setup.PROVIDERS, "memory")


def parser(description: str) -> argparse.ArgumentParser:
    """The example flags, plus the memory provider and the Nexus ingress."""
    parser = _setup.parser(description, PROVIDERS)
    parser.add_argument(
        "--http",
        default="http://127.0.0.1:7243",
        help="the server's Nexus HTTP ingress, for s8",
    )
    return parser


def ids(prefix: str) -> tuple[str, str]:
    """A fresh workflow id and its own task queue, so reruns never collide."""
    workflow_id = f"{prefix}-{uuid.uuid4().hex[:8]}"
    return workflow_id, f"tq-{workflow_id}"


def banner(title: str, provider: str) -> None:
    """Head each scenario's output, so a run of all of them reads in sections."""
    print(f"\n== {title} [{provider}]")
