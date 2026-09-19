"""Stream providers, one module each.

Importing this package registers every provider whose dependencies are
present. A tree that lacks a provider's dependencies simply does not offer
that name; nothing else changes, because everything above the provider is
shared.
"""

from __future__ import annotations

_KNOWN = ("memory", "workflow_streams", "native", "redis", "nexus")

for _name in _KNOWN:
    try:
        __import__(f"{__name__}.{_name}")
    except ModuleNotFoundError as error:
        # Two things may be missing: the provider module itself, or a
        # dependency from outside this package. A name missing inside
        # temporalio is a broken provider, and hiding that would turn its
        # traceback into "no such provider".
        missing = error.name or ""
        if missing != f"{__name__}.{_name}" and (
            not missing or missing.startswith("temporalio")
        ):
            raise
