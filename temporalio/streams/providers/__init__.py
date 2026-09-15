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
    except ImportError:
        # This tree does not carry that provider. The registry error message
        # lists what is actually available.
        pass
