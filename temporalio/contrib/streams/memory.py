"""Streams in this process's memory, for tests and local development."""

from __future__ import annotations

from temporalio.bridge.proto.streams import MemoryStoreConfig, StreamStoreConfig
from temporalio.contrib.streams._plugin import StreamStorePlugin

__all__ = ["MemoryStreams"]


class MemoryStreams(StreamStorePlugin):
    """A store in this process's memory.

    Core holds it, so the client's handles and every Worker built from the
    client share it. Nothing survives the process, and another process sees
    none of it.
    """

    def __init__(self) -> None:
        """Create the store's configuration."""
        super().__init__(
            "temporalio.contrib.streams.MemoryStreams",
            StreamStoreConfig(memory=MemoryStoreConfig()),
        )
