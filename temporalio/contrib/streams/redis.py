"""Streams in the application's Redis."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

from temporalio.bridge.proto.streams import RedisStoreConfig, StreamStoreConfig
from temporalio.contrib.streams._plugin import StreamStorePlugin

__all__ = ["RedisStreams"]


class RedisStreams(StreamStorePlugin):
    """A store in a Redis or a Redis Cluster that Core connects to.

    One instance per Redis deployment. Pass it to the client as a plugin,
    and open handles from the client with
    :func:`temporalio.contrib.streams.get_stream_handle`. Redis 7.0 or later
    is required, with ``maxmemory-policy noeviction``.
    """

    def __init__(
        self,
        url: str | Sequence[str],
        *,
        cluster: bool = False,
        key_prefix: str = "temporal-streams",
        retention: timedelta = timedelta(days=7),
        blocking_reads_per_node: int | None = None,
        response_timeout: timedelta | None = None,
    ) -> None:
        """Create the store's configuration.

        Args:
            url: A ``redis://`` or ``rediss://`` URL, or the seed nodes of a
                cluster. Credentials and TLS settings come from the URL.
            cluster: Whether ``url`` names a Redis Cluster. Every key of one
                Workflow's streams shares a hash tag, so a cluster serves
                them from one slot.
            key_prefix: Prepended to every key, so streams can share a Redis
                with other data and an ACL can scope them.
            retention: How long a stream keeps a record, and how long after
                its last write the stream itself lives.
            blocking_reads_per_node: How many reads may wait on one node at
                once, each on a connection of its own. Core's default when
                unset.
            response_timeout: How long a short call waits for its reply.
                Core's default when unset.

        Raises:
            ValueError: No URL was given, ``key_prefix`` is empty, or
                ``retention`` is shorter than a millisecond.
        """
        urls = [url] if isinstance(url, str) else list(url)
        if not urls:
            raise ValueError("RedisStreams needs a URL")
        if not key_prefix:
            raise ValueError("key_prefix must not be empty")
        if retention < timedelta(milliseconds=1):
            raise ValueError(
                f"retention must be at least a millisecond, got {retention}"
            )
        config = RedisStoreConfig(
            urls=urls,
            cluster=cluster,
            key_prefix=key_prefix,
            blocking_reads_per_node=blocking_reads_per_node or 0,
        )
        config.retention.FromTimedelta(retention)
        if response_timeout is not None:
            config.response_timeout.FromTimedelta(response_timeout)
        super().__init__(
            "temporalio.contrib.streams.RedisStreams",
            StreamStoreConfig(redis=config),
        )
