"""The plugin a provider is.

A provider is registered once, as a plugin:
``Client.connect(plugins=[provider])``. The client keeps it, and every Worker
built from that client inherits it. ``Worker(plugins=[provider])`` and
``Replayer(plugins=[provider])`` register it on a Worker alone.

On a Worker the plugin adds one interceptor and nothing else, so the
Worker knows which provider it carries. No Worker internals change.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import temporalio.worker
from temporalio.contrib.streams._provider import StreamHandle
from temporalio.contrib.streams._ref import StreamRef
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from temporalio.client import Client, ClientConfig
    from temporalio.worker import ReplayerConfig, WorkerConfig

__all__ = ["StreamProviderPlugin"]


class StreamProviderPlugin(SimplePlugin, ABC):
    """The base class of every provider.

    A subclass implements :class:`temporalio.contrib.streams.StreamProvider`.
    This class makes it a client and Worker plugin. One provider serves a
    client and its Workers; registering a second, different provider on the
    same client or Worker raises ``ValueError``, because the accessors would
    otherwise pick one without saying so. A process that talks to two stores
    opens the second store's handles from the provider object.
    """

    def __init__(self, name: str) -> None:
        """Name the plugin; the name shows in the Worker's plugin list."""
        super().__init__(name)
        self._interceptor = _StreamsInterceptor(self)

    @abstractmethod
    def get_stream_handle(self, client: Client, ref: StreamRef) -> StreamHandle:
        """See :meth:`temporalio.contrib.streams.StreamProvider.get_stream_handle`."""

    @abstractmethod
    async def close(self) -> None:
        """See :meth:`temporalio.contrib.streams.StreamProvider.close`."""

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Register this provider on the client.

        Raises:
            ValueError: Another provider is already registered on the client.
        """
        for plugin in config.get("plugins", []):
            if isinstance(plugin, StreamProviderPlugin) and plugin is not self:
                raise ValueError(_two_providers(plugin, self))
        return super().configure_client(config)

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Register this provider on the Worker.

        Raises:
            ValueError: Another provider is already registered on the Worker.
        """
        config = super().configure_worker(config)
        config["interceptors"] = self._with_interceptor(config.get("interceptors"))
        return config

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Register this provider on the Replayer.

        Raises:
            ValueError: Another provider is already registered on the Replayer.
        """
        config = super().configure_replayer(config)
        config["interceptors"] = self._with_interceptor(config.get("interceptors"))
        return config

    def _with_interceptor(
        self, interceptors: Any
    ) -> list[temporalio.worker.Interceptor]:
        held = list(interceptors or [])
        for interceptor in held:
            if isinstance(interceptor, _StreamsInterceptor):
                if interceptor.provider is not self:
                    raise ValueError(_two_providers(interceptor.provider, self))
                # The same plugin on the client and the Worker configures the
                # Worker twice; one interceptor is enough.
                return held
        return [*held, self._interceptor]


def _two_providers(held: StreamProviderPlugin, new: StreamProviderPlugin) -> str:
    return (
        f"stream provider {held.name()!r} is already registered, so {new.name()!r} "
        "cannot be; register one provider and open the other's handles from the "
        "provider object"
    )


class _StreamsInterceptor(temporalio.worker.Interceptor):
    """Marks the provider a Worker carries."""

    def __init__(self, provider: StreamProviderPlugin) -> None:
        self.provider = provider
