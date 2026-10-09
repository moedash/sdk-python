"""The plugin a provider is, and how each context finds it.

A provider is registered once, as a plugin:
``Client.connect(plugins=[provider])``. The client keeps it, and every Worker
built from that client inherits it. ``Worker(plugins=[provider])`` and
``Replayer(plugins=[provider])`` register it on a Worker alone.

On a Worker the plugin adds one interceptor and nothing else. The
interceptor makes the provider reachable from an Activity through a context
variable, and from Workflow code through the Worker's extern functions,
which is how a passthrough object crosses into the sandbox. No Worker
internals change.
"""

from __future__ import annotations

import contextvars
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

import temporalio.activity
import temporalio.worker
import temporalio.workflow
from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._provider import StreamHandle
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._wire import WireRecord
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from temporalio.client import Client, ClientConfig
    from temporalio.worker import ReplayerConfig, WorkerConfig

__all__ = ["StreamProviderPlugin"]

_WORKFLOW_EXTERN = "__temporal_contrib_streams_provider"

_activity_provider: contextvars.ContextVar[StreamProviderPlugin | None] = (
    contextvars.ContextVar("__temporal_contrib_streams_provider", default=None)
)


class _WorkflowOutput(Protocol):
    """Where a Workflow's own publish goes, on the Workflow thread.

    Internal: the Worker side of a Workflow publish is not part of the
    public surface. ``publish`` is synchronous and must not block, because
    it runs on the Workflow thread.
    """

    def publish(self, records: Sequence[WireRecord]) -> None:
        """Take records the running Workflow published."""
        ...


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

    def _workflow_output(self, info: temporalio.workflow.Info) -> _WorkflowOutput:
        """The output for one run's own publish, opened on the Workflow thread.

        Raises:
            StreamUnsupportedError: The provider does not accept a Workflow's
                own publish.
        """
        raise StreamUnsupportedError(
            f"stream provider {self.name()!r} does not accept the own publish of "
            f"Workflow {info.workflow_id!r}"
        )

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
    """Makes the provider reachable from Activities and Workflow code."""

    def __init__(self, provider: StreamProviderPlugin) -> None:
        self.provider = provider

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self.provider)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        provider = self.provider
        input.unsafe_extern_functions[_WORKFLOW_EXTERN] = lambda: provider
        return None


class _ActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        provider: StreamProviderPlugin,
    ) -> None:
        super().__init__(next)
        self._provider = provider

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        token = _activity_provider.set(self._provider)
        try:
            return await self.next.execute_activity(input)
        finally:
            _activity_provider.reset(token)


def _no_provider(where: str) -> ValueError:
    return ValueError(
        f"no stream provider is registered on {where}; pass one as a plugin, for "
        "example Client.connect(..., plugins=[provider])"
    )


def provider_for_client(client: Client) -> StreamProviderPlugin:
    """The provider registered on ``client``.

    Raises:
        ValueError: No provider is registered on the client.
    """
    for plugin in client.config()["plugins"]:
        if isinstance(plugin, StreamProviderPlugin):
            return plugin
    raise _no_provider("this client")


def provider_for_activity() -> StreamProviderPlugin:
    """The provider registered on the Worker running the current Activity.

    Raises:
        ValueError: No provider is registered on the Worker or its client.
    """
    provider = _activity_provider.get()
    if provider is not None:
        return provider
    try:
        return provider_for_client(temporalio.activity.client())
    except (RuntimeError, ValueError):
        raise _no_provider("this Activity's Worker") from None


def provider_for_workflow() -> StreamProviderPlugin:
    """The provider registered on the Worker running the current Workflow.

    Raises:
        ValueError: No provider is registered on the Worker.
    """
    found = temporalio.workflow.extern_functions().get(_WORKFLOW_EXTERN)
    if found is None:
        raise _no_provider("this Workflow's Worker")
    return found()
