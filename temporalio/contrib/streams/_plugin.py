"""The plugin a store configuration is, and how each context finds the store.

A configuration class is registered once, on the client:
``Client.connect(plugins=[RedisStreams(...)])``. Connecting the client asks
Core for the store it names. Core serves that one store to the client's
handles and to every Worker built from the client, so a Workflow's publish
and an outside read meet in the same place.

On a Worker the plugin hands Core the store, which commits a Workflow's
records with its Workflow Task, and adds one interceptor. The interceptor
tells Workflow code, through the Worker's extern functions, that the Worker
has a store, so a publish on a Worker without one fails at the call.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

import temporalio.bridge.client
import temporalio.worker
import temporalio.workflow
from temporalio.bridge.proto.streams import (
    DeleteOwnerRequest,
    StreamOwnerKind,
    StreamStoreConfig,
)
from temporalio.bridge.streams import StreamCallFailure, StreamStore
from temporalio.bridge.streams_generated import StreamService
from temporalio.contrib.streams._errors import error_from_failure
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from temporalio.bridge.temporal_sdk_bridge import StreamStoreRef
    from temporalio.client import Client, ClientConfig
    from temporalio.service import ConnectConfig, ServiceClient
    from temporalio.worker import WorkerConfig

__all__ = ["StreamStorePlugin"]

_WORKFLOW_EXTERN = "__temporal_contrib_streams_store"

T = TypeVar("T")


class StreamStorePlugin(SimplePlugin):
    """The base class of the store configuration classes.

    Use :class:`temporalio.contrib.streams.redis.RedisStreams` or
    :class:`temporalio.contrib.streams.memory.MemoryStreams`. One instance
    is one store. Registering a second, different store on the same client
    raises ``ValueError``, because a handle could otherwise pick either.
    """

    def __init__(self, name: str, config: StreamStoreConfig) -> None:
        """Name the plugin and the store Core connects to."""
        super().__init__(name)
        self._config = config
        self._service: StreamService | None = None
        self._store: StreamStore | None = None
        self._connecting: asyncio.Lock | None = None

    async def _promote(self, stage: StageRef) -> None:
        """Make the staged batch ``stage`` visible to readers, in order.

        Internal. Called once History shows the marker that names the
        stage. Promoting a stage twice, or one the provider does not hold,
        does nothing.
        """
        raise StreamUnsupportedError(
            f"stream provider {self.name()!r} cannot promote stage {stage.token!r} "
            f"of Workflow {stage.workflow_id!r}"
        )

    async def _abort(self, stage: StageRef) -> None:
        """Drop the staged batch ``stage`` without making it visible.

        Internal. Called once History shows that the task which staged it
        failed. Aborting a stage twice, or one the provider does not hold,
        does nothing.
        """
        raise StreamUnsupportedError(
            f"stream provider {self.name()!r} cannot abort stage {stage.token!r} "
            f"of Workflow {stage.workflow_id!r}"
        )

    async def _close_chain(
        self, namespace: str, workflow_id: str, first_run_id: str
    ) -> None:
        """Refuse further appends to the streams of an ended run chain.

        Internal. Called best effort by the Worker after a run's final
        Workflow Task. A provider without a close gate does nothing.
        """
        del namespace, workflow_id, first_run_id

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Register this store on the client.

        Raises:
            ValueError: Another store is already registered on the client.
        """
        for plugin in config.get("plugins", []):
            if isinstance(plugin, StreamStorePlugin) and plugin is not self:
                raise ValueError(_two_stores(plugin, self))
        return super().configure_client(config)

    async def connect_service_client(
        self,
        config: ConnectConfig,
        next: Callable[[ConnectConfig], Awaitable[ServiceClient]],
    ) -> ServiceClient:
        """Connect the client, then the store, so Workers built from it have one.

        A lazy client connects here too, since Core needs its connection to
        ask about a stream's owner.

        Raises:
            temporalio.contrib.streams.StreamError: Core could not reach the
                store.
        """
        service_client = await next(config)
        await self._connected(service_client)
        return service_client

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Hand the client's store to the Worker.

        Raises:
            ValueError: The plugin is not registered on the Worker's client,
                or that client was not made with ``Client.connect``.
        """
        config = super().configure_worker(config)
        client = config["client"]  # type:ignore[reportTypedDictNotRequiredAccess]
        if self not in client.config()["plugins"] or self._store is None:
            raise ValueError(
                f"stream store {self.name()!r} must be registered on the Worker's "
                "client, as Client.connect(..., plugins=[store])"
            )
        held = list(config.get("interceptors") or [])
        # The same plugin on the client and the Worker configures the Worker twice.
        if not any(isinstance(i, _StreamsInterceptor) for i in held):
            held.append(_StreamsInterceptor())
        config["interceptors"] = held
        return config

    @property
    def _temporal_stream_store(self) -> StreamStoreRef | None:
        # The Worker looks for this name on its plugins and gives the store to Core.
        return self._store.ref if self._store is not None else None

    async def delete_workflow_streams(self, namespace: str, workflow_id: str) -> int:
        """Delete every stream of ``workflow_id`` in ``namespace``.

        An admin helper for cleanup and compliance requests. Deleting a
        Workflow does not delete its streams. Their retention bounds what is
        left, and this removes it at once, across every run chain of the
        Workflow id. It asks Temporal nothing, so it can run after the
        Workflow is gone. On Redis it walks the keyspace, so run it rarely,
        not on a hot path.

        Delete only after the chain closed and its producers stopped. A late
        append writes a fresh stream with no dedupe state and no close flag.

        Returns:
            How many store keys were deleted.

        Raises:
            ValueError: No client connected this store yet.
            temporalio.contrib.streams.StreamStorageError: The store failed;
                some keys may be deleted.
        """
        if self._service is None:
            raise ValueError(f"no client has connected stream store {self.name()!r}")
        request = DeleteOwnerRequest(
            namespace=namespace,
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=workflow_id,
        )
        return (await call(self._service.delete_owner(request))).deleted

    async def _service_for(self, client: Client) -> StreamService:
        """The stream service, connecting the store if no client did yet."""
        if self._service is None:
            await self._connected(client.service_client)
        assert self._service is not None
        return self._service

    async def _connected(self, service_client: ServiceClient) -> None:
        if self._connecting is None:
            self._connecting = asyncio.Lock()
        async with self._connecting:
            if self._store is not None:
                return
            bridge: temporalio.bridge.client.Client = await cast(
                Any, service_client
            )._connected_client()
            self._store = await call(StreamStore.connect(bridge, self._config))
            self._service = StreamService(self._store)


async def call(pending: Awaitable[T]) -> T:
    """Await a stream call, raising Core's failure as the error a caller catches."""
    try:
        return await pending
    except StreamCallFailure as failure:
        raise error_from_failure(failure.failure) from None


def _two_stores(held: StreamStorePlugin, new: StreamStorePlugin) -> str:
    return (
        f"stream store {held.name()!r} is already registered, so {new.name()!r} "
        "cannot be; register one store per client"
    )


class _StreamsInterceptor(temporalio.worker.Interceptor):
    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        input.unsafe_extern_functions[_WORKFLOW_EXTERN] = lambda: True
        return None


def store_for_client(client: Client) -> StreamStorePlugin:
    """The store registered on ``client``.

    Raises:
        ValueError: No store is registered on the client.
    """
    for plugin in client.config()["plugins"]:
        if isinstance(plugin, StreamStorePlugin):
            return plugin
    raise ValueError(
        "no stream store is registered on this client; pass one as a plugin, for "
        "example Client.connect(..., plugins=[RedisStreams(...)])"
    )


def worker_has_store() -> bool:
    """Whether the Worker running the current Workflow has a stream store."""
    return _WORKFLOW_EXTERN in temporalio.workflow.extern_functions()
