"""The plugin a provider is, and how each context finds it.

A provider is registered once, as a plugin:
``Client.connect(plugins=[provider])``. The client keeps it, and every Worker
built from that client inherits it. ``Worker(plugins=[provider])`` and
``Replayer(plugins=[provider])`` register it on a Worker alone.

On a Worker the plugin adds one interceptor per Worker. The interceptor
makes the provider reachable from an Activity through a context variable,
and gives Workflow code the Worker's output coordinator through the
Worker's extern functions, which is how a passthrough object crosses into
the sandbox. The Worker finds the same coordinator on the interceptor
through an internal attribute and calls it around each activation, so a
Workflow's own publish commits with its Workflow Task.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
from abc import ABC, abstractmethod
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

import temporalio.activity
import temporalio.worker
import temporalio.workflow
from temporalio.contrib.streams._cursor import progress_counter
from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._notify import StreamNotifier, chain_first_run_id
from temporalio.contrib.streams._output import (
    OutputCoordinator,
    StagedBatch,
    StageRef,
)
from temporalio.contrib.streams._provider import StreamHandle, StreamProducer
from temporalio.contrib.streams._record import Cursor
from temporalio.contrib.streams._ref import StreamRef
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from temporalio.client import Client, ClientConfig
    from temporalio.worker import ReplayerConfig, WorkerConfig

__all__ = ["StreamProviderPlugin"]

logger = logging.getLogger(__name__)

_WORKFLOW_EXTERN = "__temporal_contrib_streams_output"

_activity_provider: contextvars.ContextVar[StreamProviderPlugin | None] = (
    contextvars.ContextVar("__temporal_contrib_streams_provider", default=None)
)


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
        # Notifiers by namespace, owner, chain and topic, once notify_on_append
        # is on.
        self._notifiers: dict[tuple[str, str, str, str], StreamNotifier] | None = None
        self._notifying: set[asyncio.Task[None]] = set()

    def notify_on_append(self) -> Self:
        """Tell each stream's notifier on the server when the stream moves.

        With this on, every append and finish through this provider, and every
        batch a Workflow publishes once it is visible, notifies the notifier
        of that stream and topic, folded, with one call in flight per stream
        (see :class:`StreamNotifier`). That is
        what makes a caller of a stream-returning Nexus operation see progress.
        Close a stream with :meth:`close_stream`.

        Needs a server with the stream notifier.

        .. warning::
            This API is experimental.
        """
        if self._notifiers is None:
            self._notifiers = {}
        return self

    async def close_stream(
        self, client: Client, ref: StreamRef, result: Any = None
    ) -> None:
        """Close ``ref``'s stream on the server's notifier with ``result``.

        Every Nexus operation that handed out this stream completes with
        ``result``. The records stay readable. Waits for the notification in
        flight first.

        Raises:
            temporalio.service.RPCError: The server refused the close, or the
                Workflow could not be described.

        .. warning::
            This API is experimental.
        """
        first_run_id = await chain_first_run_id(client, ref)
        # A write's notification may still be finding its chain.
        await asyncio.gather(*self._notifying, return_exceptions=True)
        key = (client.namespace, ref.workflow_id, first_run_id, ref.topic)
        notifier = (self._notifiers or {}).pop(key, None) or StreamNotifier(
            client, ref, first_run_id=first_run_id
        )
        # One above the newest record, so the close outranks every notification.
        latest = await self.get_stream_handle(client, ref).latest(topic=ref.topic)
        await notifier.close(result, progress_counter(latest) + 1)

    def _notified_producer(
        self,
        client: Client | None,
        ref: StreamRef,
        topic: str,
        producer: StreamProducer[Any],
    ) -> StreamProducer[Any]:
        """``producer``, notifying after each write when notifications are on."""
        if self._notifiers is None or client is None:
            return producer
        return _NotifyingProducer(producer, self, client, ref, topic)

    def _notify_promoted(self, client: Client, stage: StageRef) -> None:
        """Notify each topic of a Workflow's batch once it is visible."""
        if self._notifiers is None:
            return
        ref = StreamRef.for_workflow(stage.workflow_id)
        for topic in stage.topics:
            self._track(self._notify_latest(client, ref, topic, stage.first_run_id))

    def _track(self, notifying: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(notifying)
        self._notifying.add(task)
        task.add_done_callback(self._notifying.discard)

    async def _notify_latest(
        self, client: Client, ref: StreamRef, topic: str, first_run_id: str
    ) -> None:
        # A promotion has no cursor of its own, so the newest record's is told.
        try:
            first_run_id = first_run_id or await chain_first_run_id(client, ref)
            latest = await self.get_stream_handle(client, ref).latest(topic=topic)
        except Exception:
            logger.warning(
                "Could not find the newest record of Workflow %r topic %r to notify",
                ref.workflow_id,
                topic,
                exc_info=True,
            )
            return
        self._notifier(client, ref, topic, first_run_id).notify(
            latest.token, progress_counter(latest)
        )

    def _notifier(
        self, client: Client, ref: StreamRef, topic: str, first_run_id: str
    ) -> StreamNotifier:
        assert self._notifiers is not None
        key = (client.namespace, ref.workflow_id, first_run_id, topic)
        notifier = self._notifiers.get(key)
        if notifier is None:
            notifier = StreamNotifier(
                client, ref, topic=topic, first_run_id=first_run_id
            )
            self._notifiers[key] = notifier
        return notifier

    @abstractmethod
    def get_stream_handle(self, client: Client, ref: StreamRef) -> StreamHandle:
        """See :meth:`temporalio.contrib.streams.StreamProvider.get_stream_handle`."""

    @abstractmethod
    async def close(self) -> None:
        """See :meth:`temporalio.contrib.streams.StreamProvider.close`."""

    async def _stage(self, batch: StagedBatch) -> str:
        """Hold a Workflow's published batch, unseen, and return its stage token.

        Internal. Called off the Workflow thread before the completion that
        commits the batch goes to Core.

        Raises:
            StreamUnsupportedError: The provider does not accept a Workflow's
                own publish.
        """
        raise StreamUnsupportedError(
            f"stream provider {self.name()!r} does not accept the own publish of "
            f"Workflow {batch.workflow_id!r}"
        )

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
        client = config["client"]  # type:ignore[reportTypedDictNotRequiredAccess]
        config["interceptors"] = self._with_interceptor(
            config.get("interceptors"), client, client.namespace
        )
        return config

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Register this provider on the Replayer.

        Raises:
            ValueError: Another provider is already registered on the Replayer.
        """
        config = super().configure_replayer(config)
        config["interceptors"] = self._with_interceptor(
            config.get("interceptors"), None, config.get("namespace", "default")
        )
        return config

    def _with_interceptor(
        self, interceptors: Any, client: Client | None, namespace: str
    ) -> list[temporalio.worker.Interceptor]:
        held = list(interceptors or [])
        for interceptor in held:
            if isinstance(interceptor, _StreamsInterceptor):
                if interceptor.provider is not self:
                    raise ValueError(_two_providers(interceptor.provider, self))
                # The same plugin on the client and the Worker configures the
                # Worker twice; one interceptor is enough.
                return held
        return [*held, _StreamsInterceptor(self, client, namespace)]


class _NotifyingProducer(StreamProducer[Any]):
    """A producer that notifies the stream's notifier after each write."""

    def __init__(
        self,
        inner: StreamProducer[Any],
        plugin: StreamProviderPlugin,
        client: Client,
        ref: StreamRef,
        topic: str,
    ) -> None:
        self._inner = inner
        self._plugin = plugin
        self._client = client
        self._ref = ref
        self._topic = topic
        # The chain's first run, found once per producer: a producer writes
        # to one chain, and a new chain on the Workflow id gets new producers.
        self._first_run_id: asyncio.Future[str] | None = None

    @property
    def producer_id(self) -> str:
        """See :attr:`temporalio.contrib.streams.StreamProducer.producer_id`."""
        return self._inner.producer_id

    @property
    def attempt(self) -> int:
        """See :attr:`temporalio.contrib.streams.StreamProducer.attempt`."""
        return self._inner.attempt

    async def append(self, *values: Any) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamProducer.append`."""
        cursor = await self._inner.append(*values)
        if values:
            self._notify(cursor)
        return cursor

    async def finish(self) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamProducer.finish`."""
        cursor = await self._inner.finish()
        self._notify(cursor)
        return cursor

    def _notify(self, cursor: Cursor) -> None:
        # The write landed, so a notification that cannot go must not turn it
        # into an error the caller might retry with other content (DD-68).
        try:
            counter = progress_counter(cursor)
        except Exception:
            logger.warning(
                "Could not notify the stream after the write at %r",
                cursor.token,
                exc_info=True,
            )
            return
        found = self._first_run_id
        if found is not None and found.done() and found.exception() is None:
            self._notifier(found.result()).notify(cursor.token, counter)
        else:
            self._plugin._track(self._notify_once_found(cursor.token, counter))

    async def _notify_once_found(self, position: str, counter: int) -> None:
        if self._first_run_id is None:
            self._first_run_id = asyncio.ensure_future(
                chain_first_run_id(self._client, self._ref)
            )
        found = self._first_run_id
        try:
            first_run_id = await asyncio.shield(found)
        except Exception:
            if self._first_run_id is found:
                # The next write tries again.
                self._first_run_id = None
            logger.warning(
                "Could not find the run chain of Workflow %r to notify",
                self._ref.workflow_id,
                exc_info=True,
            )
            return
        self._notifier(first_run_id).notify(position, counter)

    def _notifier(self, first_run_id: str) -> StreamNotifier:
        return self._plugin._notifier(
            self._client, self._ref, self._topic, first_run_id
        )


def _two_providers(held: StreamProviderPlugin, new: StreamProviderPlugin) -> str:
    return (
        f"stream provider {held.name()!r} is already registered, so {new.name()!r} "
        "cannot be; register one provider and open the other's handles from the "
        "provider object"
    )


class _StreamsInterceptor(temporalio.worker.Interceptor):
    """Makes the provider reachable from Activities and Workflow code.

    One per Worker, because the output coordinator holds that Worker's runs.
    """

    def __init__(
        self, provider: StreamProviderPlugin, client: Client | None, namespace: str
    ) -> None:
        self.provider = provider
        self.output = OutputCoordinator(provider, client, namespace)

    @property
    def _temporal_activation_hook(self) -> OutputCoordinator:
        # The Worker looks for this name on its interceptors; see
        # temporalio.worker._workflow._ActivationHook.
        return self.output

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self.provider)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        output = self.output
        input.unsafe_extern_functions[_WORKFLOW_EXTERN] = lambda: output
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


def output_for_workflow() -> OutputCoordinator:
    """The output coordinator of the Worker running the current Workflow.

    Raises:
        ValueError: No provider is registered on the Worker.
    """
    found = temporalio.workflow.extern_functions().get(_WORKFLOW_EXTERN)
    if found is None:
        raise _no_provider("this Workflow's Worker")
    return found()
