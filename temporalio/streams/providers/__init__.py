"""Stream providers, one module each.

A provider that serves workers is a :class:`temporalio.worker.Plugin`.
:class:`ProviderPlugin` is the plugin half the providers in this tree share:
it hands the provider to the worker and the replayer as their
``stream_provider`` and leaves their execution alone. A provider that holds
connections closes them through its own ``close()``, not with the worker,
because the same provider serves handles outside any worker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager

import temporalio.worker
from temporalio.client import WorkflowHistory
from temporalio.streams._provider import StreamProvider
from temporalio.worker import (
    Replayer,
    ReplayerConfig,
    Worker,
    WorkerConfig,
    WorkflowReplayResult,
)

__all__ = ["ProviderPlugin"]


class ProviderPlugin(StreamProvider, temporalio.worker.Plugin):
    """The worker plugin every provider in this tree is built on.

    Subclasses implement :class:`temporalio.streams.StreamProvider`; this
    class supplies the four plugin hooks, so ``Worker(plugins=[provider])``
    and ``Replayer(plugins=[provider])`` reach the provider through their
    ``stream_provider`` option and the worker installs the interceptor that
    calls the workflow half's lifecycle hooks.
    """

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Set this provider as the worker's ``stream_provider``."""
        config["stream_provider"] = self
        return config

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Set this provider as the replayer's ``stream_provider``."""
        config["stream_provider"] = self
        return config

    async def run_worker(
        self, worker: Worker, next: Callable[[Worker], Awaitable[None]]
    ) -> None:
        """Run the worker unchanged."""
        await next(worker)

    def run_replayer(
        self,
        replayer: Replayer,
        histories: AsyncIterator[WorkflowHistory],
        next: Callable[
            [Replayer, AsyncIterator[WorkflowHistory]],
            AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]],
        ],
    ) -> AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]]:
        """Run the replayer unchanged."""
        return next(replayer, histories)
