"""The internal activation hook and where per-run stream state lives."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import topic, workflow_writer
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.worker import Interceptor
from tests.helpers import new_worker

EVENTS = topic("events", dict)


def client_with(client: Client, provider: MemoryStreams) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


@workflow.defn
class LoopStateProbe:
    @workflow.run
    async def run(self) -> bool:
        workflow_writer(EVENTS).publish({"n": 1})
        return any("streams" in name for name in vars(asyncio.get_running_loop()))


async def test_per_run_stream_state_is_not_kept_on_the_event_loop(client: Client):
    streams_client = client_with(client, MemoryStreams())
    async with new_worker(streams_client, LoopStateProbe) as worker:
        on_loop = await streams_client.execute_workflow(
            LoopStateProbe.run,
            id=f"streams-loop-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    assert on_loop is False


class _FailsOnce:
    def __init__(self) -> None:
        self.failed = False

    def take_jobs(self, act: Any) -> None:
        del act
        if not self.failed:
            self.failed = True
            raise RuntimeError("hook failed on its first activation")

    async def before_completion(self, *_: Any) -> None:
        pass

    def discard(self, *_: Any) -> None:
        pass

    async def after_completion(self, *_: Any) -> None:
        pass

    async def on_eviction(self, *_: Any) -> None:
        pass


class _HookInterceptor(Interceptor):
    def __init__(self) -> None:
        self._temporal_activation_hook = _FailsOnce()


@workflow.defn
class Quick:
    @workflow.run
    async def run(self) -> str:
        return "done"


async def test_a_hook_that_fails_in_take_jobs_fails_the_activation_cleanly(
    client: Client,
):
    # Raised outside the activation's error handling, the activation would
    # never be completed and the Workflow Task would wait for its timeout.
    async with new_worker(client, Quick, interceptors=[_HookInterceptor()]) as worker:
        result = await asyncio.wait_for(
            client.execute_workflow(
                Quick.run,
                id=f"streams-hook-{uuid.uuid4().hex}",
                task_queue=worker.task_queue,
            ),
            timeout=8.0,
        )
    assert result == "done"
