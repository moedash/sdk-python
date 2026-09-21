r"""Run the same agent on whichever provider is configured.

    python -m examples.streams.run workflow_streams
    python -m examples.streams.run redis     --redis redis://127.0.0.1:6379
    python -m examples.streams.run native    --address 127.0.0.1:7333
    python -m examples.streams.run nexus     --endpoint <endpoint-id>

The Nexus mode needs an endpoint that routes to the handler worker's task
queue, and the flag takes the endpoint's id, not its name::

    temporal operator nexus endpoint create --name streams-e2e \
        --target-task-queue streams-handlers-e2e
    temporal operator nexus endpoint get --name streams-e2e -o json | jq -r .id

The only provider-specific code in this file is :func:`make_provider`, which
turns a name into one constructor call. Everything below it, and all of
``agent.py``, is the same on every option: the worker takes the provider as a
plugin, the activity publishes through it, and outside code opens a handle
from it, or from the Nexus front standing in for it.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import uuid

from examples.streams.agent import DECISIONS, Agent, Generator, record_decision
from temporalio.client import Client
from temporalio.streams import RecordKind, StreamProvider
from temporalio.streams.providers import ProviderPlugin
from temporalio.worker import Worker


def make_provider(args: argparse.Namespace) -> ProviderPlugin:
    """The whole difference between the options: one constructor call."""
    name = args.behind if args.provider == "nexus" else args.provider
    if name == "workflow_streams":
        from temporalio.streams.providers.workflow_streams import (
            WorkflowStreamsProvider,
        )

        return WorkflowStreamsProvider()
    if name == "redis":
        from temporalio.streams.providers.redis import RedisStreams

        return RedisStreams(url=args.redis)
    if name == "native":
        from temporalio.streams.providers.native import NativeStreams

        return NativeStreams()
    raise SystemExit(f"unknown provider {name}")


def outside_surface(
    args: argparse.Namespace, provider: StreamProvider
) -> StreamProvider:
    """Where an outside producer or consumer opens its handle.

    The same handle either way: from the Nexus front when there is one,
    otherwise straight from the provider the worker runs on.
    """
    if args.provider != "nexus":
        return provider
    from temporalio.streams.providers.nexus import NexusStreams

    return NexusStreams(endpoint=args.endpoint, http_address=args.http)


async def main() -> None:
    """Run the loop once on the provider named on the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "provider", choices=["workflow_streams", "redis", "native", "nexus"]
    )
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--redis", default="redis://127.0.0.1:6379")
    parser.add_argument(
        "--endpoint",
        default="",
        help="nexus endpoint id, not its name; see streams_demo/README.md",
    )
    parser.add_argument("--http", default="http://127.0.0.1:7243")
    parser.add_argument(
        "--behind",
        default="workflow_streams",
        help="the store a nexus handler serves",
    )
    parser.add_argument("--records", type=int, default=3)
    parser.add_argument("--cache", type=int, default=100)
    parser.add_argument(
        "--handler-queue",
        default="streams-handlers-e2e",
        help="task queue the nexus endpoint routes to",
    )
    args = parser.parse_args()

    provider = make_provider(args)
    client = await Client.connect(args.address)
    workflow_id = f"streams-example-{uuid.uuid4().hex[:8]}"
    task_queue = f"tq-{workflow_id}"

    workers = [
        Worker(
            client,
            task_queue=task_queue,
            workflows=[Agent],
            activities=[Generator(provider).generate, record_decision],
            # Warm, because two of these transports park work against the
            # running workflow. The native provider also runs at zero, which
            # is its own result rather than something this example shows.
            max_cached_workflows=args.cache,
            plugins=[provider],
        )
    ]
    if args.provider == "nexus":
        from temporalio.streams.providers.nexus import TemporalStreamsHandler

        workers.append(
            Worker(
                client,
                task_queue=args.handler_queue,
                nexus_service_handlers=[TemporalStreamsHandler(provider, client)],
            )
        )

    front = outside_surface(args, provider)
    try:
        async with contextlib.AsyncExitStack() as running:
            for worker in workers:
                await running.enter_async_context(worker)
            handle = await client.start_workflow(
                Agent.run, args.records, id=workflow_id, task_queue=task_queue
            )
            print(f"provider={args.provider} workflow={workflow_id}")

            # Counted apart: a retried generator makes the workflow retract the
            # earlier attempt, and those records are correct output rather than
            # echoes that the workflow's own count would have to agree with.
            echoes = retractions = 0
            stream = front.get_stream_handle(client, workflow_id)
            # The read ends by itself once the workflow is closed and the tail
            # has been delivered, on every provider.
            async for record in stream.read(topic=DECISIONS, result_type=dict):
                print(
                    f"  {record.kind.name:11} {record.value} at {record.cursor.token}"
                )
                if record.kind is not RecordKind.DATA:
                    continue
                if isinstance(record.value, dict) and "echo" in record.value:
                    echoes += 1
                else:
                    retractions += 1

            decided = await handle.result()
            print(
                f"workflow decided {decided}; reader saw {echoes} echoes and "
                f"{retractions} retractions"
            )
    finally:
        if front is not provider:
            await front.close()
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
