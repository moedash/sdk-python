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

The provider is registered once, on the client, and that is the only
provider-specific line here. The workers inherit it, the Activity reaches its
workflow's stream through ``activity.stream_handle()``, and the backend below
reads through ``client.get_stream_handle()``, or through the Nexus front
standing in for the store when there is one.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import uuid

from examples.streams import _setup
from examples.streams.agent import DECISIONS, Agent, generate, record_decision
from temporalio.client import Client
from temporalio.streams import RecordKind, StreamHandle
from temporalio.worker import Worker


async def main() -> None:
    """Run the loop once on the provider named on the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", choices=[*_setup.PROVIDERS, "nexus"])
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
    # Checked before any provider exists, so a missing flag is a usage error
    # rather than a traceback with a provider left open.
    if args.provider == "nexus" and not args.endpoint:
        parser.error("nexus needs --endpoint <endpoint-id>")

    store = args.behind if args.provider == "nexus" else args.provider
    provider = _setup.make_provider(store, args)
    client = await Client.connect(args.address, plugins=[provider])
    workflow_id = f"streams-example-{uuid.uuid4().hex[:8]}"
    task_queue = f"tq-{workflow_id}"

    workers = [
        Worker(
            client,
            task_queue=task_queue,
            workflows=[Agent],
            activities=[generate, record_decision],
            # Warm, because two of these transports park work against the
            # running workflow. The native provider also runs at zero, which
            # is its own result rather than something this example shows.
            max_cached_workflows=args.cache,
        )
    ]
    front = None
    if args.provider == "nexus":
        from temporalio.streams.providers.nexus import (
            NexusStreams,
            TemporalStreamsHandler,
        )

        workers.append(
            Worker(
                client,
                task_queue=args.handler_queue,
                nexus_service_handlers=[TemporalStreamsHandler(provider, client)],
            )
        )
        front = NexusStreams(endpoint=args.endpoint, http_address=args.http)

    try:
        async with contextlib.AsyncExitStack() as running:
            for worker in workers:
                await running.enter_async_context(worker)
            handle = await client.start_workflow(
                Agent.run, args.records, id=workflow_id, task_queue=task_queue
            )
            print(f"provider={args.provider} workflow={workflow_id}")

            # The same handle either way: from the Nexus front when there is
            # one, otherwise from the provider registered on the client.
            stream: StreamHandle = (
                front.get_stream_handle(client, workflow_id)
                if front is not None
                else client.get_stream_handle(workflow_id)
            )
            # Counted apart: a retried generator makes the workflow retract the
            # earlier attempt, and those records are correct output rather than
            # echoes that the workflow's own count would have to agree with.
            echoes = retractions = 0
            # The read ends by itself once the workflow is closed and the tail
            # has been delivered, on every provider.
            async for record in stream.read(topic=DECISIONS):
                print(
                    f"  {record.kind.name:11} {record.value} at {record.cursor.token}"
                )
                if record.kind is not RecordKind.DATA:
                    continue
                assert record.value is not None
                if record.value.echo is not None:
                    echoes += 1
                else:
                    retractions += 1

            decided = await handle.result()
            print(
                f"workflow decided {decided}; reader saw {echoes} echoes and "
                f"{retractions} retractions"
            )
    finally:
        if front is not None:
            await front.close()
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
