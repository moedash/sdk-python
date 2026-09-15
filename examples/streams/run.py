"""Run the same agent on whichever provider is configured.

    python -m examples.streams.run workflow_streams
    python -m examples.streams.run redis     --redis redis://127.0.0.1:6399
    python -m examples.streams.run native    --address 127.0.0.1:7233
    python -m examples.streams.run nexus     --endpoint <endpoint-id>

The only provider-specific code in this file is :func:`configure_provider`,
which turns a name into one ``streams.configure`` call. Everything below it,
and all of ``agent.py``, is the same on every option.
"""

from __future__ import annotations

import argparse
import asyncio
import uuid

from temporalio import streams
from temporalio.client import Client
from temporalio.worker import Worker

from examples.streams.agent import Agent, generate, record_decision


def configure_provider(args: argparse.Namespace) -> None:
    """The whole difference between the options."""
    if args.provider == "workflow_streams":
        streams.configure(provider="workflow_streams")
    elif args.provider == "redis":
        streams.configure(provider="redis", url=args.redis)
    elif args.provider == "native":
        streams.configure(provider="native")
    elif args.provider == "nexus":
        # The worker still needs a store; the endpoint is how everything
        # outside the worker reaches it without naming one.
        streams.configure(provider=args.behind)
    else:
        raise SystemExit(f"unknown provider {args.provider}")


async def ensure_stream(args: argparse.Namespace, client: Client, workflow_id: str) -> None:
    """Create the inbound stream when the provider needs it to pre-exist.

    The two storage providers want opposite orders, which is worth knowing
    before writing an application against either. The native provider
    resolves a subscription against a stream the server already has, and a
    workflow cannot create one from workflow code because that would be I/O,
    so whoever starts the workflow creates it first. The client-side provider
    is the mirror image: it names streams under the run's chain key, so its
    producer needs the workflow to exist already. Option 0 mints on first
    publish and does not care.
    """
    if args.provider != "native":
        return
    from temporalio.client_stream import StreamClient
    from temporalio.streams.providers.native import inbound_stream_id

    streams_client = StreamClient.connect(args.address, client.namespace)
    await streams_client.create(inbound_stream_id(workflow_id, "inputs"))


def outside_surface(args: argparse.Namespace):
    """Where an outside producer or consumer connects.

    The same handles either way: through the Nexus endpoint when there is
    one, otherwise straight at the configured provider.
    """
    if args.provider == "nexus":
        from temporalio.streams._provider import instance

        return instance("nexus", endpoint=args.endpoint, http_address=args.http)
    return streams


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "provider", choices=["workflow_streams", "redis", "native", "nexus"]
    )
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--redis", default="redis://127.0.0.1:6399")
    parser.add_argument("--endpoint", default="", help="nexus endpoint id")
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

    configure_provider(args)
    client = await Client.connect(args.address)
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
            **streams.worker_options(),
        )
    ]
    if args.provider == "nexus":
        from temporalio.streams.providers.nexus import TemporalStreamsHandler

        workers.append(
            Worker(
                client,
                task_queue=args.handler_queue,
                nexus_service_handlers=[
                    TemporalStreamsHandler(client, provider=args.behind)
                ],
            )
        )

    async with workers[0]:
        async with workers[-1] if len(workers) > 1 else _null():
            await ensure_stream(args, client, workflow_id)
            handle = await client.start_workflow(
                Agent.run, args.records, id=workflow_id, task_queue=task_queue
            )
            print(f"provider={args.provider} workflow={workflow_id}")

            reader = await outside_surface(args).consumer(
                client if args.provider != "nexus" else None,
                workflow_id=workflow_id,
            )
            seen = 0
            records = reader.read(type=dict, topic="decisions")
            try:
                async for record in records:
                    print(
                        f"  {record.kind.name:11} {record.value} at {record.cursor.token}"
                    )
                    if record.kind is streams.RecordKind.FINISH:
                        break
                    seen += 1
            finally:
                # Closed before the workflow is released, because a reader
                # still polling this run would have its in-flight poll
                # cancelled when the workflow lets its readers go.
                await records.aclose()

            await handle.signal(Agent.release)
            decided = await handle.result()
            print(f"workflow decided {decided}, reader saw {seen}")


class _null:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


if __name__ == "__main__":
    asyncio.run(main())
