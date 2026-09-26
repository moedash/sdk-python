"""Provider selection for the examples: the one place a store is named.

Every example takes the provider's name on the command line, builds it here,
and registers it once on the client. Nothing else in the examples names a
store: workers built from the client inherit the provider, and each context
asks for its stream through ``workflow.stream_reader`` or
``workflow.stream_writer``, ``activity.stream_handle()`` and
``client.get_stream_handle()``.
"""

from __future__ import annotations

import argparse

from temporalio.client import Client
from temporalio.streams.providers import ProviderPlugin

PROVIDERS = ("workflow_streams", "native", "redis")


def parser(description: str) -> argparse.ArgumentParser:
    """The flags every example shares."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("provider", choices=PROVIDERS)
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--redis", default="redis://127.0.0.1:6379")
    return parser


def make_provider(name: str, args: argparse.Namespace) -> ProviderPlugin:
    """The whole difference between the stores: one constructor call."""
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


async def connect(args: argparse.Namespace) -> tuple[Client, ProviderPlugin]:
    """A client with the provider registered on it, and the provider to close later."""
    provider = make_provider(args.provider, args)
    return await Client.connect(args.address, plugins=[provider]), provider
