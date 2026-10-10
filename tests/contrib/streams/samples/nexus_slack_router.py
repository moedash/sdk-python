"""Streams over Nexus: a handler streams messages, and a caller Workflow routes them.

The handler side runs a ``Conversation`` Workflow per request. It publishes
one message per reply on its own stream and closes the stream when it is
done. The ``respond`` Nexus operation hands that stream to its caller.

The caller side is a ``SlackRouter`` Workflow. It starts ``respond``, reads
the messages as they arrive, and files each one under its channel. The
operation completes when the stream closes, with the summary the
``Conversation`` Workflow passed to the close.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
    close_workflow_stream,
)
from temporalio.worker import Worker


@dataclass
class Message:
    channel: str
    text: str


MESSAGES = topic("messages", Message)


@nexusrpc.service
class ConversationService:
    respond: nexusrpc.Operation[str, str]


# Handler side


@workflow.defn
class Conversation:
    """Publishes the replies to one request, then closes its stream."""

    @workflow.run
    async def run(self, request: str) -> None:
        writer = workflow_writer(MESSAGES)
        replies = [
            Message("#ops", f"{request}: deploy started"),
            Message("#sales", f"{request}: customer notified"),
            Message("#ops", f"{request}: deploy finished"),
        ]
        for reply in replies:
            writer.publish(reply)
            await workflow.sleep(timedelta(milliseconds=100))
        writer.finish()
        await close_workflow_stream(f"{len(replies)} messages", topic=MESSAGES)


async def open_conversation(ctx: Any, request: str) -> StreamRef:
    """Starts the Workflow that produces the stream and names the stream."""
    workflow_id = f"conversation-{ctx.request_id}"
    await temporalio.nexus.client().start_workflow(
        Conversation.run,
        request,
        id=workflow_id,
        task_queue=temporalio.nexus.info().task_queue,
    )
    return StreamRef.for_workflow(workflow_id, topic=MESSAGES.name)


@nexusrpc.handler.service_handler(service=ConversationService)
class ConversationHandler:
    @nexusrpc.handler.operation_handler
    def respond(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_conversation)


# Caller side


@dataclass
class Routed:
    channels: dict[str, list[str]] = field(default_factory=dict)
    summary: str = ""


@workflow.defn
class SlackRouter:
    """Reads a conversation's messages and files each under its channel."""

    @workflow.run
    async def run(self, endpoint: str, request: str) -> Routed:
        client = workflow.create_nexus_client(
            service=ConversationService, endpoint=endpoint
        )
        handle = await client.start_operation(ConversationService.respond, request)
        reader = StreamReader(handle, item_type=Message, endpoint=endpoint)
        routed = Routed()
        while (batch := await reader.next()) is not None:
            for message in batch:
                routed.channels.setdefault(message.channel, []).append(message.text)
        routed.summary = await handle
        return routed


async def main(client: Client, endpoint: str, task_queue: str) -> Routed:
    """Run one conversation through the router and return what it routed.

    ``endpoint`` must be a Nexus endpoint that targets ``task_queue``. The
    server needs CHASM, ``nexusoperation.enableProgress`` and
    ``streamnotifier.enabled``.
    """
    # The handler's Worker carries the store, tells the server's notifier when
    # the stream moves, and serves the reads on the same endpoint.
    provider = MemoryStreams().notify_on_append()
    streams = TemporalStreamsHandler(provider)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Conversation, SlackRouter],
        nexus_service_handlers=[ConversationHandler(), streams],
        plugins=[provider],
    ):
        routed = await client.execute_workflow(
            SlackRouter.run,
            args=[endpoint, "release 42"],
            id=f"router-{task_queue}",
            task_queue=task_queue,
        )
        await streams.close()
    await provider.close()
    return routed
