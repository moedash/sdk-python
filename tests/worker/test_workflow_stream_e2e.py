"""A workflow publishing to a stream it owns and reading its own messages back.

Path A and Path C together, both from workflow code. Needs a Temporal server
built from the AI-198 branch, because neither the stream service nor the
commands exist on a released one:

    TEMPORAL_STREAM_TARGET=localhost:7233 uv run pytest tests/worker/test_workflow_stream_e2e.py

Skipped otherwise, rather than passing against a server that has no idea what a
stream is.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream commands",
)

STREAM = "output"

# Event type numbers rather than names: this SDK's api protos predate both
# events, so the attributes arrive as unknown fields and only the type is
# readable here. The server's own tests are where the offsets are asserted.
EVENT_STREAM_SUBSCRIBED = 61
EVENT_STREAM_MESSAGES_ADDED = 62


@workflow.defn
class PublishAndRead:
    @workflow.run
    async def run(self) -> list[str]:
        # Published before subscribing, because a subscription resolves against
        # the streams the workflow already owns and this is what creates one.
        workflow.add_stream_messages(
            [b"alpha", b"beta", b"gamma"], stream_id=STREAM, topic="progress"
        )
        workflow.add_stream_messages([b"delta"], stream_id=STREAM)

        workflow.subscribe_stream(STREAM, start_offset=0)

        received: list[str] = []
        while len(received) < 4:
            for body in await workflow.read_stream(STREAM):
                received.append(body.decode())
        return received


async def test_workflow_reads_back_what_it_published() -> None:
    client = await Client.connect(TARGET or "")
    task_queue = "publish-tq-" + uuid.uuid4().hex[:8]
    wf_id = "publish-wf-" + uuid.uuid4().hex[:8]

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishAndRead],
        # Every task after the first is a replay, so completing at all means the
        # reissued publish matched the event the first run wrote.
        max_cached_workflows=0,
    ):
        result = await asyncio.wait_for(
            client.execute_workflow(
                PublishAndRead.run, id=wf_id, task_queue=task_queue
            ),
            timeout=60,
        )

    assert result == ["alpha", "beta", "gamma", "delta"]

    counts = {EVENT_STREAM_MESSAGES_ADDED: 0, EVENT_STREAM_SUBSCRIBED: 0}
    async for event in client.get_workflow_handle(wf_id).fetch_history_events():
        if event.event_type in counts:
            counts[event.event_type] += 1

    # Two calls carrying four messages, so two events: the event is per batch,
    # which is what makes batching free.
    assert counts[EVENT_STREAM_MESSAGES_ADDED] == 2
    assert counts[EVENT_STREAM_SUBSCRIBED] == 1


@workflow.defn
class PublishOnSignal:
    """Publishes a token per signal, which is the shape an agent turn has."""

    def __init__(self) -> None:
        self._pending: list[str] = []
        self._done = False

    @workflow.run
    async def run(self) -> int:
        published = 0
        while True:
            await workflow.wait_condition(lambda: self._pending or self._done)
            while self._pending:
                workflow.add_stream_messages([self._pending.pop(0).encode()])
                published += 1
            if self._done:
                return published

    @workflow.signal
    async def emit(self, token: str) -> None:
        self._pending.append(token)

    @workflow.signal
    async def finish(self) -> None:
        self._done = True


async def test_a_client_follows_what_a_workflow_publishes() -> None:
    """The harness shape: the workflow writes, something outside it reads live.

    A stream a workflow owns has no id, so the reader addresses it by the
    owner. It also attaches before the first publish, which is what a UI opening
    on a session does.
    """
    from temporalio.client_stream import StreamClient

    client = await Client.connect(TARGET or "")
    streams = StreamClient.connect(TARGET or "")
    task_queue = "follow-tq-" + uuid.uuid4().hex[:8]
    wf_id = "follow-wf-" + uuid.uuid4().hex[:8]

    tokens = [f"token-{i}" for i in range(5)]
    received: list[str] = []

    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[PublishOnSignal],
            max_cached_workflows=0,
        ):
            handle = await client.start_workflow(
                PublishOnSignal.run, id=wf_id, task_queue=task_queue
            )

            # Attached before anything is published, so the stream does not
            # exist yet and the first poll has to park rather than fail.
            reader = streams.workflow_stream(wf_id)
            assert (await reader.describe()).head_offset == 0

            async def follow() -> None:
                async for message in reader.follow():
                    received.append(message.data.decode())
                    if len(received) == len(tokens):
                        return

            following = asyncio.ensure_future(follow())
            for token in tokens:
                await handle.signal(PublishOnSignal.emit, token)
            await asyncio.wait_for(following, timeout=60)

            await handle.signal(PublishOnSignal.finish)
            assert await asyncio.wait_for(handle.result(), timeout=60) == len(tokens)
    finally:
        await streams.close()

    assert received == tokens
