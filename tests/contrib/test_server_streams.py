"""The Workflow Streams surface over a server-side stream.

Both producers and the consumer, since the point of the surface is that an
application does not have to know which of them wrote a given item. Needs a
Temporal server built from the AI-198 branch:

    TEMPORAL_STREAM_TARGET=localhost:7233 uv run pytest tests/contrib/test_server_streams.py
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.server_streams import WorkflowStream, WorkflowStreamClient
from temporalio.worker import Worker

TARGET = os.environ.get("TEMPORAL_STREAM_TARGET")

pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set TEMPORAL_STREAM_TARGET to a server with the stream service",
)

TOPIC = "turn_events"


@dataclass
class Event:
    source: str
    text: str


@activity.defn
async def emit_from_activity(count: int) -> None:
    async with WorkflowStreamClient.from_within_activity() as client:
        events = client.topic(TOPIC, type=Event)
        for i in range(count):
            events.publish(Event(source="activity", text=f"token {i}"))


@workflow.defn
class Emitting:
    def __init__(self) -> None:
        self._events = WorkflowStream().topic(TOPIC, type=Event)

    @workflow.run
    async def run(self, count: int) -> None:
        self._events.publish(Event(source="workflow", text="turn started"))
        await workflow.execute_activity(
            emit_from_activity,
            count,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self._events.publish(Event(source="workflow", text="turn ended"))


async def test_both_producers_reach_one_subscriber() -> None:
    client = await Client.connect(TARGET or "")
    task_queue = "ss-tq-" + uuid.uuid4().hex[:8]
    wf_id = "ss-wf-" + uuid.uuid4().hex[:8]
    tokens = 4

    seen: list[Event] = []
    offsets: list[int] = []

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Emitting],
        activities=[emit_from_activity],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            Emitting.run, tokens, id=wf_id, task_queue=task_queue
        )

        # Subscribed before the Workflow has published anything, which is what
        # a consumer attaching to a session does.
        # Subscribed before the Workflow has published anything, which is what
        # a consumer attaching to a session does.
        stream = WorkflowStreamClient.create(client, wf_id)

        async def read() -> None:
            async for item in stream.subscribe(
                topics=[TOPIC], from_offset=0, result_type=Event
            ):
                seen.append(item.data)
                offsets.append(item.offset)
                if len(seen) == tokens + 2:
                    return

        reading = asyncio.ensure_future(read())
        await asyncio.wait_for(handle.result(), timeout=60)
        await asyncio.wait_for(reading, timeout=60)

    # The Workflow's own publishes bracket the Activity's, and both are on one
    # log in the order the server took them.
    assert [e.source for e in seen] == ["workflow"] + ["activity"] * tokens + ["workflow"]
    assert seen[0].text == "turn started"
    assert seen[-1].text == "turn ended"
    assert offsets == list(range(tokens + 2))

    # A consumer that arrives after the fact reads the same thing, and is not
    # left tailing: the Workflow has ended, so its stream is finished and the
    # subscription ends on its own.
    late = WorkflowStreamClient.create(client, wf_id)
    assert await late.get_offset() == tokens + 2
    replayed = [
        item.data.text
        async for item in late.subscribe(topics=[TOPIC], from_offset=0, result_type=Event)
    ]
    assert replayed == [e.text for e in seen]


async def test_a_reader_resumes_from_an_offset_it_was_given() -> None:
    client = await Client.connect(TARGET or "")
    task_queue = "ss-resume-tq-" + uuid.uuid4().hex[:8]
    wf_id = "ss-resume-wf-" + uuid.uuid4().hex[:8]

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Emitting],
        activities=[emit_from_activity],
        max_cached_workflows=0,
    ):
        await client.execute_workflow(
            Emitting.run, 3, id=wf_id, task_queue=task_queue
        )

    stream = WorkflowStreamClient.create(client, wf_id)
    first = [
        item
        async for item in stream.subscribe(topics=[TOPIC], from_offset=0, result_type=Event)
    ]
    assert len(first) == 5

    # Resuming past the second item skips exactly the two before it, so the
    # offset a reader was handed is the position it means.
    resumed = [
        item.data.text
        async for item in stream.subscribe(
            topics=[TOPIC], from_offset=first[1].offset + 1, result_type=Event
        )
    ]
    assert resumed == [item.data.text for item in first[2:]]
