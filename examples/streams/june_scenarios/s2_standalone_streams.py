"""Scenario "Client starts and consume stream", the three standalone alternatives.

Status: implemented for alts 1, 2 and 3; alt 3 in the ref-argument shape.

Why: a standalone stream has an id of its own and no owner, the client
creates and seals it on purpose, and a ``StreamRef`` carries it as data into
a workflow start, an activity or a Nexus operation.

    python -m examples.streams.june_scenarios.s2_standalone_streams native --address 127.0.0.1:7433
    python -m examples.streams.june_scenarios.s2_standalone_streams memory --address 127.0.0.1:7433
    python -m examples.streams.june_scenarios.s2_standalone_streams redis --address 127.0.0.1:7433

His alt 1 is a read that blocks until the stream exists. On ``native`` a
read on an id nobody has created parks on the server and delivers the first
record once a ``create_stream`` and an append land; ``memory`` and ``redis``
answer such a read with ``StreamNotFoundError`` instead, so the code shows
whichever the provider does.

His alt 2 is ``stream = await client.create_stream(ProgressUpdate,
stream_id=...)``. Ours is ``await client.create_stream(stream_id,
max_records=3)``: the policy is the stream's, the type is the topic's. An
outside producer appends, a second handle opened by id reads from
``BEGINNING``, which the policy has moved past the oldest record, and
``close()`` seals the stream: a late append raises ``StreamClosedError`` and
a read opened afterwards ends by itself once the retained tail is delivered.

His alt 3 is a client-side stream made durable by the start call. Ours
creates the stream first and passes ``handle.ref()`` as the workflow's
argument; the workflow hands the ref to its activity, which opens it with
``activity.stream_handle(ref)`` and appends, and the client follows the same
ref. A start that commits the stream with the workflow, so a crash between
the two calls cannot leave an orphan, remains the design question.

``workflow_streams`` keeps a stream inside a workflow's own log, so it has
nowhere to put a stream with no owner and declines.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import activity, streams, workflow
from temporalio.streams import (
    RecordKind,
    StreamClosedError,
    StreamHandle,
    StreamNotFoundError,
    StreamRecord,
    StreamRef,
)
from temporalio.worker import Worker


@dataclass
class ProgressUpdate:
    """One line of progress on the session."""

    message: str


PROGRESS = streams.topic("progress", ProgressUpdate)


def show(record: StreamRecord[Any]) -> None:
    """One line per record."""
    print(f"    {record.kind.name:6} {record.producer_id:8} {record.value}")


async def first(reader: StreamHandle) -> StreamRecord[Any]:
    """The first record of a read on ``PROGRESS``, closing the read after it.

    The read is opened in here, so a provider that refuses a missing stream at
    the call refuses it inside the task that waits for the record.
    """
    async with contextlib.aclosing(reader.read(topic=PROGRESS)) as reading:
        async for record in reading:
            return record
    raise RuntimeError("the read ended before a record arrived")


@activity.defn
async def report_progress(session: StreamRef) -> int:
    """Append three steps to the stream the ref names.

    Inside an activity the producer's identity is the activity's own, so a
    retry of this activity lands each step once.
    """
    producer = activity.stream_handle(session).producer()
    for step in range(1, 4):
        await producer.append(ProgressUpdate(f"step {step} done"))
        await asyncio.sleep(0.2)
    await producer.finish()
    return 3


@workflow.defn
class Session:
    """Receives the stream as a ref and hands it to its activity."""

    @workflow.run
    async def run(self, session: StreamRef) -> int:
        """Run the activity that writes to the stream; the workflow never touches it."""
        return await workflow.execute_activity(
            report_progress, session, start_to_close_timeout=timedelta(minutes=1)
        )


async def run(args: argparse.Namespace) -> None:
    """Wait for a stream, run one through its life, then pass one into a workflow."""
    _common.banner("s2 standalone streams", args.provider)
    if args.provider == "workflow_streams":
        print(
            "  workflow_streams keeps a stream inside a workflow's log, so it has "
            "nowhere to put a stream with no owner; skipped"
        )
        return
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s2")
    stream_id = f"session-{workflow_id.rsplit('-', 1)[1]}"
    try:
        print("  alt 1: a reader asks for a stream nobody has created yet")
        reader = client.get_stream_handle(stream_id=stream_id)
        waiting = asyncio.create_task(first(reader))
        await asyncio.sleep(0.5)
        parked: asyncio.Task[StreamRecord[Any]] | None = waiting
        if waiting.done():
            # memory and redis do not wait for a stream to be created.
            try:
                waiting.result()
            except StreamNotFoundError as error:
                print(f"    StreamNotFoundError: {error}")
            parked = None
        else:
            print("    the read is parked on the server until the stream exists")
        created = await client.create_stream(stream_id, max_records=3)
        await created.producer(
            topic=PROGRESS, producer_id="operator", attempt=1
        ).append(ProgressUpdate("created"))
        if parked is not None:
            record = await parked
            print(
                "    and it delivered the first record once the create and an append landed:"
            )
            show(record)

        print("  alt 2: produce from outside, read by id, seal it")
        operator = created.producer(topic=PROGRESS, producer_id="operator", attempt=2)
        for note in ("watching", "looks good", "wrapping up"):
            await operator.append(ProgressUpdate(note))
        await operator.finish()
        await created.close()
        print("    max_records=3 moved the floor: BEGINNING is the oldest record kept")
        by_id = client.get_stream_handle(stream_id=stream_id)
        async for record in by_id.read(topic=PROGRESS):
            show(record)
        try:
            await created.producer(
                topic=PROGRESS, producer_id="late", attempt=1
            ).append(ProgressUpdate("too late"))
        except StreamClosedError as error:
            print(f"    a late append is refused: StreamClosedError: {error}")

        print("  alt 3: create, then start the workflow with the stream's ref")
        session = await client.create_stream(f"{stream_id}-run")
        ref = session.ref(topic=PROGRESS)
        print(f"    passing {ref}")
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Session],
            activities=[report_progress],
        ):
            handle = await client.start_workflow(
                Session.run, ref, id=workflow_id, task_queue=task_queue
            )
            # The ref names the topic, not its type; the typed topic says
            # what to decode as.
            async for record in client.get_stream_handle(ref).read(topic=PROGRESS):
                show(record)
                if record.kind is RecordKind.FINISH:
                    break
            print(f"    the workflow returned {await handle.result()}")
        await session.close()
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
