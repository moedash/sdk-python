"""Scenario "Client starts and consume stream", the three standalone alternatives.

Status: implemented for alt 2, with one open note for alts 1 and 3.

Why: the server holds standalone streams with their own id and lifecycle, and
the stream client creates, appends to, truncates and reads them today.

    python -m examples.streams.june_scenarios.s2_standalone_streams native --address 127.0.0.1:7333

His alt 2 is ``stream = await client.create_stream(ProgressUpdate,
stream_id=...)`` and then ``start_workflow(..., arg=stream)``. Ours creates
the stream through ``temporalio.client_stream.StreamClient`` and passes its
id, a plain string, as the workflow argument. The workflow's activity appends
to it, an outside producer appends beside it, and the client follows it
until the stream is closed. The orphan he warns about is real: nothing ties
the stream to the workflow, so a crash between the two calls leaves it to
its retention.

Two of his asks are open questions on the blueprint, not built. His alt 1,
a read that blocks until the stream exists, is shown below as what happens
today: the reader probes and gets ``StreamNotFoundError``. His alt 3, a
client-side stream made durable by the start call, is the stream-reference
and append-with-start follow-on.

Only the native provider reaches the stream service, so on any other
provider this scenario says so and stops. It also shows ``BEGINNING`` on a
moved floor, which ``s1`` cannot show on native: after a truncate, an offset
below the floor is refused, and the ``earliest`` start lands on the oldest
record left.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import timedelta

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import activity, workflow
from temporalio.api.stream.v1 import StreamRecord, StreamRecordKind, StreamStartPosition
from temporalio.client_stream import StreamClient, StreamEntry
from temporalio.converter import PayloadConverter
from temporalio.service import RPCError
from temporalio.streams import RecordKind, StreamError, StreamNotFoundError
from temporalio.worker import Worker


@dataclass
class ProgressUpdate:
    """One line of progress on the session."""

    message: str


def encode(
    converter: PayloadConverter, update: ProgressUpdate, producer_id: str
) -> StreamRecord:
    """A standalone stream takes wire records, so the value becomes a payload here."""
    return StreamRecord(
        kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
        body=converter.to_payload(update),
        producer_id=producer_id,
    )


def show(converter: PayloadConverter, entry: StreamEntry) -> None:
    """One line per entry."""
    record = entry.record
    kind = RecordKind(record.kind or RecordKind.DATA)
    value = (
        converter.from_payload(record.body, ProgressUpdate)
        if kind is RecordKind.DATA
        else None
    )
    print(f"    {entry.offset}: {kind.name:6} {record.producer_id:8} {value}")


@activity.defn
async def report_progress(stream_id: str) -> int:
    """Append three steps to the standalone stream named by ``stream_id``.

    Each append carries the producer id and a sequence, so a retry of this
    activity lands each step once.
    """
    client = activity.client()
    service = StreamClient.connect(
        client.service_client.config.target_host, client.namespace
    )
    try:
        stream = service.get(stream_id)
        converter = client.data_converter.payload_converter
        for step in range(1, 4):
            update = ProgressUpdate(f"step {step} done")
            await stream.append(
                encode(converter, update, "agent"),
                producer_id="agent",
                sequence=step,
            )
            await asyncio.sleep(0.2)
        await stream.finish_writing("agent")
        return 3
    finally:
        await service.close()


@workflow.defn
class Session:
    """Receives the stream's id as its argument and hands it to its activity."""

    @workflow.run
    async def run(self, stream_id: str) -> int:
        """Run the activity that writes to the stream; the workflow never touches it."""
        return await workflow.execute_activity(
            report_progress, stream_id, start_to_close_timeout=timedelta(minutes=1)
        )


async def run(args: argparse.Namespace) -> None:
    """Probe, create, produce from two places, consume, then truncate."""
    _common.banner("s2 standalone streams", args.provider)
    if args.provider != "native":
        print(
            "  standalone streams live on the server's stream service, which only "
            "the native provider reaches; skipped"
        )
        return
    client, provider = await _setup.connect(args)
    service = StreamClient.connect(
        client.service_client.config.target_host, client.namespace
    )
    converter = client.data_converter.payload_converter
    workflow_id, task_queue = _common.ids("june-s2")
    stream_id = f"session-{workflow_id.rsplit('-', 1)[1]}"
    try:
        print("  alt 1 today: a reader probes a stream nobody has created")
        try:
            await service.get(stream_id).read()
        except StreamNotFoundError as error:
            print(f"    StreamNotFoundError: {error}")

        print("  alt 2: create, pass the id to the workflow, produce, consume")
        stream = await service.create(stream_id, retention=600)

        async def consume() -> None:
            async for entry in stream.follow():
                show(converter, entry)

        consumer = asyncio.create_task(consume())
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Session],
            activities=[report_progress],
        ):
            handle = await client.start_workflow(
                Session.run, stream_id, id=workflow_id, task_queue=task_queue
            )
            for n, note in enumerate(("watching", "looks good"), start=1):
                await stream.append(
                    encode(converter, ProgressUpdate(note), "operator"),
                    producer_id="operator",
                    sequence=n,
                )
            await stream.finish_writing("operator")
            await handle.result()

        await stream.truncate(2)
        await stream.close()
        await consumer

        print("  the floor moved to offset 2")
        try:
            await stream.read(from_offset=0)
        except (StreamError, RPCError) as error:
            print(f"    offset 0 refused: {type(error).__name__}: {error}")
        print("    from the earliest record left:")
        async for entry in stream.follow(start=StreamStartPosition(earliest=True)):
            show(converter, entry)
    finally:
        await service.close()
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
