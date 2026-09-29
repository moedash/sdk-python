"""Scenario "Workflow as Consumer".

Status: implemented for the workflow's own inbound topic; consuming a foreign
standalone stream from workflow code is unsupported by design.

Why: a workflow reads its own topics as recorded observations that replay
re-supplies, and rule 5 keeps reading somebody else's stream out of this
release.

    python -m examples.streams.june_scenarios.s7_workflow_consumer workflow_streams
    python -m examples.streams.june_scenarios.s7_workflow_consumer native --address 127.0.0.1:7333

His shape is ``workflow.StreamHandle[ProgressUpdate](stream_id="...",
offset=offset)`` with ``continue_as_new(update.offset)``. Ours is
``workflow.stream_reader(COMMANDS)`` on the workflow's own ``commands``
topic, fed from outside by a producer.

Two deliberate differences. First, the stream: his reads a standalone
stream by id, and ours reads only a topic the workflow owns. Reading a
foreign stream from workflow code is not exposed; the server half exists
(external subscriptions), and the memo's foreign-read sketch covers the SDK
half. Second, what crosses continue-as-new: his offset works because his
stream outlives the run. A workflow's topic belongs to one run on the native
and ``workflow_streams`` providers, a successor's starts empty, and a cursor
from the previous run is refused there. So the run hands over at a batch
boundary, marked by the sender's ``FINISH``, and what it carries is its own
checkpoint: what it has applied so far. The sender waits for the successor
before writing the next batch, which is the one piece of coordination the
per-run topic asks for.

The memory provider keys a topic by workflow rather than by run, so a
successor would read its predecessor's batches again; it is skipped there.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.streams import RecordKind
from temporalio.worker import Worker


@dataclass
class Command:
    """One instruction from outside."""

    op: str


@dataclass
class Checkpoint:
    """What a run carries into its successor instead of a cursor."""

    run: int = 1
    applied: list[str] = field(default_factory=list)


COMMANDS = streams.topic("commands", Command)
STOP = "stop"


@workflow.defn
class ConsumerWorkflow:
    """Reads its commands topic batch by batch, one batch per run."""

    @workflow.run
    async def run(self, checkpoint: Checkpoint) -> list[str]:
        """Apply this run's batch, then continue as new or stop."""
        stop = False
        async for record in workflow.stream_reader(COMMANDS):
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            assert record.value is not None
            workflow.logger.info("got command %s", record.value.op)
            if record.value.op == STOP:
                stop = True
                continue
            checkpoint.applied.append(f"run {checkpoint.run}: {record.value.op}")
        if stop:
            return checkpoint.applied
        # Taken at every batch boundary here, so the demo crosses runs; a
        # real consumer would also wait for is_continue_as_new_suggested().
        workflow.continue_as_new(
            Checkpoint(run=checkpoint.run + 1, applied=checkpoint.applied)
        )


async def current_run(client: Client, workflow_id: str, previous: str | None) -> str:
    """Wait until the chain's newest run is a new one and still open."""
    while True:
        description = await client.get_workflow_handle(workflow_id).describe()
        assert description.run_id is not None
        if description.run_id != previous and description.close_time is None:
            return description.run_id
        await asyncio.sleep(0.2)


async def run(args: argparse.Namespace) -> None:
    """Send three batches, one per run, and print what the chain applied."""
    _common.banner("s7 workflow consumer", args.provider)
    if args.provider == "memory":
        print(
            "  the memory provider keeps one topic across the chain, not one per "
            "run; skipped"
        )
        return
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s7")
    batches = [["open", "resize"], ["rotate"], ["close", STOP]]
    try:
        async with Worker(client, task_queue=task_queue, workflows=[ConsumerWorkflow]):
            handle = await client.start_workflow(
                ConsumerWorkflow.run,
                Checkpoint(),
                id=workflow_id,
                task_queue=task_queue,
            )
            run_id: str | None = None
            for number, batch in enumerate(batches, start=1):
                run_id = await current_run(client, workflow_id, run_id)
                # A producer made now writes to the run that is current now.
                sender = client.get_stream_handle(workflow_id).producer(
                    topic=COMMANDS, producer_id="console", attempt=1
                )
                for op in batch:
                    await sender.append(Command(op))
                await sender.finish()
                print(f"    batch {number} sent to run ...{run_id[-6:]}: {batch}")
            for line in await handle.result():
                print(f"    applied {line}")
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
