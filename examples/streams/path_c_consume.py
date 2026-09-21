"""Path C: the workflow consumes a topic fed from outside, with a cold cache.

    python -m examples.streams.path_c_consume workflow_streams
    python -m examples.streams.path_c_consume native --address 127.0.0.1:7333
    python -m examples.streams.path_c_consume redis --redis redis://127.0.0.1:6379

The workflow reads its ``commands`` topic with ``workflow.stream_reader`` and
runs an Activity for each record; the topic is defined once, so the reader's
records and the Activity's argument share one type. A read is an observation
the SDK records: what the reader handed to workflow code commits with the
Workflow Task, so replay re-supplies the same records in the same order and
each Activity result is matched to the command that caused it. The worker
runs with the workflow cache off, so every Workflow Task rebuilds the
workflow from History and the loop completing at all is the proof.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta

from examples.streams import _setup
from temporalio import activity, streams, workflow
from temporalio.streams import RecordKind
from temporalio.worker import Worker


@dataclass
class Command:
    """One instruction from the console."""

    op: str


COMMANDS = streams.topic("commands", Command)


@activity.defn
async def apply(command: Command) -> str:
    """Carry out one command."""
    return f"applied {command.op}"


@workflow.defn
class Controller:
    """Acts on each command as it arrives, until the sender finishes."""

    @workflow.run
    async def run(self) -> list[str]:
        """Return what was applied, in the order the commands arrived."""
        commands = workflow.stream_reader(COMMANDS)
        applied: list[str] = []
        async for record in commands:
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is not RecordKind.DATA:
                continue
            assert record.value is not None
            applied.append(
                await workflow.execute_activity(
                    apply, record.value, start_to_close_timeout=timedelta(minutes=1)
                )
            )
        return applied


async def main() -> None:
    """Feed commands from the backend, one at a time, and print what was applied."""
    args = _setup.parser(__doc__ or "").parse_args()
    client, provider = await _setup.connect(args)
    workflow_id = f"path-c-{uuid.uuid4().hex[:8]}"
    task_queue = f"tq-{workflow_id}"
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Controller],
            activities=[apply],
            # Off, so every task replays the recorded reads from History.
            max_cached_workflows=0,
        ):
            handle = await client.start_workflow(
                Controller.run, id=workflow_id, task_queue=task_queue
            )
            console = client.get_stream_handle(workflow_id).producer(
                topic=COMMANDS, producer_id="console", attempt=1
            )
            for op in ("open", "resize", "close"):
                await console.append(Command(op))
                # Spaced out, so the commands arrive across several tasks.
                await asyncio.sleep(0.3)
            await console.finish()
            print(f"workflow {workflow_id} applied {await handle.result()}")
    finally:
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
