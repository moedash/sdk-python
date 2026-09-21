"""Path A: the workflow publishes, and a backend follows from now.

    python -m examples.streams.path_a_publish workflow_streams
    python -m examples.streams.path_a_publish native --address 127.0.0.1:7333
    python -m examples.streams.path_a_publish redis --redis redis://127.0.0.1:6379

The workflow reports its progress on a topic with ``workflow.stream_writer``.
``publish`` is a plain call: the record is buffered and commits with the
Workflow Task, so a reader never sees a step the workflow did not commit. The
backend positions itself with ``latest()`` and reads what comes after, the
way a UI attaches to a job that is already running; the read ends by itself
once the workflow is closed and the tail has been delivered.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from examples.streams import _setup
from temporalio import workflow
from temporalio.worker import Worker

PROGRESS = "progress"


@workflow.defn
class Job:
    """Works through ``steps`` and publishes each one on ``progress``."""

    @workflow.run
    async def run(self, steps: int) -> int:
        """Publish one record per step, then finish the topic."""
        progress = workflow.stream_writer(PROGRESS)
        for step in range(steps):
            progress.publish({"step": step, "of": steps})
            # A timer between steps, so each record commits with its own
            # Workflow Task and a follower sees them arrive one at a time.
            await workflow.sleep(timedelta(milliseconds=200))
        progress.finish()
        return steps


async def main() -> None:
    """Run the job and follow its progress from outside."""
    args = _setup.parser(__doc__ or "").parse_args()
    client, provider = await _setup.connect(args)
    workflow_id = f"path-a-{uuid.uuid4().hex[:8]}"
    task_queue = f"tq-{workflow_id}"
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Job]):
            handle = await client.start_workflow(
                Job.run, 5, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)
            # Follow from now: whatever landed before this point is not
            # re-read, which is how a client attaches to a running job.
            since = await stream.latest(topic=PROGRESS)
            async for record in stream.read(
                topic=PROGRESS, after=since, result_type=dict
            ):
                print(f"  {record.kind.name:7} {record.value} at {record.cursor.token}")
            print(f"workflow {workflow_id} took {await handle.result()} steps")
    finally:
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
