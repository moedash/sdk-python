"""Path A: a Workflow publishes its progress, and a client reads it.

The Workflow's publish becomes visible when its Workflow Task is accepted,
and never if the task fails. The client follows the stream until the
Workflow closes.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker

PROGRESS = topic("progress", dict)


@workflow.defn
class ProcessOrder:
    @workflow.run
    async def run(self, order_id: str) -> str:
        progress = workflow_writer(PROGRESS)
        for step in ("reserved", "charged", "shipped"):
            progress.publish({"order": order_id, "step": step})
            await workflow.sleep(timedelta(milliseconds=100))
        progress.finish()
        return "done"


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "order-1"
) -> list[str]:
    """Run one order and return the steps the client saw.

    ``client`` must carry the stream provider, for example
    ``Client.connect(..., plugins=[RedisStreams("redis://localhost:6379")])``.
    """
    async with Worker(client, task_queue=task_queue, workflows=[ProcessOrder]):
        handle = await client.start_workflow(
            ProcessOrder.run, "order-1", id=workflow_id, task_queue=task_queue
        )
        steps = []
        async for record in get_stream_handle(client, handle.id).read(topic=PROGRESS):
            if record.kind is RecordKind.DATA and record.value is not None:
                steps.append(record.value["step"])
        await handle.result()
        return steps
