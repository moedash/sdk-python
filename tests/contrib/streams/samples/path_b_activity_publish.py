"""Path B: an Activity streams tokens to its Workflow's stream.

The Activity writes as itself (its Activity id with the scheduling run id,
and its Temporal attempt), so a retry is reported to readers as
``SUPERSEDED`` and the reader can drop what the failed attempt wrote.
Records an Activity appends are visible at once.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

TOKENS = topic("tokens", str)


@activity.defn
async def generate(prompt: str) -> str:
    producer = activity_handle().producer(topic=TOKENS)
    words = f"an answer to {prompt}".split()
    if activity.info().attempt == 1:
        # The first attempt fails halfway, as a model call might.
        await producer.append(*words[:2])
        raise ApplicationError("the model call dropped")
    await producer.append(*words)
    await producer.finish()
    return " ".join(words)


@workflow.defn
class Answer:
    @workflow.run
    async def run(self, prompt: str) -> str:
        return await workflow.execute_activity(
            generate,
            prompt,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "answer-1"
) -> str:
    """Run one prompt and return the text the client assembled.

    ``client`` must carry the stream store, for example
    ``Client.connect(..., plugins=[RedisStreams("redis://localhost:6379")])``.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[Answer], activities=[generate]
    ):
        handle = await client.start_workflow(
            Answer.run, "streams", id=workflow_id, task_queue=task_queue
        )
        text: list[str] = []
        async for record in get_stream_handle(client, handle.id).read(topic=TOKENS):
            if record.kind is RecordKind.SUPERSEDED:
                # A retry started over, so what the failed attempt wrote goes.
                # This topic has one producer. With several, keep the text
                # per record.producer_id and clear only that producer's.
                text.clear()
            elif record.kind is RecordKind.DATA and record.value is not None:
                text.append(record.value)
        await handle.result()
        return " ".join(text)
