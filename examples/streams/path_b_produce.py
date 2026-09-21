"""Path B: an Activity and a backend produce, and a backend consumes.

    python -m examples.streams.path_b_produce workflow_streams
    python -m examples.streams.path_b_produce native --address 127.0.0.1:7333
    python -m examples.streams.path_b_produce redis --redis redis://127.0.0.1:6379

Outside workflow code a stream is reached through a handle: an Activity asks
its context with ``activity.stream_handle()``, which is its own workflow
pinned to its run, and a backend asks its client with
``client.get_stream_handle(workflow_id)``. Both hand out the same handle,
with the same verbs. A producer writes on its own account, visible as soon
as the store accepts the record, under an identity that lets readers tell a
retry from a new attempt: the Activity's producer takes the Activity's own id
and attempt, a backend names its own.

The model Activity here fails halfway through its first attempt. Its retry
starts over, and the consumer sees that as a ``SUPERSEDED`` record before the
new attempt's first token, so it can drop what the earlier attempt produced.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

from examples.streams import _setup
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.streams import RecordKind
from temporalio.worker import Worker

INPUTS = "inputs"
NOTES = "notes"


@activity.defn
async def generate(count: int) -> None:
    """Stream ``count`` tokens onto this workflow's ``inputs`` topic.

    No workflow id and no run id: the handle is this Activity's own
    workflow, pinned to its run, and the producer's identity is the
    Activity's, so a retry deduplicates and a new attempt is reported.
    """
    model = activity.stream_handle().producer(topic=INPUTS)
    for n in range(count):
        await model.append({"token": n})
        if n == 1 and activity.info().attempt == 1:
            raise RuntimeError("the model connection dropped")
    await model.finish()


@workflow.defn
class Session:
    """Runs the model, then stays open until the backend has said its piece."""

    def __init__(self) -> None:
        """Start open."""
        self._closed = False

    @workflow.signal
    def close(self) -> None:
        """Let the run end; a backend producer needs the run open to append."""
        self._closed = True

    @workflow.run
    async def run(self, count: int) -> None:
        """Generate ``count`` tokens, then wait to be closed."""
        await workflow.execute_activity(
            generate,
            count,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100), maximum_attempts=3
            ),
        )
        await workflow.wait_condition(lambda: self._closed)


async def main() -> None:
    """Run the session; produce from the backend; consume both topics."""
    args = _setup.parser(__doc__ or "").parse_args()
    client, provider = await _setup.connect(args)
    workflow_id = f"path-b-{uuid.uuid4().hex[:8]}"
    task_queue = f"tq-{workflow_id}"
    try:
        async with Worker(
            client, task_queue=task_queue, workflows=[Session], activities=[generate]
        ):
            handle = await client.start_workflow(
                Session.run, 3, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)

            # A backend producer names its own identity.
            notes = stream.producer(topic=NOTES, producer_id="operator", attempt=1)
            await notes.append({"note": "reviewing this session"})
            await notes.finish()

            # A backend consumer keeps the tokens per attempt and drops an
            # attempt the moment a newer one starts writing.
            tokens: dict[int, list[Any]] = {}
            async for record in stream.read(topic=INPUTS, result_type=dict):
                if record.kind is RecordKind.SUPERSEDED:
                    assert record.supersession is not None
                    dropped = tokens.pop(record.supersession.previous_attempt, [])
                    print(
                        f"  attempt {record.supersession.previous_attempt} superseded"
                    )
                    print(f"  dropped {dropped}")
                    continue
                if record.kind is RecordKind.FINISH:
                    print(f"  {record.producer_id} attempt {record.attempt} finished")
                    break
                assert record.value is not None
                tokens.setdefault(record.attempt, []).append(record.value["token"])
            print(f"kept {tokens}")

            async for record in stream.read(topic=NOTES, result_type=dict):
                if record.kind is RecordKind.FINISH:
                    break
                print(f"  note from {record.producer_id}: {record.value}")

            await handle.signal(Session.close)
            await handle.result()
    finally:
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
