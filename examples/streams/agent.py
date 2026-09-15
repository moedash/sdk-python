"""The workflow and activities. Identical on every provider.

Nothing here names a store, a transport, or an option. The loop reads its
inbound stream, decides, publishes the decision, and runs an ordinary
activity in the same workflow task, which is the shape the design doc calls
Paths A, B and C together.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, streams, workflow


@activity.defn
async def generate(workflow_id: str, count: int) -> None:
    """Stream model output into the workflow's inbound stream.

    The producer carries this activity's own id and attempt, so a retry
    deduplicates and a new attempt is reported to readers as a supersession.
    """
    producer = await streams.producer(
        activity.client(), workflow_id=workflow_id, stream="inputs"
    )
    for n in range(count):
        await producer.append({"n": n})
    await producer.finish()


@activity.defn
async def record_decision(decision: dict) -> str:
    """An ordinary activity, run from the same task that read and published."""
    return f"recorded {decision['echo']}"


@workflow.defn
class Agent:
    def __init__(self) -> None:
        # Lets the provider install what it needs before the first task
        # completes. A no-op except on the transport that serves outside
        # readers through handlers on this workflow.
        streams.prepare()
        self._done = False

    @workflow.signal
    def release(self) -> None:
        """Lets the run end once a reader has finished following it.

        Only needed because an Option 0 stream dies with its workflow, so a
        demo that ends immediately would leave nothing to read.
        """
        self._done = True

    @workflow.run
    async def run(self, count: int) -> int:
        inputs = streams.reader("inputs", type=dict, idle_timeout=timedelta(seconds=1))
        decisions = streams.writer("decisions")

        await workflow.start_activity(
            generate,
            args=[workflow.info().workflow_id, count],
            start_to_close_timeout=timedelta(minutes=1),
        )

        seen = 0
        async for record in inputs:
            if record.kind is streams.RecordKind.FINISH:
                break
            if record.kind is streams.RecordKind.SUPERSEDED:
                await decisions.publish(
                    {"retracting_attempt": record.value.previous_attempt}
                )
                continue
            seen += 1
            decision = {"echo": record.value["n"]}
            await decisions.publish(decision)
            await workflow.execute_activity(
                record_decision,
                decision,
                start_to_close_timeout=timedelta(minutes=1),
            )

        await decisions.finish()
        await workflow.wait_condition(lambda: self._done)
        # Lets go of anything the provider parked against this run. A no-op
        # except on the transport that parks an outside reader here.
        streams.drain()
        await workflow.wait_condition(workflow.all_handlers_finished)
        return seen
