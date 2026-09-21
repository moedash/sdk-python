"""The workflow and activities. Identical on every provider.

Nothing here names a store, a transport, or an option. The loop reads its
``inputs`` topic, decides, publishes the decision, and runs an ordinary
activity in the same workflow task, which is the shape the design doc calls
Paths A, B and C together. The activity that streams model output takes the
provider from whoever built the worker, because an activity has no runtime
to ask for it the way workflow code does.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.streams import RecordKind, StreamProvider

INPUTS = "inputs"
DECISIONS = "decisions"


class Generator:
    """The model activity, bound to the provider the process constructed."""

    def __init__(self, provider: StreamProvider) -> None:
        """Publish through ``provider``, the same instance the worker runs on."""
        self._provider = provider

    @activity.defn
    async def generate(self, workflow_id: str, count: int) -> None:
        """Stream model output onto the workflow's ``inputs`` topic.

        The producer carries this activity's own id and attempt, so a retry
        deduplicates and a new attempt is reported to readers as a
        supersession.
        """
        stream = self._provider.get_stream_handle(activity.client(), workflow_id)
        model = stream.producer(topic=INPUTS)
        for n in range(count):
            await model.append({"n": n})
        await model.finish()


@activity.defn
async def record_decision(decision: dict) -> str:
    """An ordinary activity, run from the same task that read and published."""
    return f"recorded {decision['echo']}"


@workflow.defn
class Agent:
    """Reads ``inputs``, publishes a decision each time, ends on FINISH."""

    @workflow.run
    async def run(self, count: int) -> int:
        """Decide on at most ``count`` inputs, then return how many landed."""
        inputs = workflow.stream_reader(INPUTS, result_type=dict)
        decisions = workflow.stream_writer(DECISIONS)

        generating = workflow.start_activity(
            Generator.generate,
            args=[workflow.info().workflow_id, count],
            start_to_close_timeout=timedelta(minutes=1),
            # Bounded, so a generator that cannot finish gives up instead of
            # retrying forever while every attempt streams from the start.
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        seen = 0
        async for record in inputs:
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is RecordKind.SUPERSEDED:
                assert record.supersession is not None
                decisions.publish(
                    {"retracting_attempt": record.supersession.previous_attempt}
                )
                continue
            assert record.value is not None
            seen += 1
            decision = {"echo": record.value["n"]}
            decisions.publish(decision)
            await workflow.execute_activity(
                record_decision,
                decision,
                start_to_close_timeout=timedelta(minutes=1),
            )
            if seen >= count:
                break
        # A generator that exhausted its attempts fails the run with its cause
        # here, rather than being forgotten once the loop has what it wanted.
        await generating

        decisions.finish()
        return seen
