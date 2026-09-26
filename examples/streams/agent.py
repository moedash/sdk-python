"""The workflow and activities. Identical on every provider.

Nothing here names a store, a transport, or an option. The two topics are
defined once, with the types their records carry, and the workflow, the
Activity and the backend in ``run.py`` all refer to them. The loop reads its
``inputs`` topic, decides, publishes the decision, and runs an ordinary
activity in the same workflow task, which is the shape the design doc calls
Paths A, B and C together. The Activity that streams model output asks its
context for its own workflow's stream, the way workflow code asks its
runtime, so the file is the same whichever provider the process registered.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, streams, workflow
from temporalio.common import RetryPolicy
from temporalio.streams import RecordKind


@dataclass
class Token:
    """One piece of model output."""

    n: int


@dataclass
class Decision:
    """What the workflow decided about a token, or which attempt it retracted."""

    echo: int | None = None
    retracting_attempt: int | None = None


INPUTS = streams.topic("inputs", Token)
DECISIONS = streams.topic("decisions", Decision)


@activity.defn
async def generate(count: int) -> None:
    """Stream model output onto this workflow's ``inputs`` topic.

    No workflow id and no run id: the handle is this Activity's own
    workflow, pinned to its run. The producer carries the Activity's own id
    and attempt, so a retry deduplicates and a new attempt is reported to
    readers as a supersession.
    """
    model = activity.stream_handle().producer(topic=INPUTS)
    for n in range(count):
        await model.append(Token(n))
    await model.finish()


@activity.defn
async def record_decision(decision: Decision) -> str:
    """An ordinary activity, run from the same task that read and published."""
    return f"recorded {decision.echo}"


@workflow.defn
class Agent:
    """Reads ``inputs``, publishes a decision each time, ends on FINISH."""

    @workflow.run
    async def run(self, count: int) -> int:
        """Decide on at most ``count`` inputs, then return how many landed."""
        decisions = workflow.stream_writer(DECISIONS)

        generating = workflow.start_activity(
            generate,
            count,
            start_to_close_timeout=timedelta(minutes=1),
            # Bounded, so a generator that cannot finish gives up instead of
            # retrying forever while every attempt streams from the start.
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        consuming = asyncio.create_task(self._consume(count, decisions))

        # Raced rather than awaited in turn: an attempt that fails writes no
        # FINISH, so a generator that exhausts its attempts leaves the reader
        # waiting forever. Its failure ends the run with its cause instead.
        done, _ = await workflow.wait(
            [consuming, generating], return_when=asyncio.FIRST_COMPLETED
        )
        if generating in done and consuming not in done:
            try:
                await generating
            except BaseException:
                consuming.cancel()
                raise
        seen = await consuming
        await generating

        decisions.finish()
        return seen

    async def _consume(
        self, count: int, decisions: workflow.StreamWriter[Decision]
    ) -> int:
        seen = 0
        async for record in workflow.stream_reader(INPUTS):
            if record.kind is RecordKind.FINISH:
                break
            if record.kind is RecordKind.SUPERSEDED:
                assert record.supersession is not None
                decisions.publish(
                    Decision(retracting_attempt=record.supersession.previous_attempt)
                )
                continue
            assert record.value is not None
            seen += 1
            decision = Decision(echo=record.value.n)
            decisions.publish(decision)
            await workflow.execute_activity(
                record_decision,
                decision,
                start_to_close_timeout=timedelta(minutes=1),
            )
            if seen >= count:
                break
        return seen
