r"""Scenario "Activity as Producer", as return type (async generator).

Status: emulated.

Why: the yield-based activity signature is not built; each ``yield`` becomes
an append on the workflow's default topic, and his "load last heartbeat and
resume from snapshot" is a heartbeat checkpoint the retry reads back.

    python -m examples.streams.june_scenarios.s6_activity_as_generator workflow_streams

His shape is ``yield ProgressUpdate(...)`` inside the activity. Ours is
``await progress.append(ProgressUpdate(...))`` on
``activity.stream_handle().producer()``, followed by
``activity.heartbeat(next_step)`` once the step is safely written.

The first attempt dies after writing step 3 and before checkpointing it. The
retry reads the checkpoint, so it starts again at step 3 rather than at the
top, and writes under a new attempt. The reader sees that change as a
``SUPERSEDED`` record and keys what it keeps by step, so step 3 arriving
twice, once per attempt, leaves one copy. The heartbeat is a hint, not the
truth: it can lag the stream, which is why the retry repeats a step rather
than skipping one.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.streams import RecordKind
from temporalio.worker import Worker


@dataclass
class ProgressUpdate:
    """One step of the agent loop."""

    step: int
    message: str


STEPS = (
    "Turn started",
    "Turn ended",
    "Tool call started",
    "Tool call ended",
    "Turn started",
    "Turn ended",
)
CRASH_AFTER_STEP = 3


@activity.defn
async def agent_activity(prompt: str) -> str:
    """His generator: each ``yield`` is an append, each safe point a heartbeat."""
    info = activity.info()
    # load last heartbeat and resume from snapshot
    start = int(info.heartbeat_details[0]) if info.heartbeat_details else 0
    progress = activity.stream_handle().producer()
    for step in range(start, len(STEPS)):
        await progress.append(ProgressUpdate(step, STEPS[step]))
        if step == CRASH_AFTER_STEP and info.attempt == 1:
            raise RuntimeError("the worker died mid-stream")
        # The checkpoint is the next step to write. A throttled heartbeat
        # still reaches the server, because the worker sends the last one
        # with the failure.
        activity.heartbeat(step + 1)
        await asyncio.sleep(0.1)
    await progress.finish()
    return f"answered {prompt!r} in attempt {info.attempt} from step {start}"


@workflow.defn
class AgentSession:
    """Runs the generator activity and returns what it answered."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        """Run the activity with a short retry, so the crash retries at once."""
        return await workflow.execute_activity(
            agent_activity,
            prompt,
            start_to_close_timeout=timedelta(minutes=1),
            heartbeat_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=200), maximum_attempts=3
            ),
        )


async def run(args: argparse.Namespace) -> None:
    """Read the generator's values across the crash and the resumed retry."""
    _common.banner("s6 activity as generator", args.provider)
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s6")
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[AgentSession],
            activities=[agent_activity],
        ):
            handle = await client.start_workflow(
                AgentSession.run, "plan a trip", id=workflow_id, task_queue=task_queue
            )
            kept: dict[int, str] = {}
            # A failed attempt writes no FINISH, so the first one seen is the
            # attempt that completed, and the read can stop there.
            records = client.get_stream_handle(workflow_id).read(
                result_type=ProgressUpdate
            )
            async with contextlib.aclosing(records) as reading:
                async for record in reading:
                    if record.kind is RecordKind.SUPERSEDED:
                        assert record.supersession is not None
                        change = record.supersession
                        print(
                            f"    SUPERSEDED attempt {change.previous_attempt} "
                            f"by attempt {change.attempt}"
                        )
                    elif record.kind is RecordKind.FINISH:
                        print(f"    FINISH     attempt {record.attempt}")
                        break
                    else:
                        assert record.value is not None
                        update = record.value
                        kept[update.step] = update.message
                        print(
                            f"    DATA       attempt {record.attempt} "
                            f"step {update.step} {update.message}"
                        )
            print(f"  kept one copy of steps {sorted(kept)}")
            print(f"  {await handle.result()}")
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
