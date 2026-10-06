"""Scenario "Activity as Producer", as named handle, separate from return type.

Status: implemented.

Why: ``activity.stream_handle()`` reaches the workflow's topics, the
activity's own streams with ``scope="activity"``, or a standalone
activity's own streams, by a static rule and with the same producer verbs.

    python -m examples.streams.june_scenarios.s5_activity_producers memory
    python -m examples.streams.june_scenarios.s5_activity_producers workflow_streams

His three shapes and ours, one line each:

- ``activity.StreamHandle[ProgressUpdate](workflow_id=..., name="progress")``
  is ``activity.stream_handle().producer(topic=PROGRESS)``: the workflow
  that scheduled the activity, pinned to its run, which is Path B.
- ``activity.StreamHandle[ProgressUpdate](name="progress")`` in an activity
  that owns its streams is ``activity.stream_handle(scope="activity")``
  inside a workflow, and plain ``activity.stream_handle()`` in a standalone
  activity. The rule is static because a stream is created by its first
  write, so a rule that probed for one would send a retry somewhere else.
- ``client.StreamHandle[...](stream_id=...)``, a standalone stream, is the
  stream client in ``s2``.

A topic on an activity's own streams and the same topic on its workflow are
two streams, which part (b) shows by writing ``progress`` to both. Parts (b)
and (c) need a store that can hold an activity-owned stream: memory and
redis can, and on any other provider the client's first handle raises
``StreamUnsupportedError``, which is printed in their place.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import activity, streams, workflow
from temporalio.client import Client
from temporalio.streams import RecordKind, StreamHandle, StreamUnsupportedError
from temporalio.worker import Worker


@dataclass
class ProgressUpdate:
    """One line of progress from a tool call."""

    message: str


@dataclass
class ToolCallInput:
    """The call, and which stream its progress goes to.

    ``target`` is ``"workflow"`` for the scheduling workflow's topic,
    ``"own"`` for the activity's own streams inside a workflow, and
    ``"standalone"`` for a standalone activity's own default topic.
    """

    tool: str
    target: str


PROGRESS = streams.topic("progress", ProgressUpdate)


@activity.defn
async def tool_call_activity(input: ToolCallInput) -> str:
    """His tool call: progress before and after, and the answer as the result."""
    if input.target == "workflow":
        progress = activity.stream_handle().producer(topic=PROGRESS)
    elif input.target == "own":
        progress = activity.stream_handle(scope="activity").producer(topic=PROGRESS)
    else:
        # A standalone activity owns its streams without a scope, and naming
        # no topic writes its default one.
        progress = activity.stream_handle().producer()
    await progress.append(ProgressUpdate(f"{input.tool} started"))
    await asyncio.sleep(0.3)
    await progress.append(ProgressUpdate(f"{input.tool} ended"))
    await progress.finish()
    return f"{input.tool} answered"


@workflow.defn
class ToolWorkflow:
    """Schedules the tool call once per stream target it is asked for."""

    @workflow.run
    async def run(self, own_streams: bool) -> list[str]:
        """Run (a), and (b) when the store can hold an activity's own streams."""
        timeout = timedelta(seconds=30)
        answers = [
            await workflow.execute_activity(
                tool_call_activity,
                ToolCallInput("search", "workflow"),
                activity_id="tool-a",
                start_to_close_timeout=timeout,
            )
        ]
        if own_streams:
            answers.append(
                await workflow.execute_activity(
                    tool_call_activity,
                    ToolCallInput("fetch", "own"),
                    activity_id="tool-b",
                    start_to_close_timeout=timeout,
                )
            )
        return answers


async def show(stream: StreamHandle, topic: streams.StreamTopic | None) -> None:
    """Print one stream until its producer finishes."""
    records = (
        stream.read(topic=topic)
        if topic is not None
        else stream.read(result_type=ProgressUpdate)
    )
    async with contextlib.aclosing(records) as reading:
        async for record in reading:
            value = record.value.message if record.value else ""
            print(f"    {record.kind.name:6} {record.producer_id:6} {value}")
            if record.kind is RecordKind.FINISH:
                break


def supports_activity_owners(client: Client) -> bool:
    """Ask the provider for an activity handle; one that cannot hold it says so."""
    try:
        client.get_stream_handle(activity_id="probe")
    except StreamUnsupportedError as error:
        print(f"    StreamUnsupportedError: {error}")
        return False
    return True


async def run(args: argparse.Namespace) -> None:
    """Produce from an activity three ways and read each stream from outside."""
    _common.banner("s5 activity producers", args.provider)
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s5")
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ToolWorkflow],
            activities=[tool_call_activity],
        ):
            print("  can this provider hold a stream an activity owns?")
            own = supports_activity_owners(client)
            handle = await client.start_workflow(
                ToolWorkflow.run, own, id=workflow_id, task_queue=task_queue
            )
            print(f"  workflow returned {await handle.result()}")

            print("  (a) activity in a workflow, onto the workflow's progress topic")
            await show(client.get_stream_handle(workflow_id), PROGRESS)
            if not own:
                print("  (b) and (c) skipped: they need an activity-owned stream")
                return

            print("  (b) same activity with scope='activity', onto its own progress")
            await show(
                client.get_stream_handle(workflow_id, activity_id="tool-b"), PROGRESS
            )

            print("  (c) standalone activity, onto its own default topic")
            activity_id = workflow_id.replace("june-s5", "saa")
            started = await client.start_activity(
                tool_call_activity,
                ToolCallInput("summarize", "standalone"),
                id=activity_id,
                task_queue=task_queue,
                start_to_close_timeout=timedelta(seconds=30),
            )
            await show(client.get_stream_handle(activity_id=activity_id), None)
            print(f"    result {await started.result()}")
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
