"""Scenario "Workflow as Producer", as a named handle.

Status: implemented.

Why: a workflow publishes to a typed topic of its own stream, and a client
handle without a run id follows the execution chain across continue-as-new.

    python -m examples.streams.june_scenarios.s3_workflow_producer workflow_streams
    python -m examples.streams.june_scenarios.s3_workflow_producer native --address 127.0.0.1:7333

His shape is ``workflow.StreamHandle[ProgressUpdate](name="progress")``
and ``.send(...)``. Ours is ``workflow.stream_writer(PROGRESS)`` and
``.publish(...)``, which commits with the Workflow Task. His standalone
variant, a workflow writing to a stream it does not own, is not offered:
a workflow publishes only to its own topics, and an activity writes to a
standalone stream instead, as in ``s2``.

The loop is his, turn by turn: publish, call the model, publish, call a
tool, publish. Continue-as-new is taken when the server suggests it, and
also every two turns here so a short demo crosses runs. Each run's topic
belongs to that run, and the client's read walks from one run to the next
without being told.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import timedelta

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import activity, streams, workflow
from temporalio.worker import Worker


@dataclass
class ProgressUpdate:
    """What the agent is doing now, and which run of the chain said it."""

    message: str
    run: int


@dataclass
class LlmResult:
    """The model's answer: done, or a tool to call."""

    done: bool
    tool: str = ""


@dataclass
class AgentInput:
    """The prompt, and where a continued run picks up."""

    prompt: str
    turn: int = 0
    run: int = 1


PROGRESS = streams.topic("progress", ProgressUpdate)
TURNS_PER_RUN = 2
TOTAL_TURNS = 5


@activity.defn
async def call_llm(input: AgentInput) -> LlmResult:
    """A stand-in model: asks for a tool until the last turn."""
    if input.turn + 1 >= TOTAL_TURNS:
        return LlmResult(done=True)
    return LlmResult(done=False, tool=f"search #{input.turn + 1}")


@activity.defn
async def call_tool(result: LlmResult) -> str:
    """A stand-in tool: its answer becomes the next prompt."""
    return f"results of {result.tool}"


@workflow.defn
class AgentWorkflow:
    """His turn loop, publishing progress on the ``progress`` topic."""

    @workflow.run
    async def run(self, input: AgentInput) -> str:
        """Run turns until the model is done, continuing as new along the way."""
        progress = workflow.stream_writer(PROGRESS)
        prompt, turn, run = input.prompt, input.turn, input.run
        timeout = timedelta(seconds=30)

        def send(message: str) -> None:
            progress.publish(ProgressUpdate(message, run))

        while True:
            if (
                workflow.info().is_continue_as_new_suggested()
                or turn - input.turn >= TURNS_PER_RUN
            ):
                workflow.continue_as_new(AgentInput(prompt, turn, run + 1))
            send(f"turn {turn} started")
            result = await workflow.execute_activity(
                call_llm, AgentInput(prompt, turn), start_to_close_timeout=timeout
            )
            send(f"turn {turn} ended")
            if result.done:
                break
            send(f"tool call {result.tool} started")
            prompt = await workflow.execute_activity(
                call_tool, result, start_to_close_timeout=timeout
            )
            send(f"tool call {result.tool} ended")
            turn += 1
        progress.finish()
        return f"done after {turn + 1} turns"


async def run(args: argparse.Namespace) -> None:
    """Start the agent and follow its progress across every run of the chain."""
    _common.banner("s3 workflow producer", args.provider)
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s3")
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[AgentWorkflow],
            activities=[call_llm, call_tool],
        ):
            handle = await client.start_workflow(
                AgentWorkflow.run,
                AgentInput("plan a trip"),
                id=workflow_id,
                task_queue=task_queue,
            )
            # No run id on the handle, so the read follows the chain and ends
            # when its last run is closed and the tail has been delivered.
            async for record in client.get_stream_handle(workflow_id).read(
                topic=PROGRESS
            ):
                if record.value is None:
                    print(f"    {record.kind.name}")
                    continue
                print(f"    run {record.value.run}  {record.value.message}")
            print(f"  {await handle.result()}")
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
