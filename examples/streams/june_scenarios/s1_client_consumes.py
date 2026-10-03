"""Scenario "Client starts and consume stream": primary and named execution stream.

Status: implemented.

Why: a workflow's default topic is the primary stream he reads without a
name, and a typed topic is his named stream, so both sketches are one call.

    python -m examples.streams.june_scenarios.s1_client_consumes workflow_streams
    python -m examples.streams.june_scenarios.s1_client_consumes native --address 127.0.0.1:7333
    python -m examples.streams.june_scenarios.s1_client_consumes memory --address 127.0.0.1:7333

His shape is ``handle.stream(ScoreUpdate)`` and
``handle.stream(ScoreUpdate, name="scores")``. Ours keeps the address on the
handle and the name and type on the topic:
``client.get_stream_handle(wid).read(result_type=ScoreUpdate)`` for the
default topic and ``.read(topic=SCORES)`` for the named one.

The game plays two halves and waits for a signal between them, so the reads
below land at known places. The client reads the first half from the start
of the default topic, then joins the named topic at ``END`` and sees only
the second half, then asks for the ``last=3`` records, which count the
``FINISH`` record too. Last, it reads a truncated topic from ``BEGINNING``,
which starts at the oldest record left rather than at offset zero. Only the
memory provider can truncate from here; an owned topic on the native server
is budgeted and never truncated, and ``s2`` shows a moved floor on a
standalone stream there.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import streams, workflow
from temporalio.streams import END, RecordKind, StreamRecord
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.worker import Worker


@dataclass
class ScoreUpdate:
    """The score at one moment of the game."""

    home_score: int
    away_score: int
    clock: str


SCORES = streams.topic("scores", ScoreUpdate)


@workflow.defn
class Game:
    """Publishes every score change on the default topic and on ``scores``."""

    def __init__(self) -> None:
        """Start in the first half."""
        self._second_half = False

    @workflow.signal
    def second_half(self) -> None:
        """Let the second half start."""
        self._second_half = True

    @workflow.run
    async def run(self, per_half: int) -> str:
        """Play two halves of ``per_half`` updates each and return the final score."""
        primary = workflow.stream_writer()
        named = workflow.stream_writer(SCORES)
        home = away = 0
        for half in (1, 2):
            if half == 2:
                await workflow.wait_condition(lambda: self._second_half)
            for minute in range(per_half):
                if minute % 2 == 0:
                    home += 1
                else:
                    away += 1
                update = ScoreUpdate(home, away, clock=f"H{half} {minute:02}'")
                primary.publish(update)
                named.publish(update)
                await workflow.sleep(timedelta(milliseconds=200))
        primary.finish()
        named.finish()
        return f"{home}-{away}"


def show(record: StreamRecord[ScoreUpdate]) -> None:
    """One line per record."""
    print(f"    {record.kind.name:6} {record.value}")


async def run(args: argparse.Namespace) -> None:
    """Play the game and read it four ways."""
    _common.banner("s1 client consumes", args.provider)
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s1")
    per_half = 3
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Game]):
            handle = await client.start_workflow(
                Game.run, per_half, id=workflow_id, task_queue=task_queue
            )
            stream = client.get_stream_handle(workflow_id)

            print("  (a) default topic, no name, from the start: the first half")
            seen = 0
            async with contextlib.aclosing(
                stream.read(result_type=ScoreUpdate)
            ) as primary:
                async for record in primary:
                    show(record)
                    seen += record.kind is RecordKind.DATA
                    if seen == per_half:
                        break

            print("  (b) named topic from END: only what lands after joining")

            async def follow_from_end() -> None:
                joined = stream.read(topic=SCORES, after=END)
                async with contextlib.aclosing(joined) as records:
                    async for record in records:
                        show(record)
                        if record.kind is RecordKind.FINISH:
                            break

            joined = asyncio.create_task(follow_from_end())
            # END resolves on the reader's first poll, not at this call, so the
            # half-time whistle waits until that poll has had time to land.
            await asyncio.sleep(1.0)
            await handle.signal(Game.second_half)
            await joined
            print(f"  final score {await handle.result()}")

            print("  (c) named topic, last=3: FINISH counts as one of the three")
            async for record in stream.read(topic=SCORES, last=3):
                show(record)

            print("  (d) BEGINNING after the floor moved")
            if isinstance(provider, MemoryStreams):
                # Truncation is a store's retention, not part of the provider
                # contract, so only the in-process store offers it by hand.
                provider.truncate(workflow_id, SCORES.name, keep=2)
                async for record in stream.read(topic=SCORES):
                    show(record)
            else:
                print(
                    f"    {args.provider} cannot truncate a workflow's topic from "
                    "outside; see s2 for BEGINNING on a moved floor"
                )
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
