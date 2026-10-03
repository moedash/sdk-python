r"""Scenario "Workflow as Producer", as return type (async generator).

Status: emulated.

Why: the async-generator run signature is sugar that is not built, and
publishing each value to the default topic, then ``FINISH``, then returning
the result is what that sugar would lower to.

    python -m examples.streams.june_scenarios.s4_workflow_as_generator workflow_streams
    python -m examples.streams.june_scenarios.s4_workflow_as_generator \
        native --address 127.0.0.1:7333

His shape is ``async def run(...) -> workflow.Stream[ScoreUpdate,
GameFinalResult]`` with ``yield`` per update and ``return`` for the result.
Ours replaces each ``yield`` with ``workflow.stream_writer().publish(...)``
on the default topic, so a reader needs no topic name, and the ``return``
stays an ordinary workflow result. The client reads the updates until
``FINISH`` and then fetches the result from the workflow handle, which is
the two halves his generator type would have handed it at once.
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
from temporalio.streams import RecordKind
from temporalio.worker import Worker


@dataclass
class ScoreUpdate:
    """What each ``yield`` would have produced."""

    home_score: int
    away_score: int
    clock: str


@dataclass
class GameFinalResult:
    """What the ``return`` produces."""

    winner: str
    final_score: str


@dataclass
class GameState:
    """One poll of the game feed."""

    home_score: int
    away_score: int
    clock: str
    is_finished: bool


@dataclass
class GamePoll:
    """Which game to poll, and which poll this is."""

    game_id: str
    tick: int


@activity.defn
async def get_game_state(poll: GamePoll) -> GameState:
    """A stand-in feed that reports the game over on its fourth poll."""
    tick = poll.tick
    return GameState(
        home_score=(tick + 1) // 2 + tick // 3,
        away_score=tick // 2,
        clock=f"{tick * 20}'",
        is_finished=tick >= 3,
    )


@workflow.defn
class LiveScoresWorkflow:
    """His generator, with ``yield`` spelled as a publish to the default topic."""

    @workflow.run
    async def run(self, game_id: str) -> GameFinalResult:
        """Publish each score, then finish the topic and return the result."""
        scores = workflow.stream_writer()
        tick = 0
        while True:
            game = await workflow.execute_activity(
                get_game_state,
                GamePoll(game_id, tick),
                start_to_close_timeout=timedelta(seconds=30),
            )
            # yield ScoreUpdate(...)
            scores.publish(ScoreUpdate(game.home_score, game.away_score, game.clock))
            if game.is_finished:
                break
            tick += 1
            await workflow.sleep(timedelta(milliseconds=200))
        # The end of the generator, so a reader knows no more values follow.
        scores.finish()
        home, away = game.home_score, game.away_score
        return GameFinalResult(
            winner="home" if home > away else "away" if away > home else "draw",
            final_score=f"{home}-{away}",
        )


async def run(args: argparse.Namespace) -> None:
    """Read the yielded values, then the returned one."""
    _common.banner("s4 workflow as generator", args.provider)
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s4")
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[LiveScoresWorkflow],
            activities=[get_game_state],
        ):
            handle = await client.start_workflow(
                LiveScoresWorkflow.run, "game-7", id=workflow_id, task_queue=task_queue
            )
            # FINISH is the end of the generator, so the read stops there
            # rather than waiting for the run to close.
            records = client.get_stream_handle(workflow_id).read(
                result_type=ScoreUpdate
            )
            async with contextlib.aclosing(records) as updates:
                async for record in updates:
                    if record.kind is RecordKind.FINISH:
                        print("    FINISH: the generator is exhausted")
                        break
                    assert record.value is not None
                    update = record.value
                    print(
                        f"    Score: {update.home_score}-{update.away_score} "
                        f"at {update.clock}"
                    )
            print(f"    returned {await handle.result()}")
    finally:
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
