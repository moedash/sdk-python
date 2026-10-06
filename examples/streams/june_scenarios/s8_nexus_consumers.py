r"""Scenarios over Nexus: client consumer, operation handler, workflow consumer.

Status: "Client as Consumer over Standalone Nexus" and "Nexus operation
handler" returning a stream are implemented; "Workflow as Consumer over
Nexus" is unsupported by design.

Why: the ``NexusStreams`` front serves outside reads through one endpoint,
so a client consumes over Nexus today, while a workflow's reads ride its
Workflow Task and never cross Nexus.

    python -m examples.streams.june_scenarios.s8_nexus_consumers workflow_streams

The run creates its own Nexus endpoint through the operator service, routed
to its worker's task queue, and deletes it at the end. ``--http`` is the
server's Nexus HTTP ingress, which the front posts to.

(a) His consumer activity starts a stream operation, or on a retry gets the
operation handle by id, and reads from the offset in its heartbeat. Ours
reads through the front with ``front.get_stream_handle(client,
workflow_id).read(topic=SCORES, after=cursor)`` and heartbeats each record's
cursor. No operation handle is needed to resume: every read is a short sync
operation, so the cursor alone is the resume point. The first attempt dies
after two records and the retry picks up after the second.

(b) His handler returns ``Stream[ProgressUpdate]``. Ours returns a
``temporalio.streams.StreamRef``, the SDK's name for one stream, taken from
the producing workflow's handle with ``ref(topic=SCORES)``; the calling
workflow passes it on as its result and the client opens it with
``get_stream_handle(ref)`` on a client whose provider is the front, so the
read goes over Nexus. The ref is plain data to the operation's IDL; a stream
type of its own there is the nexgen follow-on.

Not shown, by design: a workflow consuming over Nexus. The workflow half of
a provider records its reads on the Workflow Task, which cannot cross an
RPC, so the front has no workflow half. A workflow reads its own topics, as
in ``s7``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta

import nexusrpc
import nexusrpc.handler

from examples.streams import _setup
from examples.streams.june_scenarios import _common
from temporalio import activity, nexus, streams, workflow
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import (
    CreateNexusEndpointRequest,
    DeleteNexusEndpointRequest,
)
from temporalio.client import Client
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy
from temporalio.streams import BEGINNING, Cursor, RecordKind, StreamRef
from temporalio.streams.providers.nexus import NexusStreams, TemporalStreamsHandler
from temporalio.worker import Worker


@dataclass
class ScoreUpdate:
    """One score change."""

    home_score: int
    away_score: int


@dataclass
class Consumed:
    """What the consumer activity's last attempt read, and where it resumed."""

    attempt: int
    resumed_after: str
    scores: list[str]


@dataclass
class GameRequest:
    """Which game the operation should start."""

    game_id: str


@dataclass
class CallerInput:
    """The endpoint to call and the game to ask for."""

    endpoint: str
    game_id: str


SCORES = streams.topic("scores", ScoreUpdate)


@workflow.defn
class ScoresProducer:
    """Publishes a short game on ``scores``."""

    @workflow.run
    async def run(self, updates: int) -> None:
        """Publish ``updates`` scores, spaced out, then finish the topic."""
        scores = workflow.stream_writer(SCORES)
        for n in range(updates):
            scores.publish(ScoreUpdate(home_score=(n + 1) // 2, away_score=n // 2))
            await workflow.sleep(timedelta(milliseconds=300))
        scores.finish()


class Consumer:
    """The consumer activity, holding the front it reads through."""

    def __init__(self, front: NexusStreams) -> None:
        """Read through ``front``."""
        self._front = front

    @activity.defn
    async def consume_scores(self, workflow_id: str) -> Consumed:
        """His standalone-Nexus consumer: resume from the cursor in the heartbeat."""
        info = activity.info()
        token = str(info.heartbeat_details[0]) if info.heartbeat_details else ""
        after = Cursor(token) if token else BEGINNING
        stream = self._front.get_stream_handle(activity.client(), workflow_id)
        scores: list[str] = []
        async with contextlib.aclosing(
            stream.read(topic=SCORES, after=after)
        ) as reading:
            async for record in reading:
                if record.kind is RecordKind.FINISH:
                    break
                assert record.value is not None
                scores.append(f"{record.value.home_score}-{record.value.away_score}")
                activity.heartbeat(record.cursor.token)
                if info.attempt == 1 and len(scores) == 2:
                    raise RuntimeError("the consumer died after two records")
        return Consumed(info.attempt, token or "BEGINNING", scores)


@nexusrpc.service
class ScoresService:
    """A service whose operation hands back a stream reference."""

    start_game: nexusrpc.Operation[GameRequest, StreamRef]


@nexusrpc.handler.service_handler(service=ScoresService)
class ScoresHandler:
    """Starts the producing workflow and returns where to read it."""

    def __init__(self, task_queue: str) -> None:
        """Start producers on ``task_queue``."""
        self._task_queue = task_queue

    @nexusrpc.handler.sync_operation
    async def start_game(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: GameRequest
    ) -> StreamRef:
        """His "pre-create and start" handler, returning a ref to the stream."""
        # Reusing a running workflow makes a retried start of this sync
        # operation hand back the same ref.
        client = nexus.client()
        await client.start_workflow(
            ScoresProducer.run,
            3,
            id=input.game_id,
            task_queue=self._task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return client.get_stream_handle(input.game_id).ref(topic=SCORES)


@workflow.defn
class Caller:
    """Calls the operation and passes the reference on; it never reads the stream."""

    @workflow.run
    async def run(self, input: CallerInput) -> StreamRef:
        """Return the stream reference the operation handed back."""
        client = workflow.create_nexus_client(
            service=ScoresService, endpoint=input.endpoint
        )
        return await client.execute_operation(
            ScoresService.start_game, GameRequest(input.game_id)
        )


async def run(args: argparse.Namespace) -> None:
    """Consume through the front from an activity, then read a returned reference."""
    _common.banner("s8 nexus consumers", args.provider)
    client, provider = await _setup.connect(args)
    workflow_id, task_queue = _common.ids("june-s8")
    endpoint_name = workflow_id
    created = await client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=endpoint_name,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(
                        namespace=client.namespace, task_queue=task_queue
                    )
                ),
            )
        )
    )
    front = NexusStreams(endpoint=endpoint_name, http_address=args.http)
    consumer_activities = Consumer(front)
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ScoresProducer, Caller],
            activities=[consumer_activities.consume_scores],
            # The front's handler serves the store this worker writes to, and
            # the scores service sits beside it behind the same endpoint.
            nexus_service_handlers=[
                TemporalStreamsHandler(provider, client),
                ScoresHandler(task_queue),
            ],
        ):
            print("  (a) activity consumer through the front, resuming from heartbeat")
            producer = await client.start_workflow(
                ScoresProducer.run, 5, id=f"{workflow_id}-a", task_queue=task_queue
            )
            consumer = await client.start_activity(
                consumer_activities.consume_scores,
                producer.id,
                id=f"{workflow_id}-consumer",
                task_queue=task_queue,
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=200)),
            )
            consumed = await consumer.result()
            print(
                f"    attempt {consumed.attempt} resumed after {consumed.resumed_after}"
            )
            print(f"    and read {consumed.scores}")
            await producer.result()

            print(
                "  (b) an operation returns a StreamRef; the client opens it on the front"
            )
            ref = await client.execute_workflow(
                Caller.run,
                CallerInput(endpoint_name, f"{workflow_id}-b"),
                id=f"{workflow_id}-caller",
                task_queue=task_queue,
            )
            print(f"    operation returned {ref}")
            # The ref is opened on whatever provider the client carries; this
            # one carries the front, so the read goes over Nexus.
            config = client.config()
            config["plugins"] = [front]
            fronted = Client(**config)
            async for record in fronted.get_stream_handle(ref).read(
                result_type=ScoreUpdate
            ):
                print(f"    {record.kind.name:6} {record.value}")
    finally:
        await front.close()
        await client.operator_service.delete_nexus_endpoint(
            DeleteNexusEndpointRequest(
                id=created.endpoint.id, version=created.endpoint.version
            )
        )
        await provider.close()


async def main() -> None:
    """Parse the flags and run the scenario."""
    await run(_common.parser(__doc__ or "").parse_args())


if __name__ == "__main__":
    asyncio.run(main())
