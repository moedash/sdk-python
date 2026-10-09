"""Workflow-published records carry the run that published them."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RUN_ID_KEY,
    Cursor,
    RecordKind,
    StreamRef,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._wire import from_wire, to_wire
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter
from tests.helpers import new_worker

EVENTS = topic("events", dict)


def client_with(client: Client, provider: MemoryStreams) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


async def read_all(records: Any, timeout: float = 10.0) -> list:
    out = []

    async def pull() -> None:
        async for record in records:
            out.append(record)

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    return out


def test_the_run_id_rides_in_the_record_metadata():
    converter = DataConverter.default.payload_converter
    wire = to_wire(converter, topic="t", kind=RecordKind.DATA, value=1, run_id="r1")
    assert wire.metadata[RUN_ID_KEY].data == b"r1"
    assert wire.metadata[RUN_ID_KEY].metadata["encoding"] == b"binary/plain"
    assert from_wire(converter, Cursor("c"), wire, int).run_id == "r1"
    plain = to_wire(converter, topic="t", kind=RecordKind.DATA, value=1)
    assert RUN_ID_KEY not in plain.metadata
    assert from_wire(converter, Cursor("c"), plain, int).run_id == ""


@workflow.defn
class PublishThenContinue:
    @workflow.run
    async def run(self, runs_left: int) -> None:
        workflow_writer(EVENTS).publish({"runs_left": runs_left})
        if runs_left:
            workflow.continue_as_new(runs_left - 1)
        workflow_writer(EVENTS).finish()


async def test_each_run_stamps_its_own_id_across_continue_as_new(client: Client):
    provider = MemoryStreams()
    streams_client = client_with(client, provider)
    workflow_id = f"streams-run-id-{uuid.uuid4().hex}"
    stream = provider.get_stream_handle(
        streams_client, StreamRef.for_workflow(workflow_id)
    )
    await stream.producer(topic=EVENTS, producer_id="backend", attempt=1).append(
        {"from": "backend"}
    )
    async with new_worker(streams_client, PublishThenContinue) as worker:
        handle = await streams_client.start_workflow(
            PublishThenContinue.run, 1, id=workflow_id, task_queue=worker.task_queue
        )
        first_run = handle.first_execution_run_id
        await handle.result()
        last_run = (await handle.describe()).run_id

    records = await read_all(stream.read(topic=EVENTS))
    assert [(r.kind, r.value, r.run_id) for r in records] == [
        (RecordKind.DATA, {"from": "backend"}, ""),
        (RecordKind.DATA, {"runs_left": 1}, first_run),
        (RecordKind.DATA, {"runs_left": 0}, last_run),
        (RecordKind.FINISH, None, last_run),
    ]
    assert first_run != last_run
