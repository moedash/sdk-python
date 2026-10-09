"""Owners and readers this release does not support say so.

Only a Workflow owns a stream in this release, and only Activities and
clients read one. Everything else raises ``StreamUnsupportedError``.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRef,
    StreamUnsupportedError,
    activity_handle,
    topic,
    workflow_reader,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.testing import ActivityEnvironment
from tests.helpers import new_worker

EVENTS = topic("events", dict)


def client_with(client: Client, provider: MemoryStreams) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


def test_a_ref_refuses_owner_kinds_this_release_lacks():
    for kind in ("activity", "standalone"):
        with pytest.raises(StreamUnsupportedError, match="only Workflow-owned"):
            StreamRef(kind, "x")  # type: ignore[arg-type]


@workflow.defn
class TriesToRead:
    @workflow.run
    async def run(self) -> str:
        try:
            workflow_reader(EVENTS)
        except StreamUnsupportedError as error:
            return str(error)
        return "read"


async def test_a_workflow_cannot_read_a_stream(client: Client):
    streams_client = client_with(client, MemoryStreams())
    async with new_worker(streams_client, TriesToRead) as worker:
        said = await streams_client.execute_workflow(
            TriesToRead.run,
            id=f"streams-unsupported-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    assert "not supported in this release" in said


async def test_an_activity_with_no_workflow_has_no_stream():
    env = ActivityEnvironment()
    env.info = dataclasses.replace(
        ActivityEnvironment.default_info(), workflow_id=None, workflow_run_id=None
    )

    async def standalone() -> None:
        activity_handle()

    with pytest.raises(StreamUnsupportedError, match="owned by an Activity"):
        await env.run(standalone)
