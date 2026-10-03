"""Which owner an owned-stream call names on the wire.

A workflow owner goes out in the workflow fields, which a server without
owner support still routes on. An activity owner needs the owner reference:
a standalone activity is routed on its own id, and an activity a workflow
scheduled is routed on the workflow.
"""

from __future__ import annotations

from typing import Any

import temporalio.api.streamservice.v1 as stream
from temporalio.api.stream.v1 import StreamRecord, StreamStartPosition
from temporalio.client_stream import StreamClient


class _Recorder:
    """Stands in for the stub and answers every call with an empty response."""

    def __init__(self) -> None:
        self.requests: list[Any] = []

    def __getattr__(self, method: str) -> Any:
        responses = {
            "AddWorkflowMessages": stream.AddWorkflowMessagesResponse,
            "PollWorkflowMessages": stream.PollWorkflowMessagesResponse,
            "DescribeWorkflowStream": stream.DescribeWorkflowStreamResponse,
        }

        async def call(request: Any, **_: Any) -> Any:
            self.requests.append(request.frontend_request)
            return responses[method]()

        return call


def _client(recorder: _Recorder) -> StreamClient:
    client = StreamClient.__new__(StreamClient)
    client._stub = recorder
    client._namespace = "ns"
    return client


async def _every_call(handle: Any) -> None:
    await handle.append(StreamRecord())
    await handle.poll(wait=False)
    await handle.describe()


async def test_a_workflow_owner_uses_the_workflow_fields():
    recorder = _Recorder()
    await _every_call(_client(recorder).workflow_stream("wf", owner_run_id="run"))
    for request in recorder.requests:
        assert (request.workflow_id, request.owner_run_id) == ("wf", "run")
        assert not request.HasField("owner")


async def test_a_standalone_activity_is_its_own_owner():
    recorder = _Recorder()
    await _every_call(_client(recorder).activity_stream("act", run_id="run"))
    for request in recorder.requests:
        assert request.owner == stream.StreamOwner(
            kind=stream.STREAM_OWNER_KIND_ACTIVITY, id="act", run_id="run"
        )
        assert request.workflow_id == ""


async def test_a_workflow_activity_is_reached_through_its_workflow():
    recorder = _Recorder()
    await _every_call(
        _client(recorder).activity_stream("act", "reasoning", workflow_id="wf")
    )
    for request in recorder.requests:
        assert request.owner == stream.StreamOwner(
            kind=stream.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY, id="wf", activity_id="act"
        )
        assert request.stream_name == "reasoning"


async def test_a_first_read_carries_its_start_position():
    recorder = _Recorder()
    handle = _client(recorder).workflow_stream("wf")
    await handle.read(start=StreamStartPosition(last_n=3))
    await handle.read(from_offset=4)
    first, later = recorder.requests
    assert first.start_position == StreamStartPosition(last_n=3)
    assert first.from_offset == 0
    assert not later.HasField("start_position")
