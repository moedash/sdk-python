"""What the stream client retries, and what it raises at once.

The stream service is reached over a channel of the client's own, outside
sdk-core, so the retry Core gives every other call is reproduced here. These
pin its edges with a scripted stub: a throttled read or numbered append goes
again, an append the server could not tell from its repeat does not, a code
Core would not retry is raised at once, and cancelling the caller lands during
the wait between attempts.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from types import SimpleNamespace
from typing import Any

import grpc
import grpc.aio
import pytest

import temporalio.api.streamservice.v1 as stream
import temporalio.converter
from temporalio import client_stream
from temporalio.api.common.v1 import GrpcStatus
from temporalio.api.enums.v1 import ResourceExhaustedCause
from temporalio.api.errordetails.v1 import ResourceExhaustedFailure
from temporalio.api.stream.v1 import StreamRecord
from temporalio.client_stream import WorkflowStreamHandle, _to_service
from temporalio.service import RetryConfig, RPCError, RPCStatusCode
from temporalio.streams import StreamNotFoundError
from temporalio.streams._record import RecordKind
from temporalio.streams._wire import to_wire
from temporalio.streams.providers.native import NativeStreamHandle

FAST = RetryConfig(
    initial_interval_millis=1,
    max_interval_millis=2,
    max_elapsed_time_millis=5000,
    max_retries=5,
)


@pytest.fixture(autouse=True)
def fast_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    # The floor under a throttled wait is a second, which is right for a
    # caller and wrong for a test.
    monkeypatch.setattr(
        client_stream,
        "_THROTTLE",
        RetryConfig(
            initial_interval_millis=1,
            max_interval_millis=2,
            max_elapsed_time_millis=None,
            max_retries=0,
        ),
    )


class _Stub:
    """Answers each method from a script of exceptions and responses, in order."""

    def __init__(self, **scripts: list[Any]) -> None:
        self.calls: dict[str, int] = defaultdict(int)
        self._scripts = scripts

    def __getattr__(self, method: str) -> Any:
        script = self._scripts[method]

        async def call(_request: Any, **_: Any) -> Any:
            self.calls[method] += 1
            outcome = script.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        return call


def _error(
    code: grpc.StatusCode,
    details: str = "",
    *,
    cause: ResourceExhaustedCause.ValueType | None = None,
) -> grpc.aio.AioRpcError:
    trailing = grpc.aio.Metadata()
    if cause is not None:
        status = GrpcStatus(code=code.value[0], message=details)
        status.details.add().Pack(ResourceExhaustedFailure(cause=cause))
        trailing.add("grpc-status-details-bin", status.SerializeToString())
    return grpc.aio.AioRpcError(code, grpc.aio.Metadata(), trailing, details=details)


def _throttled() -> grpc.aio.AioRpcError:
    return _error(
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        "service rate limit exceeded",
        cause=ResourceExhaustedCause.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
    )


def _page(*records: stream.StreamRecord) -> stream.PollWorkflowMessagesResponse:
    return stream.PollWorkflowMessagesResponse(
        frontend_response=stream.PollMessagesOutput(
            records=list(records),
            next_offset=len(records),
            head_offset=len(records),
            closed=True,
            run_id="run",
        )
    )


def _appended() -> stream.AddWorkflowMessagesResponse:
    return stream.AddWorkflowMessagesResponse(
        frontend_response=stream.AddMessagesOutput(
            first_offset=0, next_offset=1, count=1
        )
    )


def _handle(stub: _Stub, retry: RetryConfig = FAST) -> WorkflowStreamHandle:
    return WorkflowStreamHandle(stub, "ns", "wf", "topic", "run", retry_config=retry)


async def test_a_throttled_poll_is_read_again() -> None:
    stub = _Stub(PollWorkflowMessages=[_throttled(), _page()])
    page = await _handle(stub).poll()
    assert page.closed
    assert stub.calls["PollWorkflowMessages"] == 2


async def test_a_throttled_numbered_append_goes_again() -> None:
    stub = _Stub(AddWorkflowMessages=[_throttled(), _appended()])
    appended = await _handle(stub).append(StreamRecord(), producer_id="p", sequence=1)
    assert appended.next_offset == 1
    assert stub.calls["AddWorkflowMessages"] == 2


async def test_a_numbered_append_survives_an_ambiguous_failure() -> None:
    # The server holds the producer's sequence, so whichever attempt landed,
    # the repeat comes back with the original offsets.
    stub = _Stub(AddWorkflowMessages=[_error(grpc.StatusCode.UNAVAILABLE), _appended()])
    await _handle(stub).append(StreamRecord(), producer_id="p", sequence=1)
    assert stub.calls["AddWorkflowMessages"] == 2


async def test_an_unnumbered_append_is_not_made_again_after_an_ambiguous_failure() -> (
    None
):
    stub = _Stub(AddWorkflowMessages=[_error(grpc.StatusCode.UNAVAILABLE), _appended()])
    with pytest.raises(RPCError) as raised:
        await _handle(stub).append(StreamRecord())
    assert raised.value.status == RPCStatusCode.UNAVAILABLE
    assert stub.calls["AddWorkflowMessages"] == 1


async def test_an_unnumbered_append_goes_again_after_a_refusal() -> None:
    # Throttling is answered before the handler runs, so nothing landed.
    stub = _Stub(AddWorkflowMessages=[_throttled(), _appended()])
    await _handle(stub).append(StreamRecord())
    assert stub.calls["AddWorkflowMessages"] == 2


async def test_a_code_core_would_not_retry_is_raised_at_once() -> None:
    stub = _Stub(
        PollWorkflowMessages=[_error(grpc.StatusCode.INVALID_ARGUMENT, "bad"), _page()]
    )
    with pytest.raises(RPCError) as raised:
        await _handle(stub).poll()
    assert raised.value.status == RPCStatusCode.INVALID_ARGUMENT
    assert stub.calls["PollWorkflowMessages"] == 1

    stub = _Stub(PollWorkflowMessages=[_error(grpc.StatusCode.NOT_FOUND), _page()])
    with pytest.raises(StreamNotFoundError):
        await _handle(stub).poll()
    assert stub.calls["PollWorkflowMessages"] == 1


async def test_the_budget_is_bounded() -> None:
    stub = _Stub(PollWorkflowMessages=[_throttled() for _ in range(10)])
    with pytest.raises(RPCError) as raised:
        await _handle(
            stub, RetryConfig(initial_interval_millis=1, max_retries=3)
        ).poll()
    assert raised.value.status == RPCStatusCode.RESOURCE_EXHAUSTED
    assert stub.calls["PollWorkflowMessages"] == 3


async def test_a_full_stream_is_not_waited_out() -> None:
    full = _error(
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        "stream holds 10 of its budget of 10 records",
        cause=ResourceExhaustedCause.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
    )
    stub = _Stub(AddWorkflowMessages=[full, _appended()])
    with pytest.raises(RPCError) as raised:
        await _handle(stub).append(StreamRecord(), producer_id="p", sequence=1)
    assert raised.value.status == RPCStatusCode.RESOURCE_EXHAUSTED
    assert stub.calls["AddWorkflowMessages"] == 1

    too_large = _error(
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        "grpc: received message larger than max (5 vs. 4)",
    )
    stub = _Stub(AddWorkflowMessages=[too_large, _appended()])
    with pytest.raises(RPCError):
        await _handle(stub).append(StreamRecord(), producer_id="p", sequence=1)
    assert stub.calls["AddWorkflowMessages"] == 1


async def test_cancelling_the_caller_lands_during_the_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        client_stream,
        "_THROTTLE",
        RetryConfig(initial_interval_millis=60_000, max_elapsed_time_millis=None),
    )
    stub = _Stub(PollWorkflowMessages=[_throttled(), _page()])
    task = asyncio.ensure_future(_handle(stub).poll())
    for _ in range(10):
        await asyncio.sleep(0)
    assert stub.calls["PollWorkflowMessages"] == 1, "parked in the wait"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stub.calls["PollWorkflowMessages"] == 1


async def test_a_native_read_survives_a_throttled_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    converter = temporalio.converter.default()
    record = _to_service(
        to_wire(
            converter.payload_converter,
            topic="topic",
            kind=RecordKind.DATA,
            value="hello",
            producer_id="p",
            sequence=1,
        )
    )
    stub = _Stub(PollWorkflowMessages=[_throttled(), _page(record)])
    client: Any = SimpleNamespace(data_converter=converter)
    handle = NativeStreamHandle(client, "wf", "run")

    def open_stream(_topic: str, _run_id: str) -> WorkflowStreamHandle:
        return _handle(stub)

    monkeypatch.setattr(handle, "_stream", open_stream)

    values = [item.value async for item in handle.read(topic="topic")]

    assert values == ["hello"]
    assert stub.calls["PollWorkflowMessages"] == 2
