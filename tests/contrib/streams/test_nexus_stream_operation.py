"""A Nexus operation whose start hands its caller a stream.

The handler attaches the caller's callback to the stream's notifier and puts
the stream reference in the operation token. A producer's appends notify the
notifier, folded with one call in flight, and closing the stream completes
the operation with the close result.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.nexus
from temporalio import workflow
from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import (
    AttachStreamCallbackRequest,
    DescribeStreamNotifierRequest,
    DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse,
    DetachStreamCallbackRequest,
    NotifyStreamRequest,
)
from temporalio.client import Client
from temporalio.contrib.streams import StreamClosedError, StreamRef, workflow_writer
from temporalio.contrib.streams._cursor import BEGINNING, progress_counter
from temporalio.contrib.streams._output import StageRef
from temporalio.contrib.streams._record import Cursor, RecordKind
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    StreamNotifier,
    StreamOperationHandler,
    close_workflow_stream,
    stream_ref_from_token,
)
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from temporalio.worker._nexus import _NexusTaskCancellation
from tests.helpers import new_worker
from tests.helpers.nexus import make_nexus_endpoint_name

REF = StreamRef.for_workflow("owner-1", topic="tokens")


@dataclass
class FakeWorkflowService:
    """Records the notifier calls. A call can be held until released."""

    attached: list[AttachStreamCallbackRequest] = field(default_factory=list)
    detached: list[DetachStreamCallbackRequest] = field(default_factory=list)
    notified: list[NotifyStreamRequest] = field(default_factory=list)
    hold: asyncio.Event | None = None
    in_flight: int = 0
    max_in_flight: int = 0
    fail_next: bool = False
    # The first run of each Workflow id's current chain, as describe answers.
    chains: dict[str, str] = field(default_factory=dict)
    described: list[DescribeWorkflowExecutionRequest] = field(default_factory=list)

    async def describe_workflow_execution(
        self, request: DescribeWorkflowExecutionRequest
    ) -> DescribeWorkflowExecutionResponse:
        self.described.append(request)
        response = DescribeWorkflowExecutionResponse()
        response.workflow_execution_info.first_run_id = self.chains.get(
            request.execution.workflow_id, "chain-1"
        )
        return response

    async def attach_stream_callback(self, request: AttachStreamCallbackRequest) -> Any:
        self.attached.append(request)

    async def detach_stream_callback(self, request: DetachStreamCallbackRequest) -> Any:
        self.detached.append(request)

    async def notify_stream(self, request: NotifyStreamRequest) -> Any:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.hold is not None:
                await self.hold.wait()
            if self.fail_next:
                self.fail_next = False
                raise RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b"")
            self.notified.append(request)
        finally:
            self.in_flight -= 1


class FakeClient:
    """Enough of a client for the handler and the notifier, delegating the
    rest to a real one when given."""

    def __init__(
        self, service: FakeWorkflowService, real: Client | None = None
    ) -> None:
        self.workflow_service = service
        self.namespace = "default"
        self.data_converter = DataConverter.default
        self._real = real

    def __getattr__(self, name: str) -> Any:
        if self._real is None:
            raise AttributeError(name)
        return getattr(self._real, name)


def fake_client(service: FakeWorkflowService, real: Client | None = None) -> Client:
    client: Any = FakeClient(service, real)
    return client


def start_context(callback_url: str | None = "temporal://system") -> Any:
    return nexusrpc.handler.StartOperationContext(
        service="ChatService",
        operation="chat",
        headers={},
        task_cancellation=_NexusTaskCancellation(),
        request_id="attach-1",
        callback_url=callback_url,
        callback_headers={"nexus-callback-token": "abc"},
    )


def cancel_context() -> Any:
    return nexusrpc.handler.CancelOperationContext(
        service="ChatService",
        operation="chat",
        headers={},
        task_cancellation=_NexusTaskCancellation(),
    )


async def open_stream(_ctx: Any, _prompt: str) -> StreamRef:
    return REF


def handler_with(
    monkeypatch: pytest.MonkeyPatch, service: FakeWorkflowService
) -> StreamOperationHandler[str, str]:
    monkeypatch.setattr("temporalio.nexus.client", lambda: FakeClient(service))
    return StreamOperationHandler(open_stream)


async def test_start_attaches_the_callers_callback_and_hands_back_the_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeWorkflowService()
    handler = handler_with(monkeypatch, service)

    result = await handler.start(start_context(), "hi")

    assert isinstance(result, nexusrpc.handler.StartOperationResultAsync)
    assert stream_ref_from_token(result.token) == REF
    [attach] = service.attached
    assert attach.namespace == "default"
    assert attach.request_id == "attach-1"
    assert attach.callback.url == "temporal://system"
    assert dict(attach.callback.header) == {"nexus-callback-token": "abc"}
    assert attach.stream_ref == StreamReference(
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id="owner-1",
        run_id="chain-1",
        topic="tokens",
    )


async def test_the_attach_carries_the_start_token_and_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeWorkflowService()
    handler = handler_with(monkeypatch, service)
    before = time.time()

    result = await handler.start(start_context(), "hi")

    [attach] = service.attached
    # The notifier hands both back with progress and the completion, so a
    # completion that beats the start response still names the operation.
    assert attach.operation_token == result.token
    assert attach.HasField("start_time")
    assert before - 1 <= attach.start_time.ToSeconds() <= time.time() + 1


async def test_a_pinned_ref_attaches_by_its_chains_first_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeWorkflowService(chains={"owner-1": "run-1"})
    monkeypatch.setattr("temporalio.nexus.client", lambda: FakeClient(service))
    pinned = StreamRef.for_workflow("owner-1", run_id="run-2", topic="tokens")

    async def open_pinned(_ctx: Any, _prompt: str) -> StreamRef:
        return pinned

    result = await StreamOperationHandler(open_pinned).start(start_context(), "hi")

    assert stream_ref_from_token(result.token) == pinned
    # One notifier per run chain: a Continue-as-New keeps it, a new chain on
    # the same Workflow id gets its own.
    assert service.described[0].execution.run_id == "run-2"
    assert service.attached[0].stream_ref.run_id == "run-1"


async def test_a_start_without_a_callback_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeWorkflowService()
    handler = handler_with(monkeypatch, service)

    with pytest.raises(nexusrpc.HandlerError) as raised:
        await handler.start(start_context(callback_url=None), "hi")

    assert raised.value.type == nexusrpc.HandlerErrorType.BAD_REQUEST
    assert not service.attached


async def test_cancel_detaches_the_callback_it_attached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeWorkflowService()
    handler = handler_with(monkeypatch, service)
    started = await handler.start(start_context(), "hi")

    await handler.cancel(cancel_context(), started.token)

    [detach] = service.detached
    assert detach.request_id == "attach-1"
    assert detach.stream_ref == service.attached[0].stream_ref


async def test_cancel_detaches_from_the_chain_it_attached_to(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeWorkflowService()
    handler = handler_with(monkeypatch, service)
    started = await handler.start(start_context(), "hi")
    # The Workflow id moved on to a new chain since the start.
    service.chains["owner-1"] = "chain-2"

    await handler.cancel(cancel_context(), started.token)

    assert service.detached[0].stream_ref.run_id == "chain-1"


def test_a_token_that_names_no_stream_is_refused() -> None:
    for token in ["", "not-a-token", "eyJ2IjogMn0"]:
        with pytest.raises(ValueError):
            stream_ref_from_token(token)


def test_counters_come_from_the_stores_position() -> None:
    assert progress_counter(BEGINNING) == 0
    assert progress_counter(Cursor("memory:abcd1234:0")) == 1
    assert progress_counter(Cursor("memory:abcd1234:41")) == 42
    assert progress_counter(Cursor("redis:abcd1234:1700000000000-0")) == (
        1700000000000 << 20
    )
    assert progress_counter(Cursor("redis:abcd1234:1700000000000-7")) == (
        1700000000000 << 20 | 7
    )
    # Later in one millisecond, and a later millisecond, both rank higher.
    assert progress_counter(Cursor("redis:x:5-1")) < progress_counter(
        Cursor("redis:x:5-2")
    )
    assert progress_counter(Cursor("redis:x:5-999999999")) < progress_counter(
        Cursor("redis:x:6-0")
    )
    assert progress_counter(Cursor("redis:x:1700000000000-0")) < 2**63
    with pytest.raises(ValueError):
        progress_counter(Cursor("other:x:not-a-position"))


async def test_notifications_fold_with_one_call_in_flight() -> None:
    hold = asyncio.Event()
    service = FakeWorkflowService(hold=hold)
    notifier = StreamNotifier(fake_client(service), REF)

    for counter, position in enumerate(("p1", "p2", "p3", "p4", "p5"), start=1):
        notifier.notify(position, counter)
        await asyncio.sleep(0)
    hold.set()
    await notifier.flush()

    assert [request.position for request in service.notified] == ["p1", "p5"]
    assert service.max_in_flight == 1
    first, second = service.notified
    assert (first.counter, second.counter) == (1, 5)
    assert not first.close and not second.close


async def test_close_waits_for_the_call_in_flight_and_carries_the_result() -> None:
    hold = asyncio.Event()
    service = FakeWorkflowService(hold=hold)
    notifier = StreamNotifier(fake_client(service), REF)
    notifier.notify("p1", 1)
    await asyncio.sleep(0)

    closing = asyncio.create_task(notifier.close("done", 2))
    await asyncio.sleep(0.01)
    assert not closing.done()
    hold.set()
    await closing

    assert service.max_in_flight == 1
    last = service.notified[-1]
    assert last.close
    assert last.counter == 2
    assert (
        DataConverter.default.payload_converter.from_payload(last.close_result)
        == "done"
    )


async def test_a_notification_after_close_sends_nothing() -> None:
    service = FakeWorkflowService()
    notifier = StreamNotifier(fake_client(service), REF)
    await notifier.close(None, 1)
    sent = len(service.notified)

    notifier.notify("late", 2)
    await notifier.flush()

    assert len(service.notified) == sent


async def test_a_failed_notification_leaves_the_next_one_to_tell_the_reader() -> None:
    service = FakeWorkflowService(fail_next=True)
    notifier = StreamNotifier(fake_client(service), REF)

    notifier.notify("p1", 1)
    await notifier.flush()
    notifier.notify("p2", 2)
    await notifier.flush()

    assert [request.position for request in service.notified] == ["p2"]


async def test_a_provider_that_notifies_tells_the_notifier_after_each_append(
    client: Client,
) -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    stream_client: Any = FakeClient(service, real=client)
    ref = StreamRef.for_workflow(f"owner-{uuid.uuid4()}", topic="tokens")
    handle = provider.get_stream_handle(stream_client, ref)
    producer = handle.producer(producer_id="p", attempt=1)

    first = await producer.append("a")
    last = await producer.append("b", "c")
    await provider.close_stream(stream_client, ref, "done")

    counters = [request.counter for request in service.notified]
    assert counters == sorted(counters) and len(set(counters)) == len(counters)
    assert service.notified[-1].counter > progress_counter(last)
    positions = [request.position for request in service.notified if not request.close]
    # The two appends may fold into one notification, but the newest is told.
    assert positions[-1] == last.token
    assert set(positions) <= {first.token, last.token}
    assert all(
        request.stream_ref.workflow_id == ref.workflow_id
        and request.stream_ref.topic == "tokens"
        for request in service.notified
    )
    assert service.notified[-1].close
    # The server refuses a reference without its chain.
    assert all(request.stream_ref.run_id for request in service.notified)
    await provider.close()


async def test_a_reused_workflow_id_and_topic_notifies_the_new_chain(
    client: Client,
) -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    stream_client: Any = FakeClient(service, real=client)
    ref = StreamRef.for_workflow(f"owner-{uuid.uuid4()}", topic="tokens")
    service.chains[ref.workflow_id] = "chain-1"
    first = provider.get_stream_handle(stream_client, ref).producer(
        producer_id="p", attempt=1
    )
    await first.append("a")
    await provider.close_stream(stream_client, ref, "done")

    # A new chain reuses the Workflow id and topic.
    service.chains[ref.workflow_id] = "chain-2"
    # The memory store keys by Workflow id, so the new chain's producer
    # writes as a new attempt.
    second = provider.get_stream_handle(stream_client, ref).producer(
        producer_id="p", attempt=2
    )
    await second.append("b")
    await provider.close_stream(stream_client, ref, "done again")

    chains = [
        (request.stream_ref.run_id, request.close) for request in service.notified
    ]
    assert chains == [
        ("chain-1", False),
        ("chain-1", True),
        ("chain-2", False),
        ("chain-2", True),
    ]
    await provider.close()


async def append_to(provider: MemoryStreams, client: Any, workflow_id: str) -> None:
    ref = StreamRef.for_workflow(workflow_id, topic="tokens")
    producer = provider.get_stream_handle(client, ref).producer(
        producer_id="p", attempt=1
    )
    await producer.append("a")
    await asyncio.sleep(0.01)


def notifier_owners(provider: MemoryStreams) -> list[str]:
    notifiers = provider._notifiers  # type: ignore[reportPrivateUsage]
    return [workflow_id for _, workflow_id, _, _ in notifiers or ()]


async def test_a_closed_streams_notifier_is_dropped(client: Client) -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    stream_client: Any = FakeClient(service, real=client)
    workflow_id = f"owner-{uuid.uuid4()}"
    await append_to(provider, stream_client, workflow_id)
    assert notifier_owners(provider) == [workflow_id]

    await provider.close_stream(
        stream_client, StreamRef.for_workflow(workflow_id, topic="tokens")
    )

    assert notifier_owners(provider) == []
    await provider.close()


async def test_notifiers_beyond_the_cap_are_dropped_least_recent_first(
    client: Client,
) -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append(max_notifiers=2)
    stream_client: Any = FakeClient(service, real=client)
    first, second, third = (f"owner-{uuid.uuid4()}" for _ in range(3))

    await append_to(provider, stream_client, first)
    await append_to(provider, stream_client, second)
    # A write makes its notifier the most recent.
    await append_to(provider, stream_client, first)
    await append_to(provider, stream_client, third)

    assert sorted(notifier_owners(provider)) == sorted([first, third])
    await provider.close()


async def test_an_ended_chain_drops_its_notifiers(client: Client) -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    stream_client: Any = FakeClient(service, real=client)
    ended, running = f"owner-{uuid.uuid4()}", f"owner-{uuid.uuid4()}"
    await append_to(provider, stream_client, ended)
    await append_to(provider, stream_client, running)

    provider._forget_chain("default", ended, "chain-1")  # type: ignore[reportPrivateUsage]

    assert notifier_owners(provider) == [running]
    await provider.close()


async def test_flush_waits_for_every_notification_in_flight(client: Client) -> None:
    hold = asyncio.Event()
    service = FakeWorkflowService(hold=hold)
    provider = MemoryStreams().notify_on_append(max_notifiers=1)
    stream_client: Any = FakeClient(service, real=client)
    # The second stream pushes out the first one's notifier while its call
    # is still out.
    await append_to(provider, stream_client, f"owner-{uuid.uuid4()}")
    await append_to(provider, stream_client, f"owner-{uuid.uuid4()}")

    flushing = asyncio.create_task(provider.flush_notifications())
    await asyncio.sleep(0.05)
    assert not flushing.done()
    hold.set()
    await asyncio.wait_for(flushing, 5)

    assert len(service.notified) == 2
    await provider.close()


async def test_a_worker_that_stops_waits_for_its_notifications(
    client: Client,
) -> None:
    hold = asyncio.Event()
    service = FakeWorkflowService(hold=hold)
    provider = MemoryStreams().notify_on_append()
    stream_client: Any = FakeClient(service, real=client)
    worker = new_worker(client, OwnerUntilDone, plugins=[provider])
    # Worker.run, not async with: the context manager cancels the run once
    # shutdown returns, and with it what plugins do after the Worker stops.
    running = asyncio.create_task(worker.run())
    await append_to(provider, stream_client, f"owner-{uuid.uuid4()}")
    await worker.shutdown()
    await asyncio.sleep(0.5)
    assert not running.done()
    hold.set()
    await asyncio.wait_for(running, 10)

    assert len(service.notified) == 1
    await provider.close()


class ForeignPositionProducer:
    """A third-party producer whose positions no counter can come from."""

    producer_id = "p"
    attempt = 1

    async def append(self, *values: Any) -> Cursor:
        del values
        return Cursor("other:x:not-a-position")

    async def finish(self) -> Cursor:
        return Cursor("other:x:not-a-position")


async def test_a_position_without_a_counter_does_not_fail_the_append() -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    producer = provider._notified_producer(  # type: ignore[reportPrivateUsage]
        fake_client(service), REF, "tokens", ForeignPositionProducer()
    )

    # The records landed, so the append answers with their cursor.
    assert (await producer.append("a")).token == "other:x:not-a-position"
    assert (await producer.finish()).token == "other:x:not-a-position"
    await asyncio.sleep(0.01)

    assert service.notified == []


def test_a_workflows_batch_notifies_each_of_its_topics_once_visible() -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    stage = StageRef(
        namespace="default",
        workflow_id="owner-1",
        first_run_id="run-1",
        token="stage-1",
        topics=("tokens", "status"),
    )

    async def promote() -> None:
        provider._notify_promoted(fake_client(service), stage)  # type: ignore[reportPrivateUsage]
        await asyncio.sleep(0.05)

    asyncio.run(promote())

    assert sorted(request.stream_ref.topic for request in service.notified) == [
        "status",
        "tokens",
    ]
    assert all(
        request.stream_ref.workflow_id == "owner-1"
        and request.stream_ref.run_id == "run-1"
        for request in service.notified
    )
    # The stage names its chain, so nothing is described.
    assert service.described == []


async def test_a_promoted_close_closes_the_store_then_the_notifier() -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams().notify_on_append()
    client = fake_client(service)
    [result] = DataConverter.default.payload_converter.to_payloads(["3 tokens"])
    ref = StreamRef.for_workflow("owner-1", topic="tokens")
    producer = provider.get_stream_handle(None, ref).producer(
        producer_id="p", attempt=1
    )
    await producer.append("a")
    stage = StageRef(
        namespace="default",
        workflow_id="owner-1",
        first_run_id="run-1",
        token="stage-1",
        topics=("tokens", "status"),
        closes=(("tokens", result),),
    )

    provider._notify_promoted(client, stage)  # type: ignore[reportPrivateUsage]
    await provider.flush_notifications()

    # Closed in the store: a reader ends, and an append is refused.
    with pytest.raises(StreamClosedError):
        await producer.append("b")
    records = [record async for record in provider.get_stream_handle(None, ref).read()]
    assert [record.value for record in records] == ["a"]
    [close] = [request for request in service.notified if request.close]
    assert close.stream_ref.topic == "tokens"
    # The server refuses a reference without its chain.
    assert close.stream_ref.run_id == "run-1"
    assert close.counter == progress_counter(records[-1].cursor) + 1
    assert (
        DataConverter.default.payload_converter.from_payload(close.close_result)
        == "3 tokens"
    )
    # The other topic is notified, not closed.
    assert sorted(request.stream_ref.topic for request in service.notified) == [
        "status",
        "tokens",
    ]
    await provider.close()


def test_a_provider_without_notifications_tells_nobody() -> None:
    service = FakeWorkflowService()
    provider = MemoryStreams()
    stage = StageRef(
        namespace="default",
        workflow_id="owner-1",
        first_run_id="run-1",
        token="stage-1",
        topics=("tokens",),
    )

    async def promote() -> None:
        provider._notify_promoted(fake_client(service), stage)  # type: ignore[reportPrivateUsage]
        await asyncio.sleep(0.05)

    asyncio.run(promote())

    assert service.notified == []


# Live: a caller Workflow sees the producer's appends as progress and the
# close result as the operation's result. Needs a server with Nexus progress
# and the stream notifier; any other server makes the test skip.


@nexusrpc.service
class ChatService:
    chat: nexusrpc.Operation[str, str]


@dataclass
class Observed:
    counters: list[int]
    result: str


@workflow.defn(name="StreamOperationCaller")
class StreamOperationCaller:
    def __init__(self) -> None:
        self._counters: list[int] = []

    @workflow.run
    async def run(self, endpoint: str) -> Observed:
        nexus_client = workflow.create_nexus_client(
            service=ChatService, endpoint=endpoint
        )
        handle = await nexus_client.start_operation(ChatService.chat, "hello")
        last = 0
        while (progress := await handle.progress(after_counter=last)) is not None:
            self._counters.append(progress.counter)
            last = progress.counter
        return Observed(counters=list(self._counters), result=await handle)

    @workflow.query
    def counters(self) -> list[int]:
        return list(self._counters)


@workflow.defn(name="OwnerUntilDone")
class OwnerUntilDone:
    """Owns a stream until signaled done."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


async def _skip_without_notifier(client: Client, ref: StreamRef) -> None:
    try:
        await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id=ref.workflow_id,
                    topic=ref.topic,
                ),
            )
        )
    except RPCError as err:
        if err.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip(f"server has no stream notifier: {err.message}")
        # A server that keys the notifier by run chain refuses the probe's
        # empty run id, which still shows it has the notifier.
        if err.status not in (
            RPCStatusCode.NOT_FOUND,
            RPCStatusCode.INVALID_ARGUMENT,
        ):
            raise


async def _wait_for(predicate: Any, attempts: int = 100) -> Any:
    for _ in range(attempts):
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.1)
    return None


async def test_a_caller_sees_appends_as_progress_and_the_close_as_the_result(
    client: Client, env: WorkflowEnvironment
) -> None:
    ref = StreamRef.for_workflow(f"owner-{uuid.uuid4()}", topic="tokens")
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-operation-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    provider = MemoryStreams().notify_on_append()
    started = asyncio.Event()

    async def open_owned_stream(_ctx: Any, _prompt: str) -> StreamRef:
        # The notifier is keyed by the owner's run chain, so the owner runs.
        await client.start_workflow(
            OwnerUntilDone.run, id=ref.workflow_id, task_queue=task_queue
        )
        started.set()
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(open_owned_stream)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerUntilDone],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        handle = await client.start_workflow(
            StreamOperationCaller.run,
            endpoint,
            id=f"stream-operation-caller-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await asyncio.wait_for(started.wait(), 10)
        # Progress that reaches the caller before its started event is dropped
        # (DD-43), so the first append waits for the operation to be started.

        async def operation_started() -> bool:
            history = await handle.fetch_history()
            return any(
                event.HasField("nexus_operation_started_event_attributes")
                for event in history.events
            )

        assert await _wait_for(operation_started)
        producer = provider.get_stream_handle(client, ref).producer(
            producer_id="writer", attempt=1
        )
        await producer.append("a")

        async def first_progress() -> list[int]:
            return await handle.query(StreamOperationCaller.counters)

        assert await _wait_for(first_progress), "the caller never saw progress"
        await producer.append("b", "c")
        await provider.close_stream(client, ref, "done")

        observed = await asyncio.wait_for(handle.result(), 20)
        await client.get_workflow_handle(ref.workflow_id).signal(OwnerUntilDone.done)

    assert observed.result == "done"
    assert observed.counters, observed
    assert observed.counters == sorted(set(observed.counters)), observed
    await provider.close()


TOKENS_TOPIC = "tokens"


@workflow.defn(name="StreamOwnerThatPublishes")
class StreamOwnerThatPublishes:
    """Publishes its tokens on its own stream, then waits to be told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, tokens: list[str]) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for token in tokens:
            writer.publish(token)
            await workflow.sleep(timedelta(milliseconds=200))
        writer.finish()
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


async def test_a_workflows_own_publishes_reach_the_caller_as_progress(
    client: Client, env: WorkflowEnvironment
) -> None:
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-owner-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    provider = MemoryStreams().notify_on_append()

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            StreamOwnerThatPublishes.run,
            ["a", "b", "c"],
            id=owner_id,
            task_queue=task_queue,
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, StreamOwnerThatPublishes],
        nexus_service_handlers=[ChatServiceHandler()],
        plugins=[provider],
    ):
        caller = await client.start_workflow(
            StreamOperationCaller.run,
            endpoint,
            id=f"stream-owner-caller-{uuid.uuid4()}",
            task_queue=task_queue,
        )

        # The Workflow's batches are notified only once they are visible,
        # which is after the task that published them completed.
        async def several_progresses() -> list[int] | None:
            counters = await caller.query(StreamOperationCaller.counters)
            return counters if len(counters) >= 2 else None

        assert await _wait_for(several_progresses), "the caller saw too little progress"
        await client.get_workflow_handle(owner_id).signal(StreamOwnerThatPublishes.done)
        await provider.close_stream(client, ref, "3 tokens")
        observed = await asyncio.wait_for(caller.result(), 20)

    assert observed.result == "3 tokens"
    assert observed.counters == sorted(set(observed.counters)), observed
    await provider.close()


@workflow.defn(name="StreamOwnerThatCloses")
class StreamOwnerThatCloses:
    """Publishes its tokens on its own stream, then closes it from Workflow code."""

    @workflow.run
    async def run(self, tokens: list[str]) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for token in tokens:
            writer.publish(token)
            await workflow.sleep(timedelta(milliseconds=200))
        writer.finish()
        close_workflow_stream(f"{len(tokens)} tokens", topic=TOKENS_TOPIC)


async def test_a_workflow_closes_its_own_stream_through_system_nexus(
    client: Client, env: WorkflowEnvironment
) -> None:
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    provider = MemoryStreams().notify_on_append()

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            StreamOwnerThatCloses.run,
            ["a", "b", "c"],
            id=owner_id,
            task_queue=task_queue,
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, StreamOwnerThatCloses],
        nexus_service_handlers=[ChatServiceHandler()],
        plugins=[provider],
    ):
        observed = await asyncio.wait_for(
            client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )
        owner_result = await client.get_workflow_handle(owner_id).result()

    assert observed.result == "3 tokens"
    assert owner_result is None
    await provider.close()


# Two producer processes whose clocks disagree: the second appends after the
# first, but its clock is ten seconds behind. The counters come from the store's
# position, so the second notification still ranks above the first.


@pytest.mark.skipif(
    not os.environ.get("STREAMS_REDIS_URL"),
    reason="set STREAMS_REDIS_URL to run the Redis provider tests",
)
async def test_producers_with_skewed_clocks_still_notify_in_increasing_order(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    from temporalio.contrib.streams.redis import RedisStreams

    service = FakeWorkflowService()
    prefix = f"test-{uuid.uuid4().hex}"
    first = RedisStreams(os.environ["STREAMS_REDIS_URL"], key_prefix=prefix)
    second = RedisStreams(os.environ["STREAMS_REDIS_URL"], key_prefix=prefix)
    first.notify_on_append()
    second.notify_on_append()
    real_time_ns = time.time_ns
    async with new_worker(client, OwnerUntilDone) as worker:
        owner = await client.start_workflow(
            OwnerUntilDone.run,
            id=f"skew-owner-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        ref = StreamRef.for_workflow(owner.id, topic="tokens")
        ahead = first.get_stream_handle(fake_client(service, client), ref).producer(
            producer_id="ahead", attempt=1
        )
        behind = second.get_stream_handle(fake_client(service, client), ref).producer(
            producer_id="behind", attempt=1
        )

        monkeypatch.setattr(time, "time_ns", lambda: real_time_ns() + 10_000_000_000)
        await ahead.append("a")
        await asyncio.sleep(0.2)
        monkeypatch.setattr(time, "time_ns", real_time_ns)
        await behind.append("b")
        await asyncio.sleep(0.2)
        await owner.signal(OwnerUntilDone.done)
        await owner.result()

    counters = [request.counter for request in service.notified]
    assert len(counters) == 2, counters
    assert counters[0] < counters[1], counters
    await first.close()
    await second.close()


def _provider_for(backing: str) -> Any:
    if backing == "memory":
        return MemoryStreams().notify_on_append()
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the Redis provider tests")
    from temporalio.contrib.streams.redis import RedisStreams

    return RedisStreams(url, key_prefix=f"test-{uuid.uuid4().hex}").notify_on_append()


@workflow.defn(name="OwnerThatClosesInOneTask")
class OwnerThatClosesInOneTask:
    """Publishes its tokens and closes its stream in one Workflow Task, then
    keeps running, so only the close can end a read."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, count: int) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for n in range(count):
            writer.publish(f"t{n}")
        close_workflow_stream(f"{count} tokens", topic=TOKENS_TOPIC)
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_a_close_in_the_task_of_the_last_publishes_comes_after_them(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    provider = _provider_for(backing)
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            OwnerThatClosesInOneTask.run, 50, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerThatClosesInOneTask],
        nexus_service_handlers=[ChatServiceHandler()],
        plugins=[provider],
    ):
        observed = await asyncio.wait_for(
            client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )

        # The operation completed, so the store is closed and holds every
        # record: a read gets them all and ends while the owner still runs.
        async def read_all() -> list[Any]:
            return await _values(provider.get_stream_handle(client, ref))

        values = await asyncio.wait_for(read_all(), 10)
        owner = client.get_workflow_handle(owner_id)
        await owner.signal(OwnerThatClosesInOneTask.done)
        await owner.result()
        history = await owner.fetch_history()

    assert observed.result == "50 tokens"
    assert values == [f"t{n}" for n in range(50)]
    # The close is recorded with the task's publishes, so a replay commits
    # the same batch.
    await Replayer(
        workflows=[OwnerThatClosesInOneTask], plugins=[provider]
    ).replay_workflow(history)
    await provider.close()


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_an_append_after_an_outside_close_is_refused(
    client: Client, backing: str
) -> None:
    provider = _provider_for(backing)
    async with new_worker(client, OwnerUntilDone) as worker:
        owner = await client.start_workflow(
            OwnerUntilDone.run, id=f"owner-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        ref = StreamRef.for_workflow(owner.id, topic=TOKENS_TOPIC)
        await _skip_without_notifier(client, ref)
        handle = provider.get_stream_handle(client, ref)
        producer = handle.producer(producer_id="owner-activity", attempt=1)
        first = await producer.append("a")

        await provider.close_stream(client, ref, "done")

        with pytest.raises(StreamClosedError):
            await producer.append("b")
        # A retry of a batch that landed before the close still answers.
        retried = handle.producer(producer_id="owner-activity", attempt=1)
        assert await retried.append("a") == first
        values = await asyncio.wait_for(_values(handle), 10)
        await owner.signal(OwnerUntilDone.done)

    assert values == ["a"]
    await provider.close()


async def _values(handle: Any) -> list[Any]:
    return [
        record.value async for record in handle.read() if record.kind is RecordKind.DATA
    ]
