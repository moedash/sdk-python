"""A Nexus operation whose start hands its caller a stream.

The handler attaches the caller's callback to the stream's notifier on the
server and puts the stream reference in the operation token. Core notifies
the notifier after each append and each visible Workflow batch, and closing
the stream completes the operation with the close result.
"""

from __future__ import annotations

import asyncio
import base64
import json
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
)
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    StreamClosedError,
    StreamRef,
    StreamStorePlugin,
    get_stream_handle,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
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
    """Records the notifier calls the operation makes."""

    attached: list[AttachStreamCallbackRequest] = field(default_factory=list)
    detached: list[DetachStreamCallbackRequest] = field(default_factory=list)
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


class FakeClient:
    """Enough of a client for the operation handler."""

    def __init__(self, service: FakeWorkflowService) -> None:
        self.workflow_service = service
        self.namespace = "default"
        self.data_converter = DataConverter.default


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
    # A version 1 token, from before the token named the chain.
    old = base64.urlsafe_b64encode(
        json.dumps(
            {
                "v": 1,
                "attach": "attach-1",
                "chain": "chain-1",
                "ref": {
                    "kind": "workflow",
                    "workflow_id": "owner-1",
                    "run_id": None,
                    "topic": "tokens",
                },
            }
        ).encode()
    ).decode()
    for token in ["", "not-a-token", "eyJ2IjogMn0", old]:
        with pytest.raises(ValueError):
            stream_ref_from_token(token)


async def test_notifications_are_turned_on_before_the_store_connects(
    client: Client,
) -> None:
    store = MemoryStreams().notify_on_append(max_notifiers=10)
    assert store._config.notify_on_append
    assert store._config.max_notifiers == 10
    # Nothing is out before a client connects the store, nor right after.
    await store.flush_notifications()
    await connect(client, store)
    await store.flush_notifications()
    with pytest.raises(ValueError, match="before a client connects"):
        store.notify_on_append()


# Live: a caller Workflow sees the stream's progress and the close result.
# These need a server with Nexus progress and the stream notifier, and skip on
# any other server.


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


async def connect(client: Client, store: StreamStorePlugin) -> Client:
    """A client of the test server that carries ``store``."""
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=[store],
    )


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


def _store_for(backing: str) -> StreamStorePlugin:
    if backing == "memory":
        return MemoryStreams().notify_on_append()
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the Redis cases")
    from temporalio.contrib.streams.redis import RedisStreams

    return RedisStreams(url, key_prefix=f"test-{uuid.uuid4().hex}").notify_on_append()


async def _values(client: Client, ref: StreamRef) -> list[Any]:
    return [
        record.value
        async for record in get_stream_handle(client, ref).read()
        if record.kind is RecordKind.DATA
    ]


async def test_a_caller_sees_appends_as_progress_and_the_close_as_the_result(
    client: Client, env: WorkflowEnvironment
) -> None:
    ref = StreamRef.for_workflow(f"owner-{uuid.uuid4()}", topic="tokens")
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-operation-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = MemoryStreams().notify_on_append()
    streams_client = await connect(client, store)
    started = asyncio.Event()

    async def open_owned_stream(_ctx: Any, _prompt: str) -> StreamRef:
        # The notifier is keyed by the owner's run chain, so the owner runs.
        await streams_client.start_workflow(
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
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerUntilDone],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        handle = await streams_client.start_workflow(
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
        producer = get_stream_handle(streams_client, ref).producer(
            producer_id="writer", attempt=1
        )
        await producer.append("a")

        async def first_progress() -> list[int]:
            return await handle.query(StreamOperationCaller.counters)

        assert await _wait_for(first_progress), "the caller never saw progress"
        await producer.append("b", "c")
        await store.close_stream(streams_client, ref, "done")
        await store.flush_notifications()

        observed = await asyncio.wait_for(handle.result(), 20)
        await streams_client.get_workflow_handle(ref.workflow_id).signal(
            OwnerUntilDone.done
        )

    assert observed.result == "done"
    assert observed.counters, observed
    assert observed.counters == sorted(set(observed.counters)), observed


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
    store = MemoryStreams().notify_on_append()
    streams_client = await connect(client, store)

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
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, StreamOwnerThatPublishes],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        caller = await streams_client.start_workflow(
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
        await streams_client.get_workflow_handle(owner_id).signal(
            StreamOwnerThatPublishes.done
        )
        await store.close_stream(streams_client, ref, "3 tokens")
        observed = await asyncio.wait_for(caller.result(), 20)

    assert observed.result == "3 tokens"
    assert observed.counters == sorted(set(observed.counters)), observed


@workflow.defn(name="StreamOwnerThatCloses")
class StreamOwnerThatCloses:
    """Publishes its tokens on its own stream, then closes it from Workflow code."""

    @workflow.run
    async def run(self, tokens: list[str]) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for token in tokens:
            writer.publish(token)
            await workflow.sleep(timedelta(milliseconds=200))
        close_workflow_stream(f"{len(tokens)} tokens", topic=TOKENS_TOPIC)
        # A second close of the topic does nothing.
        close_workflow_stream("again", topic=TOKENS_TOPIC)


async def test_a_workflow_closes_its_own_stream(
    client: Client, env: WorkflowEnvironment
) -> None:
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = MemoryStreams().notify_on_append()
    streams_client = await connect(client, store)

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
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, StreamOwnerThatCloses],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        observed = await asyncio.wait_for(
            streams_client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )
        owner_result = await streams_client.get_workflow_handle(owner_id).result()

    assert observed.result == "3 tokens"
    assert owner_result is None


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
    store = _store_for(backing)
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    streams_client = await connect(client, store)

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
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerThatClosesInOneTask],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        observed = await asyncio.wait_for(
            streams_client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )
        # The operation completed, so the store is closed and holds every
        # record: a read gets them all and ends while the owner still runs.
        values = await asyncio.wait_for(_values(streams_client, ref), 10)
        owner = streams_client.get_workflow_handle(owner_id)
        await owner.signal(OwnerThatClosesInOneTask.done)
        await owner.result()
        history = await owner.fetch_history()

    assert observed.result == "50 tokens"
    assert values == [f"t{n}" for n in range(50)]
    # The close is recorded with the task's publishes, so a replay commits the
    # same batch, with no store.
    await Replayer(workflows=[OwnerThatClosesInOneTask]).replay_workflow(history)


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_an_append_after_an_outside_close_is_refused(
    client: Client, backing: str
) -> None:
    store = _store_for(backing)
    streams_client = await connect(client, store)
    async with new_worker(streams_client, OwnerUntilDone) as worker:
        owner = await streams_client.start_workflow(
            OwnerUntilDone.run, id=f"owner-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        ref = StreamRef.for_workflow(owner.id, topic=TOKENS_TOPIC)
        await _skip_without_notifier(client, ref)
        handle = get_stream_handle(streams_client, ref)
        producer = handle.producer(producer_id="owner-activity", attempt=1)
        first = await producer.append("a")

        await store.close_stream(streams_client, ref, "done")

        with pytest.raises(StreamClosedError):
            await producer.append("b")
        # A retry of a batch that landed before the close still answers.
        retried = handle.producer(producer_id="owner-activity", attempt=1)
        assert await retried.append("a") == first
        values = await asyncio.wait_for(_values(streams_client, ref), 10)
        await owner.signal(OwnerUntilDone.done)

    assert values == ["a"]


# The store half of the close above. It needs no stream notifier, so it runs on
# any server.


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_a_close_with_the_last_publishes_closes_the_store_after_them(
    client: Client, backing: str
) -> None:
    store = _store_for(backing)
    # Without the notifier, Core closes only the store.
    store._config.notify_on_append = False
    streams_client = await connect(client, store)
    async with new_worker(streams_client, OwnerThatClosesInOneTask) as worker:
        owner = await streams_client.start_workflow(
            OwnerThatClosesInOneTask.run,
            50,
            id=f"owner-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        ref = StreamRef.for_workflow(owner.id, topic=TOKENS_TOPIC)
        # Only the store close can end this read, since the owner still runs.
        values = await asyncio.wait_for(_values(streams_client, ref), 15)
        await owner.signal(OwnerThatClosesInOneTask.done)
        await owner.result()

    assert values == [f"t{n}" for n in range(50)]


async def test_a_worker_flushes_on_stop_and_a_replay_after_it_runs(
    client: Client,
) -> None:
    # The Worker flushes its notifications when it stops. That flush ran past
    # the Worker's shutdown, and leaving `async with` used to cancel it, and
    # with it whatever the caller awaited next, here a replay.
    store = MemoryStreams().notify_on_append()
    streams_client = await connect(client, store)
    async with new_worker(streams_client, OwnerThatClosesInOneTask) as worker:
        owner = await streams_client.start_workflow(
            OwnerThatClosesInOneTask.run,
            5,
            id=f"owner-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        ref = StreamRef.for_workflow(owner.id, topic=TOKENS_TOPIC)
        await _skip_without_notifier(client, ref)
        await asyncio.wait_for(_values(streams_client, ref), 15)
        await owner.signal(OwnerThatClosesInOneTask.done)
        await owner.result()
    history = await owner.fetch_history()
    await Replayer(workflows=[OwnerThatClosesInOneTask]).replay_workflow(history)
