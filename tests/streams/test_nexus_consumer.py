"""The Nexus operation that consumes a stream through its notification channel.

The unit cases drive :class:`StreamConsumerOperation` with a client stand-in
over the memory store and the deliveries a server would post, and catch what
it posts back as completions. The live cases need a server with notification
channels and a Nexus HTTP ingress, named with ``-E host:port`` and
``TEMPORAL_HTTP``; they put the memory store behind the Nexus front, because
this layer carries no store whose producers notify a channel themselves, so
the producer notifies the stream's channel by hand the way such a store
would. The consumer's HTTP port is ``STREAM_CONSUMER_PORT``, 8813 by default.
"""

from __future__ import annotations

import asyncio
import contextlib
import email.utils
import json
import os
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import nexusrpc
import nexusrpc.handler
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from google.protobuf import json_format

import temporalio.api.notification.v1
import temporalio.converter
from temporalio import workflow
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import (
    CreateNexusEndpointRequest,
    DeleteNexusEndpointRequest,
)
from temporalio.client import Callback, Client
from temporalio.exceptions import NexusOperationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import BEGINNING, StreamProvider, StreamRecord, StreamRef
from temporalio.streams._ref import open_ref
from temporalio.streams.providers import nexus
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.streams.providers.nexus import (
    STREAM_CONSUMER_TOKEN_HEADER,
    NexusStreams,
    StreamConsumerOperation,
    TemporalStreamsHandler,
    stream_channel_name,
    stream_consumer_operation,
)
from temporalio.streams.providers.nexus_consumer_service import (
    CollectStream,
    CollectStreamHandler,
    NexusHttpService,
    _NeverCancelled,
    collect_values,
)
from temporalio.worker import Worker
from tests.helpers import assert_eventually
from tests.streams.test_nexus_provider import _own_endpoint

VALUES = "values"
LISTENER_URL = "http://127.0.0.1:8813/deliveries"
CALLBACK_URL = "http://caller.invalid/namespaces/default/nexus/callback"
CALLBACK_HEADERS = {"temporal-callback-token": "caller-token"}

_HTTP = os.environ.get("TEMPORAL_HTTP", "http://127.0.0.1:7243")
_PORT = int(os.environ.get("STREAM_CONSUMER_PORT", "8813"))


# ---------------------------------------------------------------------------
# Stand-ins.
# ---------------------------------------------------------------------------


def _as_client(stand_in: object) -> Client:
    """Pass a stand-in where a client is typed; the consumer uses only what both have."""
    return cast(Client, stand_in)


class _ClientStandIn:
    """What the consumer asks of a client: a provider, a converter, the channel calls."""

    def __init__(self, store: MemoryStreams) -> None:
        self._store = store
        self.data_converter = temporalio.converter.DataConverter.default
        self.registered: list[tuple[str, Callback]] = []
        self.unregistered: list[tuple[str, str]] = []

    def get_stream_handle(self, ref: StreamRef) -> Any:
        # The memory store opens a handle without a client.
        return open_ref(self._store, _as_client(None), ref)

    async def register_channel_listener(self, channel: str, callback: Callback) -> str:
        self.registered.append((channel, callback))
        return f"listener-{len(self.registered)}"

    async def unregister_channel_listener(self, channel: str, listener_id: str) -> None:
        self.unregistered.append((channel, listener_id))


@dataclass
class _Posted:
    url: str
    headers: dict[str, str]
    body: bytes

    @property
    def state(self) -> str:
        return self.headers["Nexus-Operation-State"]


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> list[_Posted]:
    """Catch the completions the consumer posts to the caller's callback."""
    caught: list[_Posted] = []

    def record(url: str, body: bytes, headers: Mapping[str, str], timeout: Any) -> None:
        del timeout
        caught.append(_Posted(url, dict(headers), body))

    monkeypatch.setattr(nexus, "_post_completion", record)
    return caught


def _start_context(
    callback_url: str | None = CALLBACK_URL,
) -> nexusrpc.handler.StartOperationContext:
    return nexusrpc.handler.StartOperationContext(
        service="CollectStream",
        operation="collect",
        headers={},
        request_id=uuid.uuid4().hex,
        callback_url=callback_url,
        callback_headers=dict(CALLBACK_HEADERS),
        task_cancellation=_NeverCancelled(),
    )


def _cancel_context() -> nexusrpc.handler.CancelOperationContext:
    return nexusrpc.handler.CancelOperationContext(
        service="CollectStream",
        operation="collect",
        headers={},
        task_cancellation=_NeverCancelled(),
    )


def _notification(
    channel: str, counter: int, *, position: bytes = b"", closed: bool = False
) -> bytes:
    """The body the server posts: the notification as protobuf JSON."""
    proto = temporalio.api.notification.v1.Notification(
        channel=channel, counter=counter, position=position
    )
    if closed:
        proto.metadata["closed"].CopyFrom(
            temporalio.converter.DataConverter.default.payload_converter.to_payload(
                True
            )
        )
    return json_format.MessageToJson(proto).encode()


@dataclass
class _Rig:
    store: MemoryStreams
    client: _ClientStandIn
    operation: StreamConsumerOperation[list[Any]]
    ref: StreamRef
    channel: str
    producer: Any

    async def start(self) -> str:
        result = await self.operation.start(_start_context(), self.ref)
        assert isinstance(result, nexusrpc.handler.StartOperationResultAsync)
        await self.settled(result.token)
        return result.token

    async def settled(self, token: str) -> None:
        """Wait for the read the start kicked off."""
        opening = self.operation._states[token].opening
        assert opening is not None
        await asyncio.wait_for(opening, 10)

    def headers(self, token: str) -> dict[str, str]:
        return {STREAM_CONSUMER_TOKEN_HEADER: token}

    async def deliver(self, token: str, counter: int, *, closed: bool = False) -> Any:
        return await self.operation.deliver(
            self.headers(token), _notification(self.channel, counter, closed=closed)
        )


async def _rig(
    consume: Callable[[StreamRecord[Any], list[Any]], list[Any]] = collect_values,
) -> _Rig:
    store = MemoryStreams()
    stream_id = f"consumed-{uuid.uuid4().hex}"
    handle = await store.create_standalone_stream(None, stream_id)
    ref = handle.ref(topic=VALUES)
    client = _ClientStandIn(store)
    operation = stream_consumer_operation(
        consume,
        initial=list,
        listener_url=LISTENER_URL,
        client=_as_client(client),
    )
    return _Rig(
        store,
        client,
        operation,
        ref,
        stream_channel_name(ref),
        handle.producer(topic=VALUES, producer_id="writer", attempt=1),
    )


# ---------------------------------------------------------------------------
# The helper's state machine.
# ---------------------------------------------------------------------------


def test_the_channel_name_folds_the_owner_and_the_topic():
    assert (
        stream_channel_name(StreamRef.for_standalone("s-1", topic="values"))
        == "stream:standalone:s-1:values"
    )
    assert (
        stream_channel_name(StreamRef.for_workflow("wf", topic="t"))
        == "stream:workflow:wf::t"
    )
    assert (
        stream_channel_name(StreamRef.for_workflow("wf", run_id="r", topic="t"))
        == "stream:workflow:wf:r:t"
    )
    assert (
        stream_channel_name(StreamRef.for_activity("act", workflow_id="wf", topic="t"))
        == "stream:activity:wf::act:t"
    )


async def test_start_registers_the_listener_and_reads_what_was_there(
    posted: list[_Posted],
):
    rig = await _rig()
    await rig.producer.append("a", "b")
    token = await rig.start()

    assert rig.operation.tokens == [token]
    assert rig.client.registered == [
        (
            rig.channel,
            Callback(url=LISTENER_URL, headers={STREAM_CONSUMER_TOKEN_HEADER: token}),
        )
    ]
    state = rig.operation.state(token)
    assert state is not None
    assert state.value == ["a", "b"]
    assert (state.reads, state.deliveries, state.records) == (1, 0, 2)
    assert state.cursor != BEGINNING
    assert state.listener_id == "listener-1"
    assert posted == []


async def test_a_delivery_reads_from_the_cursor_once_and_in_order(
    posted: list[_Posted],
):
    rig = await _rig()
    await rig.producer.append("a")
    token = await rig.start()

    # Records after the registration arrive on the next delivery, each once.
    await rig.producer.append("b", "c")
    delivery = await rig.deliver(token, 1)
    assert (delivery.known, delivery.read, delivery.records, delivery.completed) == (
        True,
        True,
        2,
        False,
    )
    state = rig.operation.state(token)
    assert state is not None and state.value == ["a", "b", "c"]

    # A delivery that brings nothing new reads nothing.
    delivery = await rig.deliver(token, 1)
    assert (delivery.read, delivery.records) == (False, 0)
    state = rig.operation.state(token)
    assert state is not None and (state.reads, state.deliveries) == (2, 2)

    # A burst the channel folded into one delivery costs one read.
    await rig.producer.append("d", "e", "f")
    delivery = await rig.operation.deliver(
        {STREAM_CONSUMER_TOKEN_HEADER.lower(): token}, _notification(rig.channel, 4)
    )
    assert (delivery.read, delivery.records) == (True, 3)
    state = rig.operation.state(token)
    assert state is not None
    assert state.value == ["a", "b", "c", "d", "e", "f"]
    assert (state.reads, state.deliveries, state.records) == (3, 3, 6)
    assert posted == []


async def test_the_close_completes_through_the_callers_callback(
    posted: list[_Posted],
):
    rig = await _rig()
    await rig.producer.append("a")
    token = await rig.start()
    await rig.producer.append("b")

    delivery = await rig.deliver(token, 2, closed=True)
    assert (delivery.read, delivery.records, delivery.completed) == (True, 1, True)
    [completion] = posted
    assert completion.url == CALLBACK_URL
    assert completion.state == "succeeded"
    assert completion.headers["temporal-callback-token"] == "caller-token"
    assert completion.headers["Nexus-Operation-Token"] == token
    assert completion.headers["Content-Type"] == "application/json"
    assert email.utils.parsedate_to_datetime(
        completion.headers["Nexus-Operation-Start-Time"]
    )
    assert json.loads(completion.body) == ["a", "b"]
    assert rig.client.unregistered == [(rig.channel, "listener-1")]
    assert rig.operation.state(token) is None
    assert rig.operation.tokens == []

    # Once complete, the token is unknown and a late delivery is ignored.
    delivery = await rig.deliver(token, 3)
    assert (delivery.known, delivery.read) == (False, False)
    assert len(posted) == 1


async def test_a_finish_record_closes_the_operation(posted: list[_Posted]):
    rig = await _rig()
    token = await rig.start()
    await rig.producer.append("only")
    await rig.producer.finish()

    delivery = await rig.deliver(token, 1)
    assert delivery.completed
    [completion] = posted
    assert completion.state == "succeeded"
    assert json.loads(completion.body) == ["only"]
    assert rig.client.unregistered == [(rig.channel, "listener-1")]


async def test_a_stream_finished_before_the_start_completes_at_once(
    posted: list[_Posted],
):
    rig = await _rig()
    await rig.producer.append("a")
    await rig.producer.finish()
    token = await rig.start()
    assert rig.operation.state(token) is None
    [completion] = posted
    assert completion.state == "succeeded"
    assert json.loads(completion.body) == ["a"]


async def test_cancel_unregisters_and_reports_canceled(posted: list[_Posted]):
    rig = await _rig()
    token = await rig.start()

    await rig.operation.cancel(_cancel_context(), token)
    assert rig.client.unregistered == [(rig.channel, "listener-1")]
    [completion] = posted
    assert completion.state == "canceled"
    assert completion.headers["Nexus-Operation-Token"] == token
    assert completion.headers["Content-Type"] == "application/json"
    assert "canceled" in json.loads(completion.body)["message"]
    assert rig.operation.state(token) is None

    with pytest.raises(nexusrpc.HandlerError) as unknown:
        await rig.operation.cancel(_cancel_context(), token)
    assert unknown.value.type is nexusrpc.HandlerErrorType.NOT_FOUND


async def test_a_delivery_for_an_operation_not_held_here_is_ignored(
    posted: list[_Posted],
):
    rig = await _rig()
    await rig.producer.append("a")
    delivery = await rig.operation.deliver({}, _notification(rig.channel, 1))
    assert (delivery.token, delivery.known) == (None, False)
    delivery = await rig.deliver("nobody", 1)
    assert (delivery.token, delivery.known, delivery.read) == ("nobody", False, False)
    assert posted == []


async def test_a_consume_failure_fails_the_operation(posted: list[_Posted]):
    def fussy(record: StreamRecord[Any], values: list[Any]) -> list[Any]:
        if record.value == "bad":
            raise ValueError("cannot take bad")
        return collect_values(record, values)

    rig = await _rig(fussy)
    token = await rig.start()
    await rig.producer.append("good", "bad", "later")
    delivery = await rig.deliver(token, 1)
    assert (delivery.records, delivery.completed) == (1, False)
    [completion] = posted
    assert completion.state == "failed"
    assert json.loads(completion.body)["message"] == "ValueError: cannot take bad"
    assert rig.client.unregistered == [(rig.channel, "listener-1")]
    assert rig.operation.state(token) is None


async def test_a_read_failure_leaves_the_operation_for_the_retry(
    posted: list[_Posted], monkeypatch: pytest.MonkeyPatch
):
    rig = await _rig()
    token = await rig.start()
    await rig.producer.append("a")
    handle = rig.client.get_stream_handle

    def broken(ref: StreamRef) -> Any:
        del ref
        raise RuntimeError("the store is away")

    monkeypatch.setattr(rig.client, "get_stream_handle", broken)
    with pytest.raises(RuntimeError, match="away"):
        await rig.deliver(token, 1)
    monkeypatch.setattr(rig.client, "get_stream_handle", handle)
    delivery = await rig.deliver(token, 1)
    assert (delivery.read, delivery.records) == (True, 1)
    assert posted == []


async def test_close_lets_go_of_every_listener_without_completing(
    posted: list[_Posted],
):
    rig = await _rig()
    first = await rig.start()
    second = await rig.start()
    assert rig.operation.tokens == [first, second]
    await rig.operation.close()
    assert rig.operation.tokens == []
    assert rig.client.unregistered == [
        (rig.channel, "listener-1"),
        (rig.channel, "listener-2"),
    ]
    assert posted == []


async def test_a_null_result_travels_without_a_content_type(posted: list[_Posted]):
    def nothing(record: StreamRecord[Any], value: None) -> None:
        del record
        return value

    store = MemoryStreams()
    handle = await store.create_standalone_stream(None, "silent")
    ref = handle.ref(topic=VALUES)
    client = _ClientStandIn(store)
    operation = stream_consumer_operation(
        nothing,
        initial=lambda: None,
        listener_url=LISTENER_URL,
        client=_as_client(client),
    )
    result = await operation.start(_start_context(), ref)
    assert isinstance(result, nexusrpc.handler.StartOperationResultAsync)
    await handle.producer(topic=VALUES, producer_id="w", attempt=1).finish()
    await operation.deliver(
        {STREAM_CONSUMER_TOKEN_HEADER: result.token},
        _notification(stream_channel_name(ref), 1),
    )
    [completion] = posted
    assert completion.state == "succeeded"
    assert "Content-Type" not in completion.headers
    assert completion.body == b""


async def test_the_listener_url_must_be_given():
    with pytest.raises(ValueError, match="listener_url"):
        stream_consumer_operation(collect_values, initial=list, listener_url="")


# ---------------------------------------------------------------------------
# The standalone service speaks the Nexus HTTP protocol.
# ---------------------------------------------------------------------------


async def test_the_service_speaks_the_nexus_http_protocol(posted: list[_Posted]):
    rig = await _rig()
    service = NexusHttpService(
        nexusrpc.handler.Handler([CollectStreamHandler(rig.operation)]), [rig.operation]
    )
    converter = temporalio.converter.DataConverter.default.payload_converter
    ref_body = converter.to_payload(rig.ref).data
    async with TestClient(TestServer(service.application())) as http:
        # A start carries the completion callback in the query and its
        # headers under the callback prefix; an asynchronous start is a 201.
        answer = await http.post(
            "/CollectStream/collect",
            params={"callback": CALLBACK_URL},
            data=ref_body,
            headers={
                "Content-Type": "application/json",
                "Nexus-Request-Id": "req-1",
                "Nexus-Callback-Temporal-Callback-Token": "caller-token",
                "Request-Timeout": "9.5s",
            },
        )
        assert answer.status == 201
        token = (await answer.json())["token"]
        assert rig.operation.tokens == [token]
        held = rig.operation._states[token]
        assert held.callback_url == CALLBACK_URL
        assert held.callback_headers == {"temporal-callback-token": "caller-token"}
        await rig.settled(token)

        # The delivery route reaches the operation the token names.
        await rig.producer.append("a")
        answer = await http.post(
            service.deliveries_path,
            data=_notification(rig.channel, 1),
            headers={
                "Content-Type": "application/json",
                "Temporal-Notification-Channel": rig.channel,
                STREAM_CONSUMER_TOKEN_HEADER: token,
            },
        )
        assert answer.status == 200
        state = rig.operation.state(token)
        assert state is not None and state.value == ["a"]

        # A cancel names the token in its header and is accepted; a second one
        # finds nothing and says so.
        answer = await http.post(
            "/CollectStream/collect/cancel", headers={"Nexus-Operation-Token": token}
        )
        assert answer.status == 202
        assert [p.state for p in posted] == ["canceled"]
        answer = await http.post(
            "/CollectStream/collect/cancel", headers={"Nexus-Operation-Token": token}
        )
        assert answer.status == 404
        assert "token" in (await answer.json())["message"]

        # What is not a stream reference is the caller's fault.
        answer = await http.post(
            "/CollectStream/collect",
            data=b'{"kind": "nowhere"}',
            headers={"Content-Type": "application/json"},
        )
        assert answer.status == 400
        answer = await http.post("/Other/op", data=b"{}")
        assert answer.status == 404


# ---------------------------------------------------------------------------
# Live: an external stream whose producer notifies the stream's channel.
# ---------------------------------------------------------------------------


@dataclass
class ConsumeInput:
    """Which endpoint and service to call, which stream to hand it, and whether to cancel."""

    endpoint: str
    service: str
    ref: StreamRef
    cancel: bool = False


@workflow.defn
class ConsumeCaller:
    """Starts the consumer operation with a ref and awaits its result."""

    def __init__(self) -> None:
        self._go = False

    @workflow.signal
    def go(self) -> None:
        """Let a cancelling run cancel now."""
        self._go = True

    @workflow.run
    async def run(self, input: ConsumeInput) -> Any:
        """Return the operation's result, or ``"canceled"`` after a cancel."""
        client = workflow.create_nexus_client(
            service=input.service, endpoint=input.endpoint
        )
        handle = await client.start_operation("collect", input.ref, output_type=list)
        if not input.cancel:
            return await handle
        await workflow.wait_condition(lambda: self._go)
        handle.cancel()
        try:
            await handle
        except NexusOperationError:
            return "canceled"
        return "completed"


@contextlib.asynccontextmanager
async def _own_external_endpoint(
    client: Client, name: str, url: str
) -> AsyncIterator[str]:
    """Register a Nexus endpoint whose target is ``url`` for one test's life."""
    created = await client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=name,
                target=EndpointTarget(external=EndpointTarget.External(url=url)),
            )
        )
    )
    try:
        yield name
    finally:
        await client.operator_service.delete_nexus_endpoint(
            DeleteNexusEndpointRequest(
                id=created.endpoint.id, version=created.endpoint.version
            )
        )


@dataclass
class _Live:
    store: StreamProvider
    fronted: Client
    consumer: StreamConsumerOperation[list[Any]]
    endpoint: str
    service: str


@contextlib.asynccontextmanager
async def _consumer_service(
    client: Client, store: StreamProvider | None = None
) -> AsyncIterator[_Live]:
    """Stand up the front over ``store`` and the standalone consumer behind an endpoint."""
    store = store or MemoryStreams()
    uid = uuid.uuid4().hex
    stream_handler = TemporalStreamsHandler(store, client)
    handler_tq = f"streams-{uid}"
    async with (
        _own_endpoint(client, f"streams-{uid}", handler_tq) as front_endpoint,
        Worker(client, task_queue=handler_tq, nexus_service_handlers=[stream_handler]),
    ):
        front = NexusStreams(
            endpoint=front_endpoint, http_address=_HTTP, read_wait=timedelta(seconds=2)
        )
        config = client.config()
        config["plugins"] = [front]
        fronted = Client(**config)
        consumer = stream_consumer_operation(
            collect_values,
            initial=list,
            listener_url=f"http://127.0.0.1:{_PORT}/deliveries",
            client=fronted,
        )
        service = NexusHttpService(
            nexusrpc.handler.Handler([CollectStreamHandler(consumer)]),
            [consumer],
            data_converter=fronted.data_converter,
        )
        runner = web.AppRunner(service.application())
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", _PORT).start()
        try:
            async with _own_external_endpoint(
                client, f"consumers-{uid}", f"http://127.0.0.1:{_PORT}"
            ) as endpoint:
                yield _Live(store, fronted, consumer, endpoint, CollectStream.__name__)
        finally:
            await consumer.close()
            await runner.cleanup()
            await stream_handler.close()


async def _listener_token(client: Client, channel: str) -> str:
    """The operation token the one callback listener on ``channel`` carries."""

    async def registered() -> str:
        # The channel comes into being with the registration, so a describe
        # that races it is answered with not found.
        try:
            description = await client.describe_channel(channel)
        except RPCError as err:
            assert err.status != RPCStatusCode.NOT_FOUND, "channel not created yet"
            raise
        assert len(description.listeners) == 1, description.listeners
        [listener] = description.listeners
        assert listener.callback is not None
        # The server reports the registration's headers in lower case.
        headers = {
            key.lower(): value for key, value in listener.callback.headers.items()
        }
        return headers[STREAM_CONSUMER_TOKEN_HEADER.lower()]

    return await assert_eventually(registered, timeout=timedelta(seconds=30))


async def _consumed(
    consumer: StreamConsumerOperation[list[Any]], token: str, count: int
) -> None:
    async def reached() -> None:
        state = consumer.state(token)
        assert state is not None and state.records == count, state

    await assert_eventually(reached, timeout=timedelta(seconds=30))


@pytest.mark.needs_channel_server
async def test_a_standalone_service_consumes_an_external_stream_through_its_channel(
    client: Client,
):
    store = MemoryStreams()
    stream_id = f"consumed-{uuid.uuid4().hex}"
    handle = await store.create_standalone_stream(None, stream_id)
    ref = handle.ref(topic=VALUES)
    channel = stream_channel_name(ref)
    producer = handle.producer(topic=VALUES, producer_id="writer", attempt=1)

    async with _consumer_service(client, store) as live:
        # Written before anyone listens: the channel keeps the notification,
        # and the registration is handed it once.
        await producer.append("a", "b")
        assert await client.notify_channel(channel, position=b"2", counter=2) == 0

        async with Worker(
            client, task_queue=f"callers-{stream_id}", workflows=[ConsumeCaller]
        ) as worker:
            run = await client.start_workflow(
                ConsumeCaller.run,
                ConsumeInput(live.endpoint, live.service, ref),
                id=f"caller-{stream_id}",
                task_queue=worker.task_queue,
            )
            token = await _listener_token(client, channel)
            await _consumed(live.consumer, token, 2)

            # Written after the registration: each notification is delivered
            # and read from the cursor; the retained one brought nothing new.
            await producer.append("c")
            assert await client.notify_channel(channel, position=b"3", counter=3) == 1
            await _consumed(live.consumer, token, 3)
            await producer.append("d", "e", "f")
            for counter in (4, 5, 6):
                await client.notify_channel(
                    channel, position=b"%d" % counter, counter=counter
                )
            await _consumed(live.consumer, token, 6)
            state = live.consumer.state(token)
            assert state is not None
            assert state.value == ["a", "b", "c", "d", "e", "f"]
            # Every read moved the cursor, none repeated a record, and a burst
            # of three cost at most three reads.
            assert state.reads <= 1 + 1 + 3
            assert state.deliveries >= 2

            # The close completes the operation with what was consumed.
            await producer.append("g")
            await producer.finish()
            await client.notify_channel(
                channel, position=b"7", counter=7, metadata={"closed": True}
            )
            assert await asyncio.wait_for(run.result(), 60) == [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
            ]
            assert live.consumer.state(token) is None
            assert (await client.describe_channel(channel)).listeners == []


@pytest.mark.needs_channel_server
async def test_cancel_unregisters_the_listener(client: Client):
    store = MemoryStreams()
    stream_id = f"consumed-{uuid.uuid4().hex}"
    handle = await store.create_standalone_stream(None, stream_id)
    ref = handle.ref(topic=VALUES)
    channel = stream_channel_name(ref)

    async with (
        _consumer_service(client, store) as live,
        Worker(
            client, task_queue=f"callers-{stream_id}", workflows=[ConsumeCaller]
        ) as worker,
    ):
        run = await client.start_workflow(
            ConsumeCaller.run,
            ConsumeInput(live.endpoint, live.service, ref, cancel=True),
            id=f"caller-{stream_id}",
            task_queue=worker.task_queue,
        )
        token = await _listener_token(client, channel)
        await run.signal(ConsumeCaller.go)
        assert await asyncio.wait_for(run.result(), 60) == "canceled"
        assert live.consumer.state(token) is None
        assert (await client.describe_channel(channel)).listeners == []


# ---------------------------------------------------------------------------
# Live: the handler hosted in a Temporal worker, deliveries through the
# frontend's Nexus route of a companion operation.
# ---------------------------------------------------------------------------


@nexusrpc.service
class CollectInWorker:
    """The consumer next to the synchronous operation the channel posts to."""

    collect: nexusrpc.Operation[StreamRef, list[Any]]
    deliver: nexusrpc.Operation[dict[str, Any], None]


@nexusrpc.handler.service_handler(service=CollectInWorker)
class CollectInWorkerHandler:
    """Hosts the consumer in a worker; ``deliver`` is the listener's URL target."""

    def __init__(self, consumer: StreamConsumerOperation[list[Any]]) -> None:
        self._consumer = consumer

    @nexusrpc.handler.operation_handler
    def collect(self) -> nexusrpc.handler.OperationHandler[StreamRef, list[Any]]:
        return self._consumer

    @nexusrpc.handler.sync_operation
    async def deliver(
        self, ctx: nexusrpc.handler.StartOperationContext, input: dict[str, Any]
    ) -> None:
        # The server's delivery arrives as a start of this operation: the
        # notification is the input and the listener's headers are the
        # request's.
        await self._consumer.deliver(ctx.headers, input)


@pytest.mark.needs_channel_server
async def test_a_worker_hosted_consumer_is_reached_through_the_frontend(
    client: Client,
):
    """The frontend takes the channel's post as a start of the companion operation.

    What the worker-hosted shape still lacks is the completion: the start
    context a worker hands the handler names ``temporal://system`` as the
    callback, which only a workflow or activity run the server completes
    can reach, not an HTTP post. So this case checks the deliveries and
    stops before the close; the completion through a run is a follow-up.
    """
    store = MemoryStreams()
    stream_id = f"consumed-{uuid.uuid4().hex}"
    handle = await store.create_standalone_stream(None, stream_id)
    ref = handle.ref(topic=VALUES)
    channel = stream_channel_name(ref)
    producer = handle.producer(topic=VALUES, producer_id="writer", attempt=1)
    uid = uuid.uuid4().hex
    stream_handler = TemporalStreamsHandler(store, client)

    async with (
        _own_endpoint(client, f"streams-{uid}", f"streams-{uid}") as front_endpoint,
        Worker(
            client, task_queue=f"streams-{uid}", nexus_service_handlers=[stream_handler]
        ),
    ):
        front = NexusStreams(
            endpoint=front_endpoint, http_address=_HTTP, read_wait=timedelta(seconds=2)
        )
        config = client.config()
        config["plugins"] = [front]
        fronted = Client(**config)
        # The listener's URL is the frontend's route to the companion
        # operation, so the endpoint has to exist before the consumer does.
        created = await client.operator_service.create_nexus_endpoint(
            CreateNexusEndpointRequest(
                spec=EndpointSpec(
                    name=f"hosted-{uid}",
                    target=EndpointTarget(
                        worker=EndpointTarget.Worker(
                            namespace=client.namespace, task_queue=f"hosted-{uid}"
                        )
                    ),
                )
            )
        )
        consumer = stream_consumer_operation(
            collect_values,
            initial=list,
            listener_url=(
                f"{_HTTP}/nexus/endpoints/{created.endpoint.id}/services/"
                f"{CollectInWorker.__name__}/deliver"
            ),
        )
        try:
            async with (
                Worker(
                    fronted,
                    task_queue=f"hosted-{uid}",
                    nexus_service_handlers=[CollectInWorkerHandler(consumer)],
                ),
                Worker(
                    client, task_queue=f"callers-{uid}", workflows=[ConsumeCaller]
                ) as callers,
            ):
                await producer.append("a")
                run = await client.start_workflow(
                    ConsumeCaller.run,
                    ConsumeInput(f"hosted-{uid}", CollectInWorker.__name__, ref),
                    id=f"caller-{uid}",
                    task_queue=callers.task_queue,
                )
                token = await _listener_token(client, channel)
                await _consumed(consumer, token, 1)
                await producer.append("b", "c")
                assert (
                    await client.notify_channel(channel, position=b"3", counter=3) == 1
                )
                await _consumed(consumer, token, 3)
                state = consumer.state(token)
                assert state is not None
                assert state.value == ["a", "b", "c"]
                assert state.deliveries >= 1
                held = consumer._states[token]
                assert held.callback_url is not None
                assert held.callback_url.startswith("temporal://"), held.callback_url
                await run.terminate()
        finally:
            await consumer.close()
            await stream_handler.close()
            await client.operator_service.delete_nexus_endpoint(
                DeleteNexusEndpointRequest(
                    id=created.endpoint.id, version=created.endpoint.version
                )
            )


# ---------------------------------------------------------------------------
# Live, waiting for a server: a native standalone stream notifies its own
# channel, so nobody notifies by hand.
# ---------------------------------------------------------------------------


@pytest.mark.needs_stream_channel_server
async def test_a_native_standalone_stream_is_consumed_through_its_channel(
    client: Client,
):
    provider: StreamProvider | None = client.config().get("stream_provider")
    assert provider is not None, "the client needs the native provider registered"
    stream_id = f"consumed-{uuid.uuid4().hex}"
    handle = await client.create_stream(stream_id)
    ref = handle.ref(topic=VALUES)
    producer = handle.producer(topic=VALUES, producer_id="writer", attempt=1)

    async with _consumer_service(client, provider) as live:
        await producer.append("a", "b")
        async with Worker(
            client, task_queue=f"callers-{stream_id}", workflows=[ConsumeCaller]
        ) as worker:
            run = await client.start_workflow(
                ConsumeCaller.run,
                ConsumeInput(live.endpoint, live.service, ref),
                id=f"caller-{stream_id}",
                task_queue=worker.task_queue,
            )
            token = await _listener_token(client, stream_channel_name(ref))
            await _consumed(live.consumer, token, 2)
            # The server notifies the stream's channel on every append and on
            # the close, so the records reach the consumer without a notify.
            await producer.append("c")
            await _consumed(live.consumer, token, 3)
            await producer.finish()
            await handle.close()
            assert await asyncio.wait_for(run.result(), 60) == ["a", "b", "c"]
            assert (
                await client.describe_channel(stream_channel_name(ref))
            ).listeners == []
