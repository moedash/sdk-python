"""The stream service, served by a Worker and called over the Nexus HTTP ingress.

Every case runs against the stock dev server, started here with an HTTP port,
through a Nexus endpoint the case creates and deletes, so the request goes
through the same ingress a remote caller's does. The cases follow the
conformance suite where the service passes a provider's behavior through, and
pin what only the service decides: subscriptions per reader, idle expiry,
error mapping and the wire form of records.

The cases are parametrized over ``BACKINGS``, the providers the service runs
over. Each yields a :class:`Backing`.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import nexusrpc
import pytest
import pytest_asyncio

import temporalio.api.nexus.v1
import temporalio.api.operatorservice.v1
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    RecordKind,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamProvider,
    StreamRef,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    AppendInput,
    AppendOutput,
    HTTPStatusError,
    ReadInput,
    ReadOutput,
    TemporalStreams,
    TemporalStreamsHandler,
    TemporalStreamsHttpClient,
)
from temporalio.contrib.streams.nexus._generated import client as generated_client
from temporalio.contrib.streams.nexus._handler import _handler_error
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import PollerBehaviorSimpleMaximum, Worker
from tests import DEV_SERVER_DOWNLOAD_VERSION
from tests.helpers import find_free_port, new_worker

_SERVICE = "temporal.sdk.streams.v1.TemporalStreams"


@dataclass
class Server:
    """The dev server these cases share, and its Nexus HTTP ingress."""

    env: WorkflowEnvironment
    http: str

    @property
    def client(self) -> Client:
        return self.env.client


@pytest_asyncio.fixture(scope="module")  # type: ignore[reportUntypedFunctionDecorator]
async def server() -> AsyncIterator[Server]:
    port = find_free_port()
    env = await WorkflowEnvironment.start_local(
        dev_server_extra_args=[
            "--http-port",
            str(port),
            "--dynamic-config-value",
            'system.system.refreshNexusEndpointsMinWait="0s"',
        ],
        dev_server_download_version=DEV_SERVER_DOWNLOAD_VERSION,
    )
    try:
        yield Server(env, f"http://127.0.0.1:{port}")
    finally:
        await env.shutdown()


@dataclass
class Backing:
    """One provider the service runs over, and what the cases may ask of it."""

    name: str
    provider: StreamProvider
    truncate: Callable[[str, str, int], Awaitable[None]] | None = None
    """Drops all but the newest records of a Workflow's topic, standing in
    for retention, or ``None`` when the provider offers no way to."""
    host: Callable[[str], Awaitable[None]] | None = None
    """Starts the Workflow that owns a stream, when the store needs the owner
    to exist before a stream is written."""


@workflow.defn
class StreamOwner:
    """Owns a stream until it is told to finish."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@asynccontextmanager
async def _memory_backing(client: Client) -> AsyncIterator[Backing]:
    provider = MemoryStreams()

    async def truncate(workflow_id: str, name: str, keep: int) -> None:
        provider.truncate(workflow_id, name, keep=keep, namespace=client.namespace)

    yield Backing("memory", provider, truncate=truncate)
    await provider.close()


BACKINGS: dict[str, Callable[[Client], Any]] = {"memory": _memory_backing}


@dataclass
class Service:
    """A handler served by a Worker, and a caller on its endpoint."""

    caller: TemporalStreamsHttpClient
    handler: TemporalStreamsHandler
    backing: Backing
    client: Client
    endpoint: str


@asynccontextmanager
async def serve(
    server: Server,
    backing: Backing,
    *,
    client: Client | None = None,
    **handler_options: Any,
) -> AsyncIterator[Service]:
    """Serve ``backing`` behind a fresh endpoint, deleted on the way out.

    The handler's Worker runs on ``client``, the server's own by default.
    """
    handler = TemporalStreamsHandler(backing.provider, **handler_options)
    task_queue = f"streams-service-{uuid.uuid4().hex}"
    endpoint_name = f"streams-{uuid.uuid4().hex}"
    worker_client = server.client if client is None else client
    async with Worker(
        worker_client,
        task_queue=task_queue,
        nexus_service_handlers=[handler],
        # As the handler recommends: with the default five pollers over the
        # dev server's partitions, an append behind parked reads was seen to
        # wait for one of them to finish.
        nexus_task_poller_behavior=PollerBehaviorSimpleMaximum(20),
    ):
        created = await server.client.operator_service.create_nexus_endpoint(
            temporalio.api.operatorservice.v1.CreateNexusEndpointRequest(
                spec=temporalio.api.nexus.v1.EndpointSpec(
                    name=endpoint_name,
                    target=temporalio.api.nexus.v1.EndpointTarget(
                        worker=temporalio.api.nexus.v1.EndpointTarget.Worker(
                            namespace=server.client.namespace,
                            task_queue=task_queue,
                        )
                    ),
                )
            )
        )
        endpoint = created.endpoint
        try:
            yield Service(
                TemporalStreamsHttpClient(
                    f"{server.http}/nexus/endpoints/{endpoint.id}/services/{_SERVICE}"
                ),
                handler,
                backing,
                worker_client,
                endpoint_name,
            )
        finally:
            await server.client.operator_service.delete_nexus_endpoint(
                temporalio.api.operatorservice.v1.DeleteNexusEndpointRequest(
                    id=endpoint.id, version=endpoint.version
                )
            )
            await handler.close()


@pytest_asyncio.fixture(params=sorted(BACKINGS))  # type: ignore[reportUntypedFunctionDecorator]
async def backing(
    request: pytest.FixtureRequest, server: Server
) -> AsyncIterator[Backing]:
    async with BACKINGS[request.param](server.client) as found:
        yield found


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def service(server: Server, backing: Backing) -> AsyncIterator[Service]:
    async with serve(server, backing) as found:
        yield found


async def stream_of(service: Service, topic: str = "out") -> StreamRef:
    """A fresh stream, its owner started when the backing needs one."""
    workflow_id = f"streams-service-{uuid.uuid4().hex}"
    if service.backing.host is not None:
        await service.backing.host(workflow_id)
    return StreamRef.for_workflow(workflow_id, topic=topic)


def body(text: str) -> bytes:
    """A serialized payload, as a writer's converter and codec leave it."""
    return Payload(
        metadata={"encoding": b"binary/test"}, data=text.encode()
    ).SerializeToString()


def records_of(answer: ReadOutput) -> list[WireRecord]:
    return [WireRecord.FromString(record.record) for record in answer.records]


def texts(answer: ReadOutput) -> list[str]:
    return [record.body.data.decode() for record in records_of(answer)]


async def append(
    service: Service,
    ref: StreamRef,
    *texts_: str,
    producer_id: str = "p",
    attempt: int = 1,
    sequence: int = 1,
) -> AppendOutput:
    return await service.caller.append(
        AppendInput(
            stream=ref,
            producer_id=producer_id,
            attempt=attempt,
            sequence=sequence,
            payloads=[body(text) for text in texts_],
        )
    )


async def read(
    service: Service,
    ref: StreamRef,
    after: str = "",
    *,
    wait_ms: int = 2000,
    max_records: int | None = None,
) -> ReadOutput:
    return await service.caller.read(
        ReadInput(
            stream=ref, after_token=after, wait_ms=wait_ms, max_records=max_records
        )
    )


def refused_as(error: HTTPStatusError) -> str:
    """The stream error class the handler refused the call with.

    It leads the message and is the cause's application error type, and the
    two have to agree.
    """
    failure = json.loads(error.detail)
    cause_type = failure["details"]["cause"]["applicationFailureInfo"]["type"]
    assert failure["message"].startswith(f"{cause_type}: "), failure["message"]
    return cause_type


async def test_an_append_reads_back_as_the_stored_record(service: Service):
    ref = await stream_of(service)
    landed = await append(service, ref, "one", "two")
    answer = await read(service, ref)
    stored = records_of(answer)
    assert [
        (r.topic, r.kind, r.producer_id, r.attempt, r.sequence) for r in stored
    ] == [
        ("out", RecordKind.DATA, "p", 1, 1),
        ("out", RecordKind.DATA, "p", 1, 2),
    ]
    # With no codec on the handler, the body comes back as the writer sent it.
    assert stored[0].body == Payload.FromString(body("one"))
    assert answer.records[-1].token == landed.cursor
    assert answer.next_token == landed.cursor
    assert answer.done is False


async def test_a_batch_lands_in_order_and_a_cursor_resumes_strictly_after(
    service: Service,
):
    ref = await stream_of(service)
    first = await append(service, ref, "one")
    await append(service, ref, "two", "three", sequence=2)
    assert texts(await read(service, ref, first.cursor)) == ["two", "three"]
    answer = await read(service, ref)
    # A cursor a reader saw resumes the same way as one an append returned.
    resumed = await read(service, ref, answer.records[1].token)
    assert texts(resumed) == ["three"]
    assert [r.sequence for r in records_of(answer)] == [1, 2, 3]


async def test_max_records_caps_an_answer_and_the_next_call_continues(
    service: Service,
):
    ref = await stream_of(service)
    await append(service, ref, "a", "b", "c", "d", "e")
    first = await read(service, ref, max_records=2)
    second = await read(service, ref, first.next_token, max_records=2)
    third = await read(service, ref, second.next_token, max_records=2)
    assert [texts(first), texts(second), texts(third)] == [
        ["a", "b"],
        ["c", "d"],
        ["e"],
    ]


async def test_a_read_that_collects_nothing_answers_with_the_callers_cursor(
    service: Service,
):
    ref = await stream_of(service)
    landed = await append(service, ref, "one")
    answer = await read(service, ref, landed.cursor, wait_ms=200)
    assert answer.records == []
    assert answer.next_token == landed.cursor
    assert answer.done is False


async def test_a_parked_read_answers_when_a_record_arrives(service: Service):
    ref = await stream_of(service)
    landed = await append(service, ref, "one")
    parked = asyncio.ensure_future(read(service, ref, landed.cursor, wait_ms=10000))
    await asyncio.sleep(0.3)
    assert not parked.done()
    await append(service, ref, "two", sequence=2)
    answer = await asyncio.wait_for(parked, 5)
    assert texts(answer) == ["two"]


async def test_latest_only_positions_a_reader_at_the_end(service: Service):
    ref = await stream_of(service)
    empty = await service.caller.read(ReadInput(stream=ref, latest_only=True))
    assert (empty.records, empty.next_token) == ([], "")
    landed = await append(service, ref, "one")
    latest = await service.caller.read(ReadInput(stream=ref, latest_only=True))
    assert latest.next_token == landed.cursor
    await append(service, ref, "two", sequence=2)
    assert texts(await read(service, ref, latest.next_token)) == ["two"]
    with pytest.raises(HTTPStatusError) as refused:
        await service.caller.read(
            ReadInput(stream=ref, latest_only=True, after_token=landed.cursor)
        )
    assert refused.value.status == 400
    assert refused_as(refused.value) == "ValueError"


async def test_a_retried_append_is_written_once(service: Service):
    ref = await stream_of(service)
    first = await append(service, ref, "one", "two")
    # The caller lost the answer and sends the same call again.
    assert await append(service, ref, "one", "two") == first
    answer = await read(service, ref)
    assert texts(answer) == ["one", "two"]
    assert texts(await read(service, ref, first.cursor, wait_ms=200)) == []


async def test_a_divergent_or_stale_retry_is_refused(service: Service):
    ref = await stream_of(service)
    await append(service, ref, "one")
    with pytest.raises(HTTPStatusError) as divergent:
        await append(service, ref, "different")
    assert divergent.value.status == 400
    assert not divergent.value.retryable
    assert refused_as(divergent.value) == "StreamProducerError"
    await append(service, ref, "two", sequence=2)
    with pytest.raises(HTTPStatusError) as stale:
        await append(service, ref, "one")
    assert refused_as(stale.value) == "StreamProducerError"
    assert texts(await read(service, ref)) == ["one", "two"]


async def test_a_new_attempt_crosses_without_a_supersession_record(service: Service):
    ref = await stream_of(service)
    await append(service, ref, "old", producer_id="model")
    await append(service, ref, "new", producer_id="model", attempt=2)
    stored = records_of(await read(service, ref))
    # A reader synthesizes the supersession from the attempts it sees.
    assert [(r.kind, r.attempt) for r in stored] == [
        (RecordKind.DATA, 1),
        (RecordKind.DATA, 2),
    ]


async def test_finish_is_a_call_of_its_own(service: Service):
    ref = await stream_of(service)
    await append(service, ref, "one")
    finished = await service.caller.append(
        AppendInput(stream=ref, producer_id="p", attempt=1, sequence=2, finish=True)
    )
    stored = records_of(await read(service, ref))
    assert [(r.kind, r.sequence) for r in stored] == [
        (RecordKind.DATA, 1),
        (RecordKind.FINISH, 2),
    ]
    assert not stored[1].HasField("body")
    for payloads, finish in (([body("x")], True), ([], False)):
        with pytest.raises(HTTPStatusError) as refused:
            await service.caller.append(
                AppendInput(
                    stream=ref,
                    producer_id="p",
                    attempt=1,
                    sequence=3,
                    payloads=payloads,
                    finish=finish,
                )
            )
        assert refused_as(refused.value) == "ValueError"
    assert finished.cursor == (await read(service, ref)).next_token


async def test_a_foreign_cursor_is_refused(service: Service):
    ref = await stream_of(service)
    other = await stream_of(service)
    await append(service, ref, "one")
    theirs = await append(service, other, "one")
    for cursor in (theirs.cursor, "elsewhere:0000abcd:1", "not a cursor"):
        with pytest.raises(HTTPStatusError) as refused:
            await read(service, ref, cursor)
        assert refused.value.status == 400
        assert not refused.value.retryable
        assert refused_as(refused.value) == "StreamCursorError"
    # Another topic of the same owner is another stream too.
    with pytest.raises(HTTPStatusError) as refused:
        await read(service, ref.with_topic("other"), theirs.cursor)
    assert refused_as(refused.value) == "StreamCursorError"


async def test_an_expired_cursor_is_told_apart(service: Service):
    if service.backing.truncate is None:
        pytest.skip(f"{service.backing.name} offers no way to drop records")
    ref = await stream_of(service)
    old = await append(service, ref, "one")
    await append(service, ref, "two", "three", sequence=2)
    await service.backing.truncate(ref.workflow_id, ref.topic, 1)
    with pytest.raises(HTTPStatusError) as refused:
        await read(service, ref, old.cursor)
    assert refused_as(refused.value) == "StreamExpiredError"


async def test_argument_mistakes_are_bad_requests(service: Service):
    ref = await stream_of(service)
    with pytest.raises(HTTPStatusError) as refused:
        await append(service, ref, "one", producer_id="")
    assert refused.value.status == 400
    assert refused_as(refused.value) == "ValueError"
    with pytest.raises(HTTPStatusError) as refused:
        await service.caller.append(
            AppendInput(
                stream=ref, producer_id="p", attempt=1, sequence=1, payloads=[b"\xff"]
            )
        )
    assert refused_as(refused.value) == "ValueError"


async def test_a_reference_this_release_cannot_open_is_refused(service: Service):
    # The SDK cannot build a ref of another owner kind, so the body is raw.
    body_ = json.dumps(
        {"stream": {"kind": "activity", "workflow_id": "wf", "topic": "t"}}
    ).encode()
    with pytest.raises(HTTPStatusError) as refused:
        await asyncio.to_thread(
            generated_client._post, f"{service.caller._base_url}/read", body_, {}, 10.0
        )
    # The Worker refuses an input its converter cannot decode, before the
    # handler runs, and names the reason in the cause.
    assert refused.value.status == 400
    assert not refused.value.retryable
    assert "activity" in refused.value.detail


async def test_a_read_says_done_once_the_owner_closes(service: Service):
    workflow_id = f"streams-service-{uuid.uuid4().hex}"
    ref = StreamRef.for_workflow(workflow_id, topic="out")
    async with new_worker(service.client, StreamOwner) as worker:
        owner = await service.client.start_workflow(
            StreamOwner.run, id=workflow_id, task_queue=worker.task_queue
        )
        await append(service, ref, "one")
        await owner.signal(StreamOwner.finish)
        await owner.result()
    collected: list[str] = []
    answer = await read(service, ref, wait_ms=5000)
    collected.extend(texts(answer))
    while not answer.done:
        answer = await read(service, ref, answer.next_token, wait_ms=5000)
        collected.extend(texts(answer))
    assert collected == ["one"]


async def test_two_readers_on_one_stream_keep_their_own_subscriptions(
    service: Service,
):
    ref = await stream_of(service)
    await append(service, ref, "a", "b", "c", "d")
    # The readers interleave their calls; each resumes where it left off, so
    # neither takes over the other's subscription.
    one = await read(service, ref, max_records=1)
    two = await read(service, ref, max_records=3)
    one = await read(service, ref, one.next_token, max_records=1)
    two = await read(service, ref, two.next_token, max_records=3)
    assert texts(one) == ["b"]
    assert texts(two) == ["d"]
    # Both park on the tail, at the same place, and both are answered.
    parked = [
        asyncio.ensure_future(read(service, ref, two.next_token, wait_ms=10000))
        for _ in range(2)
    ]
    await asyncio.sleep(0.3)
    await append(service, ref, "e", sequence=5)
    answers = await asyncio.wait_for(asyncio.gather(*parked), 5)
    assert [texts(answer) for answer in answers] == [["e"], ["e"]]


async def test_an_idle_subscription_expires(server: Server, backing: Backing):
    async with serve(server, backing, idle_timeout=timedelta(seconds=0.5)) as service:
        ref = await stream_of(service)
        await append(service, ref, "a", "b")
        first = await read(service, ref, max_records=1)
        assert len(service.handler._idle) == 1
        await asyncio.sleep(1.0)
        assert len(service.handler._idle) == 0
        # The reader's next call opens a fresh subscription from its cursor.
        assert texts(await read(service, ref, first.next_token)) == ["b"]


async def test_the_longest_idle_subscription_goes_first(
    server: Server, backing: Backing
):
    async with serve(server, backing, max_idle_subscriptions=2) as service:
        refs = [await stream_of(service) for _ in range(3)]
        for ref in refs:
            await append(service, ref, "a", "b")
            await read(service, ref, max_records=1)
        assert len(service.handler._idle) == 2
        assert [key[1] for key, _ in service.handler._idle.values()] == [
            ref.workflow_id for ref in refs[1:]
        ]


@workflow.defn
class StreamsThroughNexus:
    """Appends and reads through the service from Workflow code."""

    @workflow.run
    async def run(self, endpoint: str, ref: StreamRef) -> list[str]:
        streams = workflow.create_nexus_client(
            service=TemporalStreams, endpoint=endpoint
        )
        payload = Payload(metadata={"encoding": b"binary/test"}, data=b"hi")
        await streams.execute_operation(
            TemporalStreams.append,
            AppendInput(
                stream=ref,
                producer_id="wf",
                attempt=1,
                sequence=1,
                payloads=[payload.SerializeToString()],
            ),
        )
        answer = await streams.execute_operation(
            TemporalStreams.read, ReadInput(stream=ref, wait_ms=1000)
        )
        return [
            WireRecord.FromString(record.record).body.data.decode()
            for record in answer.records
        ]


async def test_a_workflow_calls_the_service_through_its_nexus_client(
    service: Service,
):
    ref = await stream_of(service)
    async with new_worker(service.client, StreamsThroughNexus) as worker:
        result = await service.client.execute_workflow(
            StreamsThroughNexus.run,
            args=[service.endpoint, ref],
            id=f"streams-through-nexus-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
    assert result == ["hi"]


class XorCodec(PayloadCodec):
    """Encrypts a payload by XOR, the way a namespace's codec would."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/xor"},
                data=bytes(b ^ 0x5A for b in payload.SerializeToString()),
            )
            for payload in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload.FromString(bytes(b ^ 0x5A for b in payload.data))
            if payload.metadata.get("encoding") == b"binary/xor"
            else payload
            for payload in payloads
        ]


async def test_a_workflow_reader_gets_plain_bodies_that_stay_encrypted_elsewhere(
    server: Server, backing: Backing
):
    # The handler and the caller share the namespace's codec. The store keeps
    # each body encrypted, the read result crosses encrypted as a whole, and
    # Workflow code sees the plain body.
    config = server.client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=XorCodec()
    )
    coded = Client(**config)
    async with serve(server, backing, client=coded) as service:
        ref = await stream_of(service)
        async with new_worker(coded, StreamsThroughNexus) as worker:
            handle = await coded.start_workflow(
                StreamsThroughNexus.run,
                args=[service.endpoint, ref],
                id=f"streams-coded-{uuid.uuid4().hex}",
                task_queue=worker.task_queue,
            )
            assert await handle.result() == ["hi"]

        history = await handle.fetch_history()
        results = [
            event.nexus_operation_completed_event_attributes.result
            for event in history.events
            if event.HasField("nexus_operation_completed_event_attributes")
        ]
        assert len(results) == 2
        assert all(result.metadata["encoding"] == b"binary/xor" for result in results)

        # Read through a handle with no codec: the stored body is the codec's.
        plain = backing.provider.get_stream_handle(server.client, ref)
        stored = plain.read(after=BEGINNING, result_type=RawValue)
        try:
            record = await asyncio.wait_for(anext(stored), 5)
        finally:
            await stored.aclose()
        assert record.value is not None
        assert record.value.payload.metadata["encoding"] == b"binary/xor"


class SlowCodec(PayloadCodec):
    """A codec that really awaits, as a KMS-backed one does."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.001)
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.001)
        return list(payloads)


async def test_a_read_that_does_not_wait_answers_with_what_the_store_holds(
    server: Server, backing: Backing
):
    # wait_ms=0 means "do not wait for new records", not "skip a fetch that
    # needs I/O": every record already in the store comes back.
    config = server.client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=SlowCodec()
    )
    async with serve(server, backing, client=Client(**config)) as service:
        ref = await stream_of(service)
        for start in range(0, 250, 50):
            await append(
                service,
                ref,
                *[f"r{n}" for n in range(start, start + 50)],
                sequence=start + 1,
            )
        sizes: list[int] = []
        after = ""
        while True:
            answer = await read(service, ref, after, wait_ms=0, max_records=100)
            if not answer.records:
                break
            sizes.append(len(answer.records))
            after = answer.next_token
        assert sizes == [100, 100, 50]


async def test_a_read_answer_stays_within_its_byte_budget(service: Service):
    ref = await stream_of(service)
    big = "x" * (400 * 1024)
    for n in range(5):
        await append(service, ref, f"{n}{big}", sequence=n + 1)
    sizes: list[int] = []
    after = ""
    while True:
        answer = await read(service, ref, after, wait_ms=0)
        if not answer.records:
            break
        assert sum(len(record.record) for record in answer.records) <= 1 << 20
        sizes.append(len(answer.records))
        after = answer.next_token
    assert sizes == [2, 2, 1]


async def test_a_record_above_the_budget_still_crosses_alone(service: Service):
    ref = await stream_of(service)
    await append(service, ref, "y" * (1 << 20) + "z", "small")
    first = await read(service, ref, wait_ms=0)
    assert len(first.records) == 1
    second = await read(service, ref, first.next_token, wait_ms=0)
    assert texts(second) == ["small"]


@pytest.mark.parametrize(
    "error, kind, retryable",
    [
        (StreamNotFoundError("x"), nexusrpc.HandlerErrorType.NOT_FOUND, False),
        (StreamCursorError("x"), nexusrpc.HandlerErrorType.BAD_REQUEST, False),
        (StreamExpiredError("x"), nexusrpc.HandlerErrorType.BAD_REQUEST, False),
        (StreamProducerError("x"), nexusrpc.HandlerErrorType.BAD_REQUEST, False),
        (StreamClosedError("x"), nexusrpc.HandlerErrorType.BAD_REQUEST, False),
        (StreamUnsupportedError("x"), nexusrpc.HandlerErrorType.NOT_IMPLEMENTED, False),
        (StreamRefusedError("x"), nexusrpc.HandlerErrorType.INTERNAL, False),
        (StreamOutcomeUnknownError("x"), nexusrpc.HandlerErrorType.UNAVAILABLE, True),
        (StreamStorageError("x"), nexusrpc.HandlerErrorType.UNAVAILABLE, True),
        (StreamError("x"), nexusrpc.HandlerErrorType.INTERNAL, True),
        (ValueError("x"), nexusrpc.HandlerErrorType.BAD_REQUEST, False),
    ],
)
def test_each_stream_condition_crosses_as_its_own_class(
    error: Exception, kind: nexusrpc.HandlerErrorType, retryable: bool
):
    crossed = _handler_error(error)  # type: ignore[arg-type]
    name = type(error).__name__
    assert crossed.type == kind
    assert crossed.retryable == retryable
    assert str(crossed).startswith(f"{name}: ")
    # What crosses is the failure the handler spells out, with no stack.
    original = crossed.original_failure
    assert original is not None
    assert original.message == str(crossed)
    assert not original.stack_trace
    details = dict(original.details or {})
    cause = details["cause"]["applicationFailureInfo"]
    assert cause["type"] == name
    assert cause.get("nonRetryable", False) == (not retryable)
    assert details["nexusHandlerFailureInfo"]["type"] == kind.name


async def test_a_refusal_carries_no_handler_stack_frames(service: Service):
    ref = await stream_of(service)
    await append(service, ref, "one")
    with pytest.raises(HTTPStatusError) as refused:
        await append(service, ref, "different")
    # The caller learns the condition, not the handler's source layout.
    assert refused_as(refused.value) == "StreamProducerError"
    assert "File " not in refused.value.detail
    assert "_handler.py" not in refused.value.detail
    failure = json.loads(refused.value.detail)
    assert not failure.get("stackTrace")
    assert not failure["details"].get("stackTrace")


async def test_the_generated_read_loop_ends_when_the_stream_does(service: Service):
    workflow_id = f"streams-service-{uuid.uuid4().hex}"
    ref = StreamRef.for_workflow(workflow_id, topic="out")
    async with new_worker(service.client, StreamOwner) as worker:
        owner = await service.client.start_workflow(
            StreamOwner.run, id=workflow_id, task_queue=worker.task_queue
        )
        landed = await append(service, ref, "one")
        await owner.signal(StreamOwner.finish)
        await owner.result()
    started = asyncio.get_running_loop().time()
    answer = await service.caller.read_until_records(
        ReadInput(stream=ref, after_token=landed.cursor), deadline=10.0
    )
    # The stream has ended, so the loop hands back the done answer at once
    # rather than asking again until its deadline.
    assert answer.done and answer.records == []
    assert asyncio.get_running_loop().time() - started < 5.0
