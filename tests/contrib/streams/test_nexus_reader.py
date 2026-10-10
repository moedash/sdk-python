"""A Workflow reads the stream a Nexus operation returns.

The reader waits on the operation's progress and reads through the stream
service as Nexus operations of the Workflow, so History records every read and
a replay sees the same batches. These tests replay histories built by hand:
the stream operation, its progress on Workflow Task scheduled events, and each
read with the answer the stream service gave.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

import temporalio.nexus
from temporalio import workflow
from temporalio.api.common.v1 import Payload, Payloads, WorkflowType
from temporalio.api.enums.v1 import EventType, StreamOwnerKind
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.history.v1 import (
    HistoryEvent,
    NexusOperationCompletedEventAttributes,
    NexusOperationFailedEventAttributes,
    NexusOperationScheduledEventAttributes,
    NexusOperationStartedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes,
)
from temporalio.api.nexus.v1 import NexusOperationProgress as ProgressProto
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeStreamNotifierRequest
from temporalio.client import Client, WorkflowHistory
from temporalio.contrib.streams import StreamRef, Supersession, workflow_writer
from temporalio.contrib.streams._record import Cursor, RecordKind
from temporalio.contrib.streams._wire import WireRecord, to_wire
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    ReadOutput,
    ReadSupersession,
    RecordWire,
    StreamOperationHandler,
    StreamReader,
    StreamRecordError,
    TemporalStreamsHandler,
    close_workflow_stream,
)
from temporalio.contrib.streams.nexus._operation import _encode_token
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import NexusOperationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from tests.helpers.nexus import make_nexus_endpoint_name

ENDPOINT = "chat-endpoint"
TASK_QUEUE = "reader-task-queue"
STREAMS_SERVICE = "temporal.sdk.streams.v1.TemporalStreams"
STREAM = StreamRef(kind="workflow", workflow_id="chat-1", run_id="", topic="tokens")

converter = DataConverter.default.payload_converter


@dataclass
class Token:
    text: str


@nexusrpc.service
class ChatService:
    chat: nexusrpc.Operation[str, str]


@dataclass
class Seen:
    batches: list[list[Token]] = field(default_factory=list)
    after_end: list[Token] | None = None
    error: str | None = None
    supersessions: list[ReadSupersession] = field(default_factory=list)
    undecodable: list[str] = field(default_factory=list)


seen: list[Seen] = []


@workflow.defn(name="ChatReader")
class ChatReader:
    @workflow.run
    async def run(self) -> None:
        client = workflow.create_nexus_client(service=ChatService, endpoint=ENDPOINT)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(handle, item_type=Token, endpoint=ENDPOINT)
        observed = Seen()
        try:
            while True:
                try:
                    batch = await reader.next()
                except StreamRecordError as error:
                    observed.undecodable.append(str(error.cursor))
                    continue
                if batch is None:
                    break
                observed.batches.append(batch)
            observed.after_end = await reader.next()
        except Exception as error:
            # The server wraps a Nexus failure in NexusOperationError; a
            # hand-built failure event reaches the Workflow unwrapped.
            cause = error.cause if isinstance(error, NexusOperationError) else error
            observed.error = type(cause).__name__
        observed.supersessions = list(reader.supersessions)
        seen.append(observed)


class History:
    """Builds the reader Workflow's History event by event."""

    def __init__(self) -> None:
        self.events: list[HistoryEvent] = []
        self._time = datetime(2026, 10, 10, tzinfo=timezone.utc)
        self._completed = 0

    def add(self, event_type: EventType.ValueType, **attributes: object) -> int:
        self._time += timedelta(milliseconds=10)
        event_time = Timestamp()
        event_time.FromDatetime(self._time)
        event = HistoryEvent(
            event_id=len(self.events) + 1,
            event_type=event_type,
            event_time=event_time,
            **attributes,  # type: ignore[arg-type]
        )
        self.events.append(event)
        return event.event_id

    def started(self) -> None:
        self.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                workflow_type=WorkflowType(name="ChatReader"),
                task_queue=TaskQueue(name=TASK_QUEUE),
                original_execution_run_id="run-id",
                first_execution_run_id="run-id",
                attempt=1,
            ),
        )

    def task(self, *progress: ProgressProto) -> None:
        scheduled = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
            workflow_task_scheduled_event_attributes=WorkflowTaskScheduledEventAttributes(
                task_queue=TaskQueue(name=TASK_QUEUE),
                start_to_close_timeout=Duration(seconds=10),
                attempt=1,
                nexus_operation_progress=list(progress),
            ),
        )
        started = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
            workflow_task_started_event_attributes=WorkflowTaskStartedEventAttributes(
                scheduled_event_id=scheduled
            ),
        )
        self._completed = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
            workflow_task_completed_event_attributes=WorkflowTaskCompletedEventAttributes(
                scheduled_event_id=scheduled, started_event_id=started
            ),
        )

    def scheduled(self, service: str, operation: str) -> int:
        return self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
            nexus_operation_scheduled_event_attributes=NexusOperationScheduledEventAttributes(
                endpoint=ENDPOINT,
                service=service,
                operation=operation,
                input=Payload(),
                workflow_task_completed_event_id=self._completed,
                request_id=f"request-{len(self.events)}",
            ),
        )

    def chat_started(self, scheduled_event_id: int) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED,
            nexus_operation_started_event_attributes=NexusOperationStartedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                operation_token=_encode_token(STREAM, "attach-request", "run-id"),
                request_id="request-chat",
            ),
        )

    def completed(self, scheduled_event_id: int, result: object) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
            nexus_operation_completed_event_attributes=NexusOperationCompletedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                result=converter.to_payload(result),
            ),
        )

    def failed(self, scheduled_event_id: int, operation: str) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_FAILED,
            nexus_operation_failed_event_attributes=NexusOperationFailedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                failure=Failure(
                    message=f"{operation} failed",
                    application_failure_info=ApplicationFailureInfo(type="Broken"),
                ),
            ),
        )

    def read(self, output: ReadOutput) -> None:
        """One read the reader issues in the task just completed, and its answer."""
        self.completed(self.scheduled(STREAMS_SERVICE, "read"), output)

    def workflow_completed(self) -> None:
        self.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
                result=Payloads(payloads=[Payload()]),
                workflow_task_completed_event_id=self._completed,
            ),
        )


def record(
    position: int,
    kind: RecordKind = RecordKind.DATA,
    text: str = "",
    *,
    attempt: int = 1,
    sequence: int | None = None,
) -> RecordWire:
    wire = to_wire(
        converter,
        topic=STREAM.topic,
        kind=kind,
        value=Token(text) if kind is RecordKind.DATA else None,
        producer_id="chat",
        attempt=attempt,
        sequence=position if sequence is None else sequence,
    )
    return RecordWire(token=f"cursor-{position}", record=wire.SerializeToString())


def answer(*records: RecordWire, done: bool = False, cursor: str = "") -> ReadOutput:
    return ReadOutput(
        records=list(records),
        next_token=records[-1].token if records else cursor,
        done=done,
    )


def progress(scheduled_event_id: int, counter: int) -> ProgressProto:
    return ProgressProto(
        scheduled_event_id=scheduled_event_id,
        position=f"cursor-{counter}",
        counter=counter,
    )


def stream_history() -> WorkflowHistory:
    """The stream gets records a and b, then c, then closes with a FINISH record."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    # The first read covers everything up to the progress the start brought.
    h.task(progress(chat, 1))
    h.read(answer(record(1, text="a"), record(2, text="b")))
    # The reader hands over a and b. The answer had records, so it reads on.
    h.task()
    h.read(answer(cursor="cursor-2"))
    # An empty answer is the tail, so it waits for newer progress.
    h.task()
    h.task(progress(chat, 2))
    h.read(answer(record(3, text="c")))
    h.task()
    h.read(answer(cursor="cursor-3"))
    h.task()
    # The stream closed, which completed the operation. The reader drains what
    # is left and ends.
    h.completed(chat, "summary")
    h.task()
    h.read(answer(record(4, RecordKind.FINISH), done=True))
    h.task()
    h.workflow_completed()
    return WorkflowHistory(workflow_id="chat-reader", events=h.events)


async def replay(
    history: WorkflowHistory, data_converter: DataConverter = DataConverter.default
) -> None:
    await Replayer(
        workflows=[ChatReader],
        workflow_runner=UnsandboxedWorkflowRunner(),
        data_converter=data_converter,
    ).replay_workflow(history)


class MarkingCodec(PayloadCodec):
    """Wraps each payload in an envelope only this codec opens."""

    encoding = b"binary/n15-test-codec"

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"encoding": self.encoding}, data=p.SerializeToString())
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload.FromString(p.data)
            if p.metadata.get("encoding") == self.encoding
            else p
            for p in payloads
        ]


@pytest.fixture(autouse=True)
def clear_seen() -> None:
    seen.clear()


async def test_the_reader_hands_over_every_record_in_batches_then_ends() -> None:
    await replay(stream_history())
    assert seen == [
        Seen(
            batches=[[Token("a"), Token("b")], [Token("c")]],
            after_end=None,
        )
    ]


async def test_the_reader_reads_the_same_on_every_replay() -> None:
    await replay(stream_history())
    await replay(stream_history())
    assert len(seen) == 2
    assert seen[0] == seen[1]


async def test_a_full_batch_is_followed_by_a_read_without_waiting() -> None:
    """A read that fills the batch may have left records behind, so the next
    call reads again before it waits for progress."""
    full = [record(n, text=str(n)) for n in range(1, 101)]
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(chat, 1))
    h.read(answer(*full))
    h.task()
    h.read(answer(record(101, text="101")))
    h.task()
    h.read(answer(cursor="cursor-101"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-101", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [
        [Token(str(n)) for n in range(1, 101)],
        [Token("101")],
    ]


async def test_after_the_operation_completes_only_the_streams_end_ends_it() -> None:
    """After the operation completes, neither a short answer nor an empty one
    ends the reader: only the stream service saying the stream is done."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task()
    # The first read finds nothing, so the reader waits.
    h.read(answer())
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(record(1, text="a")))
    h.task()
    # Empty, but not done: the stream is still open, so the reader reads on.
    h.read(answer(cursor="cursor-1"))
    h.task()
    h.read(answer(record(2, text="b")))
    h.task()
    h.read(answer(cursor="cursor-2", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")], [Token("b")]]
    assert seen[0].after_end is None


async def test_a_read_error_raises_from_next() -> None:
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(chat, 1))
    h.failed(h.scheduled(STREAMS_SERVICE, "read"), "read")
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen == [Seen(error="ApplicationError")]


async def test_a_failed_operation_raises_after_its_records_are_read() -> None:
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(chat, 1))
    h.read(answer(record(1, text="a")))
    h.task()
    h.read(answer(cursor="cursor-1"))
    h.task()
    h.failed(chat, "chat")
    h.task()
    h.read(answer(record(2, text="b")))
    h.task()
    # The read waited a moment and found nothing more.
    h.read(answer(cursor="cursor-2"))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen == [
        Seen(batches=[[Token("a")], [Token("b")]], error="ApplicationError")
    ]


async def test_the_reader_records_where_a_producer_attempt_was_superseded() -> None:
    """The second attempt of the producer rewrites from its first record. The
    batch carries both attempts' data, and the reader says where the second
    attempt took over."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(chat, 1))
    h.read(
        answer(
            record(1, text="a"),
            record(2, text="b"),
            record(3, text="a2", attempt=2, sequence=1),
        )
    )
    h.task()
    h.read(answer(cursor="cursor-3"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-3", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a"), Token("b"), Token("a2")]]
    assert seen[0].supersessions == [
        ReadSupersession(
            index=2,
            cursor=Cursor("cursor-2"),
            supersession=Supersession(
                producer_id="chat", previous_attempt=1, attempt=2
            ),
        )
    ]


async def test_bodies_reach_the_workflow_decoded_by_the_callers_codec() -> None:
    """A read's answer is one Nexus operation result, encoded by the handler
    Worker's codec with the bodies inside it in the clear. The caller Worker's
    codec opens it before Workflow code sees it."""
    codec = MarkingCodec()
    history = stream_history()
    for event in history.events:
        attributes = event.nexus_operation_completed_event_attributes
        if event.HasField("nexus_operation_completed_event_attributes"):
            [encoded] = await codec.encode([attributes.result])
            attributes.result.CopyFrom(encoded)
    await replay(
        history, dataclasses.replace(DataConverter.default, payload_codec=codec)
    )
    assert seen[0].batches == [[Token("a"), Token("b")], [Token("c")]]


# Live: a caller Workflow reads every record of a stream a handler hands it,
# through the stream service on the operation's own endpoint, with a payload
# codec on every side. Needs a server with Nexus progress and the stream
# notifier; any other server makes the test skip.


@dataclass
class LiveSeen:
    batches: list[list[Token]]
    after_end: list[Token] | None
    result: str


@workflow.defn(name="LiveChatReader")
class LiveChatReader:
    @workflow.run
    async def run(self, endpoint: str) -> LiveSeen:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(handle, item_type=Token, endpoint=endpoint)
        batches: list[list[Token]] = []
        while (batch := await reader.next()) is not None:
            batches.append(batch)
        return LiveSeen(
            batches=batches, after_end=await reader.next(), result=await handle
        )


@workflow.defn(name="LiveTokenOwner")
class LiveTokenOwner:
    """Publishes its tokens on its own stream, then waits to be told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, texts: list[str]) -> None:
        writer = workflow_writer(STREAM.topic)
        for text in texts:
            writer.publish(Token(text))
            await workflow.sleep(timedelta(milliseconds=100))
        writer.finish()
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


async def test_live_a_workflow_reads_every_record_then_none(
    client: Client, env: WorkflowEnvironment
) -> None:
    codec = MarkingCodec()
    data_converter = dataclasses.replace(DataConverter.default, payload_codec=codec)
    config = client.config()
    config["data_converter"] = data_converter
    client = Client(**config)
    owner_id = f"token-owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-reader-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    provider = MemoryStreams().notify_on_append()
    texts = [f"token-{n}" for n in range(7)]

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            LiveTokenOwner.run, texts, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    streams = TemporalStreamsHandler(provider)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LiveChatReader, LiveTokenOwner],
        nexus_service_handlers=[ChatServiceHandler(), streams],
        plugins=[provider],
    ):
        caller = await client.start_workflow(
            LiveChatReader.run,
            endpoint,
            id=f"stream-reader-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        owner = client.get_workflow_handle(owner_id)

        async def owner_finished_publishing() -> bool:
            try:
                history = await owner.fetch_history()
            except RPCError:
                return False
            timers = sum(
                event.HasField("timer_fired_event_attributes")
                for event in history.events
            )
            return timers >= len(texts)

        for _ in range(200):
            if await owner_finished_publishing():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("the owner never finished publishing")
        await owner.signal(LiveTokenOwner.done)
        await provider.close_stream(client, ref, "7 tokens")
        seen_live = await asyncio.wait_for(caller.result(), 30)
        await streams.close()

    assert [token for batch in seen_live.batches for token in batch] == [
        Token(text) for text in texts
    ]
    assert seen_live.after_end is None
    assert seen_live.result == "7 tokens"

    # Each read's answer is one Nexus operation result, encoded once by the
    # handler Worker's codec, with the bodies inside it decoded.
    history = await caller.fetch_history()
    read_ids = {
        event.event_id
        for event in history.events
        if event.nexus_operation_scheduled_event_attributes.operation == "read"
    }
    reads = [
        event.nexus_operation_completed_event_attributes.result
        for event in history.events
        if event.nexus_operation_completed_event_attributes.scheduled_event_id
        in read_ids
    ]
    assert all(read.metadata.get("encoding") == codec.encoding for read in reads)
    assert reads, "no read result went through the codec"
    bodies = []
    for encoded in reads:
        [plain] = await codec.decode([encoded])
        answer = converter.from_payload(plain, ReadOutput)
        bodies += [
            WireRecord.FromString(stored.record).body for stored in answer.records
        ]
    data_bodies = [body for body in bodies if body.ByteSize()]
    assert data_bodies
    assert all(
        body.metadata.get("encoding") == b"json/plain" for body in data_bodies
    ), "a body crossed still encoded"

    await Replayer(
        workflows=[LiveChatReader], data_converter=data_converter
    ).replay_workflow(history)
    await provider.close()


@workflow.defn(name="LiveLabelReader")
class LiveLabelReader:
    """Reads the stream and keeps each token's label, the text before its
    colon, so large bodies stay out of its result."""

    @workflow.run
    async def run(self, endpoint: str) -> list[str]:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(handle, item_type=Token, endpoint=endpoint)
        labels: list[str] = []
        while (batch := await reader.next()) is not None:
            labels += [token.text.split(":", 1)[0] for token in batch]
        await handle
        return labels


@workflow.defn(name="LiveIdleOwner")
class LiveIdleOwner:
    """Owns a stream that others write to, until told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


class SlowCodec(MarkingCodec):
    """A codec that does real async work, as one calling a key service does."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.001)
        return await super().encode(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.001)
        return await super().decode(payloads)


# Memory, memory behind a codec that awaits, and Redis: each one's fetch takes
# real time, which a read that does not wait must still complete.
BACKINGS = ["memory", "memory-async-codec", "redis"]


def _notifying_provider(backing: str) -> Any:
    if backing.startswith("memory"):
        return MemoryStreams().notify_on_append()
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to read over Redis")
    from temporalio.contrib.streams.redis import RedisStreams

    return RedisStreams(url, key_prefix=f"reader-{uuid.uuid4().hex}").notify_on_append()


async def _read_what_is_appended(
    client: Client,
    env: WorkflowEnvironment,
    backing: str,
    texts: list[str],
    per_append: int,
) -> list[str]:
    """Appends ``texts`` from outside the owner once the caller is attached,
    closes the stream, and answers with the labels the caller read."""
    provider = _notifying_provider(backing)
    if backing == "memory-async-codec":
        config = client.config()
        config["data_converter"] = dataclasses.replace(
            DataConverter.default, payload_codec=SlowCodec()
        )
        client = Client(**config)
    owner_id = f"idle-owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-reader-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            LiveIdleOwner.run, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    streams = TemporalStreamsHandler(provider)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LiveLabelReader, LiveIdleOwner],
        nexus_service_handlers=[ChatServiceHandler(), streams],
        plugins=[provider],
    ):
        caller = await client.start_workflow(
            LiveLabelReader.run,
            endpoint,
            id=f"stream-reader-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        for _ in range(200):
            history = await caller.fetch_history()
            if any(
                event.HasField("nexus_operation_started_event_attributes")
                for event in history.events
            ):
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("the stream operation never started")
        producer = provider.get_stream_handle(client, ref).producer(
            producer_id="burst", attempt=1
        )
        for start in range(0, len(texts), per_append):
            await producer.append(
                *(Token(text) for text in texts[start : start + per_append])
            )
        await provider.close_stream(client, ref, "done")
        labels = await asyncio.wait_for(caller.result(), 60)
        await client.get_workflow_handle(owner_id).signal(LiveIdleOwner.done)
        await streams.close()
    await provider.close()
    return labels


@pytest.mark.parametrize("backing", BACKINGS)
async def test_live_a_reader_gets_every_record_of_a_burst(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    labels = await _read_what_is_appended(
        client, env, backing, [f"{n}:" for n in range(700)], per_append=50
    )
    assert labels == [str(n) for n in range(700)]


@pytest.mark.parametrize("backing", BACKINGS)
async def test_live_a_reader_reads_on_past_answers_cut_by_the_byte_budget(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    """Each answer holds about three of these records, so most answers are
    short of the record limit without being the stream's tail."""
    body = "x" * 300_000
    labels = await _read_what_is_appended(
        client, env, backing, [f"{n}:{body}" for n in range(12)], per_append=1
    )
    assert labels == [str(n) for n in range(12)]


@workflow.defn(name="LiveOwnerThatClosesInOneTask")
class LiveOwnerThatClosesInOneTask:
    """Publishes its tokens and closes its stream in one Workflow Task, then
    keeps running until told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, texts: list[str]) -> None:
        writer = workflow_writer(STREAM.topic)
        for text in texts:
            writer.publish(Token(text))
        close_workflow_stream(f"{len(texts)} tokens", topic=STREAM.topic)
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


@pytest.mark.parametrize("backing", BACKINGS)
async def test_live_a_close_with_the_last_publishes_loses_none_of_them(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    """The completion can reach the reader before the task's records are
    promoted, but the stream is only done once they are. Promotion is slowed
    here past any wait a reader would make for stragglers."""
    provider = _notifying_provider(backing)
    promote = provider._promote

    async def slow_promote(stage: Any) -> None:
        await asyncio.sleep(3)
        await promote(stage)

    provider._promote = slow_promote
    if backing == "memory-async-codec":
        config = client.config()
        config["data_converter"] = dataclasses.replace(
            DataConverter.default, payload_codec=SlowCodec()
        )
        client = Client(**config)
    owner_id = f"closing-owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-reader-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    texts = [f"{n}:" for n in range(300)]

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            LiveOwnerThatClosesInOneTask.run, texts, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    streams = TemporalStreamsHandler(provider)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LiveLabelReader, LiveOwnerThatClosesInOneTask],
        nexus_service_handlers=[ChatServiceHandler(), streams],
        plugins=[provider],
    ):
        labels = await asyncio.wait_for(
            client.execute_workflow(
                LiveLabelReader.run,
                endpoint,
                id=f"stream-reader-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            60,
        )
        await client.get_workflow_handle(owner_id).signal(
            LiveOwnerThatClosesInOneTask.done
        )
        await streams.close()
    await provider.close()
    assert labels == [str(n) for n in range(300)]


async def test_an_undecodable_record_raises_and_the_reader_goes_on_past_it() -> None:
    """A body that does not decode into the item type is not skipped quietly:
    the records before it are handed over, then next() raises, and the next
    call goes on after it."""
    bad = to_wire(
        converter,
        topic=STREAM.topic,
        kind=RecordKind.DATA,
        value="not a token",
        producer_id="chat",
        attempt=1,
        sequence=2,
    )
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(chat, 1))
    h.read(
        answer(
            record(1, text="a"),
            RecordWire(token="cursor-2", record=bad.SerializeToString()),
            record(3, text="c"),
        )
    )
    # a is handed over, the bad record raises, then the reader reads after it.
    h.task()
    h.read(answer(record(3, text="c")))
    h.task()
    h.read(answer(cursor="cursor-3"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-3", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")], [Token("c")]]
    assert seen[0].undecodable == ["cursor-2"]
