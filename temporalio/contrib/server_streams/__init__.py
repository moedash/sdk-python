"""Server-side streams behind the Workflow Streams API.

This is the same surface as :mod:`temporalio.contrib.workflow_streams`, backed
by a Temporal-owned log instead of by Signals and Updates. An application swaps
the import and keeps its code: publishing from a Workflow is still a plain
call, publishing from an Activity is still a buffered handle, and a consumer
still subscribes by topic from an offset. An Activity's appends carry its own
id and attempt, so a retried Activity's repeat is deduplicated by the server
rather than written twice.

What changes is underneath. A publish is a Workflow Command whose payload never
enters History, so History gets one fixed-size event per Workflow Task rather
than a Signal per batch. A consumer reads the log directly rather than
long-polling an Update, so there is no per-Workflow limit on how many can read
at once, and a closed Workflow stays readable until its stream's retention
expires.

As in the shipped feature, a topic here is a label on a record in the
Workflow's one default stream, which a consumer filters on. The provider in
:mod:`temporalio.streams.providers.native` keeps one stream per topic instead;
the two do not share a log.

Prototype support for AI-198. It needs a server built from that branch, and it
opens its own gRPC channel because sdk-core does not know the stream service
yet, which is also why it does not support TLS or API keys.
"""

from __future__ import annotations

import asyncio
import builtins
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

from temporalio import activity, workflow
from temporalio.api.stream.v1 import StreamRecord, StreamRecordKind
from temporalio.client import Client, WorkflowExecutionDescription
from temporalio.client_stream import StreamClient, WorkflowStreamHandle, shared_client
from temporalio.common import RawValue
from temporalio.converter import PayloadCodec, PayloadConverter

__all__ = [
    "RawPage",
    "TopicHandle",
    "WorkflowStream",
    "WorkflowStreamClient",
    "WorkflowStreamItem",
    "WorkflowTopicHandle",
]

T = TypeVar("T")

DEFAULT_BATCH_INTERVAL = timedelta(milliseconds=50)


@dataclass
class RawPage:
    """One read whose items are still the stored records.

    ``closed`` with ``next_offset >= head_offset`` is the end of the stream.
    The bodies are as the server holds them, codec included, for a caller
    that forwards records rather than using them.
    """

    items: list[WorkflowStreamItem[StreamRecord]]
    next_offset: int
    head_offset: int
    closed: bool


@dataclass
class WorkflowStreamItem(Generic[T]):
    """One item read from a workflow's stream.

    ``offset`` is where the item sits in the whole stream, so it is what a
    consumer hands back to resume. A topic filter leaves gaps in it.
    """

    topic: str
    data: T
    offset: int = 0


def _record(converter: PayloadConverter, topic: str, value: Any) -> StreamRecord:
    """The record one published value becomes.

    The body is the value's payload, so the encoding metadata the consumer
    needs to decode into a type travels with it and a
    :class:`temporalio.common.RawValue` passes through pre-encoded.
    """
    record = StreamRecord(topic=topic, kind=StreamRecordKind.STREAM_RECORD_KIND_DATA)
    record.body.CopyFrom(converter.to_payloads([value])[0])
    return record


def _decode(
    converter: PayloadConverter, record: StreamRecord, as_type: type | None
) -> Any:
    if as_type is None:
        return converter.from_payloads([record.body])[0]
    return converter.from_payloads([record.body], [as_type])[0]


class WorkflowTopicHandle(Generic[T]):
    """A topic on the stream the running Workflow owns."""

    def __init__(self, topic: str, value_type: type[T]) -> None:
        """Prefer :meth:`WorkflowStream.topic`."""
        self._name = topic
        self._type = value_type

    @property
    def name(self) -> str:
        """The topic name this handle is bound to."""
        return self._name

    @property
    def type(self) -> type[T]:
        """The value type this handle is bound to."""
        return self._type

    def publish(self, value: T | RawValue) -> None:
        """Append ``value`` to the Workflow's stream on this topic.

        Returns at once. There is nothing to await: the Workflow Task's
        publishes become one command the server applies in the task's own
        commit, so it costs this Workflow no round trip and no extra
        transition. The Worker's payload codec applies to the body as it does
        to any other payload the Workflow sends.
        """
        workflow._append_stream_records(
            [_record(workflow.payload_converter(), self._name, value)]
        )


class WorkflowStream:
    """The stream the running Workflow owns, from inside it.

    Construct in ``@workflow.init``. Unlike the Signals-and-Updates
    implementation this holds no state of its own: the log lives on the server,
    so there is nothing here for replay to reconstruct.
    """

    def __init__(self, prior_state: Any = None) -> None:
        """Take the same argument as the Workflow Streams version and ignore it.

        That version carried the log across a continue-as-new, because the log
        was Workflow state. Here it is not, so there is nothing to carry.
        """
        self._prior_state = prior_state

    @overload
    def topic(self, name: str) -> WorkflowTopicHandle[Any]: ...

    @overload
    def topic(self, name: str, *, type: type[T]) -> WorkflowTopicHandle[T]: ...

    def topic(self, name: str, *, type: type = object) -> WorkflowTopicHandle[Any]:
        """Bind a topic on this Workflow's stream."""
        return WorkflowTopicHandle(name, type)


class TopicHandle(Generic[T]):
    """A topic on a Workflow's stream, from outside that Workflow."""

    def __init__(
        self, client: WorkflowStreamClient, topic: str, value_type: type[T]
    ) -> None:
        """Prefer :meth:`WorkflowStreamClient.topic`."""
        self._client = client
        self._name = topic
        self._type = value_type

    @property
    def name(self) -> str:
        """The topic name this handle is bound to."""
        return self._name

    @property
    def type(self) -> type[T]:
        """The value type this handle is bound to."""
        return self._type

    def publish(self, value: T | RawValue, *, force_flush: bool = False) -> None:
        """Buffer ``value`` for the next flush.

        Buffered rather than sent, because an append costs one transition on
        the owning execution whatever its size. A token at a time would pay
        that per token.
        """
        self._client._buffer(self._name, value)
        if force_flush:
            self._client._flush_soon()

    def subscribe(
        self,
        *,
        from_offset: int = 0,
        # Spelled out because `type` in this class body is the property below.
        result_type: builtins.type | None = None,
        poll_cooldown: timedelta | None = None,
    ) -> AsyncIterator[WorkflowStreamItem[T]]:
        """Read this topic from ``from_offset`` onwards."""
        return self._client.subscribe(
            topics=[self._name],
            from_offset=from_offset,
            result_type=result_type or self._type,
            poll_cooldown=poll_cooldown,
        )


class WorkflowStreamClient:
    """Publishes to and reads from a Workflow's stream, from outside it."""

    def __init__(
        self,
        handle: WorkflowStreamHandle,
        converter: PayloadConverter,
        batch_interval: timedelta = DEFAULT_BATCH_INTERVAL,
        *,
        codec: PayloadCodec | None = None,
        describe: Callable[[], Awaitable[WorkflowExecutionDescription]] | None = None,
        producer_id: str = "",
    ) -> None:
        """Prefer :meth:`create` or :meth:`from_within_activity`.

        ``codec`` is applied to every body this client sends and receives, so
        a namespace whose payloads are encoded agrees with the Worker, whose
        payload visitor applies the same codec to the Workflow's publishes.
        ``describe`` is how an unpinned handle learns which run it follows;
        see :meth:`WorkflowStreamHandle.pin`. ``producer_id`` is who the
        appends are written as; without one they are at-least-once, because
        the server has nothing to deduplicate a retry against.
        """
        self._handle = handle
        self._converter = converter
        self._codec = codec
        self._batch_interval = batch_interval
        self._describe = describe
        self._producer_id = producer_id
        self._sequence = 0
        self._pending: tuple[list[StreamRecord], int] | None = None
        self._buffered: list[tuple[str, Any]] = []
        self._flusher: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closed = False

    @classmethod
    def create(
        cls,
        client: Client,
        workflow_id: str,
        *,
        owner_run_id: str = "",
        batch_interval: timedelta = DEFAULT_BATCH_INTERVAL,
        producer_id: str = "",
    ) -> WorkflowStreamClient:
        """Open the stream owned by ``workflow_id``.

        Without ``owner_run_id`` the current run is looked up on the first
        call and the handle pinned to it, so a reader following across a
        continue-as-new sees the run end rather than being moved to the
        successor's stream at a stale offset. Without ``producer_id`` the
        appends are at-least-once; inside an Activity,
        :meth:`from_within_activity` supplies one.
        """
        return cls(
            _stream_client(client).workflow_stream(
                workflow_id, owner_run_id=owner_run_id
            ),
            client.data_converter.payload_converter,
            batch_interval,
            codec=client.data_converter.payload_codec,
            describe=client.get_workflow_handle(workflow_id).describe,
            producer_id=producer_id,
        )

    @classmethod
    def from_within_activity(
        cls, *, batch_interval: timedelta = DEFAULT_BATCH_INTERVAL
    ) -> WorkflowStreamClient:
        """Open the stream owned by the Workflow that scheduled this Activity."""
        info = activity.info()
        if info.workflow_id is None:
            raise RuntimeError(
                "no Workflow stream to open: this Activity was not started by a "
                "Workflow"
            )
        # The Activity's output belongs to the run that scheduled it, and the
        # Activity already knows which run that is. Its id and attempt are
        # also what lets the server drop a batch a retried Activity re-sends.
        return cls.create(
            activity.client(),
            info.workflow_id,
            owner_run_id=info.workflow_run_id or "",
            batch_interval=batch_interval,
            producer_id=f"{info.activity_id}#{info.attempt}",
        )

    async def __aenter__(self) -> WorkflowStreamClient:
        """Start the background flusher."""
        self._flusher = asyncio.create_task(self._run_flusher())
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Drain what is buffered before letting the caller go.

        An Activity that returned with a batch still buffered would have
        reported work its readers never saw. The flusher is asked to stop
        rather than cancelled: a cancel landing inside its append would
        unwind with the batch it had already taken off the buffer.
        """
        self._closed = True
        self._wake.set()
        if self._flusher is not None:
            await self._flusher
            self._flusher = None
        await self.flush()

    async def _pin(self) -> None:
        if self._handle.owner_run_id or self._describe is None:
            return
        self._handle.pin((await self._describe()).run_id)

    @overload
    def topic(self, name: str) -> TopicHandle[Any]: ...

    @overload
    def topic(self, name: str, *, type: type[T]) -> TopicHandle[T]: ...

    def topic(self, name: str, *, type: type = object) -> TopicHandle[Any]:
        """Bind a topic on this Workflow's stream."""
        return TopicHandle(self, name, type)

    async def get_offset(self) -> int:
        """Where the stream currently ends.

        A reader that wants only what comes next starts here.
        """
        await self._pin()
        return (await self._handle.describe()).head_offset

    async def subscribe(
        self,
        *,
        topics: Sequence[str] = (),
        from_offset: int = 0,
        result_type: type | None = None,
        poll_cooldown: timedelta | None = None,
    ) -> AsyncIterator[WorkflowStreamItem[Any]]:
        """Yield items from ``from_offset`` as they arrive.

        ``poll_cooldown`` is accepted and ignored. It paced a client that had
        to re-ask; the server parks this read until something arrives.
        """
        del poll_cooldown
        await self._pin()
        async for entry in self._handle.follow(from_offset=from_offset, topics=topics):
            record = await self._decoded(entry.record)
            yield WorkflowStreamItem(
                topic=record.topic,
                data=_decode(self._converter, record, result_type),
                offset=entry.offset,
            )

    async def poll_raw(
        self,
        *,
        topics: Sequence[str] = (),
        from_offset: int = 0,
        wait: bool = True,
    ) -> RawPage:
        """One read, with the records left as they were stored.

        For a caller that forwards items on rather than using them. A gateway
        would only have to encode again what this decoded.
        """
        await self._pin()
        page = await self._handle.poll(
            from_offset=from_offset, topics=topics, wait=wait
        )
        return RawPage(
            items=[
                WorkflowStreamItem(
                    topic=entry.record.topic, data=entry.record, offset=entry.offset
                )
                for entry in page.entries
            ],
            next_offset=page.next_offset,
            head_offset=page.head_offset,
            closed=page.closed,
        )

    async def flush(self) -> None:
        """Append everything buffered as one batch.

        A batch whose append failed stays pending and goes out again on the
        next flush under the sequence it already had, so an append the server
        did accept is deduplicated and one it never saw still lands. Nothing
        comes off the buffer until there is a batch to replace it with.

        Raises:
            temporalio.streams.StreamProducerError: The server holds this
                producer's sequence with different content.
        """
        if self._pending is not None:
            records, sequence = self._pending
        else:
            if not self._buffered:
                return
            await self._pin()
            # Encoded before the buffer is cleared, so a converter failure
            # leaves the values where the caller can still see them.
            records = [
                await self._encoded(_record(self._converter, topic, value))
                for topic, value in self._buffered
            ]
            sequence = self._sequence
            self._buffered = []
            self._pending = (records, sequence)
        await self._pin()
        await self._handle.append(
            *records, producer_id=self._producer_id, sequence=sequence
        )
        self._sequence = sequence + len(records)
        self._pending = None

    def _buffer(self, topic: str, value: Any) -> None:
        self._buffered.append((topic, value))

    def _flush_soon(self) -> None:
        self._wake.set()

    async def _run_flusher(self) -> None:
        """Append on a fixed cadence, so a slow producer still gets delivered."""
        while not self._closed:
            try:
                await asyncio.wait_for(
                    self._wake.wait(), self._batch_interval.total_seconds()
                )
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            await self.flush()

    async def _encoded(self, record: StreamRecord) -> StreamRecord:
        if self._codec is not None and record.HasField("body"):
            record.body.CopyFrom((await self._codec.encode([record.body]))[0])
        return record

    async def _decoded(self, record: StreamRecord) -> StreamRecord:
        if self._codec is not None and record.HasField("body"):
            record.body.CopyFrom((await self._codec.decode([record.body]))[0])
        return record


def _stream_client(client: Client) -> StreamClient:
    return shared_client(client.service_client.config.target_host, client.namespace)
