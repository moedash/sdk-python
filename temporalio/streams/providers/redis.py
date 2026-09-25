"""The client-side (Redis) provider.

Streams live in a store the customer runs, Redis here, behind the external
workflow streams transport. A workflow's publish is buffered by the worker,
staged invisibly under a token when the Workflow Task completes, and promoted
only once a marker in History proves the task was accepted. Consumption is
recorded the same way, as ranges and boundaries in History.

The mapping, in one place:

- The transport keeps two Redis streams per topic, one the workflow reads and
  one outside readers read, because the direction is part of every key it
  derives. An outside producer therefore appends each record to both: the
  input stream, which wakes a workflow subscribed to the topic, and the
  output stream, where the workflow's own promoted batches land, so an
  outside reader sees the producer's records and the workflow's in one order
  and a workflow reading the topic misses nothing however early the producer
  started. The cost is one more append per record and one wake per batch.
- A record rides as the transport's payload: the serialized ``StreamRecord``
  proto as a ``binary/plain`` value, which the worker's codec encodes and
  decodes like any other payload.
- Producer identity dedupes through the transport's idempotency: the session
  is ``producer#attempt`` and every record carries a sequence, so a retried
  batch is dropped with the original position and a new attempt passes.
- Cursors are ``redis:<ms>-<seq>`` on the outside surface and name a position
  in the output stream. A workflow-side record carries ``redis:in:<ms>-<seq>``,
  a position in the input stream, and only that form seeds a workflow reader:
  the two streams number their entries independently, so an outside cursor
  cannot stand in for one. A reader opened without a cursor starts where the
  chain's predecessor run committed, which is the transport's own rule.
- Streams are keyed by the chain's first run, so a handle follows continue-as-
  new by construction and ``run_id`` only decides whose close ends a read.
- A task's publishes are staged as one batch. The transport's own per-task
  batch limits are lifted for this provider, because a synchronous publish
  cannot wait for the worker to stage a full batch; a batch it cannot stage
  fails the task.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import AsyncGenerator, Coroutine
from datetime import timedelta
from typing import Any, Generic, TypeVar

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.external_workflow_streams import (
    AFTER,
    AppendConflictError,
    AppendNotAcknowledgedError,
    ChainKeyMismatchError,
    ExternalOutputStreamProducer,
    ExternalStreamProducer,
    Offset,
    OutputAppendNotAcknowledgedError,
    StreamDirection,
    WakeNotAcknowledgedError,
    WorkflowChainKey,
    external_output_stream,
    external_stream,
)
from temporalio.contrib.external_workflow_streams import (
    BEGINNING as TRANSPORT_BEGINNING,
)
from temporalio.contrib.external_workflow_streams import (
    RecordKind as TransportRecordKind,
)
from temporalio.contrib.external_workflow_streams import (
    StreamError as TransportStreamError,
)
from temporalio.contrib.external_workflow_streams._backend import DEFAULT_WATCH_BLOCK
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._output_client import (
    _reconcile_output_stage,
)
from temporalio.converter import WorkflowSerializationContext
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._errors import (
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
)
from temporalio.streams._provider import ReadSource, WriteSink
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord
from temporalio.streams._topic import StreamTopic, resolve_topic
from temporalio.streams._wire import (
    RecordDecoder,
    WireRecord,
    cursor_position,
    mint_cursor,
    producer_identity,
    to_wire,
)
from temporalio.streams.providers import ProviderPlugin
from temporalio.worker import ReplayerConfig, WorkerConfig

__all__ = ["RedisProducer", "RedisStreamHandle", "RedisStreams"]

T = TypeVar("T")

_PROVIDER = "redis"
_INPUT_PREFIX = "in:"
_REDIS_ID = re.compile(r"\d+-\d+")
_READ_BATCH = 256

# The transport's per-task batch limits are a backpressure point that awaits
# inside the workflow, and a synchronous publish has nowhere to await. Lifted
# to where they cannot fire, so a task's publishes stage as one batch.
_UNBOUNDED_OUTPUT = external_output_stream.with_options(
    max_records=1 << 30, max_logical_bytes=1 << 40
)

logger = logging.getLogger(__name__)


def _require_topic(topic: str) -> None:
    if not topic:
        raise ValueError("topic must not be empty")


def _outside_position(after: Cursor) -> Offset | None:
    """The output-stream offset an outside cursor names, or ``None`` for BEGINNING."""
    token = cursor_position(after, provider=_PROVIDER)
    if token is None:
        return None
    if token.startswith(_INPUT_PREFIX):
        raise StreamCursorError(
            f"cursor {after.token!r} names a position in the workflow's input log, "
            "which only the workflow reads"
        )
    if not _REDIS_ID.fullmatch(token):
        raise StreamCursorError(
            f"cursor {after.token!r} does not name a Redis stream position"
        )
    return Offset(token)


def _workflow_position(after: Cursor) -> Offset | None:
    """The input-stream offset a workflow reader's cursor names, or ``None`` for BEGINNING."""
    token = cursor_position(after, provider=_PROVIDER)
    if token is None:
        return None
    if token.startswith(_INPUT_PREFIX):
        entry = token[len(_INPUT_PREFIX) :]
        if _REDIS_ID.fullmatch(entry):
            return Offset(entry)
    elif _REDIS_ID.fullmatch(token):
        raise StreamCursorError(
            f"cursor {after.token!r} names a position in the topic's output stream; "
            "a workflow reader follows the input stream, whose entry ids differ, so "
            "pass a cursor a workflow reader returned"
        )
    raise StreamCursorError(
        f"cursor {after.token!r} does not name a Redis stream position"
    )


def _drive(coroutine: Coroutine[Any, Any, None]) -> None:
    """Run a transport publish to completion without yielding to the loop.

    The transport's publish only awaits when the task's batch is full. A
    synchronous publish has nowhere to wait, so a publish that would have
    waited fails the task instead, loudly.
    """
    try:
        coroutine.send(None)
    except StopIteration:
        return
    coroutine.close()
    raise StreamError(
        "this Workflow Task's output batch is full, and a synchronous publish "
        "cannot wait for the worker to stage it"
    )


def _parse(cursor: Cursor, raw: bytes, warn: Any) -> WireRecord | None:
    try:
        return WireRecord.FromString(raw)
    except DecodeError as error:
        # Same answer as an undecodable body: skip and say so, so one bad
        # record cannot pin a reader.
        warn("skipping stream record at %s: %s", cursor, error)
        return None


class _RedisReadSource:
    """One subscription of the running workflow, over the transport's input stream."""

    def __init__(self, subscription: Any) -> None:
        self._subscription = subscription
        self._records = subscription.records()

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        while True:
            # One record per batch: the transport reports readiness per record,
            # and a batch here would invent a boundary replay never observed.
            offset, body = await self._records.__anext__()
            cursor = mint_cursor(_PROVIDER, f"{_INPUT_PREFIX}{offset.token}")
            wire = _parse(cursor, body, workflow.logger.warning)
            if wire is not None:
                return [(cursor, wire)]

    def close(self) -> None:
        self._subscription.close()


class _RedisWriteSink:
    def __init__(self, topic: str) -> None:
        self._topic = _UNBOUNDED_OUTPUT.topic(topic, type=bytes)

    def publish(self, record: WireRecord) -> None:
        # Buffered in the transport's per-task batch, staged by the worker when
        # the task completes and promoted once History proves the task was
        # accepted: rule 1 through the transport's own commit.
        _drive(self._topic.publish(record.SerializeToString()))


class _RedisWorkflowProvider:
    """The workflow half: the transport's subscriptions and staged output."""

    def __init__(self, idle_timeout: timedelta) -> None:
        self._input = external_stream.with_options(idle_timeout=idle_timeout)

    def open_reader(self, topic: str, *, after: Cursor) -> ReadSource:
        _require_topic(topic)
        position = _workflow_position(after)
        # Without a position the transport resumes where the chain's
        # predecessor run committed; with one, that is where the wait starts
        # and what the marker's header records.
        start = None if position is None else AFTER(position)
        return _RedisReadSource(
            self._input.topic(topic, type=bytes).subscribe(start_cursor=start)
        )

    def open_writer(self, topic: str) -> WriteSink:
        _require_topic(topic)
        return _RedisWriteSink(topic)

    def on_workflow_start(self) -> None:
        pass

    async def on_workflow_finish(self) -> None:
        pass


async def _chain(client: Client, workflow_id: str) -> WorkflowChainKey:
    try:
        description = await client.get_workflow_handle(workflow_id).describe()
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            raise StreamNotFoundError(
                f"workflow {workflow_id!r} was not found"
            ) from error
        raise
    return WorkflowChainKey(
        client.namespace,
        workflow_id,
        description.raw_description.workflow_execution_info.first_run_id,
    )


def _storage_error(error: Exception, what: str) -> StreamError:
    return StreamError(f"{what}: {error}")


class RedisProducer(Generic[T]):
    """Appends to a topic from outside workflow code.

    Every append is visible as soon as the store accepts it. Each record goes
    to the topic's input stream, waking a workflow subscribed to it, and to
    its output stream, where outside readers and the workflow's own records
    meet; the cursor returned names the output position.
    """

    def __init__(
        self,
        streams: RedisStreams,
        client: Client,
        workflow_id: str,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic`` of ``workflow_id``'s stream."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._converter = client.data_converter.payload_converter
        self._sequence = 0
        self._last = BEGINNING
        self._input: Any = None
        self._output: Any = None

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    @property
    def _session(self) -> str:
        # The transport dedupes on this and the sequence. The attempt is part
        # of it so a retried append is dropped while a new generation writing
        # different words at the same sequence is not.
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: T) -> Cursor:
        """Append ``values`` and return the cursor of the last record as stored.

        A repeat returns where the original landed, because the transport
        answers a byte-identical repeat with the original offset; an empty
        call returns the position of this producer's last record.
        """
        if not values:
            return self._last
        return await self._write(
            [
                to_wire(
                    self._converter,
                    topic=self._topic,
                    kind=RecordKind.DATA,
                    value=value,
                    producer_id=self._producer_id,
                    attempt=self._attempt,
                    sequence=self._sequence + index,
                )
                for index, value in enumerate(values)
            ]
        )

    async def finish(self) -> None:
        """Write ``FINISH`` for this producer on this topic.

        Written as an ordinary record rather than the transport's own
        terminal, which would end every outside read of the topic; a
        ``FINISH`` here only says this producer is done.
        """
        await self._write(
            [
                to_wire(
                    self._converter,
                    topic=self._topic,
                    kind=RecordKind.FINISH,
                    producer_id=self._producer_id,
                    attempt=self._attempt,
                    sequence=self._sequence,
                )
            ]
        )

    async def _connect(self) -> None:
        if self._output is not None:
            return
        backend = self._streams._require_backend()
        chain = await _chain(self._client, self._workflow_id)
        try:
            output = await ExternalOutputStreamProducer.connect(
                backend=backend,
                workflow=chain,
                client=self._client,
                session_id=self._session,
            )
            input_ = await ExternalStreamProducer.connect(
                backend=backend,
                workflow=chain,
                client=self._client,
                session_id=self._session,
            )
        except ChainKeyMismatchError as error:
            raise StreamNotFoundError(
                f"workflow {self._workflow_id!r} is not the chain this producer "
                f"was opened on: {error}"
            ) from error
        self._output = output.topic(self._topic, type=bytes)
        self._input = input_.topic(self._topic, type=bytes)

    async def _write(self, records: list[WireRecord]) -> Cursor:
        await self._connect()
        last: Offset | None = None
        try:
            for record in records:
                frame = record.SerializeToString()
                await self._input.publish(frame, wake=False)
                placed = await self._output.publish(frame)
                last = placed.offset
            await self._wake()
        except AppendConflictError as error:
            raise StreamProducerError(
                f"producer {self._session!r} already wrote a different record at "
                f"sequence {error.key.sequence}"
            ) from error
        except (AppendNotAcknowledgedError, OutputAppendNotAcknowledgedError) as error:
            raise _storage_error(error, "an append was not acknowledged") from error
        except TransportStreamError as error:
            raise _storage_error(error, "the store refused an append") from error
        self._sequence += len(records)
        assert last is not None
        self._last = mint_cursor(_PROVIDER, last.token)
        return self._last

    async def _wake(self) -> None:
        try:
            await self._input.wake()
        except WakeNotAcknowledgedError:
            # The records are appended; what failed is telling a consumer that
            # is no longer there to be told. A terminal record most often races
            # the consumer acting on it, so an absent consumer is the ordinary
            # ending rather than an error.
            if not await self._consumer_has_gone():
                raise
        except TransportStreamError as error:
            raise _storage_error(error, "the wake could not be sent") from error

    async def _consumer_has_gone(self) -> bool:
        """Whether the consuming execution is closing or closed.

        Asked for a few seconds rather than once: the server refuses the wake
        while the execution is closing, and at that moment its status is
        still the running one.
        """
        handle = self._client.get_workflow_handle(self._workflow_id)
        deadline = time.monotonic() + 5
        while True:
            try:
                status = (await handle.describe()).status
            except RPCError as error:
                if error.status == RPCStatusCode.NOT_FOUND:
                    return True
                raise
            if status is not None and status != WorkflowExecutionStatus.RUNNING:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.1)


class RedisStreamHandle:
    """One workflow's topics from outside, over the transport's output streams."""

    def __init__(
        self,
        streams: RedisStreams,
        client: Client,
        workflow_id: str,
        run_id: str | None,
    ) -> None:
        """Address ``workflow_id``'s topics; ``run_id`` decides whose close ends a read."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._converter = client.data_converter.payload_converter
        self._codec: StreamPayloadCodec[bytes] = StreamPayloadCodec(
            client.data_converter.with_context(
                WorkflowSerializationContext(
                    namespace=client.namespace, workflow_id=workflow_id
                )
            ),
            bytes,
        )

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` after ``after`` until the chain, or the pinned run, closes."""
        topic, result_type = resolve_topic(topic, result_type)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        position = _outside_position(after)
        return self._read(topic, position, after, result_type)

    async def _read(
        self,
        topic: str,
        position: Offset | None,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        backend = self._streams._require_backend()
        chain = await _chain(self._client, self._workflow_id)
        key = chain.stream_key(topic, direction=StreamDirection.OUTPUT)
        cursor = TRANSPORT_BEGINNING if position is None else AFTER(position)
        closed = False
        while True:
            try:
                result = await backend.read_output_after(
                    key,
                    cursor,
                    max_records=_READ_BATCH,
                    block=self._streams._poll,
                )
            except TransportStreamError as error:
                raise _storage_error(error, "the store could not be read") from error
            for placed in result.records:
                cursor = AFTER(placed.offset)
                if placed.kind is not TransportRecordKind.DATA:
                    continue
                minted = mint_cursor(_PROVIDER, placed.offset.token)
                wire = _parse(
                    minted, await self._codec.decode(placed.payload), logger.warning
                )
                if wire is None:
                    continue
                for record in decoder.decode(minted, wire):
                    yield record
            if result.pending is not None:
                # A staged batch whose task History has not settled yet is a
                # barrier: nothing past it is read until History says whether
                # the task was accepted.
                resolved = await _reconcile_output_stage(
                    backend=backend,
                    client=self._client,
                    workflow_id=self._workflow_id,
                    manifest=result.pending.manifest,
                )
                if not resolved:
                    await asyncio.sleep(self._streams._poll.total_seconds())
                continue
            if result.records:
                continue
            if closed:
                return
            # One more pass after learning the workflow closed, so a batch
            # promoted between the read and the describe is not lost.
            closed = await self._closed()

    async def _closed(self) -> bool:
        handle = self._client.get_workflow_handle(
            self._workflow_id, run_id=self._run_id
        )
        try:
            status = (await handle.describe()).status
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                # The run's History is gone; nothing more can be promoted.
                return True
            raise
        if status is None or status == WorkflowExecutionStatus.RUNNING:
            return False
        # The streams are keyed by the chain, so a run that continued as new
        # is not the end unless the handle was pinned to it.
        return not (
            self._run_id is None and status == WorkflowExecutionStatus.CONTINUED_AS_NEW
        )

    async def latest(self, *, topic: str | StreamTopic[Any]) -> Cursor:
        """The cursor of the newest committed record on ``topic``, for following from now."""
        topic, _ = resolve_topic(topic)
        backend = self._streams._require_backend()
        chain = await _chain(self._client, self._workflow_id)
        try:
            tail = await backend.output_tail(
                chain.stream_key(topic, direction=StreamDirection.OUTPUT)
            )
        except TransportStreamError as error:
            raise _storage_error(error, "the store's tail could not be read") from error
        if tail.is_beginning:
            return BEGINNING
        assert tail.offset is not None
        return mint_cursor(_PROVIDER, tail.offset.token)

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any],
        producer_id: str = "",
        attempt: int = 0,
    ) -> RedisProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return RedisProducer(
            self._streams, self._client, self._workflow_id, topic, producer_id, attempt
        )


class RedisStreams(ProviderPlugin):
    """The client-side provider over Redis streams.

    Construct one, pass it to the worker as a plugin and open handles from
    it anywhere else. The Redis client is opened on first use, so it belongs
    to the loop that uses it, and released by :meth:`close`; a ``backend``
    the caller hands in stays the caller's to close.
    """

    def __init__(
        self,
        *,
        url: str = "redis://127.0.0.1:6379",
        key_prefix: str = "temporal-streams",
        idle_timeout: timedelta = timedelta(seconds=1),
        backend: Any | None = None,
        poll_interval: timedelta = timedelta(milliseconds=500),
    ) -> None:
        """Create the provider.

        Args:
            url: The Redis to connect to when no ``backend`` is given.
            key_prefix: Prepended to every key, so one Redis serves several
                deployments.
            idle_timeout: How long a workflow reader with nothing to read
                holds its Workflow Task open before the worker parks it.
            backend: A transport backend the caller constructed and owns.
            poll_interval: How long an outside reader that is caught up waits
                for a record before asking whether the workflow closed.
        """
        self._url = url
        self._key_prefix = key_prefix
        self._idle_timeout = idle_timeout
        self._backend = backend
        self._owned_client: Any = None
        self._poll = poll_interval

    def _require_backend(self) -> Any:
        if self._backend is None:
            import redis.asyncio

            from temporalio.contrib.external_workflow_streams._redis import (
                RedisStreamBackend,
            )

            # A dead peer would otherwise hold a blocking read open forever.
            # Several block periods, so a healthy socket that is merely idle
            # inside one read window is never abandoned.
            self._owned_client = redis.asyncio.from_url(
                self._url,
                decode_responses=False,
                socket_timeout=DEFAULT_WATCH_BLOCK.total_seconds() * 6,
            )
            self._backend = RedisStreamBackend(
                client=self._owned_client, key_prefix=self._key_prefix
            )
        return self._backend

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Set this provider and its transport backend on the worker."""
        config = super().configure_worker(config)
        config["external_stream_backend"] = self._require_backend()
        return config

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Set this provider and its transport backend on the replayer."""
        config = super().configure_replayer(config)
        config["external_stream_backend"] = self._require_backend()
        return config

    def workflow_provider(self) -> _RedisWorkflowProvider:
        """The workflow half, over the transport's subscriptions and staged output."""
        return _RedisWorkflowProvider(self._idle_timeout)

    def get_stream_handle(
        self, client: Client, workflow_id: str, *, run_id: str | None = None
    ) -> RedisStreamHandle:
        """A handle on ``workflow_id``'s topics; it follows the chain by construction."""
        return RedisStreamHandle(self, client, workflow_id, run_id)

    async def close(self) -> None:
        """Release the Redis connections this provider opened."""
        client, self._owned_client = self._owned_client, None
        if client is not None:
            self._backend = None
            await client.aclose()
