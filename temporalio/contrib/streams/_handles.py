"""Reaching a stream from an Activity or from client code.

Every call goes to Core's stream service, which checks the owner, mints and
checks cursors, and synthesizes ``SUPERSEDED``. What stays here is what
needs the plaintext or the user's types: converting values, the two hashes
taken before the codec, and the codec itself.

An Activity reaches the stream of the Workflow that scheduled it, and writes
as itself: Core derives its producer id as
``<Activity id>@<scheduling run id>`` and its Temporal attempt is the
producer attempt, so a retry is reported to readers as ``SUPERSEDED``. The
run is part of the id because a stream outlives a run, and Activity ids
repeat across the runs of a chain.

Any process holding a client reaches a Workflow's stream by Workflow id and
writes with a producer id and attempt of its own.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar, cast, overload

import temporalio.activity
from temporalio.bridge.proto.streams import (
    ActivityProducer,
    AppendRecord,
    AppendRequest,
    LatestRequest,
    NamedProducer,
    ReadRequest,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.client import Client
from temporalio.contrib.streams._body import batch_digest, content_hash, encode_bodies
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    call,
    store_for_client,
)
from temporalio.contrib.streams._record import Cursor, RecordKind, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import WireRecord, from_read, to_wire

__all__ = [
    "ActivityStreamHandle",
    "StreamHandle",
    "StreamProducer",
    "activity_handle",
    "get_stream_handle",
]

T = TypeVar("T")

_READ_WAIT = timedelta(seconds=30)


class StreamProducer(Generic[T]):
    """Appends to one topic as one producer attempt.

    Every append is visible as soon as the store accepts it. Each record
    carries the producer id, the attempt and a sequence that starts at one
    for each attempt, which lets the store deduplicate a retry and lets a
    reader tell a retry from a new attempt.
    """

    def __init__(
        self,
        handle: StreamHandle,
        topic: str,
        producer_id: str,
        attempt: int,
        activity: ActivityProducer | None = None,
        next_sequence: int = 1,
    ) -> None:
        """Prefer :meth:`StreamHandle.producer`."""
        self._handle = handle
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._activity = activity
        self._sequence = next_sequence
        self._last = BEGINNING
        # A batch reads the sequence, then awaits the codec and Core, then
        # moves it on; calls one at a time keep each batch's sequences.
        self._lock = asyncio.Lock()

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The attempt this producer writes."""
        return self._attempt

    async def append(self, *values: T) -> Cursor:
        """Append ``values`` as one batch and return the cursor of its last record.

        The batch lands whole and in order. The producer moves its sequence
        past the batch only when the append succeeds, so a call that raised
        :class:`temporalio.contrib.streams.StreamOutcomeUnknownError` can be
        repeated with the same values: the store answers a repeat of the
        batch it holds last for this producer attempt with the original
        position and writes nothing.

        An empty call writes nothing and returns the cursor of this
        producer's last record, or ``BEGINNING`` when it has written none.

        Calls on one producer run one at a time, in the order they were
        made, so concurrent calls take consecutive sequences. A call that was
        cancelled may or may not have written its batch, and the producer
        cannot tell: start a new attempt (a new producer with a higher
        ``attempt``) rather than continuing this one.

        Raises:
            StreamProducerError: The sequence was already used with different
                content, or it is below the newest one the store holds for
                this producer attempt.
            StreamOutcomeUnknownError: The store may or may not have written
                the batch. Retry the same values on this producer.
            StreamRefusedError: The store refused the batch, for example
                because it is out of memory. Nothing was written.
            StreamClosedError: The stream refuses appends.
        """
        async with self._lock:
            if not values:
                return self._last
            return await self._write(RecordKind.DATA, values)

    async def finish(self) -> Cursor:
        """Write ``FINISH`` for this producer on this topic and return its cursor.

        Says this producer has nothing more to send. It does not say the
        work behind it succeeded, and it does not end anyone's read.
        """
        async with self._lock:
            return await self._write(RecordKind.FINISH, [None])

    async def _write(self, kind: RecordKind, values: Any) -> Cursor:
        converter = self._handle._client.data_converter
        wires = [
            to_wire(
                converter.payload_converter,
                topic=self._topic,
                kind=kind,
                value=value,
                producer_id=self._producer_id,
                attempt=self._attempt,
                sequence=self._sequence + index,
            )
            for index, value in enumerate(values)
        ]
        request = AppendRequest(
            stream=self._handle._address(self._topic),
            sequence=self._sequence,
            records=await _append_records(converter, wires),
            digest=batch_digest(wires),
        )
        if self._activity is not None:
            request.activity.CopyFrom(self._activity)
        else:
            request.named.CopyFrom(
                NamedProducer(producer_id=self._producer_id, attempt=self._attempt)
            )
        service = await self._handle._plugin._service_for(self._handle._client)
        response = await call(service.append(request))
        self._sequence += len(wires)
        self._last = Cursor(response.last_cursor)
        return self._last


async def _append_records(
    converter: Any, wires: list[WireRecord]
) -> list[AppendRecord]:
    bodies = iter(
        await encode_bodies(converter, [w.body for w in wires if w.HasField("body")])
    )
    records = []
    for wire in wires:
        record = AppendRecord(kind=wire.kind)
        if wire.HasField("body"):
            record.body.CopyFrom(next(bodies))
            record.content_hash = content_hash(wire.body)
        records.append(record)
    return records


class StreamHandle:
    """One stream, addressed by topic, from outside Workflow code.

    A handle on a Workflow's stream follows the run chain unless its
    :attr:`ref` is pinned to a run. A topic is a
    :class:`temporalio.contrib.streams.StreamTopic`, which carries the record
    type, or a plain string with ``result_type=`` for a name decided at
    runtime. A call that names no topic addresses the topic of :attr:`ref`.
    """

    def __init__(
        self, plugin: StreamStorePlugin, client: Client, ref: StreamRef
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.get_stream_handle`."""
        self._plugin = plugin
        self._client = client
        self._ref = ref

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on, as data another process can open."""
        return self._ref

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records on ``topic`` after ``after`` as they arrive.

        ``BEGINNING`` starts at the oldest record the topic retains, and
        ``END`` at whatever is appended after the read starts. Any other
        cursor came from a record of this stream, and the read resumes just
        past it, so a reader that keeps the last cursor it handled and hands
        it back sees every stored record once. A resumed read knows the
        attempt of the record at its cursor, so a new attempt of that
        producer still arrives as ``SUPERSEDED``. Earlier attempts of other
        producers are not known to it. The read ends when the owner's run
        chain is closed and every retained record has been delivered. The
        result is a generator, so a caller that stops early calls
        ``aclose()`` on it.

        Raises:
            ValueError: ``result_type`` was passed with a topic definition,
                or the topic is empty. Raised by this call.
            StreamCursorError: ``after`` came from another store or another
                stream. Raised by the first iteration.
            StreamExpiredError: ``after`` names a record the store no longer
                retains, or the read fell behind retention while it ran.
            StreamRecordError: A record did not decode. Read on from its
                cursor to skip it.
        """
        name, result_type = self._resolve(topic, result_type)
        return self._read(name, after, result_type)

    async def _read(
        self, topic: str, after: Cursor, result_type: type | None
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        service = await self._plugin._service_for(self._client)
        request = ReadRequest(stream=self._address(topic), after=after.token)
        request.wait.FromTimedelta(_READ_WAIT)
        while True:
            response = await call(service.read(request))
            for record in response.records:
                yield await from_read(self._client.data_converter, record, result_type)
            if response.done:
                return
            request.after = response.cursor
            request.state = response.state

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The cursor of the newest record on ``topic``, or ``BEGINNING`` when empty.

        ``read(after=latest())`` yields only what is appended after this call
        returned, which is how a client positions itself before it sends
        something the Workflow answers on the stream.
        """
        name, _ = self._resolve(topic, None)
        service = await self._plugin._service_for(self._client)
        response = await call(service.latest(LatestRequest(stream=self._address(name))))
        return Cursor(response.cursor)

    @overload
    def producer(
        self,
        *,
        topic: StreamTopic[T],
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> StreamProducer[T]: ...

    @overload
    def producer(
        self,
        *,
        topic: str | None = None,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> StreamProducer[Any]: ...

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as ``producer_id`` in ``attempt``.

        A process that restarts its work raises ``attempt``, which starts a
        new sequence and tells readers that what the earlier attempt wrote is
        superseded. Keep one producer object per ``(topic, producer_id,
        attempt)``. Each object numbers its own records from one, so a second
        object for the same session is refused as stale.

        ``next_sequence`` is the sequence of the producer's next record. A
        caller that keeps its own count across processes, such as the stream
        service's handler, passes it, so a repeat of a batch dedupes.

        Raises:
            ValueError: ``producer_id`` is empty, or ``attempt`` or
                ``next_sequence`` is below one.
        """
        if not producer_id:
            raise ValueError("producer_id must not be empty")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError(f"attempt must be an int of at least 1, got {attempt!r}")
        if next_sequence < 1:
            raise ValueError(f"next_sequence must be at least 1, got {next_sequence}")
        name, _ = self._resolve(topic, None)
        return StreamProducer(
            self, name, producer_id, attempt, next_sequence=next_sequence
        )

    def _resolve(
        self, topic: str | StreamTopic[Any] | None, result_type: type | None
    ) -> tuple[str, type | None]:
        return resolve_topic(self._ref.topic if topic is None else topic, result_type)

    def _address(self, topic: str) -> StreamAddress:
        return StreamAddress(
            namespace=self._client.namespace,
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=self._ref.workflow_id,
            run_id=self._ref.run_id or "",
            topic=topic,
        )


class ActivityStreamHandle:
    """The scheduling Workflow's stream, as the running Activity sees it.

    Reads and positions as :class:`temporalio.contrib.streams.StreamHandle`
    does. :meth:`producer` writes as this Activity, which is the only
    identity that lets readers tell this Activity's retry from a new
    producer.
    """

    def __init__(self, inner: StreamHandle, identity: ActivityProducer) -> None:
        """Prefer :func:`temporalio.contrib.streams.activity_handle`."""
        self._inner = inner
        self._identity = identity

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on, pinned to the scheduling run."""
        return self._inner.ref

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.read`."""
        # One call forwards every overload, which the checker can't match one by one.
        inner = cast(Any, self._inner)
        return inner.read(topic=topic, after=after, result_type=result_type)

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamHandle.latest`."""
        return await self._inner.latest(topic=topic)

    @overload
    def producer(self, *, topic: StreamTopic[T]) -> StreamProducer[T]: ...

    @overload
    def producer(self, *, topic: str | None = None) -> StreamProducer[Any]: ...

    def producer(
        self, *, topic: str | StreamTopic[Any] | None = None
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as this Activity attempt.

        The producer id is ``<Activity id>@<scheduling run id>`` and the
        attempt is the Activity's Temporal attempt, so the first record of a
        retry makes readers see ``SUPERSEDED``. A retry starts a new sequence,
        and the store deduplicates a repeated append within one attempt.
        """
        name, _ = self._inner._resolve(topic, None)
        identity = self._identity
        return StreamProducer(
            self._inner,
            name,
            f"{identity.activity_id}@{identity.run_id}",
            identity.attempt,
            activity=identity,
        )


def activity_handle() -> ActivityStreamHandle:
    """The stream of the Workflow that scheduled the running Activity.

    The handle is pinned to the scheduling run, so a read on it ends when
    that run closes. To write to another Workflow's stream, use
    :func:`get_stream_handle` with ``temporalio.activity.client()`` and a
    producer id of your own.

    Raises:
        StreamUnsupportedError: The Activity was not scheduled by a Workflow;
            streams owned by an Activity are not supported in this release.
        ValueError: No stream store is registered on the Worker's client.
        RuntimeError: Not called from inside an Activity.
    """
    info = temporalio.activity.info()
    if info.workflow_id is None or info.workflow_run_id is None:
        raise StreamUnsupportedError(
            "this Activity was not scheduled by a Workflow, and streams owned by "
            "an Activity are not supported in this release"
        )
    client = temporalio.activity.client()
    ref = StreamRef.for_workflow(info.workflow_id, run_id=info.workflow_run_id)
    identity = ActivityProducer(
        workflow_id=info.workflow_id,
        run_id=info.workflow_run_id,
        activity_id=info.activity_id,
        attempt=info.attempt,
    )
    inner = StreamHandle(store_for_client(client), client, ref)
    return ActivityStreamHandle(inner, identity)


def get_stream_handle(
    client: Client,
    workflow_id: str | StreamRef,
    *,
    run_id: str | None = None,
    topic: str | StreamTopic[Any] | None = None,
) -> StreamHandle:
    """A handle on a Workflow's stream, through the client's store.

    Without ``run_id`` the handle follows the Workflow's run chain, so a
    read continues across Continue-as-New and ends when the chain closes.
    With one it is pinned to that run. ``topic`` is the topic a call that
    names none addresses.

    Args:
        client: A client that carries a stream store plugin.
        workflow_id: The Workflow that owns the stream, or a
            :class:`temporalio.contrib.streams.StreamRef` that names it.
        run_id: Pin the handle to one run.
        topic: The handle's default topic. Without one it is
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.

    Raises:
        ValueError: ``run_id`` or ``topic`` was given with a ref, or no
            stream store is registered on ``client``.
    """
    if isinstance(workflow_id, StreamRef):
        if run_id is not None or topic is not None:
            raise ValueError("a StreamRef carries its own run_id and topic")
        ref = workflow_id
    else:
        ref = StreamRef.for_workflow(workflow_id, run_id=run_id, topic=topic)
    ref._require_supported()
    return StreamHandle(store_for_client(client), client, ref)
