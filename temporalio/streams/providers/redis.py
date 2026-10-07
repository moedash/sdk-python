"""The client-side (Redis) provider.

Streams live in a store the customer runs, Redis here, behind the external
workflow streams transport. A workflow's publish is buffered by the worker,
staged invisibly under a token when the Workflow Task completes, and promoted
only once a marker in History proves the task was accepted. Consumption is
recorded the same way, as ranges and boundaries in History.

The mapping, in one place:

- One Redis stream per topic, the topic's log, read by the workflow and by
  outside readers alike. The transport derives an input key and an output key
  for a topic, because the direction is part of every key it renders; this
  provider's backend renders both onto the log. An outside producer appends a
  record once, and it is where the workflow's subscription reads and where an
  outside read follows; the workflow's staged batches land in the same log
  when its task completes and are promoted there, so an outside reader sees
  the producer's records and the workflow's in one order. The workflow does
  not read its own records: the backend drops the entries the workflow staged
  from every read it serves the transport, the live read and the replay read
  alike, so a recorded range and its replay agree. The wake path is the
  transport's own: a producer appends, then wakes the subscribed run through
  the input key. A wake the server refuses because the consuming run is
  closing is sent again after a short wait: a run that continues as new hands
  the log to its successor, the records are already where the successor
  reads them, and only the wake has to follow. The retries stop when the
  chain's current run takes the wake or the chain proves terminal. A run
  still refusing when the window passes is inside a Workflow Task that tried
  to close it while the wake sat buffered; the wake is dropped, because the
  record is in the log and a run that does not close after all rechecks the
  log at its next park.
- A reset run inherits the base run's History up to the reset point, and
  with it the ranges the base run's task completions recorded. It re-reads
  them from the log and replays them against the inherited markers, so the
  batches those tasks published are not published again, and live reading
  continues from the last inherited boundary. The reset-point task itself is
  run again, and what the base run consumed after the reset point is still
  in the log, so the reset run reads it again and publishes again: an
  outside reader sees that task's batch twice, once from each run. Nothing
  in the log marks the reset.
- A record rides as the transport's payload: the serialized ``StreamRecord``
  proto as a ``binary/plain`` value, which the worker's codec encodes and
  decodes like any other payload.
- Producer identity dedupes through the transport's idempotency: the session
  is ``producer#attempt`` and every record carries a sequence, so a retried
  batch is dropped with the original position and a new attempt passes.
- Cursors are ``redis:<ms>-<seq>`` and name an entry of the topic's log, so a
  cursor a workflow reader returned seeds an outside read and the other way
  round. A reader opened without a cursor starts where the chain's
  predecessor run committed, which is the transport's own rule. An outside
  read positions ``END`` and ``last=N`` against the log on the first step of
  the generator, since the call itself cannot reach the store. A workflow
  reader starts at a cursor or at the beginning.
- A workflow's streams are keyed by the chain's first run, so a handle
  follows continue-as-new by construction and ``run_id`` only decides whose
  close ends a read.
- A task's publishes are staged as one batch. The transport's own per-task
  batch limits are lifted for this provider, because a synchronous publish
  cannot wait for the worker to stage a full batch; a batch it cannot stage
  fails the task.

- Every record carries the SHA-256 of its converted body under
  ``temporal.io/content-hash``, stamped before the payload codec runs, and the
  append script compares that hash rather than the stored bytes, so a retry
  through a codec that differs on every call is still the same append while a
  divergent one is refused. A record without a body is matched by its
  plaintext record instead.

Keys written by the earlier layout. Before the log, a topic was two keys: the
transport's input key, ``<prefix>:<namespace>:<workflow id>:<first run>:<topic>``
with every id percent-encoded, and its output key, the same with an ``output``
component before the topic. The log is the input key, so the records outside
producers wrote, the ranges a workflow recorded and the cursors its readers
returned, ``redis:in:<ms>-<seq>``, read as they did. The output key is not read
any more: batches a workflow promoted under the old layout are not delivered to
an outside reader, and an outside cursor minted then names an entry of that
key, so a reader holding one starts over from ``BEGINNING`` or positions itself
with ``latest()``. Nothing moves or deletes an old output key; it ages out under
the operator's own retention.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from dataclasses import replace
from datetime import timedelta
from typing import Any, Final, Generic, TypeVar

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.external_workflow_streams import (
    AFTER,
    AppendConflictError,
    AppendNotAcknowledgedError,
    ChainKeyMismatchError,
    ExternalStreamProducer,
    Offset,
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
from temporalio.contrib.external_workflow_streams import (
    StreamRecord as TransportRecord,
)
from temporalio.contrib.external_workflow_streams._backend import (
    DEFAULT_WATCH_BLOCK,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import StreamIntegrityError
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputStage,
    OutputStageConflictError,
    OutputStageManifest,
    OutputStageNotFoundError,
    OutputStageResolutionError,
    OutputStageStatus,
    OutputStreamRecord,
)
from temporalio.contrib.external_workflow_streams._output_client import (
    _reconcile_output_stage,
)
from temporalio.contrib.external_workflow_streams._redis import (
    _BEGINNING_SENTINEL,
    _OUTPUT_STAGE_FIELD,
    RedisStreamBackend,
    _text,
    _to_record,
)
from temporalio.converter import (
    WorkflowSerializationContext,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._body import CONTENT_HASH_KEY, content_hash
from temporalio.streams._errors import (
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.streams._provider import ReadSource, WriteSink
from temporalio.streams._record import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamRecord,
    check_read_start,
)
from temporalio.streams._ref import StreamRef
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
#: What a workflow reader's cursor carried when a topic was two keys. It named
#: an entry of the input key, which is the log now, so the form is still read.
_LEGACY_INPUT_PREFIX = "in:"
_REDIS_ID = re.compile(r"\d+-\d+")
_READ_BATCH = 256
_STAGE_FIELD: Final = _OUTPUT_STAGE_FIELD.encode()

#: How long a wake the server refused is sent again before it is given up, and
#: the pause between attempts. The pause is there because in the instant between
#: two runs of a chain the successor is not yet the run a Signal resolves to.
_WAKE_RETRY_WINDOW: Final = timedelta(seconds=5)
_WAKE_RETRY_BACKOFF: Final = timedelta(milliseconds=200)

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


def _position(after: Cursor) -> Offset | None:
    """The log entry a cursor names, or ``None`` for BEGINNING.

    Raises:
        StreamCursorError: The cursor is another provider's, or does not name
            a Redis entry id.
    """
    token = cursor_position(after, provider=_PROVIDER)
    if token is None:
        return None
    if token.startswith(_LEGACY_INPUT_PREFIX):
        token = token[len(_LEGACY_INPUT_PREFIX) :]
    if not _REDIS_ID.fullmatch(token):
        raise StreamCursorError(
            f"cursor {after.token!r} does not name a Redis stream position"
        )
    return Offset(token)


def _entry_id(token: str | bytes) -> tuple[int, int]:
    """A Redis entry id as the ``(ms, seq)`` pair it orders by."""
    text = token.decode() if isinstance(token, bytes) else token
    ms, _, seq = text.partition("-")
    return int(ms), int(seq or 0)


#: Given one page of log entries, newest first, the ids of the ones that are
#: not records to the reader asking.
_SkipIn = Callable[[list[Any]], Awaitable[set[str]]]


async def _tail_after(
    store: Any, name: str, before_last: int, *, skip_in: _SkipIn | None = None
) -> Offset | None:
    """The entry the newest ``before_last`` records of the log ``name`` come after.

    ``None`` when the log holds no more than ``before_last`` records, which
    means a read from the beginning. With ``before_last=0`` it is the newest
    record itself, the boundary a read at ``END`` starts after. Walks the log
    from its newest entry, leaving out what ``skip_in`` names.
    """
    needed = before_last + 1
    end = "+"
    seen = 0
    while True:
        page: Any = await store.xrevrange(name, end, "-", count=max(needed - seen, 16))
        if not page:
            return None
        skipped = await skip_in(page) if skip_in is not None else set()
        for entry_id, _fields in page:
            token = _text(entry_id)
            if token in skipped:
                continue
            seen += 1
            if seen == needed:
                return Offset(token)
        ms, seq = _entry_id(page[-1][0])
        if seq:
            end = f"{ms}-{seq - 1}"
        elif ms:
            end = f"{ms - 1}-18446744073709551615"
        else:
            return None


def _fields_args(record: TransportRecord) -> list[Any]:
    """The record's stored fields, name then value, as the append scripts take them."""
    args: list[Any] = []
    for name, value in sorted(record.to_fields().items()):
        args.append(name.encode())
        args.append(value)
    return args


def _append_args(record: TransportRecord, digest: str) -> list[Any]:
    """What the log append script takes: the identity and digest, then the fields.

    ``digest`` is the plaintext hash of what the record carries, taken before
    the payload codec ran, so a retry the codec encoded differently is still
    recognized as the same append.
    """
    return [
        str(record.idempotency_key).encode(),
        digest.encode(),
        *_fields_args(record),
    ]


def _plaintext_digest(wire: WireRecord) -> str:
    """The digest an append is matched by: the body's hash, or the record's without one."""
    if wire.HasField("body"):
        return content_hash(wire.body)
    return hashlib.sha256(wire.SerializeToString(deterministic=True)).hexdigest()


def _stamp_hash(wire: WireRecord) -> None:
    """Put the body's plaintext hash on ``wire`` under the shared metadata key."""
    if wire.HasField("body"):
        wire.metadata[CONTENT_HASH_KEY].CopyFrom(
            Payload(
                metadata={"encoding": b"binary/plain"},
                data=content_hash(wire.body).encode(),
            )
        )


#: Append one record to a log, or answer where it already is.
#:
#: The provider's own write rather than the transport's, so a retry is matched
#: by its plaintext digest rather than by the encoded bytes. The idempotency
#: hash is read before the log is touched, so an identity already used with a
#: different plaintext digest refuses the record rather than writing it, and one
#: used with the same digest answers with the original position, which is what
#: settles a call whose answer was lost.
_LOG_APPEND_LUA: Final = """
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  local sep = string.find(existing, '|')
  if string.sub(existing, sep + 1) ~= ARGV[2] then
    return {'conflict', ''}
  end
  return {'ok', string.sub(existing, 1, sep - 1)}
end
local id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 3))
redis.call('HSET', KEYS[2], ARGV[1], id .. '|' .. ARGV[2])
return {'ok', id}
"""


class _LogAppend:
    """One record on a topic's log, in one Redis call."""

    def __init__(self, backend: RedisStreamBackend) -> None:
        """Bind to ``backend``'s client."""
        self._backend = backend
        self._script = backend._client.register_script(_LOG_APPEND_LUA)

    async def write(self, *, name: str, record: TransportRecord, digest: str) -> Offset:
        """Append ``record`` to the log ``name`` and return where it landed.

        A repeat of the same ``(session, sequence)`` with the same ``digest``
        returns the original position and writes nothing.
        """
        outcome, placed = await self._script(
            keys=[name, f"{name}:idem"],
            args=_append_args(record, digest),
        )
        if _text(outcome) == "conflict":
            raise AppendConflictError(record.idempotency_key)
        return Offset(_text(placed))


def _is_staged(fields: Any) -> bool:
    """Whether a log entry is one the workflow staged, rather than a producer's."""
    return _STAGE_FIELD in fields


class _TopicLogBackend(RedisStreamBackend):
    """The transport's Redis backend with this provider's layout.

    One key per topic: the transport renders a topic's input key and its
    output key apart, because the direction is part of every key it derives,
    and this backend renders both onto the log, the input key's rendering.
    Every read the transport makes through an input key, the live watch and
    the replay range alike, drops the entries the workflow staged itself, so
    a workflow never reads its own records and a recorded range replays to
    what was delivered. Reads through an output key are the outside reader's
    and see the whole log, with the stage protocol deciding what is visible.
    """

    def stream_key(self, key: StreamKey) -> str:
        """The topic's log, whichever direction the transport asks for."""
        return super().stream_key(replace(key, direction=StreamDirection.INPUT))

    async def read_after(
        self,
        key: StreamKey,
        after: Any,
        *,
        max_records: int,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> list[TransportRecord]:
        """The live read, without the entries the workflow staged itself.

        A batch that held nothing but the workflow's own entries is read past
        without blocking again, so the transport's recheck, which asks for one
        record, is not answered "nothing" while a producer's record sits behind
        a staged batch.
        """
        if key.direction is StreamDirection.OUTPUT:
            return await super().read_after(
                key, after, max_records=max_records, block=block
            )
        name = self.stream_key(key)
        start = _BEGINNING_SENTINEL if after.is_beginning else after.offset.serialize()
        block_ms = None if block is None else int(block.total_seconds() * 1000)
        if block_ms is not None and block_ms <= 0:
            block_ms = None
        while True:
            found: Any = await self._client.xread(
                {name: start}, count=max_records, block=block_ms
            )
            entries = found[0][1] if found else []
            if not entries:
                return []
            records = [
                _to_record(entry_id, fields)
                for entry_id, fields in entries
                if not _is_staged(fields)
            ]
            if records:
                return records
            start = _text(entries[-1][0])
            block_ms = None

    async def abort_output(self, manifest: OutputStageManifest) -> OutputStage:
        """Resolve the stage as aborted and take its entries out of the log.

        A batch whose task was not accepted yields nothing to any reader, and
        on this provider the workflow's own entries share the log with the
        producers' records, so leaving them would count them against every
        window and bound the log holds. The stage's hashes stay, status and
        offsets included, so a reader that captured the barrier settles it the
        same way.
        """
        stage = await super().abort_output(manifest)
        if stage.records:
            await self._client.xdel(
                self.stream_key(manifest.stream_key),
                *(record.offset.serialize() for record in stage.records),
            )
        return stage

    async def _output_stage_from_offsets(
        self,
        manifest: OutputStageManifest,
        status: OutputStageStatus,
        encoded_offsets: str,
    ) -> OutputStage:
        # An aborted stage's entries are gone from the log here, so they are
        # stood in for by their positions: the stage still names what it held.
        if status is not OutputStageStatus.ABORTED:
            return await super()._output_stage_from_offsets(
                manifest, status, encoded_offsets
            )
        offsets = [Offset(value) for value in encoded_offsets.split(",") if value]
        if len(offsets) != manifest.record_count:
            raise StreamIntegrityError(
                "an output stage's stored offsets do not match its manifest"
            )
        return OutputStage(
            manifest,
            tuple(
                OutputStreamRecord(TransportRecordKind.DATA, b"", offset)
                for offset in offsets
            ),
            status,
        )

    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[TransportRecord]:
        entries: Any = await self._client.xrange(
            self.stream_key(key), first.serialize(), last.serialize()
        )
        # The same entries the live read dropped, so the range holds the count
        # the marker recorded.
        return [
            _to_record(entry_id, fields)
            for entry_id, fields in entries
            if key.direction is StreamDirection.OUTPUT or not _is_staged(fields)
        ]

    async def tail_cursor(self, key: StreamKey, *, before_last: int = 0) -> Any:
        """The boundary the newest ``before_last`` records begin after.

        Through an input key the workflow's own staged entries are not
        records, as on every read the transport makes; through an output key
        only an aborted batch's entries are left out, since a pending one may
        still commit.
        """
        name = self.stream_key(key)
        skip_in = (
            self._staged_in
            if key.direction is StreamDirection.INPUT
            else self._aborted_in(key)
        )
        after = await _tail_after(self._client, name, before_last, skip_in=skip_in)
        return TRANSPORT_BEGINNING if after is None else AFTER(after)

    @staticmethod
    async def _staged_in(page: list[Any]) -> set[str]:
        return {_text(entry_id) for entry_id, fields in page if _is_staged(fields)}

    def _aborted_in(self, key: StreamKey) -> _SkipIn:
        async def aborted(page: list[Any]) -> set[str]:
            staged = {
                _text(entry_id): _text(fields[_STAGE_FIELD])
                for entry_id, fields in page
                if _is_staged(fields)
            }
            if not staged:
                return set()
            stage_ids = sorted(set(staged.values()))
            statuses = dict(
                zip(
                    stage_ids,
                    await self._client.hmget(self._output_status_key(key), stage_ids),
                )
            )
            return {
                entry_id
                for entry_id, stage_id in staged.items()
                if _text(statuses.get(stage_id) or "")
                == OutputStageStatus.ABORTED.value
            }

        return aborted


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
    """One subscription of the running workflow, over the transport's input key."""

    def __init__(self, subscription: Any) -> None:
        self._subscription = subscription
        self._records = subscription.records()

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        while True:
            # One record per batch: the transport reports readiness per record,
            # and a batch here would invent a boundary replay never observed.
            offset, body = await self._records.__anext__()
            cursor = mint_cursor(_PROVIDER, offset.token)
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
        # accepted: rule 1 through the transport's own commit. The hash is a
        # digest over bytes already in hand, so it costs no I/O here.
        _stamp_hash(record)
        _drive(self._topic.publish(record.SerializeToString()))


class _RedisWorkflowProvider:
    """The workflow half: the transport's subscriptions and staged output."""

    def __init__(self, idle_timeout: timedelta) -> None:
        self._input = external_stream.with_options(idle_timeout=idle_timeout)

    def open_reader(
        self, topic: str, *, after: Cursor, last: int | None = None
    ) -> ReadSource:
        _require_topic(topic)
        check_read_start(after, last)
        if last is not None or after == END:
            # The tail is where the log is when the worker looks, which the
            # workflow thread cannot see.
            raise StreamUnsupportedError(
                "a workflow reader on the redis provider starts at a cursor or at "
                "the beginning"
            )
        subscribe = self._input.topic(topic, type=bytes).subscribe
        position = _position(after)
        # Without a position the transport resumes where the chain's
        # predecessor run committed; with one, that is where the wait starts
        # and what the marker's header records.
        start = None if position is None else AFTER(position)
        return _RedisReadSource(subscribe(start_cursor=start))

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


#: A chain still has a consumer while a run is in either of these: a run that
#: continued as new hands the stream to its successor rather than ending it.
_STILL_CONSUMING: Final = (
    WorkflowExecutionStatus.RUNNING,
    WorkflowExecutionStatus.CONTINUED_AS_NEW,
)

#: What the server says when a Signal reaches a run whose Workflow Task is
#: closing it: on a continue-as-new handover for an instant, and after a task
#: that tried to close while the Signal sat buffered, until the next one settles.
_CLOSING_REFUSAL: Final = "workflow is closing"


def _chain_ended(error: WakeNotAcknowledgedError) -> bool:
    """Whether the server refused a wake because the chain has ended."""
    cause = error.__cause__
    return isinstance(cause, RPCError) and cause.status == RPCStatusCode.NOT_FOUND


def _refused_as_closing(error: WakeNotAcknowledgedError) -> bool:
    """Whether the server refused a wake because the run is closing."""
    return _CLOSING_REFUSAL in str(error)


def _storage_error(error: Exception, what: str) -> StreamError:
    return StreamError(f"{what}: {error}")


def _integrity_error(error: Exception, what: str) -> StreamError:
    # Named as a loss rather than a transient read failure, because no retry brings
    # a lost record back and the caller's next move is different.
    return StreamNotFoundError(f"{what}: {error}")


class RedisProducer(Generic[T]):
    """Appends to a topic from outside workflow code.

    Every append is visible as soon as the store accepts it. Each record goes
    to the topic's log, where the workflow's subscription reads it and where
    outside readers see it beside the workflow's own records, and a workflow
    subscribed to the topic is woken; the cursor returned names the entry.
    """

    def __init__(
        self,
        streams: RedisStreams,
        client: Client,
        workflow_id: str | None,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to ``topic`` of the chain."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._converter = client.data_converter.payload_converter
        # One-based, because zero on the wire says the producer does not
        # number its records and this one does.
        self._sequence = 1
        self._last = BEGINNING
        self._input: Any = None
        self._append: _LogAppend | None = None
        self._name: str | None = None
        self._codec: StreamPayloadCodec[bytes] | None = None

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
        if self._codec is not None:
            return
        backend = self._streams._require_backend()
        self._append = _LogAppend(backend)
        assert self._workflow_id is not None
        chain = await _chain(self._client, self._workflow_id)
        try:
            # The transport's producer is bound for its wake and its key: the
            # append itself is the provider's, so a retry is matched by digest.
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
        self._input = input_.topic(self._topic, type=bytes)
        self._name = backend.stream_key(self._input.stream_key)
        self._codec = StreamPayloadCodec(
            self._client.data_converter.with_context(
                WorkflowSerializationContext(
                    namespace=chain.namespace, workflow_id=chain.workflow_id
                )
            ),
            bytes,
        )

    async def _write(self, records: list[WireRecord]) -> Cursor:
        await self._connect()
        try:
            last = await self._place(records)
        except AppendConflictError as error:
            raise StreamProducerError(
                f"producer {self._session!r} already wrote a different record at "
                f"sequence {error.key.sequence}"
            ) from error
        except AppendNotAcknowledgedError as error:
            raise _storage_error(error, "an append was not acknowledged") from error
        except TransportStreamError as error:
            raise _storage_error(error, "the store refused an append") from error
        self._sequence += len(records)
        assert last is not None
        self._last = mint_cursor(_PROVIDER, last.token)
        return self._last

    async def _place(self, records: list[WireRecord]) -> Offset | None:
        """Append ``records`` to the owner's log and return the last position."""
        assert self._codec is not None and self._append is not None
        assert self._name is not None
        last: Offset | None = None
        for index, record in enumerate(records):
            # The digest is taken and stamped before the codec runs, so a retry
            # the codec encodes differently still matches its original.
            digest = _plaintext_digest(record)
            _stamp_hash(record)
            # Built here rather than handed to the transport's publish, so the
            # digest rides along. The identity is the one the transport would
            # derive, so a record the log already holds is reused.
            staged = TransportRecord(
                kind=TransportRecordKind.DATA,
                payload=await self._codec.encode(record.SerializeToString()),
                producer_session_id=self._session,
                sequence=self._sequence + index,
            )
            last = await self._append.write(
                name=self._name, record=staged, digest=digest
            )
        await self._wake()
        return last

    async def _wake(self) -> None:
        """Wake the consuming workflow, following the chain past a closing run.

        The records are appended before this is called; what can fail is
        telling the consumer. The server refuses a Signal while the run it
        resolves to is closing, and a consumer that reads a terminal record
        straight from the store and continues as new on it closes in exactly
        that way, ahead of the wake for that record. The streams are keyed by
        the chain, so the records are already where the successor reads them
        and only the wake has to follow: it is sent again, addressed to the
        workflow id as every wake is, until the chain's current run takes it.
        A chain that has ended instead is the ordinary ending of a terminal
        record racing the consumer acting on it, not an error.

        A run that still refuses when the window passes is inside a Workflow
        Task that tried to close it while this wake sat buffered, which the
        server answers by failing that task and holding the run closed to
        Signals until the next one settles. The wake is dropped then: if the
        run closes, nothing is owed; if it does not, its next park rechecks
        the log and finds the record, the transport's own rule for a record
        appended before a park. Any other refusal that outlasts the window is
        raised.

        ``NOT_FOUND`` answers the chain question without a describe: a Signal
        names the Workflow ID alone, and the server refuses it that way only
        once the chain has ended.
        """
        deadline = time.monotonic() + _WAKE_RETRY_WINDOW.total_seconds()
        while True:
            try:
                await self._input.wake()
                return
            except WakeNotAcknowledgedError as error:
                if _chain_ended(error) or await self._chain_is_terminal():
                    return
                if time.monotonic() >= deadline:
                    if _refused_as_closing(error):
                        logger.info(
                            "dropping the wake for %r on topic %r: the run is closing a "
                            "Workflow Task, and its next park rechecks the log",
                            self._workflow_id,
                            self._topic,
                        )
                        return
                    raise
            except TransportStreamError as error:
                raise _storage_error(error, "the wake could not be sent") from error
            await asyncio.sleep(_WAKE_RETRY_BACKOFF.total_seconds())

    async def _chain_is_terminal(self) -> bool:
        """Whether the chain has ended for good, rather than handing over.

        A run that continued as new is not the end: its successor is the
        consumer now, and a closing run still describes as running. A chain
        whose History is gone has ended.
        """
        assert self._workflow_id is not None
        handle = self._client.get_workflow_handle(self._workflow_id)
        try:
            status = (await handle.describe()).status
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return True
            raise
        return status is not None and status not in _STILL_CONSUMING


class RedisStreamHandle:
    """One workflow's topics from outside, over the transport's output streams."""

    def __init__(
        self,
        streams: RedisStreams,
        client: Client,
        workflow_id: str | None,
        run_id: str | None,
    ) -> None:
        """Address the workflow's topics; ``run_id`` decides whose close ends a read."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        if workflow_id is None:
            raise ValueError("a stream handle needs a workflow_id")
        converter = client.data_converter.with_context(
            WorkflowSerializationContext(
                namespace=client.namespace, workflow_id=workflow_id
            )
        )
        self._converter = client.data_converter.payload_converter
        self._codec: StreamPayloadCodec[bytes] = StreamPayloadCodec(converter, bytes)

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` from where the read starts until the workflow closes.

        That is the chain, or the pinned run. ``END`` and ``last=`` are
        positioned against the log on the first step of the generator, since
        this call cannot reach the store.

        A cursor another provider minted, or one that is not a Redis entry id,
        is refused by this call: reading the token needs nothing from the store.

        Raises:
            ValueError: ``last`` is not positive or came with a cursor.
            StreamCursorError: The cursor is another provider's, or does not name
                a Redis entry.
        """
        check_read_start(after, last)
        topic, result_type = resolve_topic(topic, result_type)
        # A tail start is resolved in the generator; a cursor is parsed here so
        # a foreign one fails this call, not the first iteration.
        tail = last if last is not None else (0 if after == END else None)
        position = None if tail is not None else _position(after)
        return self._read(topic, position, after, result_type, tail=tail)

    async def _read(
        self,
        topic: str,
        position: Offset | None,
        after: Cursor,
        result_type: type | None,
        *,
        tail: int | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        backend = self._streams._require_backend()
        assert self._workflow_id is not None
        chain = await _chain(self._client, self._workflow_id)
        key = chain.stream_key(topic, direction=StreamDirection.OUTPUT)
        if tail is not None:
            resolved = await backend.tail_cursor(key, before_last=tail)
            position = None if resolved.is_beginning else resolved.offset
            after = (
                BEGINNING
                if position is None
                else mint_cursor(_PROVIDER, position.token)
            )
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
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
            except StreamIntegrityError as error:
                # Integrity loss is permanent, and the taxonomy the transport builds
                # on it is the difference between a retry that clears and one that
                # never will. Filed as a store read, an operator retries forever.
                raise _integrity_error(error, "the store lost records") from error
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
                try:
                    resolved = await _reconcile_output_stage(
                        backend=backend,
                        client=self._client,
                        workflow_id=self._workflow_id,
                        manifest=result.pending.manifest,
                    )
                except StreamIntegrityError as error:
                    raise _integrity_error(
                        error, "a staged batch lost records"
                    ) from error
                except (
                    OutputStageConflictError,
                    OutputStageNotFoundError,
                    OutputStageResolutionError,
                ) as error:
                    # The transport's own stage vocabulary does not cross the public
                    # surface: to a reader this is a batch that cannot be settled.
                    raise _storage_error(
                        error, "a staged batch could not be settled"
                    ) from error
                except TransportStreamError as error:
                    raise _storage_error(
                        error, "a staged batch could not be settled"
                    ) from error
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
        assert self._workflow_id is not None
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
        return not (self._run_id is None and status in _STILL_CONSUMING)

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The cursor of the newest committed record on ``topic``, for following from now.

        ``BEGINNING`` when the topic holds no committed record.
        """
        topic, _ = resolve_topic(topic)
        backend = self._streams._require_backend()
        assert self._workflow_id is not None
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

    def ref(self, *, topic: str | StreamTopic[Any] | None = None) -> StreamRef:
        """A ref to ``topic`` of this owner's stream, pinned as this handle is."""
        assert self._workflow_id is not None
        return StreamRef.for_workflow(
            self._workflow_id, run_id=self._run_id, topic=topic
        )

    async def close(self) -> None:
        """Refuse: an owned stream ends with its owner, not by a caller."""
        raise ValueError(
            "only a standalone stream can be closed; this handle is on an owned "
            "stream, which ends when its workflow or activity does"
        )

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str = "",
        attempt: int = 0,
    ) -> RedisProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return RedisProducer(
            self._streams,
            self._client,
            self._workflow_id,
            topic,
            producer_id,
            attempt,
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
        client: Any | None = None,
        poll_interval: timedelta = timedelta(milliseconds=500),
    ) -> None:
        """Create the provider.

        Args:
            url: The Redis to connect to when no ``client`` is given.
            key_prefix: Prepended to every key, so one Redis serves several
                deployments.
            idle_timeout: How long a workflow reader with nothing to read
                holds its Workflow Task open before the worker parks it.
            client: A ``redis.asyncio.Redis`` the caller opened, with
                ``decode_responses=False``, and closes itself; the provider
                puts its own key layout on top of it.
            poll_interval: How long an outside reader that is caught up waits
                for a record before asking whether the workflow closed.
        """
        self._url = url
        self._key_prefix = key_prefix
        self._idle_timeout = idle_timeout
        self._client = client
        self._backend: _TopicLogBackend | None = None
        self._owned_client: Any = None
        self._poll = poll_interval

    def _require_backend(self) -> _TopicLogBackend:
        if self._backend is None:
            client = self._client
            if client is None:
                import redis.asyncio

                # A dead peer would otherwise hold a blocking read open forever.
                # Several block periods, so a healthy socket that is merely idle
                # inside one read window is never abandoned.
                client = self._owned_client = redis.asyncio.from_url(
                    self._url,
                    decode_responses=False,
                    socket_timeout=DEFAULT_WATCH_BLOCK.total_seconds() * 6,
                )
            self._backend = _TopicLogBackend(
                client=client,
                key_prefix=self._key_prefix,
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

    def get_activity_stream_handle(
        self,
        client: Client,
        activity_id: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
    ) -> RedisStreamHandle:
        """Refuse: this provider keeps no stream an activity owns.

        Raises:
            StreamUnsupportedError: Always.
        """
        raise StreamUnsupportedError(
            "the redis provider does not host activity streams"
        )

    async def create_standalone_stream(
        self,
        client: Client,
        stream_id: str,
        *,
        retention: timedelta | None = None,
        max_records: int | None = None,
        max_bytes: int | None = None,
    ) -> RedisStreamHandle:
        """Refuse: this provider keeps no stream without an owner.

        Raises:
            StreamUnsupportedError: Always.
        """
        raise StreamUnsupportedError(
            "the redis provider does not host standalone streams"
        )

    def get_standalone_stream_handle(
        self, client: Client, stream_id: str
    ) -> RedisStreamHandle:
        """Refuse: this provider keeps no stream without an owner.

        Raises:
            StreamUnsupportedError: Always.
        """
        raise StreamUnsupportedError(
            "the redis provider does not host standalone streams"
        )

    async def close(self) -> None:
        """Release the Redis connection this provider opened; a caller's stays open."""
        self._backend = None
        client, self._owned_client = self._owned_client, None
        if client is not None:
            await client.aclose()
