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
  started. Both keys are written by one script, so a record is on both or on
  neither: split across two calls, a crash between them leaves a record the
  workflow consumes and no outside reader can ever see. The cost is one more
  append per record and one wake per batch. A wake the server refuses because
  the consuming run is closing is sent again after a short wait: a run that
  continues as new hands the stream to its successor, the records are already
  where the successor reads them, and only the wake has to follow. The
  retries stop when the chain's current run takes the wake or the chain
  proves terminal.
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
- A workflow's streams are keyed by the chain's first run, so a handle
  follows continue-as-new by construction and ``run_id`` only decides whose
  close ends a read.
- An activity's own streams are one Redis stream per topic, keyed by the
  namespace, the workflow id, empty for a standalone activity, and the
  activity id. The owner is one key component joined with ``/``, a character
  the chain keys percent-encode out of every id, so no chain key and no key
  derived from one can name an activity's stream. There is no input stream,
  because no workflow reads these, and no staging, because an activity's
  append is visible as soon as the store accepts it. The key holds the
  activity id and not its run, so a retry writes to the same stream, which a
  reader sees as ``SUPERSEDED``, and an id started again continues it. A
  read ends when the owner is terminal and the retained tail is delivered: a
  standalone activity is described directly, and a workflow's activity
  through its workflow, whose close ends the read, as does the activity
  leaving the pending set once its stream exists. An activity that never
  wrote has no stream, so a read on it waits for the workflow. Nothing gates
  an append after that, so a late attempt still lands, and a read that has
  ended does not see it.
- A task's publishes are staged as one batch. The transport's own per-task
  batch limits are lifted for this provider, because a synchronous publish
  cannot wait for the worker to stage a full batch; a batch it cannot stage
  fails the task.
- Retention is trimming, with no consumer floor. By default a record older
  than :data:`DEFAULT_RETENTION`, seven days, is trimmed by the next append
  the provider makes to its key, an activity's stream included, whatever any
  reader has reached; ``retention=None`` turns the age trim off, and
  ``max_len`` adds a count cap that is off by default. A replay that reaches
  a recorded range the trim removed fails its Workflow Task, an outside
  cursor below the trim is refused, and a fully trimmed topic reads as
  empty. A run that has to replay cold after seven days of consuming fails,
  so a long-lived consumer continues as new inside the window, or is
  configured with a longer one.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import AsyncGenerator, Coroutine, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final, Generic, TypeVar
from urllib.parse import quote

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.client import ActivityExecutionStatus, Client, WorkflowExecutionStatus
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
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._output_client import (
    _reconcile_output_stage,
)
from temporalio.contrib.external_workflow_streams._redis import (
    _BEGINNING_SENTINEL,
    RedisStreamBackend,
    _content_hash,
    _text,
    _to_record,
)
from temporalio.converter import (
    ActivitySerializationContext,
    SerializationContext,
    WorkflowSerializationContext,
)
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

__all__ = ["DEFAULT_RETENTION", "RedisProducer", "RedisStreamHandle", "RedisStreams"]

T = TypeVar("T")

_PROVIDER = "redis"
_INPUT_PREFIX = "in:"
_REDIS_ID = re.compile(r"\d+-\d+")
_READ_BATCH = 256

#: How long a record is kept when the constructor is not told otherwise.
#:
#: An age rather than a count, because the count a topic can afford depends on
#: its record size and the age does not, and because a count cap refuses a task
#: whose batch does not fit under it. Seven days is long enough to replay a
#: consumer that was evicted over a weekend and short enough that a chain
#: nobody reads any more does not keep its records for good.
DEFAULT_RETENTION: Final = timedelta(days=7)

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


def _entry_id(token: str | bytes) -> tuple[int, int]:
    """A Redis entry id as the ``(ms, seq)`` pair it orders by."""
    text = token.decode() if isinstance(token, bytes) else token
    ms, _, seq = text.partition("-")
    return int(ms), int(seq or 0)


#: Append one record to a topic's two keys, or to neither.
#:
#: The provider's own write, not the transport's: the transport appends one key at a
#: time, and the two calls that would take are not one failure. A record on the input
#: key alone is one the workflow consumes and no outside reader can ever see, and a
#: producer that comes back with a fresh sequence never heals it.
#:
#: Both idempotency hashes are read before either stream is touched, so a key already
#: used with different bytes refuses the pair rather than half-writing it. Either half
#: already present is reused, which is what settles a pair some earlier call left
#: half-written. The retention trims ride along rather than costing their own round
#: trips, and they are exact for the reason the backend's own trims are.
_PAIRED_APPEND_LUA: Final = """
local minid = ARGV[1]
local maxlen = ARGV[2]
local function placed(idem, key, digest)
  local existing = redis.call('HGET', idem, key)
  if not existing then
    return nil
  end
  local sep = string.find(existing, '|')
  if string.sub(existing, sep + 1) ~= digest then
    return 'conflict'
  end
  return string.sub(existing, 1, sep - 1)
end
local input = placed(KEYS[2], ARGV[3], ARGV[4])
local output = placed(KEYS[4], ARGV[3], ARGV[4])
if input == 'conflict' or output == 'conflict' then
  return {'conflict', '', ''}
end
local fields = {unpack(ARGV, 5)}
if not input then
  input = redis.call('XADD', KEYS[1], '*', unpack(fields))
  redis.call('HSET', KEYS[2], ARGV[3], input .. '|' .. ARGV[4])
end
if not output then
  output = redis.call('XADD', KEYS[3], '*', unpack(fields))
  redis.call('HSET', KEYS[4], ARGV[3], output .. '|' .. ARGV[4])
end
if minid ~= '' then
  redis.call('XTRIM', KEYS[1], 'MINID', minid)
  redis.call('XTRIM', KEYS[3], 'MINID', minid)
end
if maxlen ~= '' then
  redis.call('XTRIM', KEYS[1], 'MAXLEN', maxlen)
  redis.call('XTRIM', KEYS[3], 'MAXLEN', maxlen)
end
return {'ok', input, output}
"""


def _trim_floor(backend: Any) -> str:
    retention = getattr(backend, "_retention", None)
    if retention is None:
        return ""
    # The worker's clock names the floor, so a skewed worker shifts the window by
    # its skew, the same as the backend's own trim.
    floor = int((time.time() - retention.total_seconds()) * 1000)
    return f"{max(floor, 0)}-0"


def _trim_maxlen(backend: Any) -> str:
    max_len = getattr(backend, "_max_len", None)
    return "" if max_len is None else str(max_len)


def _append_args(backend: Any, record: TransportRecord) -> list[Any]:
    """What both append scripts take: the trims, the record's identity, its fields."""
    args: list[Any] = [
        _trim_floor(backend).encode(),
        _trim_maxlen(backend).encode(),
        str(record.idempotency_key).encode(),
        _content_hash(record).encode(),
    ]
    for name, value in sorted(record.to_fields().items()):
        args.append(name.encode())
        args.append(value)
    return args


class _PairedAppend:
    """One logical record on a topic's input and output keys, in one Redis call."""

    def __init__(self, backend: RedisStreamBackend) -> None:
        """Bind to ``backend``'s client and key layout."""
        self._backend = backend
        self._script = backend._client.register_script(_PAIRED_APPEND_LUA)

    async def write(
        self,
        *,
        input_key: StreamKey,
        output_key: StreamKey,
        record: TransportRecord,
    ) -> Offset:
        """Append ``record`` to both keys and return where it landed on the output key.

        A repeat of the same ``(session, sequence)`` with the same bytes returns the
        original positions and writes nothing, so a call whose answer was lost is
        settled by making it again.
        """
        outcome, _, output = await self._script(
            keys=[
                self._backend.stream_key(input_key),
                self._backend._idempotency_key(input_key),
                self._backend.stream_key(output_key),
                self._backend._idempotency_key(output_key),
            ],
            args=_append_args(self._backend, record),
        )
        if _text(outcome) == "conflict":
            raise AppendConflictError(record.idempotency_key)
        return Offset(_text(output))


#: Append one record to an activity's stream, or answer where it already is.
#:
#: The single-key form of the paired write: an activity's stream has no input
#: half and no stage, so one stream and its idempotency hash are the whole
#: write, and the retention trims ride along as they do on the pair.
_OWNED_APPEND_LUA: Final = """
local minid = ARGV[1]
local maxlen = ARGV[2]
local existing = redis.call('HGET', KEYS[2], ARGV[3])
if existing then
  local sep = string.find(existing, '|')
  if string.sub(existing, sep + 1) ~= ARGV[4] then
    return {'conflict', ''}
  end
  return {'ok', string.sub(existing, 1, sep - 1)}
end
local id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 5))
redis.call('HSET', KEYS[2], ARGV[3], id .. '|' .. ARGV[4])
if minid ~= '' then
  redis.call('XTRIM', KEYS[1], 'MINID', minid)
end
if maxlen ~= '' then
  redis.call('XTRIM', KEYS[1], 'MAXLEN', maxlen)
end
return {'ok', id}
"""


class _OwnedAppend:
    """One record on an activity's stream, in one Redis call."""

    def __init__(self, backend: RedisStreamBackend) -> None:
        """Bind to ``backend``'s client."""
        self._backend = backend
        self._script = backend._client.register_script(_OWNED_APPEND_LUA)

    async def write(self, *, name: str, record: TransportRecord) -> Offset:
        """Append ``record`` to the stream ``name`` and return where it landed.

        A repeat of the same ``(session, sequence)`` with the same bytes returns
        the original position and writes nothing.
        """
        outcome, placed = await self._script(
            keys=[name, f"{name}:idem"], args=_append_args(self._backend, record)
        )
        if _text(outcome) == "conflict":
            raise AppendConflictError(record.idempotency_key)
        return Offset(_text(placed))


def _prefix(backend: Any) -> str:
    """The key prefix ``backend`` writes under, for keys the provider derives itself."""
    return backend._key_prefix


@dataclass(frozen=True)
class _ActivityOwner:
    """The activity whose streams a handle addresses, and where Redis keeps them.

    ``workflow_id`` is ``None`` for a standalone activity. ``run_id`` pins the
    workflow's run for a workflow's activity and the activity's own run for a
    standalone one: it decides whose close ends a read and never which key is
    read, because an activity's streams belong to the activity id.
    """

    namespace: str
    workflow_id: str | None
    activity_id: str
    run_id: str | None

    def key(self, key_prefix: str, topic: str) -> str:
        """The Redis stream holding ``topic`` of this activity's streams."""
        # The chain keys percent-encode every id, so none of their components
        # holds a "/", and a component built around one can never equal a
        # chain key or a key derived from one.
        namespace = quote(self.namespace, safe="")
        owner = f"activity/{quote(self.workflow_id or '', safe='')}/"
        owner += quote(self.activity_id, safe="")
        return f"{key_prefix}:{namespace}:{owner}:{quote(topic, safe='')}"

    def context(self) -> SerializationContext:
        """What the owner's payloads are coded under.

        A workflow's activity writes the workflow's data, as it does on the
        workflow's own topics; a standalone activity has no workflow, so its
        own identity is the context.
        """
        if self.workflow_id is not None:
            return WorkflowSerializationContext(
                namespace=self.namespace, workflow_id=self.workflow_id
            )
        return ActivitySerializationContext(
            namespace=self.namespace,
            activity_id=self.activity_id,
            activity_type=None,
            activity_task_queue=None,
            workflow_id=None,
            workflow_type=None,
            is_local=False,
        )

    def __str__(self) -> str:
        """The owner, for messages."""
        if self.workflow_id is None:
            return f"activity {self.activity_id!r}"
        return f"activity {self.activity_id!r} of workflow {self.workflow_id!r}"


async def _retained(client: Any, name: str, offset: Offset) -> bool:
    """Whether the record at ``offset`` on the stream ``name`` survived trimming.

    A record at or after the first retained entry is there. On an emptied
    stream the last id Redis generated says whether the record ever was.
    """
    if not await client.exists(name):
        # Nothing was ever written under this key, so nothing was trimmed from it.
        return True
    info = await client.xinfo_stream(name)
    wanted = _entry_id(offset.token)
    first = info.get("first-entry")
    if first:
        return _entry_id(first[0]) <= wanted
    return wanted > _entry_id(info["last-generated-id"])


class _RetainingBackend(RedisStreamBackend):
    """The transport's Redis backend, trimming the key behind every append it makes.

    Trims are exact rather than approximate: Redis's approximate trim drops
    whole macro nodes only, so a stream shorter than one node, a hundred
    entries by default, would never trim and the window would not mean what
    it says. Only the streams are trimmed; the idempotency and stage hashes
    beside them keep one entry per record and stage.
    """

    def __init__(
        self,
        *,
        client: Any,
        key_prefix: str,
        retention: timedelta | None,
        max_len: int | None,
    ) -> None:
        super().__init__(client=client, key_prefix=key_prefix)
        self._retention = retention
        self._max_len = max_len

    def describe_window(self) -> str:
        """The configured window, for messages."""
        parts = []
        if self._retention is not None:
            parts.append(f"retention={self._retention}")
        if self._max_len is not None:
            parts.append(f"max_len={self._max_len}")
        return ", ".join(parts) or "no retention"

    async def append(self, key: StreamKey, record: Any) -> Any:
        placed = await super().append(key, record)
        await self._trim(key)
        return placed

    async def stage_output(
        self, manifest: OutputStageManifest, records: Sequence[StagedOutputRecord]
    ) -> OutputStage:
        # A stage is invisible until its task commits, and the trim has no consumer
        # floor to hold it: a window at or below the batch takes entries out of the
        # stage that was just written, and the commit then fails on a missing record
        # for as long as the task retries. Flooring the trim instead would need the
        # floor this provider deliberately does not keep, and would not hold anyway,
        # because the trim that removes the stage is not the one that staged it.
        if self._max_len is not None and manifest.record_count >= self._max_len:
            raise ValueError(
                f"this task publishes {manifest.record_count} records and max_len is "
                f"{self._max_len}: the window has to exceed the largest batch a task "
                "publishes, or the batch is trimmed before it commits"
            )
        stage = await super().stage_output(manifest, records)
        await self._trim(manifest.stream_key)
        return stage

    async def read_range(self, key: StreamKey, first: Offset, last: Offset) -> Any:
        # The replay read. Said here, where the trim is known, rather than left
        # to the range checks, which can only report the record as missing.
        if not await self.retains(key, first):
            raise StreamIntegrityError(
                f"the recorded range [{first}, {last}] on topic "
                f"{key.stream_name!r} is past the redis provider's retention "
                f"({self.describe_window()}): the records were trimmed, so this "
                "run cannot be replayed"
            )
        return await super().read_range(key, first, last)

    async def retains(self, key: StreamKey, offset: Offset) -> bool:
        """Whether the record at ``offset`` survived trimming."""
        return await _retained(self._client, self.stream_key(key), offset)

    async def _trim(self, key: StreamKey) -> None:
        name = self.stream_key(key)
        if self._retention is not None:
            # The worker's clock names the floor, so a skewed worker shifts
            # the window by its skew.
            floor = int((time.time() - self._retention.total_seconds()) * 1000)
            await self._client.xtrim(
                name, minid=f"{max(floor, 0)}-0", approximate=False
            )
        if self._max_len is not None:
            await self._client.xtrim(name, maxlen=self._max_len, approximate=False)


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


#: A chain still has a consumer while a run is in either of these: a run that
#: continued as new hands the stream to its successor rather than ending it.
_STILL_CONSUMING: Final = (
    WorkflowExecutionStatus.RUNNING,
    WorkflowExecutionStatus.CONTINUED_AS_NEW,
)


def _storage_error(error: Exception, what: str) -> StreamError:
    return StreamError(f"{what}: {error}")


def _integrity_error(error: Exception, what: str) -> StreamError:
    # Named as a loss rather than a transient read failure, because no retry brings
    # a trimmed record back and the caller's next move is different.
    return StreamNotFoundError(f"{what}: {error}")


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
        workflow_id: str | None,
        topic: str,
        producer_id: str,
        attempt: int,
        owner: _ActivityOwner | None = None,
    ) -> None:
        """Bind this producer to ``topic`` of ``workflow_id``'s chain, or of ``owner``'s streams."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._owner = owner
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._converter = client.data_converter.payload_converter
        self._sequence = 0
        self._last = BEGINNING
        self._input: Any = None
        self._output: Any = None
        self._pair: _PairedAppend | None = None
        self._owned: _OwnedAppend | None = None
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
        if self._owner is not None:
            # No transport producer to bind and no one to wake: an activity's
            # stream has no workflow reader and no chain to check the key against.
            self._name = self._owner.key(_prefix(backend), self._topic)
            self._owned = _OwnedAppend(backend)
            self._codec = StreamPayloadCodec(
                self._client.data_converter.with_context(self._owner.context()), bytes
            )
            return
        assert self._workflow_id is not None
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
        self._pair = _PairedAppend(backend)
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
        except (AppendNotAcknowledgedError, OutputAppendNotAcknowledgedError) as error:
            raise _storage_error(error, "an append was not acknowledged") from error
        except TransportStreamError as error:
            raise _storage_error(error, "the store refused an append") from error
        self._sequence += len(records)
        assert last is not None
        self._last = mint_cursor(_PROVIDER, last.token)
        return self._last

    async def _place(self, records: list[WireRecord]) -> Offset | None:
        """Append ``records`` where the owner keeps them and return the last position."""
        assert self._codec is not None
        last: Offset | None = None
        for index, record in enumerate(records):
            # Built here rather than handed to the transport's publish, which
            # appends one key per call. The identity is the same one the transport
            # would derive, so a record either side already holds is reused.
            staged = TransportRecord(
                kind=TransportRecordKind.DATA,
                payload=await self._codec.encode(record.SerializeToString()),
                producer_session_id=self._session,
                sequence=self._sequence + index,
            )
            if self._owned is not None:
                assert self._name is not None
                last = await self._owned.write(name=self._name, record=staged)
                continue
            assert self._pair is not None
            last = await self._pair.write(
                input_key=self._input.stream_key,
                output_key=self._output.stream_key,
                record=staged,
            )
        if self._owned is None:
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
        """
        deadline = time.monotonic() + _WAKE_RETRY_WINDOW.total_seconds()
        while True:
            try:
                await self._input.wake()
                return
            except WakeNotAcknowledgedError:
                if await self._chain_is_terminal():
                    return
                if time.monotonic() >= deadline:
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
    """One owner's topics from outside.

    The owner is ``workflow_id``'s workflow, read over the transport's output
    streams, or with ``activity_id`` an activity, read from the stream the
    provider keeps for it: a standalone activity without ``workflow_id``, or
    an activity that workflow scheduled.
    """

    def __init__(
        self,
        streams: RedisStreams,
        client: Client,
        workflow_id: str | None,
        run_id: str | None,
        activity_id: str | None = None,
    ) -> None:
        """Address the owner's topics; ``run_id`` decides whose close ends a read."""
        self._streams = streams
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._owner: _ActivityOwner | None = None
        if activity_id is not None:
            self._owner = _ActivityOwner(
                client.namespace, workflow_id, activity_id, run_id
            )
            context = self._owner.context()
        else:
            if workflow_id is None:
                raise ValueError(
                    "a stream handle needs a workflow_id or an activity_id"
                )
            context = WorkflowSerializationContext(
                namespace=client.namespace, workflow_id=workflow_id
            )
        self._converter = client.data_converter.payload_converter
        self._codec: StreamPayloadCodec[bytes] = StreamPayloadCodec(
            client.data_converter.with_context(context), bytes
        )

    def read(
        self,
        *,
        topic: str | StreamTopic[Any],
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` after ``after`` until the owner closes.

        For a workflow that is the chain, or the pinned run; for an activity it is
        the activity reaching a terminal status, learned as the module docstring
        says.

        Two refusals and they do not land together. A cursor another provider minted,
        or one naming a workflow's input log, is refused by this call: reading the
        token needs nothing from the store. A well-formed cursor the retention has
        trimmed is refused on the first step of the generator, because answering that
        needs a round trip and this call is not a coroutine. Neither yields a record
        first.

        Raises:
            StreamCursorError: The cursor is another provider's, or names a
                workflow's input log.
        """
        topic, result_type = resolve_topic(topic, result_type)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        position = _outside_position(after)
        if self._owner is not None:
            return self._read_owned(self._owner, topic, position, after, result_type)
        return self._read(topic, position, after, result_type)

    async def _read_owned(
        self,
        owner: _ActivityOwner,
        topic: str,
        position: Offset | None,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        backend = self._streams._require_backend()
        await self._require_owner(owner)
        store = backend._client
        name = owner.key(_prefix(backend), topic)
        if (
            position is not None
            and isinstance(backend, _RetainingBackend)
            and not await _retained(store, name, position)
        ):
            # Refused rather than resumed from the first retained record,
            # which would skip whatever the trim took in between.
            raise StreamCursorError(
                f"cursor {after.token!r} names a record on {topic!r} that the "
                f"provider's retention has trimmed ({backend.describe_window()})"
            )
        start = _BEGINNING_SENTINEL if position is None else position.token
        # XREAD BLOCK 0 waits forever, which is not what a zero poll asks for.
        block = int(self._streams._poll.total_seconds() * 1000) or None
        closed = False
        while True:
            found: Any = await store.xread(
                {name: start}, count=_READ_BATCH, block=block
            )
            entries = found[0][1] if found else []
            for entry_id, fields in entries:
                placed = _to_record(entry_id, fields)
                assert placed.offset is not None
                start = placed.offset.token
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
            if entries:
                continue
            if closed:
                return
            # One more pass after learning the owner is terminal, so a record
            # that landed between the read and the describe is not lost.
            closed = await self._owner_closed(owner, store, name)

    async def _require_owner(self, owner: _ActivityOwner) -> None:
        """Raise ``StreamNotFoundError`` when the server does not know ``owner``."""
        try:
            if owner.workflow_id is None:
                await self._client.get_activity_handle(
                    owner.activity_id, run_id=owner.run_id
                ).describe()
            else:
                await self._client.get_workflow_handle(
                    owner.workflow_id, run_id=owner.run_id
                ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(f"{owner} was not found") from error
            raise

    async def _owner_closed(self, owner: _ActivityOwner, store: Any, name: str) -> bool:
        """Whether the owning activity is terminal, as far as this store can tell.

        A standalone activity says so itself. The server does not describe a
        workflow's activity, so its workflow is asked: the workflow closing ends
        the read, and so does the activity leaving the pending set once its
        stream exists. Before the first write there is no stream, so an
        activity that never wrote is read until its workflow closes.
        """
        try:
            if owner.workflow_id is None:
                described = await self._client.get_activity_handle(
                    owner.activity_id, run_id=owner.run_id
                ).describe()
                return described.status != ActivityExecutionStatus.RUNNING
            description = await self._client.get_workflow_handle(
                owner.workflow_id, run_id=owner.run_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                # The owner's History is gone, so there is nothing left to follow.
                return True
            raise
        status = description.status
        if status is not None and status != WorkflowExecutionStatus.RUNNING:
            # An activity's streams are not the chain's: a run that continued
            # as new took its activities with it.
            return True
        pending = any(
            info.activity_id == owner.activity_id
            for info in description.raw_description.pending_activities
        )
        if pending:
            return False
        return bool(await store.exists(name))

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
        assert self._workflow_id is not None
        chain = await _chain(self._client, self._workflow_id)
        key = chain.stream_key(topic, direction=StreamDirection.OUTPUT)
        if (
            position is not None
            and isinstance(backend, _RetainingBackend)
            and not await backend.retains(key, position)
        ):
            # Refused rather than resumed from the first retained record,
            # which would skip whatever the trim took in between.
            raise StreamCursorError(
                f"cursor {after.token!r} names a record on {topic!r} that the "
                f"provider's retention has trimmed ({backend.describe_window()})"
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

    async def latest(self, *, topic: str | StreamTopic[Any]) -> Cursor:
        """The cursor of the newest committed record on ``topic``, for following from now.

        ``BEGINNING`` when the topic holds no committed record, which a topic whose
        records retention has all trimmed answers too: the two are the same state to
        a reader, and a read from it starts at the first record retained after it
        rather than at the tail the caller asked to follow from.
        """
        topic, _ = resolve_topic(topic)
        backend = self._streams._require_backend()
        if self._owner is not None:
            newest: Any = await backend._client.xrevrange(
                self._owner.key(_prefix(backend), topic), "+", "-", count=1
            )
            if not newest:
                return BEGINNING
            return mint_cursor(_PROVIDER, _text(newest[0][0]))
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
            self._streams,
            self._client,
            self._workflow_id,
            topic,
            producer_id,
            attempt,
            owner=self._owner,
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
        retention: timedelta | None = DEFAULT_RETENTION,
        max_len: int | None = None,
    ) -> None:
        """Create the provider.

        Args:
            url: The Redis to connect to when no ``backend`` is given.
            key_prefix: Prepended to every key, so one Redis serves several
                deployments.
            idle_timeout: How long a workflow reader with nothing to read
                holds its Workflow Task open before the worker parks it.
            backend: A transport backend the caller constructed and owns. It
                is trimmed by its owner, so it takes no ``max_len`` and no
                ``retention`` other than the default, which does not apply
                to it.
            poll_interval: How long an outside reader that is caught up waits
                for a record before asking whether the workflow closed.
            retention: Trim records older than this from a topic's input and
                output keys on every append the provider makes to them.
                :data:`DEFAULT_RETENTION`, seven days, unless the caller says
                otherwise; ``None`` keeps every record until ``max_len``
                trims it, or for good when that is unset too. This is
                retention without a consumer floor: nothing holds a record
                for a reader that has not reached it. A workflow whose replay
                reaches a recorded range past the window fails its Workflow
                Task with the transport's ``StreamIntegrityError`` until the
                window is raised, an outside ``read(after=)`` below the window
                raises ``StreamCursorError``, and a live reader that falls
                behind the window misses records. The floor the server-side
                provider keeps would need a consumer registry in Redis.
            max_len: Keep at most this many entries per key, trimmed on the
                same appends and with the same consequences. Off unless set.
                It must exceed the largest batch a task publishes, or a stage
                is trimmed before its commit; a batch at or above it is
                refused where it is staged.
        """
        if retention is not None and retention <= timedelta(0):
            raise ValueError("retention must be positive")
        if max_len is not None and max_len < 1:
            raise ValueError("max_len must be positive")
        if backend is not None and (
            retention not in (None, DEFAULT_RETENTION) or max_len is not None
        ):
            raise ValueError(
                "retention and max_len trim the backend this provider opens; a "
                "backend handed in is trimmed by its owner"
            )
        self._url = url
        self._key_prefix = key_prefix
        self._idle_timeout = idle_timeout
        self._backend = backend
        self._owned_client: Any = None
        self._poll = poll_interval
        self._retention = retention
        self._max_len = max_len

    def _require_backend(self) -> Any:
        if self._backend is None:
            import redis.asyncio

            # A dead peer would otherwise hold a blocking read open forever.
            # Several block periods, so a healthy socket that is merely idle
            # inside one read window is never abandoned.
            self._owned_client = redis.asyncio.from_url(
                self._url,
                decode_responses=False,
                socket_timeout=DEFAULT_WATCH_BLOCK.total_seconds() * 6,
            )
            self._backend = _RetainingBackend(
                client=self._owned_client,
                key_prefix=self._key_prefix,
                retention=self._retention,
                max_len=self._max_len,
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
        """A handle on the topics ``activity_id`` owns, apart from any workflow's.

        Without ``workflow_id`` the activity is a standalone one and ``run_id``
        pins its run; with one it is that workflow's activity and ``run_id``
        pins the workflow's run. The streams are keyed by the activity id and
        not its run, so a retry writes to the same ones and a reader sees the
        attempt change as ``SUPERSEDED``, and an id started again continues
        them. A read ends when the activity is terminal and the retained tail
        is delivered; the store has no gate on an append after that, so a late
        attempt still lands, and a read that has ended does not see it.
        """
        return RedisStreamHandle(self, client, workflow_id, run_id, activity_id)

    async def close(self) -> None:
        """Release the Redis connections this provider opened."""
        client, self._owned_client = self._owned_client, None
        if client is not None:
            self._backend = None
            await client.aclose()
