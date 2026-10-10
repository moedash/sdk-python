"""A stream provider on the application's Redis.

.. warning::
    This module is experimental and may change in future versions.

Records live in Redis and never pass through Temporal. Register the
provider once, ``Client.connect(..., plugins=[RedisStreams("redis://...")])``,
and every Worker built from that client publishes through it.

**Key layout.** Every key of one Workflow's streams shares a Redis Cluster
hash tag, the Workflow's run chain, so one script can touch all of them::

    <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>        the log
    <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:meta   its meta
    <prefix>:{<namespace>:<workflow id>:<first run id>}:stage:<token>    staged output

Each component, the prefix included, is percent-encoded, so a ``:`` or a
brace in an id cannot make two streams share a key. The chain is keyed by
its first run, so readers and producers follow Continue-as-New without a
handoff.

**Appends.** One Lua script writes a whole batch, so a reader never sees
part of one. The meta hash keeps one high-water field per producer attempt:
the batch's first sequence, its first and last entry ids, and its digest,
taken over the converted records before the codec. A retry of the newest
batch with the same digest returns the original position and writes
nothing; the same sequence with another digest, or any lower sequence, is
refused with :class:`temporalio.contrib.streams.StreamProducerError`. A
connection that fails while the script may have run raises
:class:`temporalio.contrib.streams.StreamOutcomeUnknownError`. No append
sends anything to Temporal.

**A Workflow's own publish** is staged next to the log and moved into it by
one script once History shows the Workflow Task's commit, so readers see a
task's records together or not at all.

**Retention.** Every script that writes a log trims it with ``XTRIM MINID``
to the provider's ``retention`` (seven days by default) and refreshes the
expiry of the log, its meta and the stage it writes, so a stream dies
``retention`` after its last write and nobody has to clean up when the
Workflow closes, is terminated or times out. The meta outlives its log by a
thirty day grace. It is the tombstone that lets a reader tell a log that
expired from one that never existed.

**Closing.** When the owner's run chain ends (complete, fail, cancel,
terminate or time out, but not Continue-as-New), the chain is marked
``closed`` and the append script refuses new batches with
:class:`temporalio.contrib.streams.StreamClosedError`; a retry of a batch
that landed before the close still returns its position. Redis cannot see
the Workflow close, so the mark is set by whoever sees it first: the Worker,
best effort, after a publishing run's final Workflow Task; a producer, when
it first writes; and a reader, when its read ends. Between the final
Workflow Task and the mark there is a closing window in which appends are
still accepted. If the Worker stops before it marks the chain, the window
lasts until a reader or a new producer sees the ended chain. Records written
in the window stay readable. The Workflow's own committed output is never
refused, so its final task's publish lands after the close.

A single topic closes when ``close_stream`` or ``close_workflow_stream``
(see :mod:`temporalio.contrib.streams.nexus`) closes it: its meta gets
``closed``, the append script refuses new batches on it with
:class:`temporalio.contrib.streams.StreamClosedError`, and a read of it
delivers what is left and ends. The owner Workflow's own output still lands
on a closed topic, with a warning from the Worker. The flag lives in the chain's keys, so a new
chain on the same Workflow id starts open.

**Stages left behind.** A Worker that stops between a Workflow Task's
commit and the promotion leaves its stage pending. A Worker that replays
the run promotes it. A reader also looks, when its read starts and when it
learns the owner ended: for every pending stage of the chain it reads the
staging run's History and promotes the stage if the commit is there, or
aborts it if that task failed. Promotion is idempotent, so the records
become visible exactly once.

**A read that falls behind.** After every blocking read the provider
reads the trim watermark on the same connection. When retention dropped
records the read had not delivered yet, the read raises
:class:`temporalio.contrib.streams.StreamExpiredError` instead of skipping
them. Records a read already fetched are delivered even if they are
trimmed afterwards.

**Expired, empty or missing.** A read that resumes from a cursor decides
at its start what the store still holds after it. With the log present, the
cursor is expired when it is older than the newest trimmed record. With the
log gone but its meta tombstone left, the cursor is expired when it is
older than the last record the log took, and otherwise the stream is just
empty after it. With neither left, nothing is known about the stream. These
raise :class:`temporalio.contrib.streams.StreamExpiredError` and
:class:`temporalio.contrib.streams.StreamNotFoundError` from the first
iteration, since deciding needs Redis.

Redis 7.0 or later is required; the provider refuses an older server the
first time it talks to it. At the same moment it reads ``maxmemory-policy``
and logs a warning for anything but ``noeviction``, because an evicting
Redis drops whole stream keys under memory pressure.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, Generic, TypeVar
from urllib.parse import quote

import redis.asyncio
import redis.exceptions
from google.protobuf.message import DecodeError

import temporalio.converter
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams._body import (
    content_fingerprint,
    decode_body,
    encode_body,
)
from temporalio.contrib.streams._cursor import (
    BEGINNING,
    END,
    cursor_position,
    mint_cursor,
    stream_hash,
)
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._output import (
    CHAIN_ENDED,
    StagedBatch,
    StageRef,
    _Decision,
    decide_token,
    events_after,
)
from temporalio.contrib.streams._plugin import StreamProviderPlugin
from temporalio.contrib.streams._provider import StreamProducer
from temporalio.contrib.streams._record import Cursor, RecordKind, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import RecordDecoder, WireRecord, to_wire
from temporalio.service import RPCError, RPCStatusCode

__all__ = ["RedisProducer", "RedisStreamHandle", "RedisStreams"]

_PROVIDER = "redis"
_READ_BATCH = 100
_OWNER_CHECK_MIN = timedelta(milliseconds=500)
_OWNER_CHECK_MAX = timedelta(seconds=5)
_TOMBSTONE_GRACE = timedelta(days=30)
_RECORD_FIELD = "r"

T = TypeVar("T")

logger = logging.getLogger(__name__)

# Trims a log to the retention window and slides the expiry of the log and
# its meta. The meta also records how many records the log ever took, the
# newest id, which is what remains of a log after it expires, and the newest
# trimmed id, because XTRIM leaves no trace a reader could compare with. A
# log that expired whole is trimmed through its last id when it comes back.
_KEEP_LUA = """
local function now_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end
local function revive(log, meta)
  if redis.call('EXISTS', log) == 0 then
    local last = redis.call('HGET', meta, 'last')
    if last then
      redis.call('HSET', meta, 'trimmed', last)
    end
  end
end
local function keep(log, meta, added, last, retention, grace)
  local floor = (now_ms() - retention) .. '-0'
  local doomed = redis.call('XREVRANGE', log, '(' .. floor, '-', 'COUNT', 1)
  if #doomed > 0 then
    redis.call('XTRIM', log, 'MINID', floor)
    redis.call('HSET', meta, 'trimmed', doomed[1][1])
  end
  redis.call('PEXPIRE', log, retention)
  redis.call('HINCRBY', meta, 'added', added)
  redis.call('HSET', meta, 'last', last)
  redis.call('PEXPIRE', meta, retention + grace)
end
"""

# KEYS: log, meta, chain. ARGV: retention ms, grace ms, session field, first
# sequence, digest, records... The high-water field reads
# "<sequence>|<first id>|<last id>|<digest>".
_APPEND_LUA = (
    _KEEP_LUA
    + """
local retention, grace = tonumber(ARGV[1]), tonumber(ARGV[2])
local field, digest_arg = ARGV[3], ARGV[5]
local held = redis.call('HGET', KEYS[2], field)
local sequence = tonumber(ARGV[4])
if held then
  local held_sequence, first, last, digest =
    string.match(held, '^(%d+)|([^|]+)|([^|]+)|(%x+)$')
  held_sequence = tonumber(held_sequence)
  if sequence == held_sequence then
    if digest == digest_arg then
      return {first, last}
    end
    return redis.error_reply('STREAMS_DIVERGENT sequence ' .. ARGV[4] ..
      ' was already written with different content')
  end
  if sequence < held_sequence then
    return redis.error_reply('STREAMS_STALE sequence ' .. ARGV[4] ..
      ' is below the newest one written, ' .. held_sequence)
  end
end
if redis.call('HGET', KEYS[3], 'closed') then
  return redis.error_reply('STREAMS_CLOSED the Workflow that owns this stream ' ..
    'has closed')
end
if redis.call('HGET', KEYS[2], 'closed') then
  return redis.error_reply('STREAMS_CLOSED the stream has closed')
end
revive(KEYS[1], KEYS[2])
local first, last
for i = 6, #ARGV do
  last = redis.call('XADD', KEYS[1], '*', 'r', ARGV[i])
  if not first then
    first = last
  end
end
redis.call('HSET', KEYS[2], field,
  ARGV[4] .. '|' .. first .. '|' .. last .. '|' .. digest_arg)
keep(KEYS[1], KEYS[2], #ARGV - 5, last, retention, grace)
return {first, last}
"""
)

# KEYS: stage, pending stages. ARGV: retention ms, stage token, stage
# description, then topic and record pairs. The pending hash names every
# stage not yet promoted or aborted, so a reader can find one left by a
# Worker that stopped, without a SCAN. Lua's unpack fails past a few
# thousand values, so the pairs go in chunks. The pending hash only ever
# lengthens its expiry, so it outlives every stage it names even when
# Workers of one chain use different retentions; otherwise a lost stage
# could not be told apart. PEXPIRE GT would skip a hash with no expiry yet.
_STAGE_LUA = """
local chunk = 1000
for i = 4, #ARGV, chunk do
  redis.call('RPUSH', KEYS[1], unpack(ARGV, i, math.min(i + chunk - 1, #ARGV)))
end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
if redis.call('PTTL', KEYS[2]) < tonumber(ARGV[1]) then
  redis.call('PEXPIRE', KEYS[2], ARGV[1])
end
"""

# KEYS: stage, pending stages, then a log and its meta per topic. ARGV:
# retention ms, grace ms, stage token, then the topics in KEYS order.
# Returns the records added, or -1 when the stage was still pending but its
# list was gone, which means retention dropped committed output.
_PROMOTE_LUA = (
    _KEEP_LUA
    + """
local was_pending = redis.call('HDEL', KEYS[2], ARGV[3])
local items = redis.call('LRANGE', KEYS[1], 0, -1)
if #items == 0 then
  if was_pending == 1 then
    return -1
  end
  return 0
end
local retention, grace = tonumber(ARGV[1]), tonumber(ARGV[2])
local slots = {}
for i = 4, #ARGV do
  slots[ARGV[i]] = {log = KEYS[2 * (i - 3) + 1], meta = KEYS[2 * (i - 3) + 2], added = 0}
end
for _, slot in pairs(slots) do
  revive(slot.log, slot.meta)
end
for i = 1, #items, 2 do
  local slot = slots[items[i]]
  slot.last = redis.call('XADD', slot.log, '*', 'r', items[i + 1])
  slot.added = slot.added + 1
end
for _, slot in pairs(slots) do
  if slot.added > 0 then
    keep(slot.log, slot.meta, slot.added, slot.last, retention, grace)
  end
end
redis.call('DEL', KEYS[1])
return #items / 2
"""
)


def _part(text: str) -> str:
    return quote(text, safe="")


class _ChainKeys:
    """The keys of one Workflow run chain's streams."""

    def __init__(
        self, prefix: str, namespace: str, workflow_id: str, first_run_id: str
    ) -> None:
        self.first_run_id = first_run_id
        self.base = (
            f"{_part(prefix)}:"
            f"{{{_part(namespace)}:{_part(workflow_id)}:{_part(first_run_id)}}}"
        )

    def log(self, topic: str) -> str:
        return f"{self.base}:t:{_part(topic)}"

    def meta(self, topic: str) -> str:
        return f"{self.log(topic)}:meta"

    def chain(self) -> str:
        return f"{self.base}:chain"

    def pending(self) -> str:
        return f"{self.base}:stages"

    def stage(self, token: str) -> str:
        return f"{self.base}:stage:{_part(token)}"


async def _awaited(value: Awaitable[T] | T) -> T:
    # redis-py types its commands for both its sync and asyncio clients.
    return await value if inspect.isawaitable(value) else value  # type: ignore[return-value]


def _topics(records: Any) -> list[str]:
    seen: dict[str, None] = {}
    for record in records:
        seen.setdefault(record.topic, None)
    return list(seen)


def _entry(entry_id: str) -> tuple[int, int]:
    milliseconds, _, sequence = entry_id.partition("-")
    try:
        return int(milliseconds), int(sequence or 0)
    except ValueError:
        raise StreamCursorError(f"{entry_id!r} is not a Redis stream id") from None


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _session_field(producer_id: str, attempt: int) -> str:
    # Length-prefixed, so an id that contains ':' cannot name another session.
    return f"hw:{len(producer_id)}:{producer_id}:{attempt}"


def _stream_error(error: redis.exceptions.RedisError, *, write: bool) -> StreamError:
    """The stream error a Redis client error stands for.

    A connection or timeout failure during a write leaves its outcome
    unknown; a reply the server sent means it refused the write.
    """
    if isinstance(
        error, (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)
    ):
        if write:
            return StreamOutcomeUnknownError(
                f"the write may or may not have been applied: {error}"
            )
        return StreamStorageError(f"Redis could not be reached: {error}")
    message = str(error)
    if message.startswith(("STREAMS_DIVERGENT", "STREAMS_STALE")):
        return StreamProducerError(message.split(" ", 1)[1])
    if message.startswith("STREAMS_CLOSED"):
        return StreamClosedError(message.split(" ", 1)[1])
    if isinstance(error, redis.exceptions.ResponseError):
        if write:
            return StreamRefusedError(f"Redis refused the write: {message}")
        return StreamStorageError(f"Redis refused the read: {message}")
    return StreamStorageError(f"Redis failed: {message}")


@asynccontextmanager
async def _mapped(*, write: bool) -> AsyncIterator[None]:
    # Applications catch stream errors; Redis client errors must not escape.
    try:
        yield
    except redis.exceptions.RedisError as error:
        raise _stream_error(error, write=write) from error


class RedisProducer(Generic[T]):
    """A producer on one topic of a Workflow's stream in Redis."""

    def __init__(
        self,
        handle: RedisStreamHandle,
        topic: str,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> None:
        """Prefer :meth:`RedisStreamHandle.producer`."""
        self._handle = handle
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = next_sequence
        self._last = BEGINNING
        self._owner_checked_at: float | None = None
        # A batch reads the sequence, then awaits the codec and Redis, then
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
        """See :meth:`temporalio.contrib.streams.StreamProducer.append`."""
        async with self._lock:
            if not values:
                return self._last
            converter = self._handle._converter.payload_converter
            return await self._write(
                [
                    to_wire(
                        converter,
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

    async def finish(self) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamProducer.finish`."""
        async with self._lock:
            return await self._write(
                [
                    to_wire(
                        self._handle._converter.payload_converter,
                        topic=self._topic,
                        kind=RecordKind.FINISH,
                        producer_id=self._producer_id,
                        attempt=self._attempt,
                        sequence=self._sequence,
                    )
                ]
            )

    async def _write(self, wires: list[WireRecord]) -> Cursor:
        digest = content_fingerprint(wires).hex()
        for wire in wires:
            await encode_body(self._handle._converter, wire)
        keys = await self._handle._keys()
        streams = self._handle._streams
        checked = self._owner_checked_at
        now = time.monotonic()
        # A producer that starts after the chain ended would otherwise write
        # until something else marks the chain closed. The mark expires, so a
        # check older than the retention is made again.
        if checked is None or now - checked > streams._retention_ms / 1000:
            await self._handle._refuse_if_ended(keys)
            self._owner_checked_at = now
        try:
            _, last = await streams._append(
                keys=[keys.log(self._topic), keys.meta(self._topic), keys.chain()],
                args=[
                    *streams._retention_args(),
                    _session_field(self._producer_id, self._attempt),
                    self._sequence,
                    digest,
                    *(wire.SerializeToString() for wire in wires),
                ],
            )
        except redis.exceptions.RedisError as error:
            raise _stream_error(error, write=True) from error
        self._sequence += len(wires)
        self._last = self._handle._cursor(self._topic, _text(last))
        return self._last


class RedisStreamHandle:
    """A Workflow's stream in Redis, from outside Workflow code."""

    def __init__(self, streams: RedisStreams, client: Client, ref: StreamRef) -> None:
        """Prefer :meth:`RedisStreams.get_stream_handle`."""
        self._streams = streams
        self._client = client
        self._ref = ref
        self._converter: temporalio.converter.DataConverter = client.data_converter
        self._chain: _ChainKeys | None = None

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on."""
        return self._ref

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.read`.

        The read long-polls the log with ``XREAD BLOCK``. ``END`` is
        resolved when iteration begins. While no record arrives, the read
        asks the server whether the owner closed, first after 500 ms and
        then twice as long each time, up to every 5 s; a record arriving
        starts the interval over. Once the owner has closed, the read
        delivers what is left and ends, and marks the chain closed. So an
        idle read may end up to 5 s after its owner closed. A read
        on a handle pinned to a run ends when that run closes, even if the
        chain continued as new. A read of a closed topic delivers what is
        left and ends within one poll.
        """
        name, result_type = self._resolve(topic, result_type)
        # Parsed here so a bad cursor fails this call, not the first
        # iteration.
        position = (
            None
            if after == END
            else cursor_position(after, provider=_PROVIDER, stream=self._hash(name))
        )
        previous = BEGINNING if after == END else after
        return self._read(name, after == END, position, previous, result_type)

    async def _read(
        self,
        topic: str,
        from_end: bool,
        position: str | None,
        previous: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        keys = await self._keys()
        log = keys.log(topic)
        redis_client = self._streams._redis
        await self._repair(keys)
        if position is not None:
            await self._refuse_lost(keys, topic, position)
        if from_end:
            async with _mapped(write=False):
                newest = await redis_client.xrevrange(log, count=1)
            last_id = _text(newest[0][0]) if newest else "0-0"
        else:
            last_id = position or "0-0"
        # Records dropped before the read started are not lost to it: they
        # were behind its start.
        seen = _entry(last_id)
        if position is None:
            trimmed = await _awaited(redis_client.hget(keys.meta(topic), "trimmed"))
            if trimmed is not None:
                seen = max(seen, _entry(_text(trimmed)))
        decoder = RecordDecoder(
            self._converter.payload_converter,
            result_type,
            after=previous,
            warn=logger.warning,
        )
        block_ms = self._streams._poll_ms
        ended = False
        # An idle reader asks about its owner less and less often, so many
        # readers parked on a quiet stream do not load the server.
        check_every = self._streams._owner_check_min
        loop = asyncio.get_running_loop()
        next_check = loop.time() + check_every
        while True:
            # The watermark is read after XREAD returns, on the same
            # connection, so a trim that raced the read is seen here.
            async with _mapped(write=False):
                async with redis_client.pipeline(transaction=False) as pipe:
                    pipe.xread(
                        {log: last_id},
                        count=self._streams._read_count,
                        block=None if ended else block_ms,
                    )
                    pipe.hget(keys.meta(topic), "trimmed")
                    pipe.hget(keys.meta(topic), "closed")
                    batch, trimmed, topic_closed = await pipe.execute()
            entries = batch[0][1] if batch else []
            if trimmed is not None:
                watermark = _entry(_text(trimmed))
                if watermark > seen and (
                    not entries or watermark < _entry(_text(entries[0][0]))
                ):
                    raise StreamExpiredError(
                        f"records after {last_id} on topic {topic!r} were dropped "
                        f"by retention while this read was behind; the newest "
                        f"dropped one is {_text(trimmed)}"
                    )
            for entry_id, fields in entries:
                last_id = _text(entry_id)
                seen = _entry(last_id)
                cursor = self._cursor(topic, last_id)
                try:
                    wire = WireRecord.FromString(fields[_RECORD_FIELD.encode()])
                except (KeyError, DecodeError) as error:
                    logger.warning("skipping stream record at %s: %s", cursor, error)
                    continue
                await decode_body(self._converter, wire)
                for record in decoder.decode(cursor, wire):
                    yield record
            if entries:
                check_every = self._streams._owner_check_min
                next_check = loop.time() + check_every
                continue
            if ended:
                return
            if topic_closed is not None:
                # One more pass, so a record that landed with the close is
                # delivered.
                ended = True
                continue
            if loop.time() < next_check:
                continue
            # One more pass after learning the owner closed, so a record
            # that landed between the read and the describe is delivered.
            ended = await self._owner_ended(keys)
            if ended:
                await self._repair(keys)
            check_every = min(check_every * 2, self._streams._owner_check_max)
            next_check = loop.time() + check_every

    async def _refuse_lost(self, keys: _ChainKeys, topic: str, position: str) -> None:
        """Raise when the store no longer holds what follows ``position``.

        Raises:
            StreamExpiredError: Records after ``position`` were dropped.
            StreamNotFoundError: Neither the log nor its tombstone is left.
        """
        async with _mapped(write=False):
            async with self._streams._redis.pipeline(transaction=False) as pipe:
                pipe.exists(keys.log(topic))
                pipe.hgetall(keys.meta(topic))
                exists, meta = await pipe.execute()
        cursor = _entry(position)
        if exists:
            trimmed = meta.get(b"trimmed")
            if trimmed is not None and cursor < _entry(_text(trimmed)):
                raise StreamExpiredError(
                    f"records after {position} on topic {topic!r} were dropped by "
                    f"retention; the newest dropped one is {_text(trimmed)}"
                )
            return
        if not meta:
            raise StreamNotFoundError(
                f"topic {topic!r} keeps no log and no tombstone, so nothing is "
                f"known about what followed {position}"
            )
        last = meta.get(b"last")
        if last is not None and cursor < _entry(_text(last)):
            raise StreamExpiredError(
                f"the log of topic {topic!r} expired with records after {position}"
            )

    async def _repair(self, keys: _ChainKeys) -> None:
        """Settle stages a Worker left behind, as History decides them.

        A Worker that stops after a Workflow Task's commit but before the
        promotion leaves the stage pending, and a run that finished is never
        replayed to promote it. Best effort: a stage History has not decided
        stays for the next reader.
        """
        streams = self._streams
        try:
            pending = await _awaited(streams._redis.hgetall(keys.pending()))
            stages: list[tuple[str, str, int, list[str]]] = []
            for raw_token, raw_description in pending.items():
                run_id, floor_text, *topics = _text(raw_description).split("\x1f")
                stages.append((_text(raw_token), run_id, int(floor_text), topics))
            started = {
                run_id: await self._run_started(run_id) for _, run_id, _, _ in stages
            }
            # Promotions append, so they go in commit order: by run, then by
            # floor within the run. A stage History has not decided holds back
            # the ones after it. A run History no longer holds is oldest.
            stages.sort(
                key=lambda s: (
                    started[s[1]] if started[s[1]] is not None else float("-inf"),
                    s[1],
                    s[2],
                )
            )
            for token, run_id, floor, topics in stages:
                if started[run_id] is None:
                    # Namespace retention removed the run, so nothing can
                    # prove the commit any more.
                    logger.warning(
                        "Aborting stream stage %s of Workflow %s: its run %s is no "
                        "longer in History",
                        token,
                        self._ref.workflow_id,
                        run_id,
                    )
                    decision = _Decision.ABORT
                else:
                    events = await events_after(
                        self._client,
                        self._client.namespace,
                        self._ref.workflow_id,
                        run_id,
                        floor,
                    )
                    decision = decide_token(events, token, floor)
                stage = StageRef(
                    self._client.namespace,
                    self._ref.workflow_id,
                    keys.first_run_id,
                    token,
                    tuple(topics),
                )
                if decision is _Decision.PROMOTE:
                    await streams._promote(stage)
                elif decision is _Decision.ABORT:
                    await streams._abort(stage)
                else:
                    return
        except Exception:
            logger.warning(
                "Could not settle the pending stages of Workflow %s",
                self._ref.workflow_id,
                exc_info=True,
            )

    async def _run_started(self, run_id: str) -> float | None:
        """When ``run_id`` started, or ``None`` when History no longer has it."""
        try:
            description = await self._client.get_workflow_handle(
                self._ref.workflow_id, run_id=run_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return None
            raise
        return description.start_time.timestamp()

    async def _owner_ended(self, keys: _ChainKeys) -> bool:
        description = await self._client.get_workflow_handle(
            self._ref.workflow_id, run_id=self._ref.run_id
        ).describe()
        status = description.status
        if self._ref.run_id is not None:
            return status is not None and status != WorkflowExecutionStatus.RUNNING
        # A Workflow id reused by a new chain means this chain has ended.
        latest = description.raw_description.workflow_execution_info
        if status not in CHAIN_ENDED and latest.first_run_id == keys.first_run_id:
            return False
        await self._streams._mark_closed(keys)
        return True

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamHandle.latest`."""
        name, _ = self._resolve(topic, None)
        keys = await self._keys()
        async with _mapped(write=False):
            newest = await self._streams._redis.xrevrange(keys.log(name), count=1)
        return self._cursor(name, _text(newest[0][0])) if newest else BEGINNING

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str,
        attempt: int,
        next_sequence: int = 1,
    ) -> StreamProducer[Any]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.producer`."""
        if not producer_id:
            raise ValueError("producer_id must not be empty")
        for label, number in (("attempt", attempt), ("next_sequence", next_sequence)):
            if isinstance(number, bool) or not isinstance(number, int) or number < 1:
                raise ValueError(
                    f"{label} must be an int of at least 1, got {number!r}"
                )
        name, _ = self._resolve(topic, None)
        return self._streams._notified_producer(
            self._client,
            self._ref,
            name,
            RedisProducer(self, name, producer_id, attempt, next_sequence),
        )

    def _resolve(
        self, topic: str | StreamTopic[Any] | None, result_type: type | None
    ) -> tuple[str, type | None]:
        return resolve_topic(self._ref.topic if topic is None else topic, result_type)

    def _hash(self, topic: str) -> str:
        return stream_hash(
            self._client.namespace, self._ref.kind, self._ref.workflow_id, topic
        )

    def _cursor(self, topic: str, entry_id: str) -> Cursor:
        return mint_cursor(_PROVIDER, self._hash(topic), entry_id)

    async def _keys(self) -> _ChainKeys:
        await self._streams._ready()
        if self._chain is None:
            first_run_id = await self._first_run_id()
            self._chain = self._streams._chain_keys(
                self._client.namespace, self._ref.workflow_id, first_run_id
            )
        return self._chain

    async def _refuse_if_ended(self, keys: _ChainKeys) -> None:
        """Mark the chain closed and raise if its latest run has ended.

        A Workflow id reused by a new chain also means this chain has ended.

        Raises:
            StreamClosedError: The chain has ended.
        """
        description = await self._client.get_workflow_handle(
            self._ref.workflow_id
        ).describe()
        latest = description.raw_description.workflow_execution_info
        if (
            description.status in CHAIN_ENDED
            or latest.first_run_id != keys.first_run_id
        ):
            await self._streams._mark_closed(keys)
            raise StreamClosedError(
                f"the Workflow {self._ref.workflow_id!r} that owns this stream has "
                "closed"
            )

    async def _first_run_id(self) -> str:
        try:
            description = await self._client.get_workflow_handle(
                self._ref.workflow_id, run_id=self._ref.run_id
            ).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"Workflow {self._ref.workflow_id!r} was not found, and its "
                    "stream is keyed by its run chain"
                ) from error
            raise
        return description.raw_description.workflow_execution_info.first_run_id


class RedisStreams(StreamProviderPlugin):
    """The Redis stream provider.

    One instance per Redis deployment. Pass it to the client as a plugin,
    and open handles from the client with
    :func:`temporalio.contrib.streams.get_stream_handle`.
    """

    def __init__(
        self,
        redis_client: str | redis.asyncio.Redis | redis.asyncio.RedisCluster,
        *,
        key_prefix: str = "temporal-streams",
        retention: timedelta = timedelta(days=7),
        poll_interval: timedelta = timedelta(milliseconds=500),
    ) -> None:
        """Create the provider.

        Args:
            redis_client: A Redis URL, or a ``redis.asyncio.Redis`` or
                ``redis.asyncio.RedisCluster`` the application owns. A client
                made from a URL is closed by :meth:`close`. Every key of one
                Workflow's streams shares a hash tag, so a cluster serves
                them from one slot.
            key_prefix: Prepended to every key, so streams can share a Redis
                with other data and an ACL can scope them.
            retention: How long a stream keeps a record, and how long after
                its last write the stream itself lives.
            poll_interval: How long a read blocks on Redis before it asks the
                server whether the owner closed.

        Raises:
            ValueError: ``key_prefix`` is empty or ``retention`` is shorter
                than a millisecond.
        """
        super().__init__("temporalio.contrib.streams.RedisStreams")
        if not key_prefix:
            raise ValueError("key_prefix must not be empty")
        if retention < timedelta(milliseconds=1):
            raise ValueError(
                f"retention must be at least a millisecond, got {retention}"
            )
        self._retention_ms = int(retention / timedelta(milliseconds=1))
        self._grace_ms = int(_TOMBSTONE_GRACE / timedelta(milliseconds=1))
        self._poll_ms = max(1, int(poll_interval / timedelta(milliseconds=1)))
        self._checked_server = False
        self._read_count = _READ_BATCH
        self._owner_check_min = _OWNER_CHECK_MIN.total_seconds()
        self._owner_check_max = _OWNER_CHECK_MAX.total_seconds()
        if isinstance(redis_client, str):
            self._redis: redis.asyncio.Redis | redis.asyncio.RedisCluster = (
                redis.asyncio.Redis.from_url(redis_client)
            )
            self._owns_redis = True
        else:
            self._redis = redis_client
            self._owns_redis = False
        self._prefix = key_prefix
        # redis-py types register_script for Redis only; RedisCluster has it.
        scripts: Any = self._redis
        self._append = scripts.register_script(_APPEND_LUA)
        self._promote_script = scripts.register_script(_PROMOTE_LUA)
        self._stage_script = scripts.register_script(_STAGE_LUA)

    def get_stream_handle(self, client: Client, ref: StreamRef) -> RedisStreamHandle:
        """A handle on the stream ``ref`` names."""
        return RedisStreamHandle(self, client, ref)

    async def close(self) -> None:
        """Close the Redis connection, if this provider made it."""
        if self._owns_redis:
            await self._redis.aclose()

    async def delete_workflow_streams(self, namespace: str, workflow_id: str) -> int:
        """Delete every stream key of ``workflow_id`` in ``namespace``.

        An admin helper for cleanup and compliance requests. Deleting a
        Workflow does not delete its streams; their retention bounds what is
        left, and this removes it at once. It covers every run chain of the
        Workflow id: logs, metas, stages and chain flags. It asks Temporal
        nothing, so it can run after the Workflow is gone. It walks the
        keyspace with ``SCAN``, so run it rarely, not on a hot path. On a
        Redis Cluster it scans every primary, since the run chains of one
        Workflow id hash to different slots.

        Returns:
            How many keys were deleted.

        Raises:
            StreamStorageError: Redis failed; some keys may be deleted.
        """
        pattern = f"{_part(self._prefix)}:{{{_part(namespace)}:{_part(workflow_id)}:*"
        deleted = 0
        batch: list[Any] = []
        async with _mapped(write=True):
            async for key in self._scan(pattern):
                batch.append(key)
                if len(batch) >= 500:
                    deleted += await _awaited(self._redis.unlink(*batch))
                    batch = []
            if batch:
                deleted += await _awaited(self._redis.unlink(*batch))
        return deleted

    def _scan(self, pattern: str) -> AsyncIterator[Any]:
        client: Any = self._redis
        if isinstance(client, redis.asyncio.RedisCluster):
            return client.scan_iter(
                match=pattern,
                count=1000,
                target_nodes=redis.asyncio.RedisCluster.PRIMARIES,
            )
        return client.scan_iter(match=pattern, count=1000)

    def _retention_args(self) -> list[int]:
        return [self._retention_ms, self._grace_ms]

    def _chain_keys(
        self, namespace: str, workflow_id: str, first_run_id: str
    ) -> _ChainKeys:
        return _ChainKeys(self._prefix, namespace, workflow_id, first_run_id)

    async def _ready(self) -> None:
        """Refuse a server older than Redis 7.0, once.

        Raises:
            StreamUnsupportedError: The server is older than Redis 7.0.
        """
        if self._checked_server:
            return
        async with _mapped(write=False):
            infos = await self._on_each_primary("info", "server")
        for info in infos:
            version = str(info.get("redis_version", "0"))
            major = int(version.split(".", 1)[0] or 0)
            if major < 7:
                raise StreamUnsupportedError(
                    f"RedisStreams needs Redis 7.0 or later, but the server reports "
                    f"redis_version {version}"
                )
        self._checked_server = True
        await self._warn_on_eviction()

    async def _on_each_primary(self, command: str, *args: Any) -> list[Any]:
        """Run a server command on the server, or on every primary of a cluster."""
        client: Any = self._redis
        if not isinstance(client, redis.asyncio.RedisCluster):
            return [await _awaited(getattr(client, command)(*args))]
        # A cluster sends such commands to one node; each primary holds its
        # own share of the keys and its own settings.
        await client.initialize()
        return [
            await getattr(client, command)(*args, target_nodes=node)
            for node in client.get_primaries()
        ]

    async def _warn_on_eviction(self) -> None:
        async with _mapped(write=False):
            try:
                policies = await self._on_each_primary("config_get", "maxmemory-policy")
            except redis.exceptions.ResponseError as error:
                # Managed services often refuse CONFIG; the policy is theirs
                # to document then.
                logger.debug("Could not read maxmemory-policy: %s", error)
                return
        for policy in policies:
            value = _text(
                policy.get("maxmemory-policy", "")
                or policy.get(b"maxmemory-policy", "")
            )
            if value and value != "noeviction":
                logger.warning(
                    "Redis maxmemory-policy is %r; under memory pressure Redis may "
                    "evict whole stream keys, losing records and their dedupe "
                    "state. Use 'noeviction' for streams.",
                    value,
                )
                return

    async def _stage(self, batch: StagedBatch) -> str:
        await self._ready()
        token = uuid.uuid4().hex
        keys = self._chain_keys(batch.namespace, batch.workflow_id, batch.first_run_id)
        items: list[Any] = []
        for record in batch.records:
            items += [record.topic, record.SerializeToString()]
        description = "\x1f".join(
            [batch.run_id, str(batch.history_floor_event_id), *_topics(batch.records)]
        )
        async with _mapped(write=True):
            await self._stage_script(
                keys=[keys.stage(token), keys.pending()],
                args=[self._retention_ms, token, description, *items],
            )
        return token

    async def _promote(self, stage: StageRef) -> None:
        keys = self._chain_keys(stage.namespace, stage.workflow_id, stage.first_run_id)
        topic_keys = [key for t in stage.topics for key in (keys.log(t), keys.meta(t))]
        async with _mapped(write=True):
            added = await self._promote_script(
                keys=[keys.stage(stage.token), keys.pending(), *topic_keys],
                args=[*self._retention_args(), stage.token, *stage.topics],
            )
        if added == -1:
            logger.warning(
                "Stream output that Workflow %s committed in stage %s was dropped "
                "by retention before it was promoted (first run %s)",
                stage.workflow_id,
                stage.token,
                stage.first_run_id,
            )
        elif added:
            async with _mapped(write=False):
                async with self._redis.pipeline(transaction=False) as pipe:
                    for topic in stage.topics:
                        pipe.hget(keys.meta(topic), "closed")
                    flags = await pipe.execute()
            for topic, flag in zip(stage.topics, flags):
                if flag is not None:
                    self._warn_closed_topic(stage.workflow_id, topic)

    async def _close_chain(
        self, namespace: str, workflow_id: str, first_run_id: str
    ) -> None:
        await self._mark_closed(self._chain_keys(namespace, workflow_id, first_run_id))

    async def _close_topic(
        self, client: Client, ref: StreamRef, topic: str, first_run_id: str
    ) -> None:
        keys = self._chain_keys(client.namespace, ref.workflow_id, first_run_id)
        async with _mapped(write=True):
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.hset(keys.meta(topic), "closed", "1")
                # A topic closed before its first record has a meta with no
                # expiry yet.
                pipe.pexpire(keys.meta(topic), self._retention_ms + self._grace_ms)
                await pipe.execute()

    async def _mark_closed(self, keys: _ChainKeys) -> None:
        async with _mapped(write=True):
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.hset(keys.chain(), "closed", "1")
                pipe.pexpire(keys.chain(), self._retention_ms + self._grace_ms)
                await pipe.execute()

    async def _abort(self, stage: StageRef) -> None:
        keys = self._chain_keys(stage.namespace, stage.workflow_id, stage.first_run_id)
        async with _mapped(write=True):
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.delete(keys.stage(stage.token))
                pipe.hdel(keys.pending(), stage.token)
                await pipe.execute()
