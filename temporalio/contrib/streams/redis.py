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
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Any, Generic, TypeVar
from urllib.parse import quote

import redis.asyncio
import redis.exceptions

import temporalio.converter
from temporalio.client import Client
from temporalio.contrib.streams._body import (
    content_fingerprint,
    encode_body,
)
from temporalio.contrib.streams._cursor import BEGINNING, mint_cursor, stream_hash
from temporalio.contrib.streams._errors import (
    StreamError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._output import StagedBatch, StageRef
from temporalio.contrib.streams._plugin import StreamProviderPlugin
from temporalio.contrib.streams._record import Cursor, RecordKind
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import WireRecord, to_wire
from temporalio.service import RPCError, RPCStatusCode

__all__ = ["RedisProducer", "RedisStreamHandle", "RedisStreams"]

_PROVIDER = "redis"
_RECORD_FIELD = "r"

T = TypeVar("T")

# KEYS: log, meta. ARGV: session field, first sequence, digest, records...
# The high-water field reads "<sequence>|<first id>|<last id>|<digest>".
_APPEND_LUA = """
local held = redis.call('HGET', KEYS[2], ARGV[1])
local sequence = tonumber(ARGV[2])
if held then
  local held_sequence, first, last, digest =
    string.match(held, '^(%d+)|([^|]+)|([^|]+)|(%x+)$')
  held_sequence = tonumber(held_sequence)
  if sequence == held_sequence then
    if digest == ARGV[3] then
      return {first, last}
    end
    return redis.error_reply('STREAMS_DIVERGENT sequence ' .. ARGV[2] ..
      ' was already written with different content')
  end
  if sequence < held_sequence then
    return redis.error_reply('STREAMS_STALE sequence ' .. ARGV[2] ..
      ' is below the newest one written, ' .. held_sequence)
  end
end
local first, last
for i = 4, #ARGV do
  last = redis.call('XADD', KEYS[1], '*', 'r', ARGV[i])
  if not first then
    first = last
  end
end
redis.call('HSET', KEYS[2], ARGV[1],
  ARGV[2] .. '|' .. first .. '|' .. last .. '|' .. ARGV[3])
return {first, last}
"""

# KEYS: stage, then one log per topic. ARGV: the topics, in KEYS order.
_PROMOTE_LUA = """
local items = redis.call('LRANGE', KEYS[1], 0, -1)
if #items == 0 then
  return 0
end
local logs = {}
for i = 1, #ARGV do
  logs[ARGV[i]] = KEYS[i + 1]
end
for i = 1, #items, 2 do
  redis.call('XADD', logs[items[i]], '*', 'r', items[i + 1])
end
redis.call('DEL', KEYS[1])
return #items / 2
"""


def _part(text: str) -> str:
    return quote(text, safe="")


class _ChainKeys:
    """The keys of one Workflow run chain's streams."""

    def __init__(
        self, prefix: str, namespace: str, workflow_id: str, first_run_id: str
    ) -> None:
        self.base = (
            f"{_part(prefix)}:"
            f"{{{_part(namespace)}:{_part(workflow_id)}:{_part(first_run_id)}}}"
        )

    def log(self, topic: str) -> str:
        return f"{self.base}:t:{_part(topic)}"

    def meta(self, topic: str) -> str:
        return f"{self.log(topic)}:meta"

    def stage(self, token: str) -> str:
        return f"{self.base}:stage:{_part(token)}"


async def _awaited(value: Awaitable[T] | T) -> T:
    # redis-py types its commands for both its sync and asyncio clients.
    return await value if inspect.isawaitable(value) else value  # type: ignore[return-value]


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
    ) -> None:
        """Prefer :meth:`RedisStreamHandle.producer`."""
        self._handle = handle
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = 1
        self._last = BEGINNING
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
        try:
            _, last = await streams._append(
                keys=[keys.log(self._topic), keys.meta(self._topic)],
                args=[
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
    ) -> Any:
        """Reading is not supported by this provider yet.

        Raises:
            StreamUnsupportedError: Always.
        """
        del topic, after, result_type
        raise StreamUnsupportedError("RedisStreams cannot read streams yet")

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
    ) -> RedisProducer[Any]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.producer`."""
        if not producer_id:
            raise ValueError("producer_id must not be empty")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError(f"attempt must be an int of at least 1, got {attempt!r}")
        name, _ = self._resolve(topic, None)
        return RedisProducer(self, name, producer_id, attempt)

    def _resolve(
        self, topic: str | StreamTopic[Any] | None, result_type: type | None
    ) -> tuple[str, type | None]:
        return resolve_topic(self._ref.topic if topic is None else topic, result_type)

    def _cursor(self, topic: str, entry_id: str) -> Cursor:
        stream = stream_hash(
            self._client.namespace, self._ref.kind, self._ref.workflow_id, topic
        )
        return mint_cursor(_PROVIDER, stream, entry_id)

    async def _keys(self) -> _ChainKeys:
        if self._chain is None:
            first_run_id = await self._first_run_id()
            self._chain = self._streams._chain_keys(
                self._client.namespace, self._ref.workflow_id, first_run_id
            )
        return self._chain

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

        Raises:
            ValueError: ``key_prefix`` is empty.
        """
        super().__init__("temporalio.contrib.streams.RedisStreams")
        if not key_prefix:
            raise ValueError("key_prefix must not be empty")
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

    def get_stream_handle(self, client: Client, ref: StreamRef) -> RedisStreamHandle:
        """A handle on the stream ``ref`` names."""
        return RedisStreamHandle(self, client, ref)

    async def close(self) -> None:
        """Close the Redis connection, if this provider made it."""
        if self._owns_redis:
            await self._redis.aclose()

    def _chain_keys(
        self, namespace: str, workflow_id: str, first_run_id: str
    ) -> _ChainKeys:
        return _ChainKeys(self._prefix, namespace, workflow_id, first_run_id)

    async def _stage(self, batch: StagedBatch) -> str:
        token = uuid.uuid4().hex
        keys = self._chain_keys(batch.namespace, batch.workflow_id, batch.first_run_id)
        items: list[Any] = []
        for record in batch.records:
            items += [record.topic, record.SerializeToString()]
        async with _mapped(write=True):
            await _awaited(self._redis.rpush(keys.stage(token), *items))
        return token

    async def _promote(self, stage: StageRef) -> None:
        keys = self._chain_keys(stage.namespace, stage.workflow_id, stage.first_run_id)
        async with _mapped(write=True):
            await self._promote_script(
                keys=[keys.stage(stage.token), *(keys.log(t) for t in stage.topics)],
                args=list(stage.topics),
            )

    async def _abort(self, stage: StageRef) -> None:
        keys = self._chain_keys(stage.namespace, stage.workflow_id, stage.first_run_id)
        async with _mapped(write=True):
            await _awaited(self._redis.delete(keys.stage(stage.token)))
