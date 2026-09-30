"""The provider over the shipped Workflow Streams transport (Option 0).

Speaks the shipped contrib feature's wire format, the
``__temporal_workflow_stream_*`` Signal, Update and Query, so interface code
and existing Workflow Streams code share one log, and old histories replay.
Records live in the owning workflow's History, which is also this provider's
limit: the shipped caps (payloads in History, the Signal cap, bounded
subscribers) are transport properties and remain.

The mapping, in one place:

- A topic is the shipped topic of the same name in the workflow's one log.
  A record rides as the item's ``Payload``: its data is the serialized
  ``StreamRecord`` proto and its encoding is ``binary/plain``. The shipped
  code stores and returns that ``Payload`` untouched, so the body's own
  encoding never meets the transport.
- Producer identity dedupes through the shipped publisher state: the
  publisher id is ``producer#attempt`` and every publish Signal carries the
  sequence its records end at, so a retried batch drops and a new attempt
  passes.

  What this transport cannot keep is the rest of that rule. A publish is a
  Signal, which has no response, so the dedupe decision is taken in the
  workflow and there is nowhere to report it. A repeat that carries
  *different* content at a sequence the log already holds is therefore
  dropped rather than refused with
  :class:`temporalio.streams.StreamProducerError`, the way the memory and
  native providers refuse it. Raising in the Signal handler is not an
  alternative: it would fail the Workflow Task on every replay and the
  caller would still learn nothing. A caller that needs a divergent retry to
  be caught wants a provider whose append is a request and a response.
- ``append()`` returns ``None``. The Signal transport learns positions at
  read time, so a caller that wants to follow from now asks ``latest()``.
- A log belongs to one run and is not carried across continue-as-new, so a
  cursor names the run as well as the offset. A handle without a run id
  reads run after run: each log through the poll Update while its run is
  open and through the tail Query once it has closed, then the successor's
  from its first record. A run that was reset is followed too, into the run
  describe names, at the position the read had reached: the reset run
  rebuilt the base run's log up to the reset point by replay, so the items
  before it sit at the same offsets. The reset itself is not reported to
  the reader as a record; that is a wire change for a later round.
- The workflow-side stream object belongs to the workflow instance, found
  through the handler the shipped class registers on it. An evicted and
  rebuilt workflow gets its own, so a task that failed leaks nothing into
  the next attempt's log and a replayed run does not see records twice.
- An activity a workflow scheduled keeps its own streams inside that
  workflow's log, under the reserved topic ``activity/<activity id>/<name>``
  with ``%`` and ``/`` in the id percent-encoded, the way the native
  provider reserves ``activity/`` in the owner's map. The record itself
  carries the plain name. A standalone activity has no workflow to host a
  log, so its streams are refused, and a workflow's own topic may not start
  with the reserved prefix.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowHandle,
    WorkflowHistoryEventFilterType,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
    WorkflowUpdateStage,
)
from temporalio.contrib.workflow_streams import (
    POLL_UPDATE_NAME,
    PUBLISH_SIGNAL_NAME,
    STREAM_DRAINING_ERROR_TYPE,
    TRUNCATED_OFFSET_ERROR_TYPE,
    PollInput,
    PollResult,
    PublishEntry,
    PublishInput,
    WorkflowStream,
)
from temporalio.converter import PayloadConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._errors import (
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
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
from temporalio.worker._workflow_instance import QUERY_HANDLER_NOT_FOUND

__all__ = [
    "WorkflowStreamsActivityHandle",
    "WorkflowStreamsHandle",
    "WorkflowStreamsProducer",
    "WorkflowStreamsProvider",
]

T = TypeVar("T")

_PROVIDER = "workflow_streams"
_TAIL_QUERY = "__temporal_streams_tail"
_LATEST_QUERY = "__temporal_streams_latest"
_START_QUERY = "__temporal_streams_start"
_ENCODING = b"binary/plain"
# The topics an activity owns live in its workflow's log under this prefix,
# so no workflow topic may start with it.
_ACTIVITY_PREFIX = "activity/"
# The same cap the shipped poll path answers under, because both are one
# response through the same server.
_MAX_TAIL_RESPONSE_BYTES = 1_000_000
# The server's failure type for an accepted Update whose run closed before
# answering it: the poll's way of saying the run is over.
_UPDATE_OUTLIVED_RUN = "AcceptedUpdateCompletedWorkflow"
# The SDK's rejection of an Update no handler is registered for. It shares
# its wording with the Query one.
_HANDLER_NOT_FOUND = QUERY_HANDLER_NOT_FOUND

logger = logging.getLogger(__name__)


def _require_topic(topic: str) -> None:
    if not topic:
        raise ValueError("topic must not be empty")
    if topic.startswith(_ACTIVITY_PREFIX):
        raise ValueError(
            f"topic {topic!r} is reserved: names under {_ACTIVITY_PREFIX!r} hold the "
            "streams of the workflow's activities on the workflow_streams provider"
        )


def _activity_topic(activity_id: str, topic: str) -> str:
    """The reserved name ``topic`` of ``activity_id``'s streams takes in the log.

    The id is percent-encoded so an id holding ``/`` cannot be read as two
    components; the name comes last, so it may hold anything.
    """
    escaped = activity_id.replace("%", "%25").replace("/", "%2F")
    return f"{_ACTIVITY_PREFIX}{escaped}/{topic}"


def _cursor(run_id: str, offset: int) -> Cursor:
    return mint_cursor(_PROVIDER, f"{run_id}:{offset}")


def _position(after: Cursor) -> tuple[str, int] | None:
    """The ``(run id, offset)`` a cursor of this provider names, or ``None`` for BEGINNING."""
    token = cursor_position(after, provider=_PROVIDER)
    if token is None:
        return None
    run_id, _, offset = token.rpartition(":")
    try:
        if not run_id:
            raise ValueError
        return run_id, int(offset)
    except ValueError:
        raise StreamCursorError(
            f"cursor {after.token!r} does not name a run and an offset on the "
            "workflow_streams provider"
        ) from None


def _wrap(record: WireRecord) -> Payload:
    return Payload(metadata={"encoding": _ENCODING}, data=record.SerializeToString())


def _unwrap(cursor: Cursor, payload: Payload, warn: Any) -> WireRecord | None:
    try:
        return WireRecord.FromString(payload.data)
    except DecodeError as error:
        # An item another publisher put on this topic, or a corrupt one:
        # skip and say so, so one bad record cannot pin a reader.
        warn("skipping stream record at %s: %s", cursor, error)
        return None


def _entry_data(payload: Payload) -> str:
    # The documented wire form of PublishEntry.data.
    return base64.b64encode(payload.SerializeToString()).decode("ascii")


class _InstanceStream:
    """A view over the shipped stream object of the running workflow instance.

    A separate class because ``WorkflowStream`` insists on being constructed
    from a method named ``__init__``.
    """

    def __init__(self, stream: WorkflowStream | None = None) -> None:
        self.stream = WorkflowStream() if stream is None else stream
        if workflow.get_query_handler(_TAIL_QUERY) is None:
            # The poll Update stops answering once the workflow is closing,
            # and a reader between polls at that moment would lose what the
            # final task published. The log is workflow state, so a Query
            # still serves it after completion.
            workflow.set_query_handler(_TAIL_QUERY, self._tail)
        if workflow.get_query_handler(_LATEST_QUERY) is None:
            workflow.set_query_handler(_LATEST_QUERY, self._latest)
        if workflow.get_query_handler(_START_QUERY) is None:
            workflow.set_query_handler(_START_QUERY, self._start)

    def _tail(self, from_offset: int, topic: str) -> dict[str, Any]:
        """One page of ``topic``'s items at or past ``from_offset``.

        Filtered and capped here rather than at the caller, because a Query
        response has to fit the server's blob limit and a log the reader only
        wants one topic of can be much larger than that.
        """
        items: list[dict[str, Any]] = []
        size = 0
        next_offset = self.stream.next_offset
        more_ready = False
        held = self.stream.items_from(from_offset)
        for offset, item_topic, payload in held:
            if item_topic != topic:
                continue
            data = base64.b64encode(payload.SerializeToString()).decode("ascii")
            if items and size + len(data) > _MAX_TAIL_RESPONSE_BYTES:
                next_offset, more_ready = offset, True
                break
            size += len(data)
            items.append({"offset": offset, "topic": item_topic, "data": data})
        return {
            "items": items,
            "next_offset": next_offset,
            "more_ready": more_ready,
            # Where the retained log starts at or past ``from_offset``: a
            # reader whose position is below it has fallen behind truncation.
            "base_offset": held[0][0] if held else next_offset,
        }

    def _latest(self, topic: str) -> int:
        """The newest offset holding a record on ``topic``, or -1 when it has none.

        The log orders every topic together, so the head of the log is not an
        answer about one topic. Scanning here costs one Query rather than
        shipping the log to the caller to find the same thing.
        """
        for offset, item_topic, _ in reversed(self.stream.items_from(0)):
            if item_topic == topic:
                return offset
        return -1

    def _start(self, topic: str, last_n: int) -> int:
        """Where a read starts: the log's head, or ``topic``'s newest ``last_n`` records.

        Zero ``last_n`` asks for the head, which is where ``END`` starts.
        """
        return start_offset(self.stream, topic, last_n)


def start_offset(stream: WorkflowStream, topic: str, last_n: int) -> int:
    """The log offset a read of ``topic`` starts at.

    ``last_n`` of zero is the head of the log, so only what is published next
    is read. Otherwise it is the offset of ``topic``'s ``last_n``-th newest
    item, or the oldest one the log holds when there are fewer. The log is
    workflow state, so the workflow half answers the same on every replay.
    """
    if last_n <= 0:
        return stream.next_offset
    items = stream.items_from(0)
    seen = 0
    for offset, item_topic, _ in reversed(items):
        if item_topic == topic:
            seen += 1
            if seen == last_n:
                return offset
    return items[0][0] if items else stream.next_offset


def _registered_stream() -> WorkflowStream | None:
    handler = workflow.get_signal_handler(PUBLISH_SIGNAL_NAME)
    if handler is None:
        return None
    stream = getattr(handler, "__self__", None)
    if not isinstance(stream, WorkflowStream):
        raise RuntimeError(
            f"the {PUBLISH_SIGNAL_NAME!r} signal on this workflow is handled by "
            "something other than a WorkflowStream, so the workflow_streams "
            "provider cannot share its log"
        )
    return stream


def _instance() -> _InstanceStream:
    # Found on the instance rather than in a process-level map keyed by run
    # id: the SDK rebuilds an evicted workflow from history as a new object,
    # and a map would hand that object the stale log with its unregistered
    # handlers and the records of a task that failed.
    stream = _registered_stream()
    return _InstanceStream() if stream is None else _InstanceStream(stream)


class _WSReadSource:
    """Reads the signal-fed log the shipped feature keeps in workflow state."""

    def __init__(
        self, stream: WorkflowStream, topic: str, start: int, run_id: str
    ) -> None:
        self._stream = stream
        self._topic = topic
        self._offset = start
        self._run_id = run_id
        self._closed = False

    async def next_batch(self) -> list[tuple[Cursor, WireRecord]]:
        while True:
            if self._closed:
                raise StopAsyncIteration
            await workflow.wait_condition(
                lambda: self._closed or self._stream.next_offset > self._offset
            )
            if self._closed:
                raise StopAsyncIteration
            batch: list[tuple[Cursor, WireRecord]] = []
            for offset, topic, payload in self._stream.items_from(self._offset):
                if topic != self._topic:
                    continue
                cursor = _cursor(self._run_id, offset)
                wire = _unwrap(cursor, payload, workflow.logger.warning)
                if wire is not None:
                    batch.append((cursor, wire))
            self._offset = self._stream.next_offset
            if batch:
                return batch

    def close(self) -> None:
        self._closed = True


class _WSWriteSink:
    def __init__(self, stream: WorkflowStream, topic: str) -> None:
        self._handle = stream.topic(topic)

    def publish(self, record: WireRecord) -> None:
        # Appending to workflow state commits with the task, and a poll
        # Update's result rides the same task completion, so a failed task
        # leaks nothing: rule 1 through the shipped mechanics.
        self._handle.publish(_wrap(record))


class _WSWorkflowProvider:
    """The workflow half: the shipped stream object of one workflow instance.

    Made per instance by the worker, so the stream object it captures on
    first use is this instance's, and the finish hook lets go of that one
    rather than of whatever the thread's handler lookup answers at the time.
    """

    def __init__(self) -> None:
        self._stream: WorkflowStream | None = None

    def _own_stream(self) -> WorkflowStream:
        if self._stream is None:
            self._stream = _instance().stream
        return self._stream

    def open_reader(
        self, topic: str, *, after: Cursor, last: int | None = None
    ) -> ReadSource:
        check_read_start(after, last)
        _require_topic(topic)
        run_id = workflow.info().run_id
        start = 0
        # The log is workflow state, so END and last= resolve against it here
        # and land on the same offset on every replay.
        if last is not None:
            return _WSReadSource(
                self._own_stream(),
                topic,
                start_offset(self._own_stream(), topic, last),
                run_id,
            )
        if after == END:
            stream = self._own_stream()
            return _WSReadSource(stream, topic, stream.next_offset, run_id)
        named = _position(after)
        if named is not None:
            if named[0] != run_id:
                raise StreamCursorError(
                    f"cursor {after.token!r} names another run; a run's log is its own"
                )
            start = named[1] + 1
        return _WSReadSource(self._own_stream(), topic, start, run_id)

    def open_writer(self, topic: str) -> WriteSink:
        _require_topic(topic)
        return _WSWriteSink(self._own_stream(), topic)

    def on_workflow_start(self) -> None:
        # Registered on the first task, because an outside reader can poll
        # before workflow code has opened anything, and an Update with no
        # handler yet is rejected rather than held. A poll that arrives in
        # that first task still runs ahead of this hook; the reader retries it.
        self._own_stream()

    async def on_workflow_finish(self) -> None:
        # An Option 0 stream dies with its run, and a parked long-poll Update
        # would otherwise hold completion open. Same recipe the shipped
        # feature documents before a return or a continue-as-new.
        if self._stream is None:
            return
        self._stream.detach_pollers()
        await workflow.wait_condition(workflow.all_handlers_finished)


class WorkflowStreamsProducer(Generic[T]):
    """Appends by sending the shipped publish Signal directly.

    Direct rather than through ``WorkflowStreamClient`` because the interface
    owns the publisher identity: it must be ``producer#attempt`` for the
    shipped dedupe to drop a retry and pass a new generation, and the client
    would use its own random id.

    Sequences are committed only after the server accepted the Signal. A
    batch whose Signal raised stays pending and goes out again under the
    same signal sequence, either when the caller retries the same values or
    ahead of whatever the caller sends next, so an ambiguous failure writes
    the batch once and loses nothing.
    """

    def __init__(
        self,
        handle: Any,
        converter: PayloadConverter,
        topic: str,
        producer_id: str,
        attempt: int,
        *,
        item_topic: str | None = None,
    ) -> None:
        """Bind this producer to ``topic`` on the workflow behind ``handle``.

        ``item_topic`` is the name the log files the records under when it
        differs from the name the records carry, as an activity's reserved
        topics do.
        """
        self._handle = handle
        self._converter = converter
        self._topic = topic
        self._item_topic = topic if item_topic is None else item_topic
        self._producer_id = producer_id
        self._attempt = attempt
        # One-based, because zero on the wire says the producer does not
        # number its records and this one does.
        self._sequence = 1
        self._pending: tuple[list[PublishEntry], int] | None = None

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    @property
    def _publisher_id(self) -> str:
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: T) -> Cursor | None:
        """Append ``values`` through the shipped publish Signal.

        Always ``None``: this transport learns positions at read time, so a
        caller that wants to follow from now asks
        :meth:`WorkflowStreamsHandle.latest`.
        """
        if not values:
            return None
        await self._send([(RecordKind.DATA, value) for value in values])
        return None

    async def finish(self) -> None:
        """Write ``FINISH`` for this producer on this topic."""
        await self._send([(RecordKind.FINISH, None)])

    def _entries(
        self, batch: list[tuple[RecordKind, Any]]
    ) -> tuple[list[PublishEntry], int]:
        sequence = self._sequence
        entries = []
        for kind, value in batch:
            wire = to_wire(
                self._converter,
                topic=self._topic,
                kind=kind,
                value=value,
                producer_id=self._producer_id,
                attempt=self._attempt,
                sequence=sequence,
            )
            sequence += 1
            entries.append(
                PublishEntry(topic=self._item_topic, data=_entry_data(_wrap(wire)))
            )
        return entries, sequence

    async def _send(self, batch: list[tuple[RecordKind, Any]]) -> None:
        entries, next_sequence = self._entries(batch)
        if self._pending is not None and self._pending[0] != entries:
            # The caller moved on from a batch whose Signal raised. It goes
            # first, under the signal sequence it already had, so a copy the
            # server did accept is dropped and one it never saw lands. The
            # new batch is then renumbered behind it.
            await self._signal(*self._pending)
            entries, next_sequence = self._entries(batch)
        await self._signal(entries, next_sequence)

    async def _signal(self, entries: list[PublishEntry], next_sequence: int) -> None:
        # The dedupe sequence is where this producer's records end, not how
        # many signals it has sent. The two differ once a retry batches its
        # records differently from the send it is repeating, and a counter of
        # signals then either drops a batch of new records or lets records
        # that are already there through a second time.
        self._pending = (entries, next_sequence)
        try:
            await self._handle.signal(
                PUBLISH_SIGNAL_NAME,
                PublishInput(
                    items=entries,
                    publisher_id=self._publisher_id,
                    sequence=next_sequence,
                ),
            )
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._handle.id!r} was not found, so its stream "
                    "cannot be appended to"
                ) from error
            raise
        self._sequence = next_sequence
        self._pending = None


class WorkflowStreamsHandle:
    """One workflow's log from outside, through the shipped poll Update and a tail Query."""

    def __init__(
        self,
        client: Client,
        workflow_id: str,
        run_id: str | None,
        poll_cooldown: timedelta,
    ) -> None:
        """Address ``workflow_id``'s log, pinned to ``run_id`` when one is given."""
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._poll_cooldown = poll_cooldown
        self._converter = client.data_converter.payload_converter

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` from where the read starts until the chain, or the pinned run, closes.

        ``BEGINNING`` is the oldest item the first retained run's log still
        holds: the poll Update reads offset zero as the log's base. ``END``
        and ``last=`` start on the current run, or the pinned one, at an
        offset its workflow answers by Query when the read starts.

        The chain is followed across continue-as-new, into the successor's
        log from its first record, and across a reset, into the run describe
        names at the position the read had reached. A handle pinned to a run
        ends with that run either way.
        """
        check_read_start(after, last)
        topic, result_type = resolve_topic(topic, result_type)
        wire_topic = self._wire_topic(topic)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        named = None if after == END else _position(after)
        if named is not None and self._run_id is not None and named[0] != self._run_id:
            raise StreamCursorError(
                f"cursor {after.token!r} names another run than this handle is pinned to"
            )
        return self._read(wire_topic, named, after, last, result_type)

    def _wire_topic(self, topic: str) -> str:
        """The name the log files ``topic`` under: the topic itself for a workflow's."""
        _require_topic(topic)
        return topic

    async def _read(
        self,
        topic: str,
        named: tuple[str, int] | None,
        after: Cursor,
        last: int | None,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        if named is not None:
            run_id, offset = named[0], named[1] + 1
        elif last is not None or after == END:
            run_id, offset = await self._start_on_current_run(topic, last or 0)
            # A synthesized record is positioned before the first one read.
            after = _cursor(run_id, offset - 1)
        else:
            run_id, offset = self._run_id or await self._first_run(), 0
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        while True:
            handle = self._handle(run_id)
            next_offset = offset
            polls = self._poll(handle, topic, offset)
            try:
                async for item_offset, payload in polls:
                    next_offset = item_offset + 1
                    for record in self._records(decoder, run_id, item_offset, payload):
                        yield record
            finally:
                await polls.aclose()
            status = await self._status(handle)
            if status is None:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} run {run_id!r} was not found"
                )
            if status == WorkflowExecutionStatus.RUNNING:
                # The subscription ended early, on an RPC timeout for
                # instance; the run is still open, so pick up where it left.
                offset = next_offset
                continue
            # What landed after the last poll is still in workflow state, so
            # the tail comes back by Query rather than being lost with the run.
            tail_offset = next_offset
            while True:
                page, tail_offset, more = await self._tail(handle, topic, tail_offset)
                for offset_, payload in page:
                    for record in self._records(decoder, run_id, offset_, payload):
                        yield record
                if not more:
                    break
            if self._run_id is not None:
                return
            following = await self._following(handle, status, tail_offset)
            if following is None:
                return
            run_id, offset = following

    async def _following(
        self,
        handle: WorkflowHandle[Any, Any],
        status: WorkflowExecutionStatus,
        position: int,
    ) -> tuple[str, int] | None:
        """The run that carries on after ``handle``'s, and where to read it from.

        A continue-as-new names its successor in the close event, and the
        successor's log starts over at zero. A reset does not: the base run is
        closed with no word of it in its own History, and only describe names
        the run reset from it. That run rebuilt its log by replaying the base
        run's History up to the reset point, so the items before that point
        sit at the same offsets in both logs, and the read carries on at the
        position it reached. Past the reset point the two logs differ, and a
        reader already there is not told: reporting the reset to consumers as
        a record is a wire change for a later round.
        """
        if status == WorkflowExecutionStatus.CONTINUED_AS_NEW:
            successor = await self._successor(handle)
            return None if successor is None else (successor, 0)
        reset_run = await self._reset_run(handle)
        return None if reset_run is None else (reset_run, position)

    async def _reset_run(self, handle: WorkflowHandle[Any, Any]) -> str | None:
        """The run ``handle``'s run was reset into, which only describe reports."""
        description = await self._describe(handle)
        if description is None:
            return None
        extended = description.raw_description.workflow_extended_info
        return extended.reset_run_id or None

    async def _poll(
        self, handle: WorkflowHandle[Any, Any], topic: str, offset: int
    ) -> AsyncGenerator[tuple[int, Payload], None]:
        """Drive the shipped poll Update on one run until that run closes.

        Written here rather than through ``WorkflowStreamClient.subscribe``
        for two reasons. That loop swallows a cancellation of the caller's
        task, so a consumer's ``asyncio.timeout`` or task cancel around a read
        would end the subscription and let the read resubscribe forever; here
        the cancellation leaves ``read()`` as what it was. And it follows
        continue-as-new with offsets this provider does not carry across
        runs, which is the outer loop's job.
        """
        cooldown = self._poll_cooldown.total_seconds()
        unhandled = False
        while True:
            try:
                update = await handle.start_update(
                    POLL_UPDATE_NAME,
                    PollInput(topics=[topic], from_offset=offset),
                    wait_for_stage=WorkflowUpdateStage.ACCEPTED,
                    result_type=PollResult,
                )
                result = await update.result()
            except WorkflowUpdateRPCTimeoutOrCancelledError as error:
                if isinstance(error.__cause__, asyncio.CancelledError):
                    # The SDK wraps a cancelled await in this error; the
                    # consumer cancelled us, so that is what comes out.
                    raise asyncio.CancelledError() from error
                # The RPC itself timed out while the run is still open.
                continue
            except WorkflowUpdateFailedError as error:
                cause = getattr(error.cause, "type", None)
                if cause == TRUNCATED_OFFSET_ERROR_TYPE:
                    # Restarting from the beginning would hand the caller
                    # records it already handled, and only the caller can
                    # decide to do that.
                    raise StreamCursorError(
                        f"offset {offset} of workflow {self._workflow_id!r} run "
                        f"{handle.run_id!r} is no longer retained"
                    ) from error
                if cause == STREAM_DRAINING_ERROR_TYPE:
                    # Pollers are detached because the run is closing; the
                    # next attempt learns how it closed.
                    await asyncio.sleep(cooldown)
                    continue
                if cause == _UPDATE_OUTLIVED_RUN:
                    return
                if _HANDLER_NOT_FOUND in str(error.cause):
                    if await self._status(handle) != WorkflowExecutionStatus.RUNNING:
                        # The run closed with the rejecting task, as a run
                        # that continues as new on its first task does; the
                        # caller describes it and follows the chain.
                        return
                    if unhandled:
                        raise StreamError(
                            f"workflow {self._workflow_id!r} run {handle.run_id!r} "
                            "does not serve the poll update: the workflow_streams "
                            "provider is not installed on its worker, and no "
                            "WorkflowStream was constructed"
                        ) from error
                    # A rejection comes back with the completion of the task
                    # that made it, so a retry reaches a later task, and the
                    # start hook registers the handler on the first one. Only
                    # a second rejection means there is no handler to wait for.
                    unhandled = True
                    await asyncio.sleep(cooldown)
                    continue
                raise StreamError(
                    f"the poll update on workflow {self._workflow_id!r} failed: {error}"
                ) from error
            except RPCError as error:
                # The run closed and its poll Update went with it, or the
                # workflow does not exist; the caller describes to tell which.
                if error.status != RPCStatusCode.NOT_FOUND:
                    raise
                return
            for item in result.items:
                if item.topic == topic:
                    yield item.offset, Payload.FromString(base64.b64decode(item.data))
            offset = result.next_offset
            if not result.more_ready and cooldown > 0:
                await asyncio.sleep(cooldown)

    def _records(
        self, decoder: RecordDecoder, run_id: str, offset: int, payload: Payload
    ) -> list[StreamRecord[Any]]:
        cursor = _cursor(run_id, offset)
        wire = _unwrap(cursor, payload, logger.warning)
        if wire is None:
            return []
        return decoder.decode(cursor, wire)

    def _handle(self, run_id: str | None) -> WorkflowHandle[Any, Any]:
        return self._client.get_workflow_handle(self._workflow_id, run_id=run_id)

    async def _describe(self, handle: WorkflowHandle[Any, Any]) -> Any | None:
        """The description of ``handle``'s run, or ``None`` when it is gone."""
        try:
            return await handle.describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return None
            raise

    async def _status(
        self, handle: WorkflowHandle[Any, Any]
    ) -> WorkflowExecutionStatus | None:
        description = await self._describe(handle)
        return None if description is None else description.status

    async def _first_run(self) -> str:
        """The oldest retained run of the chain, walking back from the latest."""
        try:
            run_id = (await self._handle(None).describe()).run_id
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} was not found"
                ) from error
            raise
        assert run_id is not None
        while True:
            previous = await self._predecessor(run_id)
            if previous is None:
                return run_id
            run_id = previous

    async def _predecessor(self, run_id: str) -> str | None:
        try:
            async for event in self._handle(run_id).fetch_history_events(page_size=1):
                attributes = event.workflow_execution_started_event_attributes
                if attributes.continued_execution_run_id:
                    return attributes.continued_execution_run_id
                # A reset run's start event is the base run's, copied, and the
                # original run id it carries is kept across resets, so it names
                # the run the chain of resets began from.
                original = attributes.original_execution_run_id
                return original if original and original != run_id else None
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
        # The run's History is gone: the chain's retained part starts here.
        return None

    async def _successor(self, handle: WorkflowHandle[Any, Any]) -> str | None:
        events = handle.fetch_history_events(
            event_filter_type=WorkflowHistoryEventFilterType.CLOSE_EVENT
        )
        try:
            async for event in events:
                if event.HasField(
                    "workflow_execution_continued_as_new_event_attributes"
                ):
                    attributes = (
                        event.workflow_execution_continued_as_new_event_attributes
                    )
                    return attributes.new_execution_run_id or None
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
            raise StreamNotFoundError(
                f"workflow {self._workflow_id!r} run {handle.run_id!r} was not found, "
                "so its successor cannot be followed"
            ) from error
        return None

    async def _tail(
        self, handle: WorkflowHandle[Any, Any], topic: str, from_offset: int
    ) -> tuple[list[tuple[int, Payload]], int, bool]:
        """One page of ``topic``'s tail as ``(items, next offset, more to come)``."""
        try:
            wire = await handle.query(
                _TAIL_QUERY, args=[from_offset, topic], result_type=dict
            )
        except WorkflowQueryFailedError as error:
            if QUERY_HANDLER_NOT_FOUND not in str(error):
                raise StreamError(
                    f"the tail query on workflow {self._workflow_id!r} failed: {error}"
                ) from error
            # The workflow never opened a stream through this provider, so
            # there is no tail to serve.
            return [], from_offset, False
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
            # The History is gone; nothing is left to serve.
            return [], from_offset, False
        if from_offset and wire.get("base_offset", from_offset) > from_offset:
            # Restarting from the base would hand the caller records it
            # already handled, and only the caller can decide to do that.
            raise StreamCursorError(
                f"offset {from_offset} of workflow {self._workflow_id!r} run "
                f"{handle.run_id!r} is no longer retained"
            )
        items = [
            (item["offset"], Payload.FromString(base64.b64decode(item["data"])))
            for item in wire["items"]
        ]
        return items, wire["next_offset"], bool(wire["more_ready"])

    async def _start_on_current_run(self, topic: str, last_n: int) -> tuple[str, int]:
        """The run a read at ``END`` or of the newest records starts on, and the offset.

        Raises:
            StreamUnsupportedError: The workflow's worker runs a provider that
                predates these reads and cannot answer where they start.
        """
        handle = self._handle(self._run_id)
        description = await self._describe(handle)
        if description is None:
            raise StreamNotFoundError(f"workflow {self._workflow_id!r} was not found")
        run_id = description.run_id
        assert run_id is not None
        try:
            offset = await self._handle(run_id).query(
                _START_QUERY, args=[topic, last_n], result_type=int
            )
        except WorkflowQueryFailedError as error:
            if QUERY_HANDLER_NOT_FOUND not in str(error):
                raise StreamError(
                    f"the start query on workflow {self._workflow_id!r} failed: {error}"
                ) from error
            raise StreamUnsupportedError(
                f"workflow {self._workflow_id!r} does not answer where a read at END "
                "or of the last records starts; its worker's provider predates them"
            ) from error
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} was not found"
                ) from error
            raise
        return run_id, offset

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The newest position holding a record on ``topic``.

        The log is one per run, so the cursor names the run it was read from:
        the pinned run, or the latest run of the chain. One log orders every
        topic, so the answer comes from a Query that scans it for this topic
        rather than from the head of the log, which usually names some other
        topic's record.
        """
        topic, _ = resolve_topic(topic)
        wire_topic = self._wire_topic(topic)
        handle = self._handle(self._run_id)
        description = await self._describe(handle)
        if description is None:
            raise StreamNotFoundError(f"workflow {self._workflow_id!r} was not found")
        try:
            head = await handle.query(_LATEST_QUERY, wire_topic, result_type=int)
        except WorkflowQueryFailedError as error:
            if QUERY_HANDLER_NOT_FOUND not in str(error):
                raise StreamError(
                    f"the latest query on workflow {self._workflow_id!r} failed: "
                    f"{error}"
                ) from error
            # The workflow never opened a stream through this provider, so
            # the topic holds nothing.
            head = -1
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} was not found"
                ) from error
            raise
        run_id = description.run_id
        assert run_id is not None
        if head >= 0:
            return _cursor(run_id, head)
        # An empty topic on the chain's first run is the beginning of the
        # stream; on a successor it is a position of its own, because
        # BEGINNING would send a chain-following read back to the first run.
        if await self._predecessor(run_id) is None:
            return BEGINNING
        return _cursor(run_id, -1)

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str = "",
        attempt: int = 0,
    ) -> WorkflowStreamsProducer[Any]:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        topic, _ = resolve_topic(topic)
        wire_topic = self._wire_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return WorkflowStreamsProducer(
            self._handle(self._run_id),
            self._converter,
            topic,
            producer_id,
            attempt,
            item_topic=wire_topic,
        )


class WorkflowStreamsActivityHandle(WorkflowStreamsHandle):
    """The streams one activity of a workflow owns, kept in that workflow's log.

    Each topic is the reserved topic ``activity/<activity id>/<name>`` of the
    workflow's log, so an activity's ``tokens`` and its workflow's ``tokens``
    are two streams. The activity belongs to one run, so the handle pins the
    run on first use and never follows a successor. A read ends when the
    workflow closes, or once the activity has been seen pending, is pending
    no longer and the read delivered a record: a stream the activity never
    wrote has nothing to close, so a read on it waits for the workflow.

    A read here polls the tail Query and describes the workflow between
    polls, every ``poll_cooldown``, rather than parking on the poll Update:
    an Update parked in the workflow returns only when the log grows, so it
    could not notice the activity ending, and each abandoned one would stay
    parked against the run's Update caps.
    """

    def __init__(
        self,
        client: Client,
        workflow_id: str,
        run_id: str | None,
        activity_id: str,
        poll_cooldown: timedelta,
    ) -> None:
        """Address ``activity_id``'s streams inside ``workflow_id``'s log."""
        super().__init__(client, workflow_id, run_id, poll_cooldown)
        self._activity_id = activity_id

    def _wire_topic(self, topic: str) -> str:
        return _activity_topic(self._activity_id, topic)

    async def _read(
        self,
        topic: str,
        named: tuple[str, int] | None,
        after: Cursor,
        last: int | None,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        if named is not None:
            run_id, offset = named[0], named[1] + 1
        elif last is not None or after == END:
            run_id, offset = await self._start_on_current_run(topic, last or 0)
            after = _cursor(run_id, offset - 1)
        else:
            run_id, offset = await self._current_run(), 0
        handle = self._handle(run_id)
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        cooldown = self._poll_cooldown.total_seconds()
        seen_pending = False
        delivered = False
        ended = False
        while True:
            more = True
            while more:
                page, offset, more = await self._tail(handle, topic, offset)
                for offset_, payload in page:
                    for record in self._records(decoder, run_id, offset_, payload):
                        delivered = True
                        yield record
            if ended:
                return
            description = await self._describe(handle)
            if description is None:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} run {run_id!r} was not found"
                )
            pending = any(
                info.activity_id == self._activity_id
                for info in description.raw_description.pending_activities
            )
            seen_pending = seen_pending or pending
            # One more pass after learning the stream ended, so a record that
            # landed between the tail and the describe is not lost.
            ended = description.status != WorkflowExecutionStatus.RUNNING or (
                seen_pending and not pending and delivered
            )
            if not ended:
                await asyncio.sleep(cooldown)

    async def _current_run(self) -> str:
        description = await self._describe(self._handle(self._run_id))
        if description is None:
            raise StreamNotFoundError(f"workflow {self._workflow_id!r} was not found")
        assert description.run_id is not None
        return description.run_id


class WorkflowStreamsProvider(ProviderPlugin):
    """The provider over the shipped Workflow Streams transport."""

    def __init__(
        self, *, poll_cooldown: timedelta = timedelta(milliseconds=100)
    ) -> None:
        """Create the provider.

        Args:
            poll_cooldown: How long an outside reader that is caught up waits
                between polls. Backlogs drain at full speed regardless.
        """
        self._poll_cooldown = poll_cooldown

    def workflow_provider(self) -> _WSWorkflowProvider:
        """The workflow half, over the running instance's shipped stream object."""
        return _WSWorkflowProvider()

    def get_stream_handle(
        self, client: Client, workflow_id: str, *, run_id: str | None = None
    ) -> WorkflowStreamsHandle:
        """A handle on ``workflow_id``'s log; without ``run_id`` it follows the chain."""
        return WorkflowStreamsHandle(client, workflow_id, run_id, self._poll_cooldown)

    def get_activity_stream_handle(
        self,
        client: Client,
        activity_id: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
    ) -> WorkflowStreamsActivityHandle:
        """A handle on the streams an activity of ``workflow_id`` owns.

        They live in the workflow's log under the reserved topics
        ``activity/<activity id>/<name>``, and ``run_id`` pins the workflow's
        run. Without ``workflow_id`` the activity is a standalone one, which
        has no workflow to host a log, so this provider refuses it; such an
        activity addresses a workflow's stream by ``workflow_id`` instead.

        Raises:
            StreamUnsupportedError: ``workflow_id`` was not given.
        """
        if workflow_id is None:
            raise StreamUnsupportedError(
                "the workflow_streams provider cannot hold a stream a standalone "
                "activity owns: its log lives inside a running workflow, and no "
                "workflow hosts this activity's"
            )
        return WorkflowStreamsActivityHandle(
            client, workflow_id, run_id, activity_id, self._poll_cooldown
        )

    async def close(self) -> None:
        """Nothing to release: the provider holds no connection of its own."""
