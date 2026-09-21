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
  publisher id is ``producer#attempt`` and every publish Signal carries a
  monotonic sequence, so a retried batch drops and a new attempt passes.
- ``append()`` returns ``None``. The Signal transport learns positions at
  read time, so a caller that wants to follow from now asks ``latest()``.
- A log belongs to one run and is not carried across continue-as-new, so a
  cursor names the run as well as the offset. A handle without a run id
  reads run after run: each log through the poll Update while its run is
  open and through the tail Query once it has closed, then the successor's
  from its first record.
- The workflow-side stream object belongs to the workflow instance, found
  through the handler the shipped class registers on it. An evicted and
  rebuilt workflow gets its own, so a task that failed leaks nothing into
  the next attempt's log and a replayed run does not see records twice.
"""

from __future__ import annotations

import base64
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowHandle,
    WorkflowHistoryEventFilterType,
    WorkflowQueryFailedError,
)
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import (
    PUBLISH_SIGNAL_NAME,
    PublishEntry,
    PublishInput,
    WorkflowStream,
    WorkflowStreamClient,
)
from temporalio.converter import PayloadConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._errors import StreamCursorError, StreamNotFoundError
from temporalio.streams._provider import ReadSource, WriteSink
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord
from temporalio.streams._wire import (
    RecordDecoder,
    WireRecord,
    cursor_position,
    mint_cursor,
    producer_identity,
    to_wire,
)
from temporalio.streams.providers import ProviderPlugin

__all__ = [
    "WorkflowStreamsHandle",
    "WorkflowStreamsProducer",
    "WorkflowStreamsProvider",
]

_PROVIDER = "workflow_streams"
_TAIL_QUERY = "__temporal_streams_tail"
_ENCODING = b"binary/plain"

logger = logging.getLogger(__name__)


def _require_topic(topic: str) -> None:
    if not topic:
        raise ValueError("topic must not be empty")


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

    def _tail(self, from_offset: int) -> list[dict[str, Any]]:
        return [
            {
                "offset": offset,
                "topic": topic,
                "data": base64.b64encode(payload.SerializeToString()).decode("ascii"),
            }
            for offset, topic, payload in self.stream.items_from(from_offset)
        ]


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
    """The workflow half: the shipped stream object of the running instance."""

    def open_reader(self, topic: str, *, after: Cursor) -> ReadSource:
        _require_topic(topic)
        run_id = workflow.info().run_id
        start = 0
        named = _position(after)
        if named is not None:
            if named[0] != run_id:
                raise StreamCursorError(
                    f"cursor {after.token!r} names another run; a run's log is its own"
                )
            start = named[1] + 1
        return _WSReadSource(_instance().stream, topic, start, run_id)

    def open_writer(self, topic: str) -> WriteSink:
        _require_topic(topic)
        return _WSWriteSink(_instance().stream, topic)

    def on_workflow_start(self) -> None:
        # Registered before the first task completes, because an outside
        # reader can poll before workflow code has opened anything, and an
        # Update with no handler yet is rejected rather than held.
        _instance()

    async def on_workflow_finish(self) -> None:
        # An Option 0 stream dies with its run, and a parked long-poll Update
        # would otherwise hold completion open. Same recipe the shipped
        # feature documents before a return or a continue-as-new.
        stream = _registered_stream()
        if stream is None:
            return
        stream.detach_pollers()
        await workflow.wait_condition(workflow.all_handlers_finished)


class WorkflowStreamsProducer:
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
    ) -> None:
        """Bind this producer to ``topic`` on the workflow behind ``handle``."""
        self._handle = handle
        self._converter = converter
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = 0
        self._signal_sequence = 0
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

    async def append(self, *values: Any) -> Cursor | None:
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
                PublishEntry(topic=self._topic, data=_entry_data(_wrap(wire)))
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
        signal_sequence = self._signal_sequence + 1
        self._pending = (entries, next_sequence)
        try:
            await self._handle.signal(
                PUBLISH_SIGNAL_NAME,
                PublishInput(
                    items=entries,
                    publisher_id=self._publisher_id,
                    sequence=signal_sequence,
                ),
            )
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._handle.id!r} was not found, so its stream "
                    "cannot be appended to"
                ) from error
            raise
        self._signal_sequence = signal_sequence
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
        topic: str,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield records on ``topic`` after ``after`` until the chain, or the pinned run, closes."""
        _require_topic(topic)
        # Parsed here so a foreign cursor fails this call, not the first
        # iteration of the generator.
        named = _position(after)
        if named is not None and self._run_id is not None and named[0] != self._run_id:
            raise StreamCursorError(
                f"cursor {after.token!r} names another run than this handle is pinned to"
            )
        return self._read(topic, named, after, result_type)

    async def _read(
        self,
        topic: str,
        named: tuple[str, int] | None,
        after: Cursor,
        result_type: type | None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        decoder = RecordDecoder(
            self._converter, result_type, after=after, warn=logger.warning
        )
        if named is not None:
            run_id, offset = named[0], named[1] + 1
        else:
            run_id, offset = self._run_id or await self._first_run(), 0
        while True:
            handle = self._handle(run_id)
            next_offset = offset
            # Pinned to one run, with no client for the shipped chain
            # following: a log is not carried across continue-as-new, so
            # the successor's offsets start over and this loop is what
            # moves from one run to the next.
            subscription = WorkflowStreamClient(handle).subscribe(
                topic,
                from_offset=offset,
                result_type=RawValue,
                poll_cooldown=self._poll_cooldown,
            )
            try:
                async for item in subscription:
                    next_offset = item.offset + 1
                    if item.topic != topic:
                        continue
                    for record in self._records(
                        decoder, run_id, item.offset, item.data.payload
                    ):
                        yield record
            except RPCError as error:
                # The run closed and its poll Update went with it, or the
                # workflow does not exist; the describe below tells which.
                if error.status != RPCStatusCode.NOT_FOUND:
                    raise
            finally:
                if isinstance(subscription, AsyncGenerator):
                    await subscription.aclose()
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
            for offset_, shipped_topic, payload in await self._tail(
                handle, next_offset
            ):
                if shipped_topic != topic:
                    continue
                for record in self._records(decoder, run_id, offset_, payload):
                    yield record
            if (
                self._run_id is not None
                or status != WorkflowExecutionStatus.CONTINUED_AS_NEW
            ):
                return
            successor = await self._successor(handle)
            if successor is None:
                return
            run_id, offset = successor, 0

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

    async def _status(
        self, handle: WorkflowHandle[Any, Any]
    ) -> WorkflowExecutionStatus | None:
        try:
            return (await handle.describe()).status
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return None
            raise

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
                return attributes.continued_execution_run_id or None
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
        # The run's History is gone: the chain's retained part starts here.
        return None

    async def _successor(self, handle: WorkflowHandle[Any, Any]) -> str | None:
        events = handle.fetch_history_events(
            event_filter_type=WorkflowHistoryEventFilterType.CLOSE_EVENT
        )
        async for event in events:
            if event.HasField("workflow_execution_continued_as_new_event_attributes"):
                attributes = event.workflow_execution_continued_as_new_event_attributes
                return attributes.new_execution_run_id or None
        return None

    async def _tail(
        self, handle: WorkflowHandle[Any, Any], from_offset: int
    ) -> list[tuple[int, str, Payload]]:
        try:
            wire = await handle.query(_TAIL_QUERY, from_offset, result_type=list)
        except WorkflowQueryFailedError as error:
            if "expected but not found" not in str(error):
                raise
            # The workflow never opened a stream through this provider, so
            # there is no tail to serve.
            return []
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
            # The History is gone; nothing is left to serve.
            return []
        return [
            (
                item["offset"],
                item["topic"],
                Payload.FromString(base64.b64decode(item["data"])),
            )
            for item in wire
        ]

    async def latest(self, *, topic: str) -> Cursor:
        """The newest position in the log, which orders every topic of this workflow.

        The log is one per run, so the cursor names the run it was read from:
        the pinned run, or the latest run of the chain.
        """
        _require_topic(topic)
        handle = self._handle(self._run_id)
        try:
            description = await handle.describe()
            head = await WorkflowStreamClient(handle).get_offset()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"workflow {self._workflow_id!r} was not found"
                ) from error
            raise
        run_id = description.run_id
        assert run_id is not None
        if head > 0:
            return _cursor(run_id, head - 1)
        # An empty log on the chain's first run is the beginning of the
        # stream; on a successor it is a position of its own, because
        # BEGINNING would send a chain-following read back to the first run.
        if await self._predecessor(run_id) is None:
            return BEGINNING
        return _cursor(run_id, -1)

    def producer(
        self, *, topic: str, producer_id: str = "", attempt: int = 0
    ) -> WorkflowStreamsProducer:
        """A producer on ``topic``; inside an activity its identity is the activity's."""
        _require_topic(topic)
        producer_id, attempt = producer_identity(producer_id, attempt)
        return WorkflowStreamsProducer(
            self._handle(self._run_id), self._converter, topic, producer_id, attempt
        )


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

    async def close(self) -> None:
        """Nothing to release: the provider holds no connection of its own."""
