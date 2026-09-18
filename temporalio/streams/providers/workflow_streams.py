"""The provider over today's Workflow Streams (Option 0).

Speaks the shipped contrib feature's wire format, the
``__temporal_workflow_stream_*`` Signal, Update and Query, so interface code
and existing Workflow Streams code interoperate on one stream, and old
histories replay. Records live in the owning workflow's History, which is
also this provider's limit: Option 0's caps (payloads in History, the Signal
cap, bounded subscribers) are transport properties and remain. Reads after
the workflow closes go through one Query this provider adds, which serves
the log from workflow state for as long as the History is retained, so a
reader between polls when the workflow completed still gets the tail.

The mapping, in one place:

- An interface record's frame rides as the item's ``Payload`` data.
- Inbound stream ``s`` is shipped topic ``in:s``; a writer topic ``t`` is
  shipped topic ``out:t``, so the two namespaces cannot collide. A producer
  appending onto the workflow's own topic ``t`` writes ``out:t`` as well,
  which is what today's activities already do through the publish Signal.
- Producer identity dedupes through the shipped publisher state: the
  publisher id is ``producer#attempt`` and every publish Signal carries a
  monotonic sequence, so a retried batch drops and a new attempt passes.
- ``append`` returns an empty cursor. The Signal transport learns positions
  at read time; that is this provider's stated deviation.
"""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import (
    PUBLISH_SIGNAL_NAME,
    PublishEntry,
    PublishInput,
    WorkflowStream,
    WorkflowStreamClient,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import _frame, _provider
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

_TAIL_QUERY = "__temporal_streams_tail"
_IN = "in:"
_OUT = "out:"


class _Runtime:
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
                "data": base64.b64encode(payload.data).decode("ascii"),
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


def _runtime() -> _Runtime:
    # Found on the instance rather than in a process-level map keyed by run
    # id: the SDK rebuilds an evicted workflow from history as a new object,
    # and a map would hand that object the stale log with its unregistered
    # handlers and the records of a task that failed.
    stream = _registered_stream()
    return _Runtime() if stream is None else _Runtime(stream)


class _WSReadSource:
    """Reads the signal-fed log the shipped feature keeps in workflow state."""

    def __init__(self, stream: WorkflowStream, shipped_topic: str, start: int) -> None:
        self._stream = stream
        self._shipped_topic = shipped_topic
        self._cursor = start
        self._closed = False

    async def next_batch(self) -> list[tuple[Cursor, bytes]]:
        while True:
            if self._closed:
                raise StopAsyncIteration
            await workflow.wait_condition(
                lambda: self._closed or self._stream.next_offset > self._cursor
            )
            if self._closed:
                raise StopAsyncIteration
            batch = [
                (Cursor(str(offset)), payload.data)
                for offset, topic, payload in self._stream.items_from(self._cursor)
                if topic == self._shipped_topic
            ]
            self._cursor = self._stream.next_offset
            if batch:
                return batch

    def close(self) -> None:
        self._closed = True


class _WSWriteSink:
    def __init__(self, stream: WorkflowStream, shipped_topic: str) -> None:
        self._handle = stream.topic(shipped_topic)

    async def publish(self, frame: bytes) -> None:
        # Appending to workflow state commits with the task, and a poll
        # Update's result rides the same task completion, so a failed task
        # leaks nothing: rule 1 through the shipped mechanics.
        payload = workflow.payload_converter().to_payloads([frame])[0]
        self._handle.publish(payload)


def _entry_data(payload: Payload) -> str:
    # The documented wire form of PublishEntry.data.
    return base64.b64encode(payload.SerializeToString()).decode("ascii")


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
        converter: Any,
        stream: str,
        topic: str,
        producer_id: str,
        attempt: int,
    ) -> None:
        """Bind this producer to one stream or topic on ``handle``."""
        self._handle = handle
        self._converter = converter
        self._stream = stream
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = 0
        self._signal_sequence = 0
        self._pending: tuple[list[PublishEntry], int] | None = None

    @property
    def _shipped_topic(self) -> str:
        return f"{_IN}{self._stream}" if self._stream else f"{_OUT}{self._topic}"

    @property
    def attempt(self) -> int:
        """The generation this producer is writing."""
        return self._attempt

    @property
    def _provider_id(self) -> str:
        return (
            f"{self._producer_id}#{self._attempt}"
            if self._attempt
            else self._producer_id
        )

    async def append(self, *values: Any) -> None:
        """Append ``values`` through the shipped publish Signal.

        Always ``None``: this transport learns positions at read time.
        """
        if not values:
            return None
        await self._send([(RecordKind.DATA, self._encode(value)) for value in values])
        return None

    async def finish(self) -> None:
        """Mark this producer done, so a reader stops waiting on it."""
        await self._send([(RecordKind.FINISH, b"")])

    def _frames(
        self, bodies: list[tuple[RecordKind, bytes]]
    ) -> tuple[list[PublishEntry], int]:
        sequence = self._sequence
        entries = []
        for kind, body in bodies:
            frame = _frame.encode(
                topic=self._topic,
                kind=kind,
                producer=self._producer_id,
                attempt=self._attempt,
                sequence=sequence,
                body=body,
            )
            sequence += 1
            entries.append(
                PublishEntry(
                    topic=self._shipped_topic, data=_entry_data(Payload(data=frame))
                )
            )
        return entries, sequence

    async def _send(self, bodies: list[tuple[RecordKind, bytes]]) -> None:
        entries, next_sequence = self._frames(bodies)
        if self._pending is not None and self._pending[0] != entries:
            # The caller moved on from a batch whose Signal raised. It goes
            # first, under the signal sequence it already had, so a copy the
            # server did accept is dropped and one it never saw lands. The
            # new batch is then renumbered behind it.
            await self._signal(*self._pending)
            entries, next_sequence = self._frames(bodies)
        await self._signal(entries, next_sequence)

    async def _signal(self, entries: list[PublishEntry], next_sequence: int) -> None:
        signal_sequence = self._signal_sequence + 1
        self._pending = (entries, next_sequence)
        await self._handle.signal(
            PUBLISH_SIGNAL_NAME,
            PublishInput(
                items=entries,
                publisher_id=self._provider_id,
                sequence=signal_sequence,
            ),
        )
        self._signal_sequence = signal_sequence
        self._sequence = next_sequence
        self._pending = None

    def _encode(self, value: Any) -> bytes:
        payload = (
            value
            if isinstance(value, Payload)
            else self._converter.to_payloads([value])[0]
        )
        return payload.SerializeToString()


class WorkflowStreamsConsumer:
    """Reads through the shipped long-poll Update, with shared supersession."""

    def __init__(
        self,
        stream_client: WorkflowStreamClient,
        shipped_topic: str | None,
        poll_cooldown: timedelta,
    ) -> None:
        """Read what ``stream_client`` reaches, one long poll at a time."""
        self._client = stream_client
        self._shipped_topic = shipped_topic
        self._poll_cooldown = poll_cooldown

    async def read(
        self,
        *,
        after: Cursor = BEGINNING,
        topic: str | None = None,
        type: type | None = None,
    ) -> AsyncIterator[StreamRecord[Any]]:
        """Yield records after ``after``, waiting for ones not written yet."""
        attempts = AttemptTracker()
        next_offset = int(after.token) + 1 if after.token else 0
        subscription = self._client.subscribe(
            self._shipped_topic,
            from_offset=next_offset,
            result_type=RawValue,
            poll_cooldown=self._poll_cooldown,
        )
        async for item in subscription:
            next_offset = item.offset + 1
            record = self._record(
                attempts, item.offset, item.topic, item.data.payload.data, topic, type
            )
            for out in record:
                yield out
        # The subscription ends when the workflow is closing or closed. What
        # landed after the last poll is still in workflow state, so the tail
        # comes back by Query rather than being lost with the run.
        for wire in await self._tail(next_offset):
            for out in self._record(
                attempts,
                wire["offset"],
                wire["topic"],
                base64.b64decode(wire["data"]),
                topic,
                type,
            ):
                yield out

    async def _tail(self, from_offset: int) -> list[dict[str, Any]]:
        try:
            wire = await self._client.handle.query(
                _TAIL_QUERY, from_offset, result_type=list
            )
        except Exception:
            # A workflow that never opened a stream has no handler to ask, and
            # one whose History is gone has nothing left to serve.
            return []

    def _record(
        self,
        attempts: AttemptTracker,
        offset: int,
        shipped_topic: str,
        frame: bytes,
        topic: str | None,
        type: type | None,
    ) -> list[StreamRecord[Any]]:
        if self._shipped_topic is None and not shipped_topic.startswith(_OUT):
            return []
        if self._shipped_topic is not None and shipped_topic != self._shipped_topic:
            return []
        cursor = Cursor(str(offset))
        try:
            kind, frame_topic, source, attempt, sequence, body = _frame.decode(frame)
        except ValueError:
            return []
        if topic is not None and frame_topic != topic:
            return []
        out: list[StreamRecord[Any]] = []
        superseded = attempts.note(source, attempt, cursor)
        if superseded is not None:
            out.append(superseded)
        out.append(
            StreamRecord(
                value=self._decode(body, type) if kind is RecordKind.DATA else None,
                cursor=cursor,
                kind=kind,
                topic=frame_topic,
                producer=source,
                attempt=attempt,
                sequence=sequence,
            )
        )
        return out

    async def latest(self, *, topic: str | None = None) -> Cursor:
        """The cursor of the last record written, for following from now."""
        del topic  # one log per workflow, whatever the topic
        head = await self._client.get_offset()
        return Cursor(str(head - 1)) if head > 0 else BEGINNING

    def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        converter = self._client.payload_converter
        if as_type is None:
            return converter.from_payloads([payload])[0]
        return converter.from_payloads([payload], [as_type])[0]


class _WorkflowStreamsProvider:
    name = "workflow_streams"

    def __init__(self) -> None:
        self._poll_cooldown = timedelta(milliseconds=100)

    def configure(self, **options: Any) -> None:
        cooldown = options.pop("poll_cooldown", None)
        if cooldown is not None:
            self._poll_cooldown = cooldown
        if options:
            raise TypeError(
                "the workflow_streams provider takes only poll_cooldown, "
                f"got {sorted(options)}"
            )

    def worker_options(self) -> dict[str, Any]:
        return {}

    def prepare(self) -> None:
        """Register the shipped publish signal and poll update handlers.

        Done here rather than on the first read, because an outside reader
        can poll before workflow code has opened anything, and an update with
        no handler yet is rejected rather than held.
        """
        _runtime()

    def drain(self) -> None:
        """Release parked pollers so the workflow can return.

        An Option 0 stream dies with its workflow, and a parked long-poll
        Update would otherwise hold completion open. Call it right before
        the workflow returns, the same obligation the shipped feature's
        ``detach_pollers`` documents.
        """
        stream = _registered_stream()
        if stream is not None:
            stream.detach_pollers()

    def open_read(
        self,
        stream: str,
        *,
        after: Cursor = BEGINNING,
        idle_timeout: timedelta | None = None,
    ) -> ReadSource:
        # Ignored: a publish Signal is a workflow event, so the wait below
        # wakes on delivery and nothing is held between records.
        del idle_timeout
        return _WSReadSource(
            _runtime().stream,
            f"{_IN}{stream}",
            int(after.token) + 1 if after.token else 0,
        )

    def open_write(self, topic: str) -> WriteSink:
        return _WSWriteSink(_runtime().stream, f"{_OUT}{topic}")

    async def producer(
        self,
        client: Client,
        *,
        workflow_id: str,
        stream: str = "",
        topic: str = "",
        producer_id: str = "",
        attempt: int = 0,
    ) -> WorkflowStreamsProducer:
        if not producer_id:
            from temporalio import activity

            producer_id = activity.info().activity_id
            attempt = attempt or activity.info().attempt
        return WorkflowStreamsProducer(
            client.get_workflow_handle(workflow_id),
            client.data_converter.payload_converter,
            stream,
            topic,
            producer_id,
            attempt,
        )

    async def consumer(
        self, client: Client, *, workflow_id: str, stream: str = ""
    ) -> WorkflowStreamsConsumer:
        return WorkflowStreamsConsumer(
            WorkflowStreamClient.create(client, workflow_id),
            f"{_IN}{stream}" if stream else None,
            self._poll_cooldown,
        )


_provider.register("workflow_streams", _WorkflowStreamsProvider)
