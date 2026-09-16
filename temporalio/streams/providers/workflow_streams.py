"""The provider over today's Workflow Streams (Option 0).

Speaks the shipped contrib feature's wire format, the
``__temporal_workflow_stream_*`` Signal, Update and Query, so interface code
and existing Workflow Streams code interoperate on one stream, and old
histories replay. Records live in the owning workflow's History, which is
also this provider's limit: Option 0's caps (payloads in History, the Signal
cap, bounded subscribers, no reads after the workflow closes) are transport
properties and remain.

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

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import (
    PublishEntry,
    PublishInput,
    WorkflowStream,
    WorkflowStreamClient,
)
from temporalio.contrib.workflow_streams._stream import _PUBLISH_SIGNAL
from temporalio.contrib.workflow_streams._types import _encode_payload
from temporalio.streams import _frame, _provider
from temporalio.streams._handles import ReadSource, WriteSink
from temporalio.streams._policy import AttemptTracker
from temporalio.streams._record import BEGINNING, Cursor, RecordKind, StreamRecord

_RUN_ATTR = "__temporal_streams_ws_runtime"
_IN = "in:"
_OUT = "out:"


class _Runtime:
    """Per-run holder for the shipped stream object.

    A separate class because ``WorkflowStream`` insists on being constructed
    from a method named ``__init__``, and the provider builds it lazily on
    the first read or write of a run.
    """

    def __init__(self) -> None:
        self.stream = WorkflowStream()


# Held per run rather than on the workflow instance, because the provider is
# asked to install its handlers from the workflow's constructor, and the
# instance is not registered with the runtime yet at that point.
_runtimes: dict[str, _Runtime] = {}


def _runtime() -> _Runtime:
    key = workflow.info().run_id
    runtime = _runtimes.get(key)
    if runtime is None:
        runtime = _Runtime()
        _runtimes[key] = runtime
    return runtime


def drain() -> None:
    """Release parked pollers so the workflow can return.

    An Option 0 stream dies with its workflow, and a parked long-poll Update
    would otherwise hold completion open. Call it right before the workflow
    returns, the same obligation the shipped feature's ``detach_pollers``
    documents. A storage provider has no such step, which is one of the
    differences the comparison table charges this transport with.
    """
    runtime = _runtimes.pop(workflow.info().run_id, None)
    if runtime is not None:
        runtime.stream.detach_pollers()


class _WSReadSource:
    """Reads the signal-fed log the shipped feature keeps in workflow state.

    Reaches into the stream's private log rather than ``get_state()``,
    because the snapshot copies the whole log per call and drops offsets,
    and this runs inside ``workflow.wait_condition``.
    """

    def __init__(self, stream: WorkflowStream, shipped_topic: str, start: int) -> None:
        self._stream = stream
        self._shipped_topic = shipped_topic
        self._cursor = start
        self._closed = False

    def _end(self) -> int:
        return self._stream._base_offset + len(self._stream._log)

    async def next_batch(self) -> list[tuple[Cursor, bytes]]:
        while True:
            if self._closed:
                raise StopAsyncIteration
            base = self._stream._base_offset
            if self._cursor < base:
                # Truncated below the cursor; resume at what remains.
                self._cursor = base
            await workflow.wait_condition(
                lambda: self._closed or self._end() > self._cursor
            )
            if self._closed:
                raise StopAsyncIteration
            batch: list[tuple[Cursor, bytes]] = []
            end = self._end()
            for offset in range(self._cursor, end):
                item = self._stream._log[offset - self._stream._base_offset]
                if item.topic == self._shipped_topic:
                    batch.append((Cursor(str(offset)), item.data.data))
            self._cursor = end
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


class WorkflowStreamsProducer:
    """Appends by sending the shipped publish Signal directly.

    Direct rather than through ``WorkflowStreamClient`` because the interface
    owns the publisher identity: it must be ``producer#attempt`` for the
    shipped dedupe to drop a retry and pass a new generation, and the client
    would use its own random id.
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
        self._handle = handle
        self._converter = converter
        self._stream = stream
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._sequence = 0
        self._signal_sequence = 0

    @property
    def _frame_topic(self) -> str:
        return self._stream or self._topic

    @property
    def _shipped_topic(self) -> str:
        return f"{_IN}{self._stream}" if self._stream else f"{_OUT}{self._topic}"

    @property
    def attempt(self) -> int:
        return self._attempt

    @property
    def _provider_id(self) -> str:
        return (
            f"{self._producer_id}#{self._attempt}" if self._attempt else self._producer_id
        )

    async def append(self, *values: Any) -> Cursor:
        entries = []
        for value in values:
            frame = _frame.encode(
                topic=self._frame_topic,
                kind=RecordKind.DATA,
                producer=self._producer_id,
                attempt=self._attempt,
                sequence=self._sequence,
                body=self._encode(value),
            )
            self._sequence += 1
            entries.append(self._entry(frame))
        await self._send(entries)
        return Cursor("")

    async def finish(self) -> None:
        frame = _frame.encode(
            topic=self._frame_topic,
            kind=RecordKind.FINISH,
            producer=self._producer_id,
            attempt=self._attempt,
            sequence=self._sequence,
            body=b"",
        )
        self._sequence += 1
        await self._send([self._entry(frame)])

    def _entry(self, frame: bytes) -> PublishEntry:
        payload = Payload(data=frame)
        return PublishEntry(topic=self._shipped_topic, data=_encode_payload(payload))

    async def _send(self, entries: list[PublishEntry]) -> None:
        self._signal_sequence += 1
        await self._handle.signal(
            _PUBLISH_SIGNAL,
            PublishInput(
                items=entries,
                publisher_id=self._provider_id,
                sequence=self._signal_sequence,
            ),
        )

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
        attempts = AttemptTracker()
        subscription = self._client.subscribe(
            self._shipped_topic,
            from_offset=int(after.token) + 1 if after.token else 0,
            result_type=RawValue,
            poll_cooldown=self._poll_cooldown,
        )
        async for item in subscription:
            if self._shipped_topic is None and not item.topic.startswith(_OUT):
                continue
            cursor = Cursor(str(item.offset))
            try:
                kind, frame_topic, source, attempt, sequence, body = _frame.decode(
                    item.data.payload.data
                )
            except ValueError:
                continue
            if topic is not None and frame_topic != topic:
                continue
            superseded = attempts.note(source, attempt, cursor)
            if superseded is not None:
                yield superseded
            yield StreamRecord(
                value=self._decode(body, type) if kind is RecordKind.DATA else None,
                cursor=cursor,
                kind=kind,
                topic=frame_topic,
                producer=source,
                attempt=attempt,
                sequence=sequence,
            )

    async def latest(self, *, topic: str | None = None) -> Cursor:
        del topic  # one log per workflow, whatever the topic
        head = await self._client.get_offset()
        return Cursor(str(head - 1)) if head > 0 else BEGINNING

    def _decode(self, body: bytes, as_type: type | None) -> Any:
        payload = Payload()
        payload.ParseFromString(body)
        converter = self._client._client.data_converter.payload_converter
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
        """Release parked pollers so the workflow can return."""
        drain()

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
