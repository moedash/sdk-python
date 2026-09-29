"""Client for Temporal server-side streams.

A stream is a durable, offset-addressed append-only sequence that lives beside
Workflow History rather than inside it. Appending schedules no Workflow Task,
and each reader holds its own cursor, so adding a reader costs nothing on the
write side. The record is ``temporal.api.stream.v1.StreamRecord`` on the wire
and in the store, so a reader in any language decodes the same bytes.

Prototype support for AI-198. Four things about it are temporary and will
change before this is a real feature:

- **Requires the ``grpc`` extra.** The rest of this SDK reaches the server
  through sdk-core, which does not know about this service yet, so the client
  here opens its own channel with ``grpcio``.
- **The protos are vendored** under ``temporalio.api.streamservice.v1`` instead of
  coming from the api submodule, because the service is still defined in the
  server. That is why the wire names read as server-internal.
- **This client is for use outside a Workflow.** Workflow code publishes and
  consumes with ``workflow.append_stream_records`` and
  ``workflow.read_stream_records`` instead.
- **No TLS or API-key support**, for the same reason: the channel is built
  here rather than by the machinery that normally handles that.

A failed call raises :class:`temporalio.streams.StreamNotFoundError` when the
server answers ``NOT_FOUND``,
:class:`temporalio.streams.StreamProducerError` when it refuses a producer
sequence it already holds, and :class:`temporalio.service.RPCError`
otherwise, never the transport's own exception type.

A failure sdk-core would retry is retried here, on the same codes and with the
same default :class:`temporalio.service.RetryConfig`, because this channel is
not Core's and gets none of its retrying. ``RESOURCE_EXHAUSTED`` backs off
longer than the rest, as in Core, so a caller the server is throttling does not
add to the load. A call the server cannot tell from its own repeat, an append
without a producer id or a create, is retried only on ``RESOURCE_EXHAUSTED``,
which the server sends before it does anything. The budget is bounded; a caller
that wants a shorter one cancels, as with ``asyncio.timeout``, and the
cancellation lands whether an attempt or a wait between attempts is in progress.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import google.protobuf.duration_pb2
import grpc
import grpc.aio
from google.protobuf.message import Message

import temporalio.api.streamservice.v1 as stream
from temporalio.api.common.v1 import GrpcStatus
from temporalio.api.enums.v1 import ResourceExhaustedCause
from temporalio.api.errordetails.v1 import ResourceExhaustedFailure
from temporalio.api.stream.v1 import StreamRecord, StreamStartPosition
from temporalio.api.streamservice.v1 import service_pb2_grpc
from temporalio.service import RetryConfig, RPCError, RPCStatusCode
from temporalio.streams import StreamNotFoundError, StreamProducerError

__all__ = [
    "Appended",
    "Page",
    "StreamClient",
    "StreamEntry",
    "StreamHandle",
    "WorkflowStreamHandle",
    "close_shared_clients",
    "shared_client",
]

_T = TypeVar("_T")

logger = logging.getLogger(__name__)

# The server refuses a producer sequence it already holds with a message and
# no typed detail, so the phrase is the only thing to match on. Both refusals
# it sends carry it: a repeat with different content, and one behind the
# sequence it accepted last.
_PRODUCER_CONFLICT = "producer sequence"

# The codes sdk-core retries.
_RETRYABLE = frozenset(
    {
        grpc.StatusCode.DATA_LOSS,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.OUT_OF_RANGE,
        grpc.StatusCode.UNAVAILABLE,
    }
)
# What the server sends before it does anything, so a call that cannot be told
# from its own repeat is still safe to make again on it.
_REFUSED = frozenset({grpc.StatusCode.RESOURCE_EXHAUSTED})
# A message over the channel's limit comes back as RESOURCE_EXHAUSTED and is
# the same size every time.
_TOO_LARGE = (
    "grpc: received message larger than max",
    "grpc: message after decompression larger than max",
    "grpc: received message after decompression larger than max",
)
# The floor under a throttled call's wait, sdk-core's own.
_THROTTLE = RetryConfig(
    initial_interval_millis=1000,
    multiplier=2.0,
    max_interval_millis=10000,
    max_elapsed_time_millis=None,
    max_retries=0,
)


class _Backoff:
    """Exponential backoff over a :class:`RetryConfig`, with sdk-core's arithmetic."""

    def __init__(self, config: RetryConfig) -> None:
        self._config = config
        self._started = time.monotonic()
        self._interval = config.initial_interval_millis / 1000
        self._failures = 0

    def next(self) -> float | None:
        """Seconds to wait before the next attempt, or ``None`` once the budget is spent."""
        config = self._config
        self._failures += 1
        if config.max_retries and self._failures >= config.max_retries:
            return None
        base = self._interval
        self._interval = min(
            base * config.multiplier, config.max_interval_millis / 1000
        )
        spread = base * config.randomization_factor
        delay = max(base + random.uniform(-spread, spread), 0.0)
        if config.max_elapsed_time_millis is not None and (
            time.monotonic() - self._started + delay
            > config.max_elapsed_time_millis / 1000
        ):
            return None
        return delay


def _raw_status(error: grpc.aio.AioRpcError) -> bytes:
    # The aio metadata iterates as (key, value) pairs at runtime, whatever
    # shape the stubs give its items.
    trailing: Any = error.trailing_metadata()
    for item in trailing or ():
        key, value = item[0], item[1]
        if key == "grpc-status-details-bin" and isinstance(value, bytes):
            return value
    return b""


def _exhausted_cause(error: grpc.aio.AioRpcError) -> int | None:
    """The cause the server attached to a ``RESOURCE_EXHAUSTED``, when it attached one."""
    raw = _raw_status(error)
    if not raw:
        return None
    status = GrpcStatus()
    status.ParseFromString(raw)
    for detail in status.details:
        if detail.Is(ResourceExhaustedFailure.DESCRIPTOR):
            failure = ResourceExhaustedFailure()
            detail.Unpack(failure)
            return failure.cause
    return None


def _worth_waiting_out(error: grpc.aio.AioRpcError) -> bool:
    """Whether a ``RESOURCE_EXHAUSTED`` is load the server will shed, rather than a limit."""
    if (error.details() or "").startswith(_TOO_LARGE):
        return False
    # A stream's budget refuses an append for as long as the stream is that
    # full, which no wait changes.
    return _exhausted_cause(error) != (
        ResourceExhaustedCause.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT
    )


def _retry_after(
    error: grpc.aio.AioRpcError,
    backoff: _Backoff,
    throttle: _Backoff,
    *,
    idempotent: bool,
) -> float | None:
    """Seconds to wait before making the call again, or ``None`` to raise it."""
    code = error.code()
    if code not in _RETRYABLE or (not idempotent and code not in _REFUSED):
        return None
    throttled = code is grpc.StatusCode.RESOURCE_EXHAUSTED
    if throttled and not _worth_waiting_out(error):
        return None
    delay = backoff.next()
    if delay is None:
        return None
    if throttled:
        delay = max(delay, throttle.next() or 0.0)
    return delay


@dataclass(frozen=True)
class Appended:
    """Where one append landed.

    ``deduplicated`` says the server already held this producer's batch at
    this sequence and wrote nothing; the offsets are then the original ones.
    """

    first_offset: int
    next_offset: int
    count: int
    deduplicated: bool = False


@dataclass(frozen=True)
class StreamEntry:
    """One record read from a stream, with where it sits.

    Offsets are assigned over the unfiltered stream, so a topic-filtered read
    hands back entries whose offsets are not contiguous. A reader that resumes
    between records takes ``offset`` rather than counting what it received.
    """

    record: StreamRecord
    offset: int = 0


@dataclass(frozen=True)
class Page:
    """One read of a stream.

    ``closed`` with ``next_offset >= head_offset`` is the end: nothing more can
    be added and this reader has everything. ``run_id`` names the execution
    holding the stream, which is how a reader of a workflow's stream learns
    which run it is on when it did not pin one.
    """

    entries: list[StreamEntry]
    next_offset: int
    head_offset: int
    closed: bool
    run_id: str = ""


def _copy_by_name(source: Message, target: Message, *, skip: frozenset[str]) -> None:
    """Copy every field ``source`` has set onto the field of ``target`` with that name.

    By descriptor rather than field by field, so a field added to
    ``StreamRecord`` crosses in both directions without anybody remembering to
    add a line here. A field the target does not have raises, which is the
    answer a reader wants: better a loud failure than a body that arrives
    without the thing that described it.
    """
    fields = target.DESCRIPTOR.fields_by_name
    for descriptor, value in source.ListFields():
        if descriptor.name in skip:
            continue
        if descriptor.name not in fields:
            raise ValueError(
                f"{source.DESCRIPTOR.full_name}.{descriptor.name} has no counterpart "
                f"on {target.DESCRIPTOR.full_name}"
            )
        field = getattr(target, descriptor.name)
        if descriptor.message_type is not None and (
            descriptor.message_type.GetOptions().map_entry
        ):
            holds_message = (
                descriptor.message_type.fields_by_name["value"].message_type is not None
            )
            for key, item in value.items():
                if holds_message:
                    field[key].CopyFrom(item)
                else:
                    field[key] = item
        elif hasattr(field, "extend"):
            # A plain repeated field; maps answered above and everything else
            # takes an assignment or a CopyFrom.
            field.extend(value)
        elif descriptor.type == descriptor.TYPE_MESSAGE:
            field.CopyFrom(value)
        else:
            setattr(target, descriptor.name, value)


def _to_service(record: StreamRecord) -> stream.StreamRecord:
    # The stored shape is the public record plus the offset a read assigns.
    # An unset body stays unset, so a FINISH record reads back as one.
    out = stream.StreamRecord()
    _copy_by_name(record, out, skip=frozenset())
    return out


def _to_public(record: stream.StreamRecord) -> StreamEntry:
    out = StreamRecord()
    # The offset is the store's, not the record's; it rides on the entry.
    _copy_by_name(record, out, skip=frozenset({"offset"}))
    return StreamEntry(record=out, offset=record.offset)


def _translate(error: grpc.aio.AioRpcError) -> Exception:
    code = error.code()
    details = error.details() or code.name
    if code is grpc.StatusCode.NOT_FOUND:
        return StreamNotFoundError(details)
    if code is grpc.StatusCode.INVALID_ARGUMENT and _PRODUCER_CONFLICT in details:
        # A producer sequence the store already holds, either with different
        # content or behind the one it accepted last. The caller asked to be
        # deduplicated and could not be, which is a condition of its own.
        return StreamProducerError(details)
    return RPCError(details, RPCStatusCode(code.value[0]), _raw_status(error))


async def _call(
    method: Callable[[Any], Awaitable[_T]],
    request: Any,
    *,
    retry_config: RetryConfig | None = None,
    idempotent: bool = True,
) -> _T:
    """Make one stub call, retried as sdk-core would, translating the failure to the SDK's.

    ``idempotent`` is false for a call the server cannot tell from its own
    repeat, which is then retried only on a refusal the server sent before it
    did anything. The request is the same object on every attempt, so a
    numbered append is deduplicated by the server whichever attempt landed.
    """
    config = retry_config or RetryConfig()
    backoff = _Backoff(config)
    throttle = _Backoff(_THROTTLE)
    attempts = 0
    while True:
        attempts += 1
        try:
            return await method(request)
        except grpc.aio.AioRpcError as error:
            delay = _retry_after(error, backoff, throttle, idempotent=idempotent)
            if delay is None:
                raise _translate(error) from error
            _log_retry(error, attempts, config)
        await asyncio.sleep(delay)


def _log_retry(error: grpc.aio.AioRpcError, attempts: int, config: RetryConfig) -> None:
    # Quiet at first and louder once half the budget is gone, as sdk-core does,
    # so a single throttled call is not a warning but a struggling one is.
    level = logging.DEBUG
    if config.max_retries and attempts * 2 >= config.max_retries:
        level = logging.WARNING
    logger.log(
        level,
        "stream call failed with %s on attempt %d, retrying: %s",
        error.code().name,
        attempts,
        error.details(),
    )


class StreamClient:
    """Creates and opens streams on a namespace."""

    def __init__(
        self,
        channel: Any,
        namespace: str,
        *,
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Wrap an existing ``grpc.aio`` channel. Prefer :meth:`connect`.

        ``retry_config`` is the policy every call made through this client
        retries under; ``None`` is the SDK's default.
        """
        self._channel = channel
        self._namespace = namespace
        self._retry_config = retry_config
        # The generated stub is typed for a synchronous channel. This client
        # drives it over ``grpc.aio``, where every call is awaited.
        self._stub: Any = service_pb2_grpc.StreamServiceStub(channel)

    @staticmethod
    def connect(
        target_host: str,
        namespace: str = "default",
        *,
        retry_config: RetryConfig | None = None,
    ) -> StreamClient:
        """Open a channel to a frontend.

        Separate from ``Client.connect`` because this does not share the
        connection the rest of the SDK uses.
        """
        return StreamClient(
            grpc.aio.insecure_channel(target_host),
            namespace,
            retry_config=retry_config,
        )

    async def close(self) -> None:
        """Close the underlying channel."""
        await self._channel.close()

    async def create(
        self,
        stream_id: str,
        *,
        retention: float | None = None,
        max_items: int | None = None,
    ) -> StreamHandle:
        """Create a stream and return a handle to it.

        ``retention`` is how long a closed stream stays readable, in seconds.
        ``max_items`` caps how many records remain readable, dropping the
        oldest, which bounds storage for a stream nobody truncates.
        """
        lifecycle = stream.StreamLifecycle()
        if retention is not None:
            lifecycle.retention.CopyFrom(
                google.protobuf.duration_pb2.Duration(seconds=int(retention))
            )
        if max_items is not None:
            lifecycle.max_items = max_items

        # A create that landed but was not answered would be refused as a
        # repeat, so it goes again only on a refusal.
        response = await _call(
            self._stub.CreateStream,
            stream.CreateStreamRequest(
                frontend_request=stream.CreateStreamInput(
                    namespace=self._namespace,
                    stream_id=stream_id,
                    lifecycle=lifecycle,
                )
            ),
            retry_config=self._retry_config,
            idempotent=False,
        )
        return StreamHandle(
            self._stub,
            self._namespace,
            stream_id,
            run_id=response.frontend_response.run_id,
            retry_config=self._retry_config,
        )

    def get(self, stream_id: str) -> StreamHandle:
        """Open an existing stream without a round trip."""
        return StreamHandle(
            self._stub, self._namespace, stream_id, retry_config=self._retry_config
        )

    def workflow_stream(
        self, workflow_id: str, name: str = "", *, owner_run_id: str = ""
    ) -> WorkflowStreamHandle:
        """Open a stream a workflow owns.

        A stream a workflow owns lives inside that workflow's execution and has
        no id of its own, so it is named by its owner and its name. An empty
        name is the workflow's default output stream, which is what
        ``workflow.append_stream_records`` writes to when it is given none.

        The stream is created by the workflow's first publish or subscription,
        or by the first append from outside. Reading one that does not exist
        yet is not an error: it reads as empty and a
        :meth:`WorkflowStreamHandle.follow` parked on it wakes when something
        is written.

        ``owner_run_id`` pins the handle to one run of the workflow; see
        :meth:`WorkflowStreamHandle.pin` for why a follower wants that.
        """
        return WorkflowStreamHandle(
            self._stub,
            self._namespace,
            workflow_id,
            name,
            owner_run_id,
            retry_config=self._retry_config,
        )

    def activity_stream(
        self,
        activity_id: str,
        name: str = "",
        *,
        workflow_id: str = "",
        run_id: str = "",
    ) -> WorkflowStreamHandle:
        """Open a stream an activity owns.

        Without ``workflow_id`` the activity is a standalone one, an execution
        of its own, and ``run_id`` pins one run of it. With ``workflow_id`` it
        is an activity that workflow scheduled, reached through the workflow,
        and ``run_id`` pins the workflow's run. Either way the stream is apart
        from the workflow's streams, one per activity execution rather than
        per attempt, and it reads as closed once the activity reaches a
        terminal status. An empty name is the activity's default stream.
        """
        return WorkflowStreamHandle(
            self._stub,
            self._namespace,
            workflow_id,
            name,
            run_id,
            activity_id=activity_id,
            retry_config=self._retry_config,
        )


class StreamHandle:
    """A handle to one standalone stream."""

    def __init__(
        self,
        stub: Any,
        namespace: str,
        stream_id: str,
        run_id: str = "",
        *,
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Prefer :meth:`StreamClient.get` or :meth:`StreamClient.create`."""
        self._stub = stub
        self._namespace = namespace
        self._id = stream_id
        # Passing this back saves the server resolving the current run on every
        # call, which is otherwise a persistence lookup per request.
        self._run_id = run_id
        self._retry_config = retry_config

    @property
    def id(self) -> str:
        """Id of the stream this handle points at."""
        return self._id

    async def append(
        self,
        *records: StreamRecord,
        producer_id: str = "",
        sequence: int = 0,
    ) -> Appended:
        """Append records and return where they landed.

        Supplying ``producer_id`` and ``sequence`` makes the append idempotent:
        a retry with the same pair returns the original offsets rather than
        appending twice, and says so. That is also what lets a failed append
        be made again here on every code sdk-core retries. Without them the
        append is at-least-once, which is only the right trade when duplicates
        are harmless, and it is made again only on a refusal the server sent
        before it did anything.
        """
        response = await _call(
            self._stub.AddMessages,
            stream.AddMessagesRequest(
                frontend_request=stream.AddMessagesInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    run_id=self._run_id,
                    records=[_to_service(record) for record in records],
                    producer_id=producer_id,
                    sequence=sequence,
                )
            ),
            retry_config=self._retry_config,
            idempotent=bool(producer_id),
        )
        return _appended(response.frontend_response)

    async def read(
        self,
        *,
        from_offset: int = 0,
        start: StreamStartPosition | None = None,
        max_records: int = 0,
        topics: Sequence[str] = (),
        wait: bool = False,
    ) -> tuple[list[StreamEntry], int]:
        """Read once from ``from_offset``, returning the entries and the
        offset to read from next.

        A reader with no offset yet passes ``start`` instead: the oldest
        record held, the tail, or the last N records. The server resolves it
        in the same read, so it cannot race with truncation, and the offset
        returned is where to continue. Passing both is refused.

        With ``wait`` set, blocks until something arrives, the stream closes,
        or the server's long-poll window elapses. A window that elapses returns
        an empty list rather than raising, so the caller just reads again.
        """
        page = await self.poll(
            from_offset=from_offset,
            start=start,
            max_records=max_records,
            topics=topics,
            wait=wait,
        )
        return page.entries, page.next_offset

    async def follow(
        self,
        *,
        from_offset: int = 0,
        start: StreamStartPosition | None = None,
        topics: Sequence[str] = (),
    ) -> AsyncIterator[StreamEntry]:
        """Yield entries as they arrive, starting at ``from_offset`` or ``start``.

        Ends once the stream is closed and this reader has drained it. A closed
        stream stays readable until its retention expires, so a reader that
        starts late still sees everything instead of having to coordinate a
        shutdown with the producer.
        """
        offset = from_offset
        while True:
            page = await self.poll(from_offset=offset, start=start, topics=topics)
            # The first page carries where the start resolved to, and every
            # later poll continues from the offset it handed back.
            start = None
            for entry in page.entries:
                yield entry
            offset = page.next_offset
            if page.closed and offset >= page.head_offset:
                return

    async def poll(
        self,
        *,
        from_offset: int = 0,
        start: StreamStartPosition | None = None,
        topics: Sequence[str] = (),
        max_records: int = 0,
        wait: bool = True,
    ) -> Page:
        """One read, with everything the caller needs to decide what to do next."""
        response = await _call(
            self._stub.PollMessages,
            stream.PollMessagesRequest(
                frontend_request=stream.PollMessagesInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    run_id=self._run_id,
                    from_offset=from_offset,
                    start_position=start,
                    max_messages=max_records,
                    topics=list(topics),
                    wait_new_messages=wait,
                )
            ),
            retry_config=self._retry_config,
        )
        return _page(response.frontend_response)

    async def truncate(self, new_base_offset: int) -> None:
        """Drop every record below ``new_base_offset``.

        A reader that asks for an offset below it is refused. One that has no
        offset yet and wants the oldest record left asks for
        ``StreamStartPosition(earliest=True)`` rather than offset zero.
        """
        await _call(
            self._stub.TruncateStream,
            stream.TruncateStreamRequest(
                frontend_request=stream.TruncateStreamInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    new_base_offset=new_base_offset,
                )
            ),
            retry_config=self._retry_config,
        )

    async def finish_writing(self, producer_id: str) -> None:
        """Declare one producer done without ending the stream for others."""
        await _call(
            self._stub.FinishWriting,
            stream.FinishWritingRequest(
                frontend_request=stream.FinishWritingInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    producer_id=producer_id,
                )
            ),
            retry_config=self._retry_config,
        )

    async def close(self) -> None:
        """Seal the stream. Readers can still drain what is already there."""
        await _call(
            self._stub.CloseStream,
            stream.CloseStreamRequest(
                frontend_request=stream.CloseStreamInput(
                    namespace=self._namespace, stream_id=self._id
                )
            ),
            retry_config=self._retry_config,
        )

    async def describe(self) -> stream.StreamState:
        """Read the stream's current frontier, floor and closed state."""
        response = await _call(
            self._stub.DescribeStream,
            stream.DescribeStreamRequest(
                frontend_request=stream.DescribeStreamInput(
                    namespace=self._namespace, stream_id=self._id
                )
            ),
            retry_config=self._retry_config,
        )
        return response.frontend_response.state


class WorkflowStreamHandle:
    """A handle to a stream a workflow or an activity owns.

    A workflow writes to its own from inside its Workflow Task, which costs it
    no transition of its own. Anything else writes through :meth:`append`,
    which costs one transition on the owning execution per batch. Both land in
    the same log in the order the server accepted them. An activity has only
    the second path.
    """

    def __init__(
        self,
        stub: Any,
        namespace: str,
        workflow_id: str,
        name: str = "",
        owner_run_id: str = "",
        *,
        activity_id: str = "",
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Prefer :meth:`StreamClient.workflow_stream` or :meth:`StreamClient.activity_stream`."""
        self._stub = stub
        self._namespace = namespace
        self._workflow_id = workflow_id
        self._name = name
        self._owner_run_id = owner_run_id
        self._activity_id = activity_id
        self._retry_config = retry_config

    @property
    def workflow_id(self) -> str:
        """Id of the workflow that owns this stream, or that scheduled the activity that does."""
        return self._workflow_id

    @property
    def activity_id(self) -> str:
        """Id of the activity that owns this stream, empty when a workflow does."""
        return self._activity_id

    def _owner(self) -> dict[str, Any]:
        # A workflow owner goes out in the workflow fields, which a server
        # without owner support still routes on; only an activity needs the
        # owner reference.
        if not self._activity_id:
            return {
                "workflow_id": self._workflow_id,
                "owner_run_id": self._owner_run_id,
            }
        if self._workflow_id:
            owner = stream.StreamOwner(
                kind=stream.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
                id=self._workflow_id,
                run_id=self._owner_run_id,
                activity_id=self._activity_id,
            )
        else:
            owner = stream.StreamOwner(
                kind=stream.STREAM_OWNER_KIND_ACTIVITY,
                id=self._activity_id,
                run_id=self._owner_run_id,
            )
        return {"owner": owner}

    @property
    def name(self) -> str:
        """Name the owner publishes under, empty for its default stream."""
        return self._name

    @property
    def owner_run_id(self) -> str:
        """The run this handle is pinned to, empty while it follows the current run."""
        return self._owner_run_id

    def pin(self, run_id: str) -> None:
        """Address one run of the workflow from now on.

        Unpinned, every call resolves to whichever run is current. A follower
        holding an offset from one run would then be redirected when the
        workflow continues as new: the successor's stream starts empty at
        offset zero, so the server reads the follower's offset as "caught up",
        parks it until the successor has published that many records, and
        then skips exactly that many. Pinned, the run's end shows up as
        ``closed`` on the page, and the caller decides whether to follow the
        successor from the beginning.
        """
        self._owner_run_id = run_id

    async def append(
        self,
        *records: StreamRecord,
        producer_id: str = "",
        sequence: int = 0,
    ) -> Appended:
        """Append from outside the workflow, as :meth:`StreamHandle.append`.

        Batch where you can. The append costs one transition on the owning
        execution whatever its size, so a batch of a hundred costs what a batch
        of one does.
        """
        response = await _call(
            self._stub.AddWorkflowMessages,
            stream.AddWorkflowMessagesRequest(
                frontend_request=stream.AddWorkflowMessagesInput(
                    namespace=self._namespace,
                    **self._owner(),
                    stream_name=self._name,
                    records=[_to_service(record) for record in records],
                    producer_id=producer_id,
                    sequence=sequence,
                )
            ),
            retry_config=self._retry_config,
            idempotent=bool(producer_id),
        )
        return _appended(response.frontend_response)

    async def read(
        self,
        *,
        from_offset: int = 0,
        start: StreamStartPosition | None = None,
        max_records: int = 0,
        topics: Sequence[str] = (),
        wait: bool = False,
    ) -> tuple[list[StreamEntry], int]:
        """Read once from ``from_offset``, as :meth:`StreamHandle.read`."""
        page = await self.poll(
            from_offset=from_offset,
            start=start,
            max_records=max_records,
            topics=topics,
            wait=wait,
        )
        return page.entries, page.next_offset

    async def follow(
        self,
        *,
        from_offset: int = 0,
        start: StreamStartPosition | None = None,
        topics: Sequence[str] = (),
    ) -> AsyncIterator[StreamEntry]:
        """Yield entries as they arrive, as :meth:`StreamHandle.follow`."""
        offset = from_offset
        while True:
            page = await self.poll(from_offset=offset, start=start, topics=topics)
            # The first page carries where the start resolved to, and every
            # later poll continues from the offset it handed back.
            start = None
            for entry in page.entries:
                yield entry
            offset = page.next_offset
            if page.closed and offset >= page.head_offset:
                return

    async def poll(
        self,
        *,
        from_offset: int = 0,
        start: StreamStartPosition | None = None,
        topics: Sequence[str] = (),
        max_records: int = 0,
        wait: bool = True,
    ) -> Page:
        """One read, with everything the caller needs to decide what to do next.

        :meth:`read` is the same call without the frontier and the closed flag.
        A caller that has to tell "nothing yet" from "nothing ever" needs both.
        On a pinned handle ``closed`` also says the run has ended.
        """
        response = await _call(
            self._stub.PollWorkflowMessages,
            stream.PollWorkflowMessagesRequest(
                frontend_request=stream.PollWorkflowMessagesInput(
                    namespace=self._namespace,
                    **self._owner(),
                    stream_name=self._name,
                    from_offset=from_offset,
                    start_position=start,
                    max_messages=max_records,
                    topics=list(topics),
                    wait_new_messages=wait,
                )
            ),
            retry_config=self._retry_config,
        )
        return _page(response.frontend_response)

    async def describe(self) -> stream.StreamState:
        """Read the stream's current frontier, floor and closed state.

        A reader that wants only what comes next starts from the head this
        reports rather than from zero.
        """
        response = await _call(
            self._stub.DescribeWorkflowStream,
            stream.DescribeWorkflowStreamRequest(
                frontend_request=stream.DescribeWorkflowStreamInput(
                    namespace=self._namespace,
                    **self._owner(),
                    stream_name=self._name,
                )
            ),
            retry_config=self._retry_config,
        )
        return response.frontend_response.state


def _appended(out: stream.AddMessagesOutput) -> Appended:
    return Appended(
        first_offset=out.first_offset,
        next_offset=out.next_offset,
        count=out.count,
        deduplicated=out.deduplicated,
    )


def _page(out: stream.PollMessagesOutput) -> Page:
    return Page(
        entries=[_to_public(record) for record in out.records],
        next_offset=out.next_offset,
        head_offset=out.head_offset,
        closed=out.closed,
        run_id=out.run_id,
    )


# One channel per loop, target and namespace, shared by every handle in the
# process. A channel is multiplexed and long lived, and callers open a handle
# per subscription, which would otherwise be a connection per subscription. The
# loop is the key because a grpc.aio channel belongs to the loop that made it,
# and it is held weakly so a loop that is gone cannot lend its channel to a
# successor that happens to reuse its id.
_shared: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, dict[tuple[str, str], StreamClient]
] = weakref.WeakKeyDictionary()


def shared_client(target_host: str, namespace: str) -> StreamClient:
    """The process-wide client for ``target_host`` and ``namespace`` on this loop."""
    per_loop = _shared.setdefault(asyncio.get_running_loop(), {})
    key = (target_host, namespace)
    existing = per_loop.get(key)
    if existing is None:
        existing = per_loop[key] = StreamClient.connect(target_host, namespace)
    return existing


async def close_shared_clients(*keys: tuple[str, str]) -> None:
    """Close the shared clients this loop opened for ``keys``, or all of them.

    A provider closes the ones it opened, named by ``(target host,
    namespace)``: another provider on the same loop may still be reading
    through a channel of its own, and taking that out from under it is not
    this one's to do. With no keys it closes every one, which is what a
    process finished with streams wants, and what a test that opened a loop
    of its own wants.
    """
    loop = asyncio.get_running_loop()
    if not keys:
        per_loop = _shared.pop(loop, {})
        closing = list(per_loop.values())
    else:
        per_loop = _shared.get(loop, {})
        closing = [per_loop.pop(key) for key in keys if key in per_loop]
    for client in closing:
        await client.close()
