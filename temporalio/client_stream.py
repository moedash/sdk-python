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
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import google.protobuf.duration_pb2
import grpc
import grpc.aio

import temporalio.api.streamservice.v1 as stream
from temporalio.api.stream.v1 import StreamRecord
from temporalio.api.streamservice.v1 import service_pb2_grpc
from temporalio.service import RPCError, RPCStatusCode
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

# The server refuses a producer sequence it already holds with a message and
# no typed detail, so the phrase is the only thing to match on. Both refusals
# it sends carry it: a repeat with different content, and one behind the
# sequence it accepted last.
_PRODUCER_CONFLICT = "producer sequence"


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


def _to_service(record: StreamRecord) -> stream.StreamRecord:
    # Field for field the public record; the stored shape only adds the offset
    # a read assigns. An unset body stays unset so a FINISH record reads back
    # as one.
    out = stream.StreamRecord(
        topic=record.topic,
        kind=record.kind,
        producer_id=record.producer_id,
        attempt=record.attempt,
        sequence=record.sequence,
    )
    if record.HasField("body"):
        out.body.CopyFrom(record.body)
    for key, value in record.metadata.items():
        out.metadata[key].CopyFrom(value)
    return out


def _to_public(record: stream.StreamRecord) -> StreamEntry:
    out = StreamRecord(
        topic=record.topic,
        kind=record.kind,
        producer_id=record.producer_id,
        attempt=record.attempt,
        sequence=record.sequence,
    )
    if record.HasField("body"):
        out.body.CopyFrom(record.body)
    for key, value in record.metadata.items():
        out.metadata[key].CopyFrom(value)
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
    raw = b""
    # The aio metadata iterates as (key, value) pairs at runtime, whatever
    # shape the stubs give its items.
    trailing: Any = error.trailing_metadata()
    for item in trailing or ():
        key, value = item[0], item[1]
        if key == "grpc-status-details-bin" and isinstance(value, bytes):
            raw = value
    return RPCError(details, RPCStatusCode(code.value[0]), raw)


async def _call(method: Callable[[Any], Awaitable[_T]], request: Any) -> _T:
    """Make one stub call, translating the transport's failure to the SDK's."""
    try:
        return await method(request)
    except grpc.aio.AioRpcError as error:
        raise _translate(error) from error


class StreamClient:
    """Creates and opens streams on a namespace."""

    def __init__(self, channel: Any, namespace: str) -> None:
        """Wrap an existing ``grpc.aio`` channel. Prefer :meth:`connect`."""
        self._channel = channel
        self._namespace = namespace
        # The generated stub is typed for a synchronous channel. This client
        # drives it over ``grpc.aio``, where every call is awaited.
        self._stub: Any = service_pb2_grpc.StreamServiceStub(channel)

    @staticmethod
    def connect(target_host: str, namespace: str = "default") -> StreamClient:
        """Open a channel to a frontend.

        Separate from ``Client.connect`` because this does not share the
        connection the rest of the SDK uses.
        """
        return StreamClient(grpc.aio.insecure_channel(target_host), namespace)

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

        response = await _call(
            self._stub.CreateStream,
            stream.CreateStreamRequest(
                frontend_request=stream.CreateStreamInput(
                    namespace=self._namespace,
                    stream_id=stream_id,
                    lifecycle=lifecycle,
                )
            ),
        )
        return StreamHandle(
            self._stub,
            self._namespace,
            stream_id,
            run_id=response.frontend_response.run_id,
        )

    def get(self, stream_id: str) -> StreamHandle:
        """Open an existing stream without a round trip."""
        return StreamHandle(self._stub, self._namespace, stream_id)

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
            self._stub, self._namespace, workflow_id, name, owner_run_id
        )


class StreamHandle:
    """A handle to one standalone stream."""

    def __init__(
        self, stub: Any, namespace: str, stream_id: str, run_id: str = ""
    ) -> None:
        """Prefer :meth:`StreamClient.get` or :meth:`StreamClient.create`."""
        self._stub = stub
        self._namespace = namespace
        self._id = stream_id
        # Passing this back saves the server resolving the current run on every
        # call, which is otherwise a persistence lookup per request.
        self._run_id = run_id

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
        appending twice, and says so. Without them the append is
        at-least-once, which is only the right trade when duplicates are
        harmless.
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
        )
        return _appended(response.frontend_response)

    async def read(
        self,
        *,
        from_offset: int = 0,
        max_records: int = 0,
        topics: Sequence[str] = (),
        wait: bool = False,
    ) -> tuple[list[StreamEntry], int]:
        """Read once from ``from_offset``, returning the entries and the
        offset to read from next.

        With ``wait`` set, blocks until something arrives, the stream closes,
        or the server's long-poll window elapses. A window that elapses returns
        an empty list rather than raising, so the caller just reads again.
        """
        page = await self.poll(
            from_offset=from_offset, max_records=max_records, topics=topics, wait=wait
        )
        return page.entries, page.next_offset

    async def follow(
        self,
        *,
        from_offset: int = 0,
        topics: Sequence[str] = (),
    ) -> AsyncIterator[StreamEntry]:
        """Yield entries as they arrive, starting at ``from_offset``.

        Ends once the stream is closed and this reader has drained it. A closed
        stream stays readable until its retention expires, so a reader that
        starts late still sees everything instead of having to coordinate a
        shutdown with the producer.
        """
        offset = from_offset
        while True:
            page = await self.poll(from_offset=offset, topics=topics)
            for entry in page.entries:
                yield entry
            offset = page.next_offset
            if page.closed and offset >= page.head_offset:
                return

    async def poll(
        self,
        *,
        from_offset: int = 0,
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
                    max_messages=max_records,
                    topics=list(topics),
                    wait_new_messages=wait,
                )
            ),
        )
        return _page(response.frontend_response)

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
        )
        return response.frontend_response.state


class WorkflowStreamHandle:
    """A handle to a stream a workflow owns.

    The workflow writes to it from inside its Workflow Task, which costs it no
    transition of its own. Anything else writes through :meth:`append`, which
    costs one transition on the owning execution per batch. Both land in the
    same log in the order the server accepted them.
    """

    def __init__(
        self,
        stub: Any,
        namespace: str,
        workflow_id: str,
        name: str = "",
        owner_run_id: str = "",
    ) -> None:
        """Prefer :meth:`StreamClient.workflow_stream`."""
        self._stub = stub
        self._namespace = namespace
        self._workflow_id = workflow_id
        self._name = name
        self._owner_run_id = owner_run_id

    @property
    def workflow_id(self) -> str:
        """Id of the workflow that owns this stream."""
        return self._workflow_id

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
                    workflow_id=self._workflow_id,
                    owner_run_id=self._owner_run_id,
                    stream_name=self._name,
                    records=[_to_service(record) for record in records],
                    producer_id=producer_id,
                    sequence=sequence,
                )
            ),
        )
        return _appended(response.frontend_response)

    async def read(
        self,
        *,
        from_offset: int = 0,
        max_records: int = 0,
        topics: Sequence[str] = (),
        wait: bool = False,
    ) -> tuple[list[StreamEntry], int]:
        """Read once from ``from_offset``, as :meth:`StreamHandle.read`."""
        page = await self.poll(
            from_offset=from_offset, max_records=max_records, topics=topics, wait=wait
        )
        return page.entries, page.next_offset

    async def follow(
        self,
        *,
        from_offset: int = 0,
        topics: Sequence[str] = (),
    ) -> AsyncIterator[StreamEntry]:
        """Yield entries as they arrive, as :meth:`StreamHandle.follow`."""
        offset = from_offset
        while True:
            page = await self.poll(from_offset=offset, topics=topics)
            for entry in page.entries:
                yield entry
            offset = page.next_offset
            if page.closed and offset >= page.head_offset:
                return

    async def poll(
        self,
        *,
        from_offset: int = 0,
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
                    workflow_id=self._workflow_id,
                    owner_run_id=self._owner_run_id,
                    stream_name=self._name,
                    from_offset=from_offset,
                    max_messages=max_records,
                    topics=list(topics),
                    wait_new_messages=wait,
                )
            ),
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
                    workflow_id=self._workflow_id,
                    owner_run_id=self._owner_run_id,
                    stream_name=self._name,
                )
            ),
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


async def close_shared_clients() -> None:
    """Close every shared client this loop opened.

    For a process that is done with streams, and for tests, which open a
    loop per case and would otherwise leave a channel behind on each.
    """
    per_loop = _shared.pop(asyncio.get_running_loop(), {})
    for client in per_loop.values():
        await client.close()
