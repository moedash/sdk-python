"""Client for Temporal server-side streams.

A stream is a durable, offset-addressed append-only sequence that lives beside
Workflow History rather than inside it. Appending schedules no Workflow Task,
and each reader holds its own cursor, so adding a reader costs nothing on the
write side.

Prototype support for AI-198. Four things about it are temporary and will
change before this is a real feature:

- **Requires the ``grpc`` extra.** The rest of this SDK reaches the server
  through sdk-core, which does not know about this service yet, so the client
  here opens its own channel with ``grpcio``.
- **The protos are vendored** under ``temporalio.api.streamservice.v1`` instead of
  coming from the api submodule, because the service is still defined in the
  server. That is why the wire names read as server-internal.
- **This client is for use outside a Workflow.** Workflow code publishes and
  consumes with ``workflow.add_stream_messages`` and ``workflow.read_stream``
  instead.
- **No TLS or API-key support**, for the same reason: the channel is built
  here rather than by the machinery that normally handles that.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional, Sequence

import google.protobuf.duration_pb2

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream
from temporalio.api.streamservice.v1 import service_pb2_grpc

__all__ = ["Message", "StreamClient", "StreamHandle", "WorkflowStreamHandle"]


@dataclass(frozen=True)
class Message:
    """One item read from a stream.

    There is deliberately no per-message offset. Offsets are assigned over the
    unfiltered stream, so when a read filters by topic the offsets of what
    comes back are not contiguous and cannot be derived here. Checkpoint on the
    ``next_offset`` that :meth:`StreamHandle.read` returns instead.
    """

    data: bytes
    topic: str = ""


class StreamClient:
    """Creates and opens streams on a namespace."""

    def __init__(self, channel: Any, namespace: str) -> None:
        """Wrap an existing ``grpc.aio`` channel. Prefer :meth:`connect`."""
        self._channel = channel
        self._namespace = namespace
        self._stub = service_pb2_grpc.StreamServiceStub(channel)

    @staticmethod
    def connect(target_host: str, namespace: str = "default") -> "StreamClient":
        """Open a channel to a frontend.

        Separate from ``Client.connect`` because this does not share the
        connection the rest of the SDK uses.
        """
        try:
            import grpc
        except ImportError as err:
            raise RuntimeError(
                "temporalio.client_stream requires the grpc extra: "
                "pip install 'temporalio[grpc]'"
            ) from err
        return StreamClient(grpc.aio.insecure_channel(target_host), namespace)

    async def close(self) -> None:
        """Close the underlying channel."""
        await self._channel.close()

    async def create(
        self,
        stream_id: str,
        *,
        retention: Optional[float] = None,
        max_items: Optional[int] = None,
    ) -> "StreamHandle":
        """Create a stream and return a handle to it.

        ``retention`` is how long a closed stream stays readable, in seconds.
        ``max_items`` caps how many messages remain readable, dropping the
        oldest, which bounds storage for a stream nobody truncates.
        """
        lifecycle = stream.StreamLifecycle()
        if retention is not None:
            lifecycle.retention.CopyFrom(
                google.protobuf.duration_pb2.Duration(seconds=int(retention))
            )
        if max_items is not None:
            lifecycle.max_items = max_items

        response = await self._stub.CreateStream(
            stream.CreateStreamRequest(
                frontend_request=stream.CreateStreamInput(
                    namespace=self._namespace,
                    stream_id=stream_id,
                    lifecycle=lifecycle,
                )
            )
        )
        return StreamHandle(
            self._stub,
            self._namespace,
            stream_id,
            run_id=response.frontend_response.run_id,
        )

    def get(self, stream_id: str) -> "StreamHandle":
        """Open an existing stream without a round trip."""
        return StreamHandle(self._stub, self._namespace, stream_id)

    def workflow_stream(
        self, workflow_id: str, name: str = ""
    ) -> "WorkflowStreamHandle":
        """Open a stream a workflow publishes to.

        A stream a workflow owns lives inside that workflow's execution and has
        no id of its own, so it is named by its owner and its name. An empty
        name is the workflow's default output stream, which is what
        ``workflow.add_stream_messages`` writes to when it is given none.

        The stream is created by the workflow's first publish. Reading one that
        does not exist yet is not an error: it reads as empty and a
        :meth:`WorkflowStreamHandle.follow` parked on it wakes when the
        workflow publishes.
        """
        return WorkflowStreamHandle(self._stub, self._namespace, workflow_id, name)


class StreamHandle:
    """A handle to one stream."""

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
        *messages: bytes,
        topic: str = "",
        producer_id: str = "",
        sequence: int = 0,
    ) -> int:
        """Append messages and return the offset the first one landed at.

        Supplying ``producer_id`` and ``sequence`` makes the append idempotent:
        a retry with the same pair returns the original offsets rather than
        appending twice. Without them the append is at-least-once, which is
        only the right trade when duplicates are harmless.
        """
        response = await self._stub.AddMessages(
            stream.AddMessagesRequest(
                frontend_request=stream.AddMessagesInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    run_id=self._run_id,
                    messages=[
                        stream.StreamMessage(
                            body=temporalio.api.common.v1.Payload(
                                data=m, metadata={"encoding": b"binary/plain"}
                            ),
                            topic=topic,
                            kind=stream.STREAM_MESSAGE_KIND_DATA,
                        )
                        for m in messages
                    ],
                    producer_id=producer_id,
                    sequence=sequence,
                )
            )
        )
        return response.frontend_response.first_offset

    async def read(
        self,
        *,
        from_offset: int = 0,
        max_messages: int = 0,
        topics: Sequence[str] = (),
        wait: bool = False,
    ) -> tuple[list[Message], int]:
        """Read once from ``from_offset``, returning the messages and the
        offset to read from next.

        With ``wait`` set, blocks until something arrives, the stream closes,
        or the server's long-poll window elapses. A window that elapses returns
        an empty list rather than raising, so the caller just reads again.
        """
        response = await self._stub.PollMessages(
            stream.PollMessagesRequest(
                frontend_request=stream.PollMessagesInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    run_id=self._run_id,
                    from_offset=from_offset,
                    max_messages=max_messages,
                    topics=list(topics),
                    wait_new_messages=wait,
                )
            )
        )
        out = response.frontend_response
        return [Message(data=m.body.data, topic=m.topic) for m in out.messages], (
            out.next_offset
        )

    async def follow(
        self,
        *,
        from_offset: int = 0,
        topics: Sequence[str] = (),
    ) -> AsyncIterator[Message]:
        """Yield messages as they arrive, starting at ``from_offset``.

        Ends once the stream is closed and this reader has drained it. A closed
        stream stays readable until its retention expires, so a reader that
        starts late still sees everything instead of having to coordinate a
        shutdown with the producer.
        """
        offset = from_offset
        while True:
            response = await self._stub.PollMessages(
                stream.PollMessagesRequest(
                    frontend_request=stream.PollMessagesInput(
                        namespace=self._namespace,
                        stream_id=self._id,
                        run_id=self._run_id,
                        from_offset=offset,
                        topics=list(topics),
                        wait_new_messages=True,
                    )
                )
            )
            out = response.frontend_response
            for msg in out.messages:
                yield Message(data=msg.body.data, topic=msg.topic)
            offset = out.next_offset
            if out.closed and offset >= out.head_offset:
                return

    async def finish_writing(self, producer_id: str) -> None:
        """Declare one producer done without ending the stream for others."""
        await self._stub.FinishWriting(
            stream.FinishWritingRequest(
                frontend_request=stream.FinishWritingInput(
                    namespace=self._namespace,
                    stream_id=self._id,
                    producer_id=producer_id,
                )
            )
        )

    async def close(self) -> None:
        """Seal the stream. Readers can still drain what is already there."""
        await self._stub.CloseStream(
            stream.CloseStreamRequest(
                frontend_request=stream.CloseStreamInput(
                    namespace=self._namespace, stream_id=self._id
                )
            )
        )

    async def describe(self) -> stream.StreamState:
        """Read the stream's current frontier, floor and closed state."""
        response = await self._stub.DescribeStream(
            stream.DescribeStreamRequest(
                frontend_request=stream.DescribeStreamInput(
                    namespace=self._namespace, stream_id=self._id
                )
            )
        )
        return response.frontend_response.state


class WorkflowStreamHandle:
    """A handle to a stream a workflow publishes to.

    The workflow writes to it from inside its Workflow Task, which costs it no
    transition of its own. Anything else writes through :meth:`append`, which
    costs one transition on the owning execution per batch. Both land in the
    same log in the order the server accepted them.
    """

    def __init__(
        self, stub: Any, namespace: str, workflow_id: str, name: str = ""
    ) -> None:
        """Prefer :meth:`StreamClient.workflow_stream`."""
        self._stub = stub
        self._namespace = namespace
        self._workflow_id = workflow_id
        self._name = name

    @property
    def workflow_id(self) -> str:
        """Id of the workflow that owns this stream."""
        return self._workflow_id

    @property
    def name(self) -> str:
        """Name the owner publishes under, empty for its default stream."""
        return self._name

    async def append(
        self,
        *messages: bytes,
        topic: str = "",
        producer_id: str = "",
        sequence: int = 0,
    ) -> int:
        """Append from outside the workflow, as :meth:`StreamHandle.append`.

        Batch where you can. The append costs one transition on the owning
        execution whatever its size, so a batch of a hundred costs what a batch
        of one does.
        """
        response = await self._stub.AddWorkflowMessages(
            stream.AddWorkflowMessagesRequest(
                frontend_request=stream.AddWorkflowMessagesInput(
                    namespace=self._namespace,
                    workflow_id=self._workflow_id,
                    stream_name=self._name,
                    messages=[
                        stream.StreamMessage(
                            body=temporalio.api.common.v1.Payload(
                                data=m, metadata={"encoding": b"binary/plain"}
                            ),
                            topic=topic,
                            kind=stream.STREAM_MESSAGE_KIND_DATA,
                        )
                        for m in messages
                    ],
                    producer_id=producer_id,
                    sequence=sequence,
                )
            )
        )
        return response.frontend_response.first_offset

    async def read(
        self,
        *,
        from_offset: int = 0,
        max_messages: int = 0,
        topics: Sequence[str] = (),
        wait: bool = False,
    ) -> tuple[list[Message], int]:
        """Read once from ``from_offset``, as :meth:`StreamHandle.read`."""
        response = await self._stub.PollWorkflowMessages(
            stream.PollWorkflowMessagesRequest(
                frontend_request=stream.PollWorkflowMessagesInput(
                    namespace=self._namespace,
                    workflow_id=self._workflow_id,
                    stream_name=self._name,
                    from_offset=from_offset,
                    max_messages=max_messages,
                    topics=list(topics),
                    wait_new_messages=wait,
                )
            )
        )
        out = response.frontend_response
        return [Message(data=m.body.data, topic=m.topic) for m in out.messages], (
            out.next_offset
        )

    async def follow(
        self,
        *,
        from_offset: int = 0,
        topics: Sequence[str] = (),
    ) -> AsyncIterator[Message]:
        """Yield messages as they arrive, as :meth:`StreamHandle.follow`."""
        offset = from_offset
        while True:
            messages, offset, closed, head = await self._poll(offset, topics)
            for msg in messages:
                yield msg
            if closed and offset >= head:
                return

    async def _poll(
        self, offset: int, topics: Sequence[str]
    ) -> tuple[list[Message], int, bool, int]:
        response = await self._stub.PollWorkflowMessages(
            stream.PollWorkflowMessagesRequest(
                frontend_request=stream.PollWorkflowMessagesInput(
                    namespace=self._namespace,
                    workflow_id=self._workflow_id,
                    stream_name=self._name,
                    from_offset=offset,
                    topics=list(topics),
                    wait_new_messages=True,
                )
            )
        )
        out = response.frontend_response
        return (
            [Message(data=m.body.data, topic=m.topic) for m in out.messages],
            out.next_offset,
            out.closed,
            out.head_offset,
        )

    async def describe(self) -> stream.StreamState:
        """Read the stream's current frontier, floor and closed state.

        A reader that wants only what comes next starts from the head this
        reports rather than from zero.
        """
        response = await self._stub.DescribeWorkflowStream(
            stream.DescribeWorkflowStreamRequest(
                frontend_request=stream.DescribeWorkflowStreamInput(
                    namespace=self._namespace,
                    workflow_id=self._workflow_id,
                    stream_name=self._name,
                )
            )
        )
        return response.frontend_response.state
