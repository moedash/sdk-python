"""Server-side streams behind the Workflow Streams API.

This is the same surface as :mod:`temporalio.contrib.workflow_streams`, backed
by a Temporal-owned log instead of by Signals and Updates. An application swaps
the import and keeps its code: publishing from a Workflow is still a plain
call, publishing from an Activity is still a buffered handle, and a consumer
still subscribes by topic from an offset.

What changes is underneath. A publish is a Workflow Command whose payload never
enters History, so History gets one fixed-size event per batch rather than a
Signal per batch. A consumer reads the log directly rather than long-polling an
Update, so there is no per-Workflow limit on how many can read at once, and a
closed Workflow stays readable until its stream's retention expires.

Prototype support for AI-198. It needs a server built from that branch, and it
opens its own gRPC channel because sdk-core does not know the stream service
yet, which is also why it does not support TLS or API keys.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, Optional, TypeVar, overload

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.client_stream import StreamClient, WorkflowStreamHandle
from temporalio.converter import DataConverter, PayloadConverter

__all__ = [
    "RawPage",
    "TopicHandle",
    "WorkflowStream",
    "WorkflowStreamClient",
    "WorkflowStreamItem",
    "WorkflowTopicHandle",
]

T = TypeVar("T")

DEFAULT_BATCH_INTERVAL = timedelta(milliseconds=50)


@dataclass
class RawPage:
    """One read whose items are still encoded.

    ``closed`` with ``next_offset >= head_offset`` is the end of the stream.
    """

    items: list["WorkflowStreamItem[bytes]"]
    next_offset: int
    head_offset: int
    closed: bool


@dataclass
class WorkflowStreamItem(Generic[T]):
    """One item read from a workflow's stream.

    ``offset`` is where the item sits in the whole stream, so it is what a
    consumer hands back to resume. A topic filter leaves gaps in it.
    """

    topic: str
    data: T
    offset: int = 0


def _encode(converter: PayloadConverter, value: Any) -> bytes:
    """Serialize one value to a stream message body.

    The payload rather than the bare bytes, so the encoding metadata the
    consumer needs to decode into a type travels with it.

    The codec chain is not applied. It cannot be applied here: a codec is async
    and a Workflow publishing to a stream is not, so running one inside Workflow
    code would be I/O on the Workflow thread. Regular payloads solve this in the
    Worker, encoding on the way out and decoding on the way in, and a stream
    needs the same plumbing before a codec can be honoured.

    It is not applied on the client path either, deliberately. Encoding one side
    and not the other is worse than encoding neither: an Activity's messages
    would reach a consuming Workflow as ciphertext it has no way to decode.
    :func:`_reject_configured_codec` is what keeps that from happening quietly.
    """
    payload = value if isinstance(value, Payload) else converter.to_payloads([value])[0]
    return payload.SerializeToString()


def _reject_configured_codec(client: Client) -> None:
    """Refuse a namespace whose payloads are meant to be encoded.

    Stream bodies bypass the codec chain, so proceeding would write payloads
    this namespace expects to be encrypted in the clear, and would do it
    silently. Raising is the only honest answer until the Worker-side plumbing
    exists.
    """
    if client.data_converter.payload_codec is not None:
        raise RuntimeError(
            "this client has a payload codec configured, and server-side stream "
            "bodies do not pass through it. Publishing would store them "
            "unencoded. Use a client without a codec, or wait for codec support "
            "on streams."
        )


def _decode(converter: PayloadConverter, body: bytes, as_type: Optional[type]) -> Any:
    payload = Payload()
    payload.ParseFromString(body)
    if as_type is None:
        return converter.from_payloads([payload])[0]
    return converter.from_payloads([payload], [as_type])[0]


class WorkflowTopicHandle(Generic[T]):
    """A topic on the stream the running Workflow owns."""

    def __init__(self, topic: str, value_type: type[T]) -> None:
        """Prefer :meth:`WorkflowStream.topic`."""
        self._name = topic
        self._type = value_type

    @property
    def name(self) -> str:
        """The topic name this handle is bound to."""
        return self._name

    @property
    def type(self) -> type[T]:
        """The value type this handle is bound to."""
        return self._type

    def publish(self, value: T | Payload) -> None:
        """Append ``value`` to the Workflow's stream on this topic.

        Returns as soon as the Command is issued. There is nothing to await:
        the append is applied in the Workflow Task's own commit, so it costs
        this Workflow no round trip and no extra transition.
        """
        workflow.add_stream_messages(
            [_encode(workflow.payload_converter(), value)], topic=self._name
        )


class WorkflowStream:
    """The stream the running Workflow owns, from inside it.

    Construct in ``@workflow.init``. Unlike the Signals-and-Updates
    implementation this holds no state of its own: the log lives on the server,
    so there is nothing here for replay to reconstruct.
    """

    def __init__(self, prior_state: Any = None) -> None:
        """Take the same argument as the Workflow Streams version and ignore it.

        That version carried the log across a continue-as-new, because the log
        was Workflow state. Here it is not, so there is nothing to carry.
        """
        self._prior_state = prior_state

    @overload
    def topic(self, name: str) -> WorkflowTopicHandle[Any]: ...

    @overload
    def topic(self, name: str, *, type: type[T]) -> WorkflowTopicHandle[T]: ...

    def topic(
        self, name: str, *, type: type = object
    ) -> WorkflowTopicHandle[Any]:
        """Bind a topic on this Workflow's stream."""
        return WorkflowTopicHandle(name, type)


class TopicHandle(Generic[T]):
    """A topic on a Workflow's stream, from outside that Workflow."""

    def __init__(self, client: "WorkflowStreamClient", topic: str, value_type: type[T]) -> None:
        """Prefer :meth:`WorkflowStreamClient.topic`."""
        self._client = client
        self._name = topic
        self._type = value_type

    @property
    def name(self) -> str:
        """The topic name this handle is bound to."""
        return self._name

    @property
    def type(self) -> type[T]:
        """The value type this handle is bound to."""
        return self._type

    def publish(self, value: T | Payload, *, force_flush: bool = False) -> None:
        """Buffer ``value`` for the next flush.

        Buffered rather than sent, because an append costs one transition on
        the owning execution whatever its size. A token at a time would pay
        that per token.
        """
        self._client._buffer(self._name, value)
        if force_flush:
            self._client._flush_soon()

    def subscribe(
        self,
        *,
        from_offset: int = 0,
        result_type: Optional[type] = None,
        poll_cooldown: Optional[timedelta] = None,
    ) -> AsyncIterator[WorkflowStreamItem[T]]:
        """Read this topic from ``from_offset`` onwards."""
        return self._client.subscribe(
            topics=[self._name],
            from_offset=from_offset,
            result_type=result_type or self._type,
            poll_cooldown=poll_cooldown,
        )


class WorkflowStreamClient:
    """Publishes to and reads from a Workflow's stream, from outside it."""

    def __init__(
        self,
        handle: WorkflowStreamHandle,
        converter: PayloadConverter,
        batch_interval: timedelta = DEFAULT_BATCH_INTERVAL,
    ) -> None:
        """Prefer :meth:`create` or :meth:`from_within_activity`."""
        self._handle = handle
        self._converter = converter
        self._batch_interval = batch_interval
        self._buffered: list[tuple[str, Any]] = []
        self._flusher: Optional[asyncio.Task[None]] = None
        self._wake = asyncio.Event()

    @classmethod
    def create(
        cls,
        client: Client,
        workflow_id: str,
        *,
        batch_interval: timedelta = DEFAULT_BATCH_INTERVAL,
    ) -> "WorkflowStreamClient":
        """Open the stream owned by ``workflow_id``."""
        _reject_configured_codec(client)
        return cls(
            _stream_client(client).workflow_stream(workflow_id),
            client.data_converter.payload_converter,
            batch_interval,
        )

    @classmethod
    def from_within_activity(
        cls, *, batch_interval: timedelta = DEFAULT_BATCH_INTERVAL
    ) -> "WorkflowStreamClient":
        """Open the stream owned by the Workflow that scheduled this Activity."""
        client = activity.client()
        return cls.create(
            client, activity.info().workflow_id, batch_interval=batch_interval
        )

    async def __aenter__(self) -> "WorkflowStreamClient":
        """Start the background flusher."""
        self._flusher = asyncio.create_task(self._run_flusher())
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Drain what is buffered before letting the caller go.

        An Activity that returned with a batch still buffered would have
        reported work its readers never saw.
        """
        if self._flusher is not None:
            self._flusher.cancel()
            try:
                await self._flusher
            except asyncio.CancelledError:
                pass
            self._flusher = None
        await self.flush()

    @overload
    def topic(self, name: str) -> TopicHandle[Any]: ...

    @overload
    def topic(self, name: str, *, type: type[T]) -> TopicHandle[T]: ...

    def topic(self, name: str, *, type: type = object) -> TopicHandle[Any]:
        """Bind a topic on this Workflow's stream."""
        return TopicHandle(self, name, type)

    async def get_offset(self) -> int:
        """Where the stream currently ends.

        A reader that wants only what comes next starts here.
        """
        return (await self._handle.describe()).head_offset

    async def subscribe(
        self,
        *,
        topics: Sequence[str] = (),
        from_offset: int = 0,
        result_type: Optional[type] = None,
        poll_cooldown: Optional[timedelta] = None,
    ) -> AsyncIterator[WorkflowStreamItem[Any]]:
        """Yield items from ``from_offset`` as they arrive.

        ``poll_cooldown`` is accepted and ignored. It paced a client that had
        to re-ask; the server parks this read until something arrives.
        """
        del poll_cooldown
        async for message in self._handle.follow(from_offset=from_offset, topics=topics):
            yield WorkflowStreamItem(
                topic=message.topic,
                data=_decode(self._converter, message.data, result_type),
                offset=message.offset,
            )

    async def poll_raw(
        self,
        *,
        topics: Sequence[str] = (),
        from_offset: int = 0,
        wait: bool = True,
    ) -> "RawPage":
        """One read, with the bodies left as they were stored.

        For a caller that forwards items on rather than using them. A gateway
        would only have to encode again what this decoded.
        """
        page = await self._handle.poll(
            from_offset=from_offset, topics=topics, wait=wait
        )
        return RawPage(
            items=[
                WorkflowStreamItem(topic=m.topic, data=m.data, offset=m.offset)
                for m in page.messages
            ],
            next_offset=page.next_offset,
            head_offset=page.head_offset,
            closed=page.closed,
        )

    async def flush(self) -> None:
        """Append everything buffered as one batch."""
        pending, self._buffered = self._buffered, []
        if not pending:
            return
        # One append per topic, because a batch carries a single topic. The
        # harness case is one topic, so this is one append.
        by_topic: dict[str, list[bytes]] = {}
        for topic, value in pending:
            by_topic.setdefault(topic, []).append(_encode(self._converter, value))
        for topic, bodies in by_topic.items():
            await self._handle.append(*bodies, topic=topic)

    def _buffer(self, topic: str, value: Any) -> None:
        self._buffered.append((topic, value))

    def _flush_soon(self) -> None:
        self._wake.set()

    async def _run_flusher(self) -> None:
        """Append on a fixed cadence, so a slow producer still gets delivered."""
        while True:
            try:
                await asyncio.wait_for(
                    self._wake.wait(), self._batch_interval.total_seconds()
                )
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            await self.flush()


# One channel per target, shared by every handle in the process. The channel is
# multiplexed and long-lived, and callers here open a client per subscription,
# which would otherwise be a connection per subscription.
_clients: dict[tuple[int, str, str], StreamClient] = {}


def _stream_client(client: Client) -> StreamClient:
    target = client.service_client.config.target_host
    # Keyed on the loop too: a gRPC channel belongs to the loop that made it.
    key = (id(asyncio.get_event_loop()), target, client.namespace)
    existing = _clients.get(key)
    if existing is None:
        existing = StreamClient.connect(target, client.namespace)
        _clients[key] = existing
    return existing
