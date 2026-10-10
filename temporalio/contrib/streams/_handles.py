"""Reaching a stream from an Activity or from client code.

An Activity reaches the stream of the Workflow that scheduled it, and writes
as itself: its producer id is ``<Activity id>@<scheduling run id>`` and its
Temporal attempt is the producer attempt, so a retry is reported to readers
as ``SUPERSEDED``. The run is part of the id because a stream outlives a
run, and Activity ids repeat across the runs of a chain.
Any process holding a client reaches a Workflow's stream by Workflow id and
writes with a producer id and attempt of its own.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, TypeVar, cast, overload

import temporalio.activity
from temporalio.client import Client
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._plugin import (
    provider_for_activity,
    provider_for_client,
)
from temporalio.contrib.streams._provider import StreamHandle, StreamProducer
from temporalio.contrib.streams._record import Cursor, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic

__all__ = ["ActivityStreamHandle", "activity_handle", "get_stream_handle"]

T = TypeVar("T")


class ActivityStreamHandle:
    """The scheduling Workflow's stream, as the running Activity sees it.

    Reads and positions as :class:`temporalio.contrib.streams.StreamHandle`
    does. :meth:`producer` writes as this Activity, which is the only
    identity that lets readers tell this Activity's retry from a new
    producer.
    """

    def __init__(self, inner: StreamHandle, producer_id: str, attempt: int) -> None:
        """Prefer :func:`temporalio.contrib.streams.activity_handle`."""
        self._inner = inner
        self._producer_id = producer_id
        self._attempt = attempt

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on, pinned to the scheduling run."""
        return self._inner.ref

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.read`."""
        # One call forwards both overloads, which the checker can't match to
        # the protocol's overloads separately.
        inner = cast(Any, self._inner)
        return inner.read(topic=topic, after=after, result_type=result_type)

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamHandle.latest`."""
        return await self._inner.latest(topic=topic)

    @overload
    def producer(self, *, topic: StreamTopic[T]) -> StreamProducer[T]: ...

    @overload
    def producer(self, *, topic: str | None = None) -> StreamProducer[Any]: ...

    def producer(
        self, *, topic: str | StreamTopic[Any] | None = None
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as this Activity attempt.

        The producer id is ``<Activity id>@<scheduling run id>`` and the
        attempt is the Activity's Temporal attempt, so the first record of a
        retry makes readers see ``SUPERSEDED``. A retry starts a new sequence, and the store
        deduplicates a repeated append within one attempt.
        """
        # One call forwards both overloads, which the checker can't match to
        # the protocol's overloads separately.
        inner = cast(Any, self._inner)
        return inner.producer(
            topic=topic, producer_id=self._producer_id, attempt=self._attempt
        )


def activity_handle() -> ActivityStreamHandle:
    """The stream of the Workflow that scheduled the running Activity.

    The handle is pinned to the scheduling run, so a read on it ends when
    that run closes. To write to another Workflow's stream, use
    :func:`get_stream_handle` with ``temporalio.activity.client()`` and a
    producer id of your own.

    Raises:
        StreamUnsupportedError: The Activity was not scheduled by a Workflow;
            streams owned by an Activity are not supported in this release.
        ValueError: No stream provider is registered on the Worker or its
            client.
        RuntimeError: Not called from inside an Activity.
    """
    info = temporalio.activity.info()
    if info.workflow_id is None:
        raise StreamUnsupportedError(
            "this Activity was not scheduled by a Workflow, and streams owned by "
            "an Activity are not supported in this release"
        )
    provider = provider_for_activity()
    ref = StreamRef.for_workflow(info.workflow_id, run_id=info.workflow_run_id)
    inner = provider.get_stream_handle(temporalio.activity.client(), ref)
    # Activity ids are counters within one run, so two runs of a chain would
    # otherwise share producer state in the stream they share.
    producer_id = f"{info.activity_id}@{info.workflow_run_id}"
    return ActivityStreamHandle(inner, producer_id, info.attempt)


def get_stream_handle(
    client: Client,
    workflow_id: str | StreamRef,
    *,
    run_id: str | None = None,
    topic: str | StreamTopic[Any] | None = None,
) -> StreamHandle:
    """A handle on a Workflow's stream, through the client's provider.

    Without ``run_id`` the handle follows the Workflow's run chain, so a
    read continues across Continue-as-New and ends when the chain closes.
    With one it is pinned to that run. ``topic`` is the topic a call that
    names none addresses.

    Args:
        client: A client that carries a stream provider plugin.
        workflow_id: The Workflow that owns the stream, or a
            :class:`temporalio.contrib.streams.StreamRef` that names it.
        run_id: Pin the handle to one run.
        topic: The handle's default topic. Without one it is
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.

    Raises:
        ValueError: ``run_id`` or ``topic`` was given with a ref, or no
            stream provider is registered on ``client``.
    """
    if isinstance(workflow_id, StreamRef):
        if run_id is not None or topic is not None:
            raise ValueError("a StreamRef carries its own run_id and topic")
        ref = workflow_id
    else:
        ref = StreamRef.for_workflow(workflow_id, run_id=run_id, topic=topic)
    return provider_for_client(client).get_stream_handle(client, ref)
