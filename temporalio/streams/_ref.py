"""A reference to one stream that crosses a process boundary as data.

A handle is bound to a client and a provider, so it cannot be a workflow
argument, an activity result or a Nexus operation result. A
:class:`StreamRef` can: it names the owner and the topic, nothing more, and
whoever receives it opens the stream on the provider its own client carries.
It carries no cursor, because a position belongs to a reader, and no provider
name, because the same owner and topic name the same stream on every provider
a deployment runs.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from temporalio.streams._record import BEGINNING, Cursor, StreamRecord
from temporalio.streams._topic import DEFAULT_TOPIC, StreamTopic, resolve_topic

if TYPE_CHECKING:
    from temporalio.client import Client
    from temporalio.streams._provider import (
        StreamHandle,
        StreamProducer,
        StreamProvider,
    )

__all__ = ["StreamOwnerKind", "StreamRef", "open_ref"]

StreamOwnerKind = Literal["workflow", "activity", "standalone"]
"""What owns a stream: a workflow, an activity, or the stream itself."""


@dataclass(frozen=True)
class StreamRef:
    """One stream, named by its owner and its topic.

    ``kind`` says what owns the stream. A ``"workflow"`` ref carries
    ``workflow_id`` and, when pinned to one run, ``run_id``. An ``"activity"``
    ref carries ``activity_id``, plus ``workflow_id`` (and its ``run_id``)
    when a workflow scheduled the activity; without ``workflow_id`` it is a
    standalone activity, and ``run_id`` then pins one run of it. A
    ``"standalone"`` ref carries ``stream_id`` and nothing else. ``topic`` is
    the topic's name, and a handle opened from a ref addresses it whenever a
    call names no topic.

    A ref comes from :meth:`temporalio.streams.StreamHandle.ref`, or from
    :meth:`for_workflow`, :meth:`for_activity` and :meth:`for_standalone`
    when only the ids are at hand. The default data converter carries it as
    JSON, so it can be a workflow argument, an activity result, or a Nexus
    operation input or result, and
    :meth:`temporalio.client.Client.get_stream_handle` and
    :func:`temporalio.activity.stream_handle` open one directly.
    """

    kind: StreamOwnerKind
    topic: str = DEFAULT_TOPIC
    workflow_id: str | None = None
    run_id: str | None = None
    activity_id: str | None = None
    stream_id: str | None = None

    def __post_init__(self) -> None:
        """Refuse a ref that names an owner its kind does not have."""
        if not self.topic:
            raise ValueError("a StreamRef needs a topic name")
        if self.kind == "workflow":
            if not self.workflow_id:
                raise ValueError("a workflow StreamRef needs a workflow_id")
            if self.activity_id is not None or self.stream_id is not None:
                raise ValueError(
                    "a workflow StreamRef carries no activity_id and no stream_id"
                )
        elif self.kind == "activity":
            if not self.activity_id:
                raise ValueError("an activity StreamRef needs an activity_id")
            if self.stream_id is not None:
                raise ValueError("an activity StreamRef carries no stream_id")
        elif self.kind == "standalone":
            if not self.stream_id:
                raise ValueError("a standalone StreamRef needs a stream_id")
            if (
                self.workflow_id is not None
                or self.run_id is not None
                or self.activity_id is not None
            ):
                raise ValueError("a standalone StreamRef carries only its stream_id")
        else:
            raise ValueError(
                f"unknown StreamRef kind {self.kind!r}; expected 'workflow', "
                "'activity' or 'standalone'"
            )

    @classmethod
    def for_workflow(
        cls,
        workflow_id: str,
        *,
        run_id: str | None = None,
        topic: str | StreamTopic[Any] | None = None,
    ) -> StreamRef:
        """A ref to ``topic`` of ``workflow_id``'s stream.

        Without ``run_id`` a handle opened from it follows the execution
        chain; with one it is pinned to that run. Without ``topic`` it names
        :data:`temporalio.streams.DEFAULT_TOPIC`.
        """
        name, _ = resolve_topic(topic)
        return cls("workflow", name, workflow_id=workflow_id, run_id=run_id)

    @classmethod
    def for_activity(
        cls,
        activity_id: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
        topic: str | StreamTopic[Any] | None = None,
    ) -> StreamRef:
        """A ref to ``topic`` of the streams ``activity_id`` owns.

        With ``workflow_id`` the activity is one that workflow scheduled and
        ``run_id`` is the workflow's run; without it the activity is a
        standalone one and ``run_id`` pins one run of it.
        """
        name, _ = resolve_topic(topic)
        return cls(
            "activity",
            name,
            workflow_id=workflow_id,
            run_id=run_id,
            activity_id=activity_id,
        )

    @classmethod
    def for_standalone(
        cls, stream_id: str, *, topic: str | StreamTopic[Any] | None = None
    ) -> StreamRef:
        """A ref to ``topic`` of the standalone stream ``stream_id``."""
        name, _ = resolve_topic(topic)
        return cls("standalone", name, stream_id=stream_id)

    def with_topic(self, topic: str | StreamTopic[Any] | None) -> StreamRef:
        """The same owner, naming ``topic`` instead.

        ``None`` names :data:`temporalio.streams.DEFAULT_TOPIC`, as a call
        that passes no topic does.
        """
        name, _ = resolve_topic(topic)
        return dataclasses.replace(self, topic=name)


def open_ref(provider: StreamProvider, client: Client, ref: StreamRef) -> StreamHandle:
    """The handle ``ref`` names, on ``provider``.

    The ref's kind picks the provider call that opens the owner, and its
    topic becomes the handle's default, so a call that names none addresses
    the stream the ref names. A provider that cannot host that owner kind
    raises :class:`temporalio.streams.StreamUnsupportedError` from the call
    that would have opened it.
    """
    if ref.kind == "workflow":
        assert ref.workflow_id is not None
        handle = provider.get_stream_handle(client, ref.workflow_id, run_id=ref.run_id)
    elif ref.kind == "activity":
        assert ref.activity_id is not None
        handle = provider.get_activity_stream_handle(
            client, ref.activity_id, workflow_id=ref.workflow_id, run_id=ref.run_id
        )
    else:
        assert ref.stream_id is not None
        handle = provider.get_standalone_stream_handle(client, ref.stream_id)
    return _RefHandle(handle, ref)


class _RefHandle:
    """A provider's handle whose default topic is the one a ref names.

    Every call passes through unchanged when it names a topic; one that
    names none gets the ref's. The wrapper exists so a ref can address a
    stream without every provider learning about refs.
    """

    def __init__(self, inner: StreamHandle, ref: StreamRef) -> None:
        self._inner = inner
        self._ref = ref

    def _topic(self, topic: str | StreamTopic[Any] | None) -> str | StreamTopic[Any]:
        return self._ref.topic if topic is None else topic

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        last: int | None = None,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        # The protocol's overloads each take one shape of topic and
        # result_type; a passthrough hands over whatever it was given.
        inner: Any = self._inner
        return inner.read(
            topic=self._topic(topic), after=after, last=last, result_type=result_type
        )

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        return await self._inner.latest(topic=self._topic(topic))

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str = "",
        attempt: int = 0,
    ) -> StreamProducer[Any]:
        inner: Any = self._inner
        return inner.producer(
            topic=self._topic(topic), producer_id=producer_id, attempt=attempt
        )

    def ref(self, *, topic: str | StreamTopic[Any] | None = None) -> StreamRef:
        if topic is None:
            return self._ref
        return self._inner.ref(topic=topic)

    async def close(self) -> None:
        await self._inner.close()
