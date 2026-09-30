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
from dataclasses import dataclass
from typing import Any, Literal

from temporalio.streams._topic import DEFAULT_TOPIC, StreamTopic, resolve_topic

__all__ = ["StreamOwnerKind", "StreamRef"]

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
