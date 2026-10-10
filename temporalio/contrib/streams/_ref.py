"""A reference to one stream that crosses a process boundary as data.

A handle is bound to a client and a provider, so it cannot be a Workflow
argument or an Activity result. A :class:`StreamRef` can: it names the owner
and the topic, nothing more, and whoever receives it opens the stream on the
provider its own client carries. It carries no cursor, because a position
belongs to a reader, and no provider name.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any, Literal

from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._topic import (
    DEFAULT_TOPIC,
    StreamTopic,
    resolve_topic,
)

__all__ = ["StreamOwnerKind", "StreamRef"]

StreamOwnerKind = Literal["workflow"]
"""The owner kinds this release opens. Only a Workflow owns a stream.

:attr:`StreamRef.kind` is a plain string, so a ref that names an owner kind
a later release adds still decodes, and is refused where a handle opens.
"""


@dataclass(frozen=True)
class StreamRef:
    """One stream, named by its owner and its topic.

    ``kind`` says what owns the stream; only ``"workflow"`` exists in this
    release. A Workflow ref carries ``workflow_id`` and, when pinned to one
    run, ``run_id``. The stream belongs to the run chain either way, and a
    read returns the chain's records. ``run_id`` changes two things: which
    chain the handle looks up, and that a read ends when that run closes,
    Continue-as-New included. Without it a reader keeps reading across
    Continue-as-New. ``topic`` is the topic a handle opened from the ref
    addresses when a call names none.

    The default data converter carries it as JSON, so it can be a Workflow
    argument or an Activity result.
    """

    kind: str
    workflow_id: str
    run_id: str | None = None
    topic: str = DEFAULT_TOPIC

    def __post_init__(self) -> None:
        """Refuse a ref that names no stream.

        Any ``kind`` decodes, since a ref of a later release can arrive as
        Workflow input, where raising would fail the Workflow Task on every
        retry. :meth:`_require_supported` refuses it where a handle opens.

        Raises:
            ValueError: ``kind`` or ``topic`` is empty, or a Workflow ref has
                no ``workflow_id``.
        """
        if not self.kind:
            raise ValueError("a StreamRef needs a kind")
        if self.kind == "workflow" and not self.workflow_id:
            raise ValueError("a Workflow StreamRef needs a workflow_id")
        if not self.topic:
            raise ValueError("a StreamRef needs a topic name")

    def _require_supported(self) -> None:
        """Refuse to open a stream whose owner kind this release lacks.

        Raises:
            StreamUnsupportedError: ``kind`` is not ``"workflow"``.
        """
        if self.kind != "workflow":
            raise StreamUnsupportedError(
                f"streams owned by kind {self.kind!r} are not supported in this "
                "release; only Workflow-owned streams are"
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

        Without ``topic`` it names
        :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        """
        name, _ = resolve_topic(topic)
        return cls("workflow", workflow_id, run_id=run_id, topic=name)

    def with_topic(self, topic: str | StreamTopic[Any] | None) -> StreamRef:
        """The same owner, naming ``topic`` instead.

        ``None`` names :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        """
        name, _ = resolve_topic(topic)
        return dataclasses.replace(self, topic=name)
