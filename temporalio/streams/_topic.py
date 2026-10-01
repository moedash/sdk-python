"""Typed topic definitions.

Temporal's idiom is to define once and refer by reference: signals, queries
and updates are decorated methods, activities and workflows are functions,
Nexus operations are typed definitions. A topic follows the same rule. It is
defined once, at module level, with the type its records decode to, and the
workflow, its activities and the backend all refer to that one definition. A
plain string names a topic too, the way a string names a signal chosen at
runtime; then the decode hint travels as ``result_type=`` on each call.

The wire does not change: a topic is a string on the proto and in every
store, and :attr:`temporalio.streams.StreamRecord.topic` is that string.

Every workflow also has a default topic, :data:`DEFAULT_TOPIC`, which a call
addresses by naming no topic at all. It is an ordinary name, so naming it
explicitly is the same topic, not an error.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, TypeVar, overload

__all__ = ["DEFAULT_TOPIC", "StreamTopic", "resolve_topic", "topic"]

T = TypeVar("T")

DEFAULT_TOPIC = "output"
"""The topic a call addresses when it names none.

The server resolves an unnamed stream of a workflow to this same name, so on
the native provider the default topic is the server's default stream, and a
store with no default of its own holds it under this name. It stays an
ordinary name rather than a reserved one for the same reason the server does
not reserve it: refusing it would make one stream reachable under two rules.
"""


@dataclass(frozen=True)
class StreamTopic(Generic[T]):
    """A topic of a workflow's stream, with the type its records decode to.

    Made with :func:`topic`. Hand it to ``temporalio.workflow.stream_reader``,
    ``temporalio.workflow.stream_writer``, and to a handle's ``read``,
    ``latest`` and ``producer``, and the record and value types follow from
    it; ``result_type=`` is not passed alongside a definition.
    """

    name: str
    result_type: type[T] | None = None
    """The type records decode to, or ``None`` for the converter's default."""


@overload
def topic(name: str, result_type: type[T]) -> StreamTopic[T]: ...


@overload
def topic(name: str, result_type: None = None) -> StreamTopic[Any]: ...


def topic(name: str, result_type: type | None = None) -> StreamTopic[Any]:
    """Define a topic of a workflow's stream.

    Define it once, at module level, and share it: the workflow reads or
    publishes it, an activity or a backend produces onto it or reads it, and
    the type it carries is inferred wherever it is used. Use a plain string
    instead only when the name is decided at runtime.

    Args:
        name: The topic's name, as it appears on every record.
        result_type: The type records on this topic decode to. Without one,
            the payload converter's default applies.

    Raises:
        ValueError: ``name`` is empty.
    """
    if not name:
        raise ValueError("topic name must not be empty")
    return StreamTopic(name, result_type)


def resolve_topic(
    topic: str | StreamTopic[Any] | None = None, result_type: type | None = None
) -> tuple[str, type | None]:
    """The name and decode hint a call means, from either form of topic.

    Providers call this once at the top of ``read``, ``latest`` and
    ``producer``, so a definition and a string are the same to the store.
    ``None`` is a call that named no topic, and means :data:`DEFAULT_TOPIC`.

    Raises:
        ValueError: A definition was given together with ``result_type``,
            which would name two types for one topic, or the name is empty.
    """
    if isinstance(topic, StreamTopic):
        if result_type is not None:
            raise ValueError(
                f"topic {topic.name!r} already carries its type; do not pass "
                "result_type= with a definition"
            )
        name, result_type = topic.name, topic.result_type
    elif topic is None:
        name = DEFAULT_TOPIC
    else:
        name = topic
    if not name:
        raise ValueError("topic must not be empty")
    return name, result_type
