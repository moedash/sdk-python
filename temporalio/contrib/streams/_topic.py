"""Typed topic definitions.

A topic is defined once, at module level, with the type its records decode
to, and the Workflow, its Activities and the client code all refer to that
one definition, the way they share a Signal or an Activity. A plain string
names a topic too, for a name decided at runtime; then the decode hint
travels as ``result_type=`` on each read.

On the wire a topic is a string, and
:attr:`temporalio.contrib.streams.StreamRecord.topic` is that string.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, overload

__all__ = ["DEFAULT_TOPIC", "StreamTopic", "resolve_topic", "topic"]

T = TypeVar("T")

DEFAULT_TOPIC = "output"
"""The topic a call addresses when it names none.

It is an ordinary name, so naming it explicitly addresses the same topic.
"""


@dataclass(frozen=True)
class StreamTopic(Generic[T]):
    """A topic of a stream, with the type its records decode to.

    Made with :func:`topic`. Pass it wherever a call takes a topic, and the
    record and value types follow from it.
    """

    name: str
    result_type: type[T] | None = None
    """The type records decode to, or ``None`` for the converter's default."""


@overload
def topic(name: str, result_type: type[T]) -> StreamTopic[T]: ...


@overload
def topic(name: str, result_type: None = None) -> StreamTopic[Any]: ...


def topic(name: str, result_type: type | None = None) -> StreamTopic[Any]:
    """Define a topic.

    Args:
        name: The topic's name, as it appears on every record.
        result_type: The type records on this topic decode to. Without one,
            the payload converter's default applies.

    Raises:
        ValueError: ``name`` is empty, longer than 256 UTF-8 bytes, or holds
            a control character.
    """
    _check_name(name)
    return StreamTopic(name, result_type)


def resolve_topic(
    topic: str | StreamTopic[Any] | None = None, result_type: type | None = None
) -> tuple[str, type | None]:
    """The name and decode hint a call means, from either form of topic.

    ``None`` means :data:`DEFAULT_TOPIC`. A handle calls this once at the
    top of each call that takes a topic, so a definition and a string reach
    the store the same way.

    Raises:
        ValueError: A definition was given together with ``result_type``,
            which would name two types for one topic, or the name is empty,
            longer than 256 UTF-8 bytes, or holds a control character.
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
    _check_name(name)
    return name, result_type


_MAX_NAME_BYTES = 256


def _check_name(name: str) -> None:
    # The same limit in every SDK: the name goes into store keys, cursor
    # hashes and Core's manifest budget, and a store may join names with a
    # control character.
    if not name:
        raise ValueError("topic name must not be empty")
    if len(name.encode()) > _MAX_NAME_BYTES:
        raise ValueError(f"topic name must be at most {_MAX_NAME_BYTES} UTF-8 bytes")
    if any(unicodedata.category(c) == "Cc" for c in name):
        raise ValueError(f"topic name {name!r} must not hold a control character")
