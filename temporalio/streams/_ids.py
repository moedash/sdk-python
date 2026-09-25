"""The store key a provider derives from a workflow id and a topic.

A workflow id may contain any character, ``:`` included, so joining the pair
with a bare ``:`` is ambiguous: ``("a:b", "c")`` and ``("a", "b:c")`` would
land in one store. Every provider that keys a store by the pair goes through
:func:`topic_key`, so they all agree and none of them collides.
"""

from __future__ import annotations

__all__ = ["topic_key"]


def _escape(component: str) -> str:
    # Percent first, so an escaped component cannot be mistaken for one that
    # already contained the escape.
    return component.replace("%", "%25").replace(":", "%3A")


def topic_key(workflow_id: str, topic: str) -> str:
    """The store key for ``topic`` of ``workflow_id``'s stream.

    Both components are percent-encoded before joining, so the only bare ``:``
    in the result is the separator.
    """
    return f"{_escape(workflow_id)}:{_escape(topic)}"
