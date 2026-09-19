"""The store key a provider derives from a workflow id and a stream name.

A workflow id may contain any character, ``:`` included, so joining the pair
with a bare ``:`` is ambiguous: ``("a:b", "c")`` and ``("a", "b:c")`` would
land in one store. Every provider that keys a store by the pair goes through
:func:`inbound_stream_id`, so they all agree and none of them collides.
"""

from __future__ import annotations

__all__ = ["inbound_stream_id"]


def _escape(component: str) -> str:
    # Percent first, so an escaped component cannot be mistaken for one that
    # already contained the escape.
    return component.replace("%", "%25").replace(":", "%3A")


def inbound_stream_id(workflow_id: str, stream: str) -> str:
    """The store id for inbound ``stream`` of ``workflow_id``.

    With an empty ``stream`` it is the id of the stream the workflow itself
    publishes, so one function keys both.

    Both components are percent-encoded before joining, so the only bare ``:``
    in the result is the separator, and an owner-stream key has none.
    """
    if not stream:
        return _escape(workflow_id)
    return f"{_escape(workflow_id)}:{_escape(stream)}"
