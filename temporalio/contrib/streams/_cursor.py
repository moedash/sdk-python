"""The cursors a read starts from without having read anything.

Core mints and checks every other cursor, so its token format is Core's own.
"""

from __future__ import annotations

from temporalio.contrib.streams._record import Cursor

__all__ = ["BEGINNING", "END"]

BEGINNING = Cursor("")
"""Read from the oldest record the stream still retains."""

END = Cursor("$end")
"""Read only what is appended after the read starts.

It is resolved when the read starts. To position a reader before the
reader's process writes something, use the handle's ``latest`` instead.
"""
