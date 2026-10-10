"""Memory provider details the conformance suite can't reach through the public surface."""

from __future__ import annotations

import pytest

from temporalio.contrib.streams import StreamProducerError, StreamRef
from temporalio.contrib.streams.memory import MemoryStreams


async def test_a_batch_that_starts_inside_the_newest_batch_is_refused():
    stream = MemoryStreams().get_stream_handle(None, StreamRef.for_workflow("w"))
    first = stream.producer(topic="out", producer_id="p", attempt=1)
    await first.append(1, 2, 3)
    # A second writer of the same session, whose sequence sits inside the
    # batch the store holds last.
    second = stream.producer(topic="out", producer_id="p", attempt=1, next_sequence=2)
    with pytest.raises(StreamProducerError):
        await second.append(9)
