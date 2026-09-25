"""What the reference provider does that the conformance suite cannot see.

The conformance suite goes through the public surface, so it can say that a
divergent retry is refused but not what the store looks like afterwards. That
is this file: a few assertions against ``MemoryStreams`` internals.
"""

from __future__ import annotations

import pytest

from temporalio.streams import StreamProducerError, topic
from temporalio.streams.providers.memory import MemoryStreams

OUT = topic("out", dict)


async def test_a_divergent_retry_leaves_the_store_alone():
    provider = MemoryStreams()
    stream = provider.get_stream_handle(None, "wf-divergent")  # type: ignore[arg-type]
    store = provider._topic("wf-divergent", OUT.name)
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1})
    assert len(store.records) == 1

    retry = stream.producer(topic=OUT, producer_id="model", attempt=1)
    with pytest.raises(
        StreamProducerError, match="already used with different content"
    ):
        await retry.append({"n": 2})
    assert len(store.records) == 1
