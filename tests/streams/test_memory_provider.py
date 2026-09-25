"""What the reference provider does that the conformance suite cannot see.

The conformance suite is the public surface, so it can say that closing a read
returns and that the topic still works afterwards, but not that the provider
let go of what the read parked on. That is this file: a few assertions against
``MemoryStreams`` internals, where holding on would leak quietly.
"""

from __future__ import annotations

import asyncio

import pytest

from temporalio.streams import StreamProducerError, topic
from temporalio.streams.providers.memory import MemoryStreams

OUT = topic("out", dict)


async def test_closing_a_parked_read_drops_its_waiter():
    provider = MemoryStreams()
    stream = provider.get_stream_handle(None, "wf-parked")  # type: ignore[arg-type]
    producer = stream.producer(topic=OUT, producer_id="model", attempt=1)
    await producer.append({"n": 1})
    store = provider._topic("wf-parked", OUT.name)

    records = stream.read(topic=OUT)
    await asyncio.wait_for(records.__anext__(), 5.0)

    pending = asyncio.ensure_future(records.__anext__())
    await asyncio.sleep(0.2)
    assert store._waiters, "the read should be parked on the topic by now"

    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    # Nothing left behind: a reader that comes and goes must not grow this
    # list for the life of the topic.
    assert store._waiters == []
    await asyncio.wait_for(records.aclose(), 5.0)


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
