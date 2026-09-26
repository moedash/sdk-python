"""What the buffered client does with a batch, without a server.

The parity claim in the module docstring is that an application swaps the
import and keeps its code, so the two things the shipped client does on every
flush have to hold here too: the append carries a producer identity, and a
batch whose append failed is kept for the retry rather than lost with it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from temporalio.api.stream.v1 import StreamRecord
from temporalio.contrib.server_streams import WorkflowStreamClient
from temporalio.converter import DataConverter


class _Handle:
    """A stream handle that records its appends and can fail the first one."""

    owner_run_id = "run"

    def __init__(self, *, fail_first: bool = False) -> None:
        self.sent: list[tuple[list[StreamRecord], str, int]] = []
        self._fail_first = fail_first

    def pin(self, run_id: str) -> None:
        del run_id

    async def append(
        self, *records: StreamRecord, producer_id: str = "", sequence: int = 0
    ) -> Any:
        self.sent.append((list(records), producer_id, sequence))
        if self._fail_first:
            self._fail_first = False
            raise ConnectionResetError("the server took it, the reply was lost")
        return None


def _client(handle: _Handle) -> WorkflowStreamClient:
    return WorkflowStreamClient(
        handle,  # type: ignore[arg-type]
        DataConverter.default.payload_converter,
        timedelta(seconds=60),
        producer_id="act#1",
    )


def _bodies(sent: tuple[list[StreamRecord], str, int]) -> list[bytes]:
    return [record.body.data for record in sent[0]]


async def test_an_append_carries_who_wrote_it_and_where_it_sits() -> None:
    handle = _Handle()
    client = _client(handle)
    client.topic("t").publish({"n": 1})
    client.topic("t").publish({"n": 2})
    await client.flush()
    client.topic("t").publish({"n": 3})
    await client.flush()

    # Without an identity the append is at-least-once, which is what the
    # shipped client refuses to be.
    assert [(who, seq) for _, who, seq in handle.sent] == [("act#1", 0), ("act#1", 2)]


async def test_a_batch_whose_append_failed_goes_out_again() -> None:
    handle = _Handle(fail_first=True)
    client = _client(handle)
    client.topic("t").publish({"n": 1})
    with pytest.raises(ConnectionResetError):
        await client.flush()
    # Taken off the buffer before the await, it would have gone with the
    # failure. It goes out again under the sequence it already had, so a copy
    # the server did accept is deduplicated and one it never saw lands.
    await client.flush()
    assert len(handle.sent) == 2
    assert _bodies(handle.sent[0]) == _bodies(handle.sent[1])
    assert [seq for _, _, seq in handle.sent] == [0, 0]

    client.topic("t").publish({"n": 2})
    await client.flush()
    assert handle.sent[2][2] == 1, "the next batch continues past the first"
