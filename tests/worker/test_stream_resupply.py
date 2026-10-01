"""A task whose re-supplied ranges stop short is made whole before it runs.

The server re-supplies a replaying workflow's recorded ranges within a budget.
These pin the worker's half: a ``DeliverStreamRecords`` job with fewer records
than its range spans has the rest fetched from the stream service, resolved as
the server resolves a subscribed name, while a whole job and an empty
observation are left alone and a range the stream no longer holds fails the
task rather than let it run on less input.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from temporalio import client_stream
from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import StreamRecord
from temporalio.bridge.proto.workflow_activation import WorkflowActivation
from temporalio.client_stream import Page, StreamEntry
from temporalio.streams import StreamNotFoundError
from temporalio.worker._stream_ranges import fill_short_stream_ranges

# The fill-in only hands the client to the shared-channel lookup, which the
# tests replace.
_CLIENT: Any = SimpleNamespace()


def _record(offset: int) -> StreamRecord:
    return StreamRecord(topic="inputs", body=Payload(data=f"r{offset}".encode()))


class _FakeStream:
    """One stream's records by offset, answering polls as the service does."""

    def __init__(self, held: dict[int, StreamRecord]) -> None:
        self.held = held
        self.polls: list[tuple[int, int]] = []

    async def poll(self, *, from_offset: int, max_records: int, wait: bool) -> Page:
        assert not wait
        self.polls.append((from_offset, max_records))
        if not self.held:
            raise StreamNotFoundError("no such stream")
        entries = [
            StreamEntry(record=self.held[o], offset=o)
            for o in range(from_offset, from_offset + max_records)
            if o in self.held
        ]
        head = max(self.held) + 1
        return Page(
            entries=entries,
            next_offset=from_offset + len(entries),
            head_offset=head,
            closed=False,
        )


class _FakeStreams:
    """The stream client: an owned stream per (workflow, name) and standalone ones by id."""

    def __init__(
        self, owned: dict[str, _FakeStream], standalone: dict[str, _FakeStream]
    ) -> None:
        self.owned = owned
        self.standalone = standalone

    def workflow_stream(
        self, workflow_id: str, name: str, *, owner_run_id: str
    ) -> _FakeStream:
        assert (workflow_id, owner_run_id) == ("wf", "run")
        return self.owned.get(name, _FakeStream({}))

    def get(self, stream_id: str) -> _FakeStream:
        return self.standalone.get(stream_id, _FakeStream({}))


def _activation(*jobs: tuple[str, int, int, int]) -> WorkflowActivation:
    """An activation with one delivery per ``(stream, from, to, records present)``."""
    act = WorkflowActivation(run_id="run")
    for stream_id, from_offset, to_offset, present in jobs:
        job = act.jobs.add()
        job.deliver_stream_records.stream_id = stream_id
        job.deliver_stream_records.from_offset = from_offset
        job.deliver_stream_records.to_offset = to_offset
        job.deliver_stream_records.records.extend(
            _record(o) for o in range(from_offset, from_offset + present)
        )
    return act


@pytest.fixture
def streams(monkeypatch: pytest.MonkeyPatch) -> _FakeStreams:
    fake = _FakeStreams(
        owned={"inputs": _FakeStream({o: _record(o) for o in range(10)})},
        standalone={"shared": _FakeStream({o: _record(o) for o in range(5)})},
    )
    monkeypatch.setattr(client_stream, "shared_client", lambda _client: fake)
    return fake


def _bodies(act: WorkflowActivation, index: int = 0) -> list[bytes]:
    return [r.body.data for r in act.jobs[index].deliver_stream_records.records]


async def test_a_short_job_is_filled_from_the_owned_stream(
    streams: _FakeStreams,
) -> None:
    act = _activation(("inputs", 2, 7, 2))
    await fill_short_stream_ranges(act, "wf", _CLIENT)
    assert _bodies(act) == [b"r2", b"r3", b"r4", b"r5", b"r6"]
    # Only the missing tail was asked for.
    assert streams.owned["inputs"].polls == [(4, 3)]


@pytest.mark.usefixtures("streams")
async def test_a_name_the_workflow_does_not_own_is_a_standalone_stream() -> None:
    act = _activation(("shared", 0, 5, 1))
    await fill_short_stream_ranges(act, "wf", _CLIENT)
    assert _bodies(act) == [b"r0", b"r1", b"r2", b"r3", b"r4"]


async def test_whole_and_empty_jobs_are_left_alone(streams: _FakeStreams) -> None:
    act = _activation(("inputs", 0, 3, 3), ("inputs", 3, 3, 0))
    await fill_short_stream_ranges(act, "wf", _CLIENT)
    assert _bodies(act, 0) == [b"r0", b"r1", b"r2"]
    assert _bodies(act, 1) == []
    assert streams.owned["inputs"].polls == []


@pytest.mark.usefixtures("streams")
async def test_a_range_the_stream_no_longer_holds_fails_loudly() -> None:
    act = _activation(("inputs", 8, 12, 1))
    with pytest.raises(
        StreamNotFoundError, match="no longer holds offsets \\[9, 12\\)"
    ):
        await fill_short_stream_ranges(act, "wf", _CLIENT)


async def test_no_client_is_needed_when_nothing_is_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(_client: Any) -> Any:
        raise AssertionError("no channel should be opened")

    monkeypatch.setattr(client_stream, "shared_client", refuse)
    act = _activation(("inputs", 0, 2, 2))
    await fill_short_stream_ranges(act, "wf", _CLIENT)
