"""What the Redis provider decides without a store: cursors, the sync publish, the wake."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from temporalio.client import WorkflowExecutionStatus
from temporalio.contrib.external_workflow_streams import (
    StreamDirection,
    WakeNotAcknowledgedError,
)
from temporalio.contrib.external_workflow_streams import (
    StreamError as TransportStreamError,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import (
    BEGINNING,
    Cursor,
    StreamCursorError,
    StreamError,
)
from temporalio.streams.providers import redis as redis_provider
from temporalio.streams.providers.redis import (
    DEFAULT_RETENTION,
    RedisProducer,
    RedisStreams,
    _ActivityOwner,
    _drive,
    _position,
    _StandaloneOwner,
)


class _ChainClient:
    """A client whose describes of the chain answer from a script of statuses.

    The last entry repeats, so a chain that stays running keeps saying so.
    """

    def __init__(self, *answers: WorkflowExecutionStatus | Exception) -> None:
        self.data_converter = DataConverter.default
        self.namespace = "default"
        self._answers = list(answers)
        self.describes = 0

    def get_workflow_handle(self, _workflow_id: str, **_: Any) -> Any:
        return self

    async def describe(self) -> Any:
        self.describes += 1
        answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        if isinstance(answer, Exception):
            raise answer
        return SimpleNamespace(status=answer)


class _RefusingInput:
    """The transport's input topic, refusing the first ``refusals`` wakes."""

    def __init__(self, refusals: int, error: Exception | None = None) -> None:
        self._refusals = refusals
        self._error = error
        self.calls = 0

    async def wake(self) -> list[str]:
        self.calls += 1
        if self.calls <= self._refusals:
            raise self._error or WakeNotAcknowledgedError(
                "workflow operation can not be applied because workflow is closing",
                pending=[],
            )
        return ["sent"]


def _waking_producer(client: _ChainClient, wakes: _RefusingInput) -> RedisProducer[Any]:
    producer: RedisProducer[Any] = RedisProducer(
        RedisStreams(), cast(Any, client), "wf", "inputs", "console", 1
    )
    producer._input = wakes
    return producer


@pytest.fixture
def quick_wake_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        redis_provider, "_WAKE_RETRY_BACKOFF", timedelta(milliseconds=10)
    )
    monkeypatch.setattr(
        redis_provider, "_WAKE_RETRY_WINDOW", timedelta(milliseconds=300)
    )


@pytest.mark.usefixtures("quick_wake_retries")
async def test_a_wake_refused_by_a_closing_run_is_sent_again_to_its_successor():
    # The chain describes as running: the run that refused the wake handed over
    # to a successor, which is where the records already are.
    client = _ChainClient(WorkflowExecutionStatus.RUNNING)
    wakes = _RefusingInput(refusals=1)
    await _waking_producer(client, wakes)._wake()
    assert (wakes.calls, client.describes) == (2, 1)


@pytest.mark.usefixtures("quick_wake_retries")
async def test_a_refused_wake_waits_for_the_successor_to_become_current():
    # In the instant between two runs the chain still describes as the
    # predecessor that continued as new, and the next Signal is refused too.
    client = _ChainClient(
        WorkflowExecutionStatus.CONTINUED_AS_NEW, WorkflowExecutionStatus.RUNNING
    )
    wakes = _RefusingInput(refusals=2)
    await _waking_producer(client, wakes)._wake()
    assert (wakes.calls, client.describes) == (3, 2)


@pytest.mark.usefixtures("quick_wake_retries")
@pytest.mark.parametrize(
    "ending",
    [
        WorkflowExecutionStatus.COMPLETED,
        WorkflowExecutionStatus.FAILED,
        WorkflowExecutionStatus.CANCELED,
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
        RPCError("gone", RPCStatusCode.NOT_FOUND, b""),
    ],
    ids=lambda ending: getattr(ending, "name", "not found"),
)
async def test_a_wake_refused_by_a_finished_chain_is_the_ordinary_ending(
    ending: WorkflowExecutionStatus | Exception,
):
    client = _ChainClient(ending)
    wakes = _RefusingInput(refusals=99)
    await _waking_producer(client, wakes)._wake()
    assert wakes.calls == 1


@pytest.mark.usefixtures("quick_wake_retries")
async def test_a_wake_refused_as_closing_for_the_whole_window_is_dropped():
    # The run is inside a Workflow Task that tried to close it while the wake
    # sat buffered. The record is in the log, so if the run stays open its
    # next park rechecks the log and finds it; the producer is not failed.
    client = _ChainClient(WorkflowExecutionStatus.RUNNING)
    wakes = _RefusingInput(refusals=99)
    await _waking_producer(client, wakes)._wake()
    assert wakes.calls > 1


@pytest.mark.usefixtures("quick_wake_retries")
async def test_a_wake_refused_for_another_reason_for_the_whole_window_is_raised():
    client = _ChainClient(WorkflowExecutionStatus.RUNNING)
    wakes = _RefusingInput(
        refusals=99, error=WakeNotAcknowledgedError("signal rate limited", pending=[])
    )
    with pytest.raises(WakeNotAcknowledgedError, match="rate limited"):
        await _waking_producer(client, wakes)._wake()
    assert wakes.calls > 1


async def test_a_wake_the_store_could_not_send_is_a_storage_error():
    client = _ChainClient(WorkflowExecutionStatus.RUNNING)
    wakes = _RefusingInput(refusals=1, error=TransportStreamError("no connection"))
    with pytest.raises(StreamError, match="could not be sent"):
        await _waking_producer(client, wakes)._wake()
    assert (wakes.calls, client.describes) == (1, 0)


class _NoRedis:
    """Enough of a Redis client to construct a backend and render its keys."""

    def register_script(self, _script: str) -> None:
        return None


def test_activity_keys_encode_their_ids_and_never_meet_chain_keys():
    # The run is part of the key: a workflow's activity is keyed by the
    # workflow's run and a standalone one by its own, so an id started again
    # in a new run starts a new stream.
    assert (
        _ActivityOwner("ns", "wf", "act", "run").key("p", "t")
        == "p:ns:activity/wf/run/act:t"
    )
    assert (
        _ActivityOwner("ns", None, "act", "run").key("p", "t")
        == "p:ns:activity//run/act:t"
    )
    # An id holding a separator is encoded, so it cannot move a boundary.
    assert (
        _ActivityOwner("n:s", "w/f", "a:c", "r/1").key("p", "t/u")
        == "p:n%3As:activity/w%2Ff/r%2F1/a%3Ac:t%2Fu"
    )
    # A key needs the run; a handle opened without one resolves it first.
    with pytest.raises(RuntimeError, match="not resolved"):
        _ActivityOwner("ns", "wf", "act", None).key("p", "t")
    # A chain key percent-encodes every id, so none of its components holds a
    # "/" however the ids are chosen, and the owner component here always does.
    backend = RedisStreamBackend(client=_NoRedis(), key_prefix="p")
    forged = StreamKey(
        namespace="ns",
        workflow_id="activity/wf/act",
        first_execution_run_id="t",
        stream_name="t",
        direction=StreamDirection.OUTPUT,
    )
    assert "/" not in backend.stream_key(forged)
    assert backend.stream_key(forged) != _ActivityOwner("ns", "wf", "act", "t").key(
        "p", "t"
    )


def test_an_activity_owner_names_itself_for_messages():
    assert str(_ActivityOwner("ns", None, "act", None)) == "activity 'act'"
    assert (
        str(_ActivityOwner("ns", "wf", "act", "run"))
        == "activity 'act' of workflow 'wf'"
    )


def test_cursors_name_entries_of_the_topics_one_log():
    assert _position(BEGINNING) is None
    position = _position(Cursor("redis:1700000000000-3"))
    assert position is not None and position.token == "1700000000000-3"
    # A workflow reader and an outside reader name the same log, so either
    # side's cursor seeds the other. The form the two-key layout minted for
    # workflow readers named an entry of the input key, which is the log now.
    legacy = _position(Cursor("redis:in:1700000000000-3"))
    assert legacy is not None and legacy.token == "1700000000000-3"
    with pytest.raises(StreamCursorError):
        _position(Cursor("memory:3"))
    with pytest.raises(StreamCursorError):
        _position(Cursor("redis:not-an-id"))
    with pytest.raises(StreamCursorError):
        _position(Cursor("redis:in:not-an-id"))


def test_a_publish_that_completes_at_once_is_driven_to_the_end():
    done = []

    async def publish() -> None:
        done.append(True)

    _drive(publish())
    assert done == [True]


def test_a_publish_that_would_wait_fails_loudly():
    async def publish() -> None:
        await asyncio.get_running_loop().create_future()

    async def run() -> None:
        with pytest.raises(StreamError, match="batch is full"):
            _drive(publish())

    asyncio.run(run())


def test_retention_options_are_checked_at_construction():
    with pytest.raises(ValueError, match="retention"):
        RedisStreams(retention=timedelta(0))
    with pytest.raises(ValueError, match="max_len"):
        RedisStreams(max_len=0)
    RedisStreams(retention=timedelta(hours=1), max_len=10)


async def test_a_client_the_caller_opened_gets_the_providers_layout_and_stays_open():
    # The layout and the trims are the provider's whichever connection it
    # runs on, and closing the provider does not close a caller's client.
    client = _NoRedis()
    provider = RedisStreams(client=client, max_len=10)
    backend = provider._require_backend()
    assert backend._client is client
    assert backend.describe_window() == f"retention={DEFAULT_RETENTION}, max_len=10"
    await provider.close()
    assert provider._require_backend() is not backend
    assert provider._require_backend()._client is client


async def test_the_default_window_is_an_age_and_can_be_turned_off():
    # Nothing is trimmed on a topic nobody appends to, so the default has to
    # be a window that every append applies; a count cap would refuse a task
    # whose batch does not fit under it, so that one stays off.
    provider = RedisStreams()
    try:
        backend = provider._require_backend()
        assert backend._retention == DEFAULT_RETENTION == timedelta(days=7)
        assert backend._max_len is None
        assert backend.describe_window() == f"retention={DEFAULT_RETENTION}"
    finally:
        await provider.close()
    unbounded = RedisStreams(retention=None)
    try:
        assert unbounded._require_backend().describe_window() == "no retention"
    finally:
        await unbounded.close()


def test_standalone_keys_have_their_own_owner_component():
    owner = _StandaloneOwner("ns", "shared")
    assert owner.meta("p") == "p:ns:standalone/shared"
    assert owner.key("p", "t") == "p:ns:standalone/shared:t"
    # A topic log always has one more component than the hash, and every id and
    # topic is encoded, so no name can land on the hash or on another topic.
    assert owner.key("p", "meta") != owner.meta("p")
    tricky = _StandaloneOwner("n:s", "a/b:c")
    assert tricky.meta("p") == "p:n%3As:standalone/a%2Fb%3Ac"
    assert tricky.key("p", "t/u") == "p:n%3As:standalone/a%2Fb%3Ac:t%2Fu"
    assert str(owner) == "standalone stream 'shared'"
