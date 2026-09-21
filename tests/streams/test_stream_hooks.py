"""The lifecycle interceptor's rules about when the finish hook runs.

Driven directly, with a stand-in runtime on the loop, because the two cases
that matter here are the ones a workflow test cannot stage on purpose: a
coroutine collected after its worker went away, and a run evicted from the
cache. Both used to reach the finish hook, and at collection time the hook
acted on whichever workflow was running on the thread.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from temporalio import workflow
from temporalio.worker._interceptor import (
    ExecuteWorkflowInput,
    WorkflowInboundInterceptor,
)
from temporalio.worker._workflow import _StreamHooksInterceptor


class _Provider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_workflow_start(self) -> None:
        self.calls.append("start")

    async def on_workflow_finish(self) -> None:
        self.calls.append("finish")


class _Streams:
    def __init__(self, provider: _Provider) -> None:
        self.provider = provider


class _FakeRuntime:
    """Only what the interceptor reads: the stream state and the eviction flag."""

    def __init__(self, provider: _Provider, *, deleting: bool = False) -> None:
        self._streams = _Streams(provider)
        self._deleting = deleting

    def workflow_streams(self) -> _Streams:
        return self._streams


class _Body(WorkflowInboundInterceptor):
    """The workflow function's stand-in: returns, raises or parks forever."""

    def __init__(self, outcome: Any) -> None:  # type: ignore[reportMissingSuperCall]
        self._outcome = outcome

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        del input
        if self._outcome is _PARK:
            await asyncio.Event().wait()
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return self._outcome


_PARK = object()
_INPUT = ExecuteWorkflowInput(type=object, run_fn=lambda: None, args=(), headers={})


@pytest.fixture
async def provider() -> Any:
    fake = _Provider()
    loop = asyncio.get_running_loop()
    workflow._Runtime.set_on_loop(loop, _FakeRuntime(fake))  # type: ignore[arg-type]
    yield fake
    workflow._Runtime.set_on_loop(loop, None)


def _evicting(fake: _Provider) -> None:
    loop = asyncio.get_running_loop()
    workflow._Runtime.set_on_loop(loop, _FakeRuntime(fake, deleting=True))  # type: ignore[arg-type]


async def test_the_finish_hook_runs_on_return(provider: _Provider):
    assert (
        await _StreamHooksInterceptor(_Body("done")).execute_workflow(_INPUT) == "done"
    )
    assert provider.calls == ["start", "finish"]


async def test_the_finish_hook_runs_when_the_function_raises(provider: _Provider):
    with pytest.raises(RuntimeError, match="boom"):
        await _StreamHooksInterceptor(_Body(RuntimeError("boom"))).execute_workflow(
            _INPUT
        )
    assert provider.calls == ["start", "finish"]


async def test_the_finish_hook_runs_on_continue_as_new(provider: _Provider):
    error = workflow.ContinueAsNewError.__new__(workflow.ContinueAsNewError)
    with pytest.raises(workflow.ContinueAsNewError):
        await _StreamHooksInterceptor(_Body(error)).execute_workflow(_INPUT)
    assert provider.calls == ["start", "finish"]


async def test_the_finish_hook_runs_on_a_workflow_cancellation(provider: _Provider):
    # A cancelled primary task is the run ending, so the provider lets go.
    with pytest.raises(asyncio.CancelledError):
        await _StreamHooksInterceptor(_Body(asyncio.CancelledError())).execute_workflow(
            _INPUT
        )
    assert provider.calls == ["start", "finish"]


async def test_the_finish_hook_does_not_run_when_the_coroutine_is_collected(
    provider: _Provider,
):
    coroutine = _StreamHooksInterceptor(_Body(_PARK)).execute_workflow(_INPUT)
    # Run up to the park, the way a worker that shut down without evicting
    # leaves the primary task, then close it as garbage collection would.
    coroutine.send(None)
    coroutine.close()
    assert provider.calls == ["start"]


async def test_the_finish_hook_does_not_run_during_eviction(provider: _Provider):
    _evicting(provider)
    with pytest.raises(asyncio.CancelledError):
        await _StreamHooksInterceptor(_Body(asyncio.CancelledError())).execute_workflow(
            _INPUT
        )
    assert provider.calls == ["start"]
