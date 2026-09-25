"""Replayer."""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from typing_extensions import TypedDict

import temporalio.api.enums.v1
import temporalio.api.history.v1
import temporalio.api.stream.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.worker
import temporalio.client
import temporalio.converter
import temporalio.runtime
import temporalio.service
import temporalio.streams
import temporalio.worker
import temporalio.workflow

from ..common import HeaderCodecBehavior
from ._interceptor import Interceptor
from ._worker import load_default_build_id
from ._workflow import _WorkflowWorker
from ._workflow_instance import (
    _DEFAULT_ENABLED_WORKFLOW_LOGIC_FLAGS,
    UnsandboxedWorkflowRunner,
    WorkflowRunner,
    _WorkflowLogicFlag,
)
from .workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)


class Replayer:
    """Replayer to replay workflows from history."""

    def __init__(
        self,
        *,
        workflows: Sequence[type],
        workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None = None,
        workflow_runner: WorkflowRunner = SandboxedWorkflowRunner(),
        unsandboxed_workflow_runner: WorkflowRunner = UnsandboxedWorkflowRunner(),
        namespace: str = "ReplayNamespace",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        plugins: Sequence[temporalio.worker.Plugin] = [],
        build_id: str | None = None,
        identity: str | None = None,
        workflow_failure_exception_types: Sequence[type[BaseException]] = [],
        debug_mode: bool = False,
        runtime: temporalio.runtime.Runtime | None = None,
        disable_safe_workflow_eviction: bool = False,
        header_codec_behavior: HeaderCodecBehavior = HeaderCodecBehavior.NO_CODEC,
        stream_provider: temporalio.streams.StreamProvider | None = None,
        stream_client: temporalio.client.Client | None = None,
    ) -> None:
        """Create a replayer to replay workflows from history.

        See :py:meth:`temporalio.worker.Worker.__init__` for a description of
        most of the arguments. Most of the same arguments need to be passed to
        the replayer that were passed to the worker when the workflow originally
        ran, ``stream_provider`` included when the workflow used streams.

        A workflow that read a server-side stream cannot be replayed from its
        history alone. History records the offsets each Workflow Task consumed
        and never the records; on a live task the server re-supplies them from
        the stream. Three cases:

        * The history carries the records in
          :py:attr:`temporalio.client.WorkflowHistory.stream_slices`, put
          there by :py:meth:`fetch_stream_slices` while the stream was
          retained and carried by ``to_json()`` and ``from_json()``. They are
          handed to the replay and no server is contacted.
        * The history carries none and ``stream_client`` is given: a client
          connected to the server that still holds the streams. The replayer
          fetches every range the completed tasks recorded from the stream
          service and hands the records to the replay, so the workflow sees
          what it saw the first time. A range the stream no longer holds fails
          that replay with :py:class:`temporalio.streams.StreamNotFoundError`.
        * The history carries none and there is no client: replaying it fails
          and the message names both remedies.

        Note, unlike the worker, for the replayer the workflow_task_executor
        will default to a new thread pool executor with no max_workers set that
        will be shared across all replay calls and never explicitly shut down.
        Users are encouraged to provide their own if needing more control.
        """
        self._config = ReplayerConfig(
            workflows=list(workflows),
            workflow_task_executor=(
                workflow_task_executor or concurrent.futures.ThreadPoolExecutor()
            ),
            workflow_runner=workflow_runner,
            unsandboxed_workflow_runner=unsandboxed_workflow_runner,
            namespace=namespace,
            data_converter=data_converter,
            interceptors=interceptors,
            build_id=build_id,
            identity=identity,
            workflow_failure_exception_types=workflow_failure_exception_types,
            debug_mode=debug_mode,
            runtime=runtime,
            disable_safe_workflow_eviction=disable_safe_workflow_eviction,
            header_codec_behavior=header_codec_behavior,
            stream_provider=stream_provider,
            stream_client=stream_client,
        )
        self._initial_config = self._config.copy()
        self._default_workflow_logic_flags = set(_DEFAULT_ENABLED_WORKFLOW_LOGIC_FLAGS)

        # Apply plugin configuration
        self.plugins = plugins
        for plugin in plugins:
            self._config = plugin.configure_replayer(self._config)

        # Validate workflows after plugin configuration
        if not self._config.get("workflows"):
            raise ValueError("At least one workflow must be specified")

    def _set_default_workflow_logic_flag(
        self, flag: _WorkflowLogicFlag, *, enabled: bool
    ) -> None:
        if enabled:
            self._default_workflow_logic_flags.add(flag)
        else:
            self._default_workflow_logic_flags.discard(flag)

    @staticmethod
    async def fetch_stream_slices(
        client: temporalio.client.Client, history: temporalio.client.WorkflowHistory
    ) -> temporalio.client.WorkflowHistory:
        """Return ``history`` with the stream records its tasks consumed attached.

        History records only the offsets each Workflow Task consumed from a
        server-side stream, so an export cannot be replayed without the
        server that still holds the stream. This fetches every recorded range
        from the stream service through ``client`` while the stream is
        retained and returns a copy of ``history`` with the records in
        :py:attr:`temporalio.client.WorkflowHistory.stream_slices`. Its
        ``to_json()`` then carries them, and a replayer given the result, or a
        ``from_json()`` of it, needs no server. Ranges recorded before a reset
        point are fetched from the run the workflow was reset from.

        Raises :py:class:`temporalio.streams.StreamNotFoundError` for a range
        the stream no longer holds.
        """
        return temporalio.client.WorkflowHistory(
            history.workflow_id,
            history.events,
            await _stream_slices(client, history),
        )

    def config(self, *, active_config: bool = False) -> ReplayerConfig:
        """Config, as a dictionary, used to create this replayer.

        Args:
            active_config: If true, return the modified configuration in use rather than the initial one
                provided to the client.

        Returns:
            Configuration, shallow-copied.
        """
        config = self._config.copy() if active_config else self._initial_config.copy()
        config["workflows"] = list(config.get("workflows", []))
        return config

    async def replay_workflow(
        self,
        history: temporalio.client.WorkflowHistory,
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResult:
        """Replay a workflow for the given history.

        Args:
            history: The history to replay. Can be fetched directly, or use
                :py:meth:`temporalio.client.WorkflowHistory.from_json` to parse
                a history downloaded via ``Temporal CLI`` or the web UI.
            raise_on_replay_failure: If ``True`` (the default), this will raise
                a :py:attr:`WorkflowReplayResult.replay_failure` if it is
                present.
        """

        async def history_iterator():
            yield history

        async with self.workflow_replay_iterator(history_iterator()) as replay_iterator:
            async for result in replay_iterator:
                if raise_on_replay_failure and result.replay_failure:
                    raise result.replay_failure
                return result
            # Should never be reached
            raise RuntimeError("No histories")

    async def replay_workflows(
        self,
        histories: AsyncIterator[temporalio.client.WorkflowHistory],
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResults:
        """Replay workflows for the given histories.

        This is a shortcut for :py:meth:`workflow_replay_iterator` that iterates
        all results and aggregates information about them.

        Args:
            histories: The histories to replay, from an async iterator.
            raise_on_replay_failure: If ``True`` (the default), this will raise
                the first replay failure seen.

        Returns:
            Aggregated results.
        """
        async with self.workflow_replay_iterator(histories) as replay_iterator:
            replay_failures: dict[str, Exception] = {}
            async for result in replay_iterator:
                if result.replay_failure:
                    if raise_on_replay_failure:
                        raise result.replay_failure
                    replay_failures[result.history.run_id] = result.replay_failure
            return WorkflowReplayResults(replay_failures=replay_failures)

    def workflow_replay_iterator(
        self, histories: AsyncIterator[temporalio.client.WorkflowHistory]
    ) -> AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]]:
        """Replay workflows for the given histories.

        This is a context manager for use via ``async with``. The value is an
        iterator for use via ``async for``.

        Args:
            histories: The histories to replay, from an async iterator.

        Returns:
            An async iterator that returns replayed workflow results as they are
            replayed.
        """

        def make_lambda(plugin, next):  # type: ignore[reportMissingParameterType]
            return lambda r, hs: plugin.run_replayer(r, hs, next)

        next_function = lambda r, hs: r._workflow_replay_iterator(hs)
        for plugin in reversed(self.plugins):
            next_function = make_lambda(plugin, next_function)

        return next_function(self, histories)

    @asynccontextmanager
    async def _workflow_replay_iterator(
        self, histories: AsyncIterator[temporalio.client.WorkflowHistory]
    ) -> AsyncIterator[AsyncIterator[WorkflowReplayResult]]:
        # Initialize variables to avoid unbound variable errors
        pusher = None
        workflow_worker_task = None
        bridge_worker_scope = None

        try:
            last_replay_failure: Exception | None
            last_replay_complete = asyncio.Event()

            # Create eviction hook
            def on_eviction_hook(
                _run_id: str,
                remove_job: temporalio.bridge.proto.workflow_activation.RemoveFromCache,
            ) -> None:
                nonlocal last_replay_failure
                if (
                    remove_job.reason
                    == temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.NONDETERMINISM
                ):
                    last_replay_failure = temporalio.workflow.NondeterminismError(
                        remove_job.message
                    )
                elif (
                    remove_job.reason
                    != temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.CACHE_FULL
                    and remove_job.reason
                    != temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.LANG_REQUESTED
                ):
                    last_replay_failure = RuntimeError(
                        f"{remove_job.reason}: {remove_job.message}"
                    )
                else:
                    last_replay_failure = None
                last_replay_complete.set()

            # Create worker referencing bridge worker
            bridge_worker: temporalio.bridge.worker.Worker
            task_queue = f"replay-{self._config.get('build_id')}"
            runtime = (
                self._config.get("runtime") or temporalio.runtime.Runtime.default()
            )
            data_converter = (
                self._config.get("data_converter")
                or temporalio.converter.DataConverter.default
            )
            workflow_worker = _WorkflowWorker(
                bridge_worker=lambda: bridge_worker,
                namespace=self._config.get("namespace", "ReplayNamespace"),
                task_queue=task_queue,
                workflows=self._config.get("workflows", []),
                workflow_task_executor=self._config.get("workflow_task_executor"),
                max_concurrent_workflow_tasks=5,
                workflow_runner=self._config.get("workflow_runner")
                or SandboxedWorkflowRunner(),
                unsandboxed_workflow_runner=self._config.get(
                    "unsandboxed_workflow_runner"
                )
                or UnsandboxedWorkflowRunner(),
                data_converter=data_converter,
                interceptors=self._config.get("interceptors", []),
                workflow_failure_exception_types=self._config.get(
                    "workflow_failure_exception_types", []
                ),
                patch_activation_callback=None,
                debug_mode=self._config.get("debug_mode", False),
                metric_meter=runtime.metric_meter,
                on_eviction_hook=on_eviction_hook,
                disable_eager_activity_execution=False,
                disable_safe_eviction=self._config.get(
                    "disable_safe_workflow_eviction", False
                ),
                should_enforce_versioning_behavior=False,
                assert_local_activity_valid=lambda a: None,
                encode_headers=self._config.get(
                    "header_codec_behavior", HeaderCodecBehavior.NO_CODEC
                )
                != HeaderCodecBehavior.NO_CODEC,
                max_workflow_task_external_storage_concurrency=1,
                default_workflow_logic_flags=frozenset(
                    self._default_workflow_logic_flags
                ),
                stream_provider=self._config.get("stream_provider"),
            )
            external_storage = data_converter.external_storage
            storage_driver_types = (
                {driver.type() for driver in external_storage.drivers}
                if external_storage
                else set()
            )

            # Create bridge worker
            bridge_worker, pusher = temporalio.bridge.worker.Worker.for_replay(
                runtime._core_runtime,
                temporalio.bridge.worker.WorkerConfig(
                    namespace=self._config.get("namespace", "ReplayNamespace"),
                    task_queue=task_queue,
                    identity_override=self._config.get("identity"),
                    # Need to tell core whether we want to consider all
                    # non-determinism exceptions as workflow fail, and whether we do
                    # per workflow type
                    nondeterminism_as_workflow_fail=workflow_worker.nondeterminism_as_workflow_fail(),
                    nondeterminism_as_workflow_fail_for_types=workflow_worker.nondeterminism_as_workflow_fail_for_types(),
                    # All values below are ignored but required by Core
                    max_cached_workflows=2,
                    tuner=temporalio.bridge.worker.TunerHolder(
                        workflow_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            2
                        ),
                        activity_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            1
                        ),
                        local_activity_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            1
                        ),
                        nexus_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            1
                        ),
                    ),
                    nonsticky_to_sticky_poll_ratio=1,
                    no_remote_activities=True,
                    disable_payload_error_limit=True,
                    task_types=temporalio.bridge.worker.WorkerTaskTypes(
                        enable_workflows=True,
                        enable_local_activities=False,
                        enable_remote_activities=False,
                        enable_nexus=False,
                    ),
                    sticky_queue_schedule_to_start_timeout_millis=1000,
                    max_heartbeat_throttle_interval_millis=1000,
                    default_heartbeat_throttle_interval_millis=1000,
                    max_activities_per_second=None,
                    max_task_queue_activities_per_second=None,
                    max_eager_activity_reservations_per_workflow_task=3,
                    graceful_shutdown_period_millis=0,
                    versioning_strategy=temporalio.bridge.worker.WorkerVersioningStrategyNone(
                        build_id_no_versioning=self._config.get("build_id")
                        or load_default_build_id(),
                    ),
                    workflow_task_poller_behavior=temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
                        2
                    ),
                    activity_task_poller_behavior=temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
                        1
                    ),
                    nexus_task_poller_behavior=temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
                        1
                    ),
                    plugins=[plugin.name() for plugin in self.plugins],
                    storage_drivers=storage_driver_types,
                ),
            )
            bridge_worker_scope = bridge_worker

            # Start worker
            workflow_worker_task = asyncio.create_task(workflow_worker.run())

            stream_client = self._config.get("stream_client")

            # Yield iterator
            async def replay_iterator() -> AsyncIterator[WorkflowReplayResult]:
                async for history in histories:
                    # The records a consuming workflow read are not in its
                    # history unless they were captured into it. Otherwise
                    # fetch them from the stream service, or report here what
                    # is missing, rather than let Core fail the first task on
                    # input it was never given.
                    stream_slices: list[bytes] = []
                    if history.stream_slices:
                        stream_slices = [
                            s.SerializeToString() for s in history.stream_slices
                        ]
                    elif stream_client is not None:
                        try:
                            fetched = await _stream_slices(stream_client, history)
                        except temporalio.streams.StreamNotFoundError as err:
                            yield WorkflowReplayResult(
                                history=history, replay_failure=err
                            )
                            continue
                        stream_slices = [s.SerializeToString() for s in fetched]
                    else:
                        missing = _stream_records_missing(history)
                        if missing is not None:
                            yield WorkflowReplayResult(
                                history=history, replay_failure=RuntimeError(missing)
                            )
                            continue

                    # Clear last complete and push history
                    last_replay_complete.clear()
                    await pusher.push_history(
                        history.workflow_id,
                        temporalio.api.history.v1.History(
                            events=history.events
                        ).SerializeToString(),
                        stream_slices,
                    )

                    # Wait for worker error or last replay to complete. This
                    # should never take more than a few seconds due to deadlock
                    # detector but we cannot add timeout just in case debug mode
                    # is enabled.
                    await asyncio.wait(  # type: ignore
                        [
                            workflow_worker_task,
                            asyncio.create_task(last_replay_complete.wait()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # If worker task complete, wait on it so it'll throw
                    if workflow_worker_task.done():
                        await workflow_worker_task
                    # Should always be set if workflow worker didn't throw
                    assert last_replay_complete.is_set()

                    yield WorkflowReplayResult(
                        history=history,
                        replay_failure=last_replay_failure,
                    )

            yield replay_iterator()
        finally:
            # Close the pusher
            if pusher is not None:
                pusher.close()
            # If the workflow worker task is not done, wait for it
            try:
                if workflow_worker_task is not None and not workflow_worker_task.done():
                    await workflow_worker_task
            except Exception:
                logger.warning("Failed to shutdown worker", exc_info=True)
            finally:
                # We must shutdown here
                try:
                    if bridge_worker_scope is not None:
                        bridge_worker_scope.initiate_shutdown()
                        await bridge_worker_scope.finalize_shutdown()
                except Exception:
                    logger.warning("Failed to finalize shutdown", exc_info=True)


def _consumed_ranges(
    history: temporalio.client.WorkflowHistory,
) -> list[tuple[int, temporalio.api.stream.v1.StreamRange]]:
    """Every range a completed task recorded, with the id of the event that recorded it."""
    out: list[tuple[int, temporalio.api.stream.v1.StreamRange]] = []
    for event in history.events:
        if not event.HasField("workflow_task_completed_event_attributes"):
            continue
        attributes = event.workflow_task_completed_event_attributes
        for consumed in attributes.consumed_stream_ranges:
            out.append((event.event_id, consumed))
    return out


def _eras(
    history: temporalio.client.WorkflowHistory,
) -> list[tuple[str, list[tuple[int, temporalio.api.stream.v1.StreamRange]]]]:
    """The recorded ranges grouped by the run whose streams hold them.

    A reset copies the base run's history into the new run, so the ranges
    before the reset point were consumed from the base run's streams, and the
    run they belong to is only named by the ``WorkflowTaskFailed`` event that
    marks the reset point, after them in the history. Ranges after the last
    reset point belong to the run itself, which that event names as well; a
    history with no reset point belongs to the run its start event names. This
    is the split the server makes when it re-supplies a cache miss.
    """
    reset = temporalio.api.enums.v1.WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW
    eras: list[tuple[str, list[tuple[int, temporalio.api.stream.v1.StreamRange]]]] = []
    pending: list[tuple[int, temporalio.api.stream.v1.StreamRange]] = []
    own_run_id = history.run_id
    for event in history.events:
        if event.HasField("workflow_task_failed_event_attributes"):
            failed = event.workflow_task_failed_event_attributes
            if failed.cause == reset and failed.base_run_id:
                eras.append((failed.base_run_id, pending))
                pending = []
                own_run_id = failed.new_run_id or own_run_id
                continue
        if not event.HasField("workflow_task_completed_event_attributes"):
            continue
        attributes = event.workflow_task_completed_event_attributes
        for consumed in attributes.consumed_stream_ranges:
            pending.append((event.event_id, consumed))
    eras.append((own_run_id, pending))
    return eras


def _stream_records_missing(history: temporalio.client.WorkflowHistory) -> str | None:
    """Why this history cannot be replayed without a stream client, or ``None``."""
    ranges = [r for _, r in _consumed_ranges(history) if r.to_offset > r.from_offset]
    if not ranges:
        return None
    streams = sorted({r.stream_id for r in ranges})
    return (
        f"workflow {history.workflow_id!r} consumed records from stream(s) "
        f"{', '.join(repr(s) for s in streams)} in {len(ranges)} task(s), and History "
        "records only the offsets. Either create the Replayer with stream_client= (a "
        "Client connected to the server that still holds the streams) so the records "
        "can be fetched from the stream service, or replay a history exported with "
        "them: Replayer.fetch_stream_slices(client, history) attaches the records "
        "while the stream is retained and WorkflowHistory.to_json() carries them."
    )


async def _stream_slices(
    client: temporalio.client.Client, history: temporalio.client.WorkflowHistory
) -> list[temporalio.api.stream.v1.StreamSlice]:
    """Fetch the records the history's tasks consumed, as ``StreamSlice`` messages.

    The shape is the one the server puts on a poll response when it re-supplies
    a cache miss: one slice per recorded range, tagged with the completion that
    recorded it, an empty range included, fetched from the run whose stream
    holds it (the run reset from, for a range recorded before a reset point).
    Raises :py:class:`temporalio.streams.StreamNotFoundError` for a range the
    stream no longer holds.
    """
    if not _consumed_ranges(history):
        return []
    # Imported here: the stream client needs the grpc extra, which a replayer
    # without a stream client never touches.
    from temporalio.client_stream import shared_client

    streams = shared_client(client.service_client.config.target_host, client.namespace)
    # The handle that served each run's stream, so later ranges of the same
    # stream go straight to it.
    served_by: dict[tuple[str, str], Any] = {}
    slices: list[temporalio.api.stream.v1.StreamSlice] = []
    for run_id, ranges in _eras(history):
        for event_id, consumed in ranges:
            stream_slice = temporalio.api.stream.v1.StreamSlice(
                stream_id=consumed.stream_id,
                run_id=run_id,
                from_offset=consumed.from_offset,
                to_offset=consumed.to_offset,
                workflow_task_completed_event_id=event_id,
            )
            if consumed.to_offset > consumed.from_offset:
                handle, records, owner_run_id = await _fetch_range(
                    streams,
                    history.workflow_id,
                    run_id,
                    consumed,
                    served_by.get((run_id, consumed.stream_id)),
                )
                served_by[(run_id, consumed.stream_id)] = handle
                stream_slice.run_id = owner_run_id or run_id
                stream_slice.records.extend(records)
            slices.append(stream_slice)
    return slices


async def _fetch_range(
    streams: Any,
    workflow_id: str,
    run_id: str,
    consumed: temporalio.api.stream.v1.StreamRange,
    known: Any,
) -> tuple[Any, list[temporalio.api.stream.v1.StreamRecord], str]:
    """The records at exactly the recorded range, with the handle that served them.

    A subscribed name is resolved as the server resolves it: a stream the
    workflow owns by that name first, else a standalone stream by that id. The
    owned stream cannot be asked whether it exists, since a name nobody wrote
    reads as empty, so the owned stream is probed for the range's first record
    and the standalone one is tried when it has nothing there.
    """
    gone = (
        f"stream {consumed.stream_id!r} no longer holds offsets "
        f"[{consumed.from_offset}, {consumed.to_offset}) that a completed task of "
        f"workflow {workflow_id!r} run {run_id!r} consumed; its records cannot be "
        "replayed"
    )
    candidates = (
        [known]
        if known is not None
        else [
            streams.workflow_stream(
                workflow_id, consumed.stream_id, owner_run_id=run_id
            ),
            streams.get(consumed.stream_id),
        ]
    )
    last: Exception | None = None
    for handle in candidates:
        try:
            records, owner_run_id = await _read_range(handle, consumed, gone)
        except temporalio.streams.StreamNotFoundError as error:
            last = error
            continue
        if records is not None:
            return handle, records, owner_run_id
    raise temporalio.streams.StreamNotFoundError(gone) from last


async def _read_range(
    handle: Any, consumed: temporalio.api.stream.v1.StreamRange, gone: str
) -> tuple[list[temporalio.api.stream.v1.StreamRecord] | None, str]:
    """Read ``[from_offset, to_offset)`` from one stream.

    ``None`` when the stream has nothing at the range's first offset, which is
    how a name that is not this stream's reads; a stream that has the start but
    not the rest, or refuses the offset as truncated or past its head, raises.
    """
    records: list[temporalio.api.stream.v1.StreamRecord] = []
    owner_run_id = ""
    offset = consumed.from_offset
    while offset < consumed.to_offset:
        try:
            page = await handle.poll(
                from_offset=offset,
                max_records=consumed.to_offset - offset,
                wait=False,
            )
        except temporalio.streams.StreamNotFoundError:
            if not records:
                return None, ""
            raise
        except temporalio.service.RPCError as error:
            # Below the truncation floor or past the head: the server refuses
            # the offset rather than answering short.
            if error.status in (
                temporalio.service.RPCStatusCode.FAILED_PRECONDITION,
                temporalio.service.RPCStatusCode.INVALID_ARGUMENT,
                temporalio.service.RPCStatusCode.OUT_OF_RANGE,
            ):
                raise temporalio.streams.StreamNotFoundError(gone) from error
            raise
        owner_run_id = page.run_id or owner_run_id
        if not page.entries:
            if not records:
                return None, ""
            raise temporalio.streams.StreamNotFoundError(gone)
        for entry in page.entries:
            if entry.offset != offset or offset >= consumed.to_offset:
                raise temporalio.streams.StreamNotFoundError(
                    f"{gone}: the stream answered offset {entry.offset} where "
                    f"{offset} was due"
                )
            records.append(entry.record)
            offset += 1
    return records, owner_run_id


class ReplayerConfig(TypedDict, total=False):
    """TypedDict of config originally passed to :py:class:`Replayer`."""

    workflows: Sequence[type]
    workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None
    workflow_runner: WorkflowRunner
    unsandboxed_workflow_runner: WorkflowRunner
    namespace: str
    data_converter: temporalio.converter.DataConverter
    interceptors: Sequence[Interceptor]
    build_id: str | None
    identity: str | None
    workflow_failure_exception_types: Sequence[type[BaseException]]
    debug_mode: bool
    runtime: temporalio.runtime.Runtime | None
    disable_safe_workflow_eviction: bool
    header_codec_behavior: HeaderCodecBehavior
    stream_provider: temporalio.streams.StreamProvider | None
    stream_client: temporalio.client.Client | None


@dataclass(frozen=True)
class WorkflowReplayResult:
    """Single workflow replay result."""

    history: temporalio.client.WorkflowHistory
    """History originally passed for this workflow replay."""

    replay_failure: Exception | None
    """Failure during replay if any.

    This does not mean your workflow exited by raising an error, but rather that
    some task failure such as
    :py:class:`temporalio.workflow.NondeterminismError` was encountered during
    replay - likely indicating your workflow code is incompatible with the
    history.

    A workflow that read a server-side stream reports here, before any task
    runs, when its records could not be fetched: a
    :py:class:`temporalio.streams.StreamNotFoundError` when the stream no
    longer holds a range a task consumed, or a ``RuntimeError`` when the
    replayer was given no ``stream_client`` to fetch them with.
    """


@dataclass(frozen=True)
class WorkflowReplayResults:
    """Results of replaying multiple workflows."""

    replay_failures: Mapping[str, Exception]
    """Replay failures, keyed by run ID."""
