"""A Workflow's own publish, committed with the Workflow Task that made it.

Internal. The writer buffers records on the Workflow thread. When an
activation that published completes, the Worker hands the completion here
before it goes to Core:

1. The records are converted already. The manifest is taken over them as
   they are, before the codec, so replay can recompute it.
2. Each body is encoded with the Workflow's data converter, so the payload
   codec and external storage apply, and the provider stages the batch. A
   staged batch is held in the store and is not visible.
3. A ``WorkflowOutputStreamCommit`` carrying the manifest and the stage
   token joins the completion. Core records it in a marker, ordered before
   the completion's other commands, so the batch is part of the task.

After the completion, the batch is promoted only when History shows the
marker that names its stage token. A completion that fails stages nothing.
Several publishing completions in one Workflow Task each commit their own
batch, and the stages are kept per run in order.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import temporalio.converter
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryReverseRequest,
)
from temporalio.bridge.proto.external_data import (
    ExternalOutputStreamManifest,
    ExternalStreamMarkerData,
)
from temporalio.bridge.proto.workflow_activation import WorkflowActivation
from temporalio.bridge.proto.workflow_completion import WorkflowActivationCompletion
from temporalio.contrib.streams._body import content_fingerprint, encode_body
from temporalio.contrib.streams._record import RecordKind
from temporalio.contrib.streams._wire import WireRecord

if TYPE_CHECKING:
    from temporalio.client import Client
    from temporalio.contrib.streams._plugin import StreamProviderPlugin

logger = logging.getLogger(__name__)

MARKER_NAME = "core_external_stream"
_MARKER_DETAILS_KEY = "external_stream"
_SCHEMA_VERSION = 1
_FINGERPRINT_VERSION = 1
_PROVIDER_FORMAT_VERSION = 1


@dataclass(frozen=True)
class StagedBatch:
    """What a provider stages for one publishing completion.

    ``records`` are in publish order across topics, with bodies already
    encoded for the store and the plaintext hash stamped.
    """

    namespace: str
    workflow_id: str
    run_id: str
    records: Sequence[WireRecord]


@dataclass(frozen=True)
class _Stage:
    token: str
    history_floor_event_id: int


class _RunOutput:
    """One run's buffered publish and the stages it is waiting on."""

    def __init__(self, workflow_id: str, run_id: str) -> None:
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.pending: list[WireRecord] = []
        self.staged: list[_Stage] = []

    def publish(self, records: Sequence[WireRecord]) -> None:
        """Buffer records the running Workflow published, on its thread."""
        self.pending.extend(records)


class _Decision(Enum):
    PROMOTE = "promote"
    ABORT = "abort"
    UNKNOWN = "unknown"


def build_manifest(
    records: Sequence[WireRecord],
    *,
    history_floor_event_id: int,
    run_id: str,
    provider_id: str,
) -> ExternalOutputStreamManifest:
    """The manifest Core records for one publishing completion.

    Taken over the records as the converter produced them, so it does not
    depend on the codec and replay recomputes the same manifest. Topics are
    in order of first publish, and the completion is one segment.
    """
    by_topic: dict[str, list[WireRecord]] = {}
    for record in records:
        by_topic.setdefault(record.topic, []).append(record)
    manifest = ExternalOutputStreamManifest(
        schema_version=_SCHEMA_VERSION,
        fingerprint_version=_FINGERPRINT_VERSION,
        history_floor_event_id=history_floor_event_id,
        run_id=run_id,
        provider_id=provider_id,
        provider_format_version=_PROVIDER_FORMAT_VERSION,
    )
    segment = manifest.segments.add()
    for topic, topic_records in by_topic.items():
        manifest.topics.add(
            topic=topic,
            record_count=len(topic_records),
            logical_byte_count=sum(
                len(record.SerializeToString(deterministic=True))
                for record in topic_records
            ),
            logical_fingerprint=content_fingerprint(topic_records),
            finished=any(
                record.kind == RecordKind.FINISH.value for record in topic_records
            ),
        )
        segment.record_counts_by_topic.append(len(topic_records))
    return manifest


def _marker_token(event: HistoryEvent) -> str | None:
    if event.event_type != EventType.EVENT_TYPE_MARKER_RECORDED:
        return None
    attributes = event.marker_recorded_event_attributes
    if attributes.marker_name != MARKER_NAME:
        return None
    payloads = attributes.details.get(_MARKER_DETAILS_KEY)
    if payloads is None or not payloads.payloads:
        return None
    marker = ExternalStreamMarkerData.FromString(payloads.payloads[0].data)
    return marker.output.stage_token if marker.HasField("output") else None


_TASK_RESULTS = (
    EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
    EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
    EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
)


def decide(events: Sequence[HistoryEvent], stage: _Stage) -> _Decision:
    """What History says about one stage.

    ``events`` are the run's events after the stage's history floor, in
    order. A marker naming the stage token proves the commit. Otherwise, a
    failed or timed out result for the task that started after the floor
    proves the completion was dropped. Anything else is not known yet, for
    example while a Local Activity holds the task open.
    """
    if any(_marker_token(event) == stage.token for event in events):
        return _Decision.PROMOTE
    for event in events:
        if event.event_id <= stage.history_floor_event_id:
            continue
        if event.event_type in _TASK_RESULTS:
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
                return _Decision.UNKNOWN
            return _Decision.ABORT
    return _Decision.UNKNOWN


class OutputCoordinator:
    """The Worker side of one Worker's Workflow publish.

    The Worker finds it on the streams interceptor and calls it around each
    activation; Workflow code reaches it through the Worker's extern
    functions to open its run's buffer.
    """

    def __init__(
        self, provider: StreamProviderPlugin, client: Client | None, namespace: str
    ) -> None:
        """Serve one Worker or Replayer."""
        self.provider = provider
        self._client = client
        self._namespace = namespace
        self._runs: dict[str, _RunOutput] = {}

    def open_run(self, workflow_id: str, run_id: str) -> _RunOutput:
        """The buffer for ``run_id``, created on first use."""
        run = self._runs.get(run_id)
        if run is None:
            run = self._runs[run_id] = _RunOutput(workflow_id, run_id)
        return run

    def take_jobs(self, act: WorkflowActivation) -> None:
        """Remove the jobs that are this coordinator's, before the run sees them."""
        if not any(job.HasField("replay_external_streams") for job in act.jobs):
            return
        kept = [job for job in act.jobs if not job.HasField("replay_external_streams")]
        del act.jobs[:]
        act.jobs.extend(kept)

    async def before_completion(
        self,
        act: WorkflowActivation,
        completion: WorkflowActivationCompletion,
        data_converter: temporalio.converter.DataConverter,
    ) -> None:
        """Stage what this activation published and commit it on the completion.

        Raises:
            RuntimeError: Core did not report the task's history floor, so the
                output cannot be committed.
        """
        run = self._runs.get(act.run_id)
        if run is None or not run.pending:
            return
        records, run.pending = run.pending, []
        if act.is_replaying:
            # The original completion committed this output already; replay
            # must not stage it again.
            return
        floor = act.history_floor_event_id
        if floor <= 0:
            raise RuntimeError(
                "Core did not report this Workflow Task's history floor, so the "
                "stream output it published cannot be committed"
            )
        manifest = build_manifest(
            records,
            history_floor_event_id=floor,
            run_id=act.run_id,
            provider_id=self.provider.name(),
        )
        encoded: list[WireRecord] = []
        for record in records:
            copy = WireRecord()
            copy.CopyFrom(record)
            encoded.append(await encode_body(data_converter, copy))
        manifest.stage_token = await self.provider._stage(
            StagedBatch(self._namespace, run.workflow_id, act.run_id, encoded)
        )
        completion.successful.commands.add().workflow_output_stream_commit.manifest.CopyFrom(
            manifest
        )
        run.staged.append(_Stage(manifest.stage_token, floor))

    def discard(self, run_id: str) -> None:
        """Drop what a failed activation published; nothing of it is staged."""
        run = self._runs.get(run_id)
        if run is not None:
            run.pending.clear()

    async def after_completion(self, run_id: str) -> None:
        """Promote the run's stages that History now shows as committed."""
        run = self._runs.get(run_id)
        if run is not None:
            await self._reconcile(run)

    async def on_eviction(self, run_id: str) -> None:
        """Settle what History already decides, then forget the run."""
        run = self._runs.pop(run_id, None)
        if run is not None:
            await self._reconcile(run)

    async def _reconcile(self, run: _RunOutput) -> None:
        if self._client is None or not run.staged:
            return
        try:
            events = await self._events_after(
                run, min(stage.history_floor_event_id for stage in run.staged)
            )
        except Exception:
            # Promotion is retried after the run's next completion, so a
            # History read that fails here only delays visibility.
            logger.warning(
                "Could not read History to settle stream output of run %s",
                run.run_id,
                exc_info=True,
            )
            return
        for stage in list(run.staged):
            if decide(events, stage) is _Decision.PROMOTE:
                await self.provider._promote(
                    self._namespace, run.workflow_id, stage.token
                )
                run.staged.remove(stage)

    async def _events_after(self, run: _RunOutput, floor: int) -> list[HistoryEvent]:
        assert self._client is not None
        events: list[HistoryEvent] = []
        token = b""
        while True:
            response = await self._client.workflow_service.get_workflow_execution_history_reverse(
                GetWorkflowExecutionHistoryReverseRequest(
                    namespace=self._namespace,
                    execution=WorkflowExecution(
                        workflow_id=run.workflow_id, run_id=run.run_id
                    ),
                    next_page_token=token,
                )
            )
            done = False
            for event in response.history.events:
                if event.event_id <= floor:
                    done = True
                    break
                events.append(event)
            token = response.next_page_token
            if done or not token:
                break
        events.reverse()
        return events
