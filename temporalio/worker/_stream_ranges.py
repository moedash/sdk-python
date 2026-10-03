"""Reading a recorded stream range back from the stream service.

History records the offsets each Workflow Task consumed from a server-side
stream and never the records. Two paths have to read those offsets back: the
replayer, for a history it was handed without its records, and a live worker
handed a task whose re-supplied ranges stop short of what History recorded.
Both resolve a subscribed name the way the server does and read exactly the
recorded offsets, so the workflow sees what it saw the first time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import temporalio.api.stream.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.service
import temporalio.streams

if TYPE_CHECKING:
    import temporalio.client

__all__ = ["fetch_range", "fill_short_stream_ranges", "read_range"]


async def fill_short_stream_ranges(
    activation: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
    workflow_id: str,
    client: temporalio.client.Client,
) -> None:
    """Fetch the records a short ``DeliverStreamRecords`` job lacks, before the activation runs.

    The server re-supplies a replaying workflow's recorded ranges on the task
    that carries them, within a budget. A job whose records stop short of its
    range is that budget's remainder: the offsets are recorded in History and
    the stream still holds them, so they are read from the stream service the
    way the replayer reads them for an exported history, and the job is made
    whole before the workflow runs on it. A job that covers its range, or that
    records an empty observation, is left as it is. The fetch reads from the
    activation's own run; a range recorded before a reset point lives on the
    base run, which the job does not name yet.

    Raises:
        temporalio.streams.StreamNotFoundError: The stream no longer holds the
            missing offsets. The task fails rather than run on less input.
    """
    short = [
        job.deliver_stream_records
        for job in activation.jobs
        if job.HasField("deliver_stream_records")
        and len(job.deliver_stream_records.records)
        < job.deliver_stream_records.to_offset - job.deliver_stream_records.from_offset
    ]
    if not short:
        return
    # Imported here: the stream client needs the grpc extra, which a worker
    # that never sees a short range never touches.
    from temporalio.client_stream import shared_client

    streams = shared_client(client)
    served_by: dict[str, Any] = {}
    for job in short:
        missing = temporalio.api.stream.v1.StreamRange(
            stream_id=job.stream_id,
            from_offset=job.from_offset + len(job.records),
            to_offset=job.to_offset,
        )
        handle, records, _ = await fetch_range(
            streams,
            workflow_id,
            activation.run_id,
            missing,
            served_by.get(job.stream_id),
        )
        served_by[job.stream_id] = handle
        job.records.extend(records)


async def fetch_range(
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
    and the standalone one is tried when it has nothing there. ``known`` is the
    handle that served this stream before, when there was one.
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
            records, owner_run_id = await read_range(handle, consumed, gone)
        except temporalio.streams.StreamNotFoundError as error:
            last = error
            continue
        if records is not None:
            return handle, records, owner_run_id
    raise temporalio.streams.StreamNotFoundError(gone) from last


async def read_range(
    handle: Any, consumed: temporalio.api.stream.v1.StreamRange, gone: str
) -> tuple[list[temporalio.api.stream.v1.StreamRecord] | None, str]:
    """Read ``[from_offset, to_offset)`` from one stream.

    ``None`` when the stream has nothing at the range's first offset, which is
    how a name that is not this stream's reads; a stream that has the start but
    not the rest, or refuses the offset as truncated or past its head, raises
    :class:`temporalio.streams.StreamNotFoundError` with ``gone`` as the reason.
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
        except temporalio.streams.StreamCursorError as error:
            # Below the truncation floor: the records a task consumed are gone.
            raise temporalio.streams.StreamNotFoundError(gone) from error
        except temporalio.service.RPCError as error:
            # Past the head, or a refusal the server does not type: it refuses
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
