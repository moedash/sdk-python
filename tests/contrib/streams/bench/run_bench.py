"""Local stream benchmarks: PERF-04 and PERF-05 on the dev server and Redis.

Not part of the test run. Run one measurement at a time, for example::

    uv run python -m tests.contrib.streams.bench.run_bench perf04-activity \\
        --redis redis://localhost:6384/0

Every process clock reading comes from ``time.perf_counter_ns`` in this one
process: the Worker, the producers and the readers all run here, so the
latencies need no clock sync. A record carries the clock reading taken just
before its publish call, and a reader subtracts it on receipt (PERF-04's
boundary: publish call to the reader's SDK receiving the record).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import subprocess
import time
import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    activity_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._output import OutputCoordinator
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

EVENTS = topic("events", dict)

# Publish clock readings of Workflow publishes, by (Workflow id, index). The
# Workflow runs unsandboxed so it can write here; the payload itself stays
# deterministic.
_workflow_sent: dict[tuple[str, int], int] = {}


def _now() -> int:
    return time.perf_counter_ns()


def _percentiles(samples_ms: Sequence[float]) -> dict[str, float]:
    if not samples_ms:
        return {"count": 0}
    ordered = sorted(samples_ms)

    def at(fraction: float) -> float:
        return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]

    return {
        "count": len(ordered),
        "p50_ms": round(at(0.50), 2),
        "p95_ms": round(at(0.95), 2),
        "p99_ms": round(at(0.99), 2),
        "max_ms": round(ordered[-1], 2),
        "mean_ms": round(statistics.fmean(ordered), 2),
    }


@workflow.defn
class Owner:
    """Owns a stream that outside producers write to."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@workflow.defn(sandboxed=False)
class WorkflowPublisher:
    """Publishes ``count`` records, one per Workflow Task."""

    @workflow.run
    async def run(self, count: int, gap_ms: int) -> None:
        writer = workflow_writer(EVENTS)
        for index in range(count):
            key = (workflow.info().workflow_id, index)
            if not workflow.unsafe.is_replaying():
                _workflow_sent.setdefault(key, _now())
            writer.publish({"i": index})
            await workflow.sleep(timedelta(milliseconds=gap_ms))


@activity.defn
async def produce(count: int, gap_ms: int) -> None:
    producer = activity_handle().producer(topic=EVENTS)
    for index in range(count):
        await producer.append({"i": index, "t": _now()})
        await asyncio.sleep(gap_ms / 1000)


@workflow.defn
class ActivityPublisher:
    @workflow.run
    async def run(self, count: int, gap_ms: int) -> None:
        await workflow.execute_activity(
            produce,
            args=[count, gap_ms],
            start_to_close_timeout=timedelta(minutes=10),
        )


class Bench:
    def __init__(self, client: Client, provider: RedisStreams, task_queue: str):
        self.client = client
        self.provider = provider
        self.task_queue = task_queue

    def stream(self, workflow_id: str) -> Any:
        return self.provider.get_stream_handle(
            self.client, StreamRef.for_workflow(workflow_id, topic=EVENTS)
        )

    async def read(
        self, workflow_id: str, count: int, sent: dict[int, int] | None = None
    ) -> tuple[list[float], int, float]:
        """Read ``count`` records; return latencies, bytes and the finish clock."""
        latencies: list[float] = []
        received_bytes = 0
        records = self.stream(workflow_id).read()
        try:
            async for record in records:
                if record.kind is not RecordKind.DATA:
                    continue
                received = _now()
                value = record.value
                started = value["t"] if sent is None else sent[value["i"]]
                latencies.append((received - started) / 1e6)
                received_bytes += len(value.get("pad", ""))
                if len(latencies) == count:
                    break
        finally:
            await records.aclose()
        return latencies, received_bytes, _now() / 1e9

    async def owner(self, workflow_id: str) -> Any:
        return await self.client.start_workflow(
            Owner.run, id=workflow_id, task_queue=self.task_queue
        )


async def perf04_activity(bench: Bench, count: int, gap_ms: int) -> dict[str, Any]:
    workflow_id = f"bench-act-{uuid.uuid4().hex}"
    handle = await bench.client.start_workflow(
        ActivityPublisher.run,
        args=[count, gap_ms],
        id=workflow_id,
        task_queue=bench.task_queue,
    )
    latencies, _, _ = await bench.read(workflow_id, count)
    await handle.result()
    return {"scenario": "perf04-activity", "gap_ms": gap_ms, **_percentiles(latencies)}


async def perf04_workflow(bench: Bench, count: int, gap_ms: int) -> dict[str, Any]:
    reads: list[float] = []
    real = OutputCoordinator._events_after

    async def timed(self: Any, run: Any, floor: int) -> Any:
        started = _now()
        try:
            return await real(self, run, floor)
        finally:
            reads.append((_now() - started) / 1e6)

    OutputCoordinator._events_after = timed  # type: ignore[method-assign]
    try:
        workflow_id = f"bench-wf-{uuid.uuid4().hex}"
        handle = await bench.client.start_workflow(
            WorkflowPublisher.run,
            args=[count, gap_ms],
            id=workflow_id,
            task_queue=bench.task_queue,
        )
        # The publish clock is filled in as the Workflow runs, so the reader
        # looks each record up on receipt.
        latencies, _, _ = await bench.read(
            workflow_id, count, sent=_SentView(workflow_id)
        )
        await handle.result()
    finally:
        OutputCoordinator._events_after = real  # type: ignore[method-assign]
    return {
        "scenario": "perf04-workflow",
        "gap_ms": gap_ms,
        **_percentiles(latencies),
        "history_reads": len(reads),
        "history_reads_per_publish": round(len(reads) / count, 2),
        "history_read": _percentiles(reads),
    }


class _SentView(dict):
    def __init__(self, workflow_id: str) -> None:
        super().__init__()
        self._workflow_id = workflow_id

    def __getitem__(self, index: int) -> int:
        return _workflow_sent[(self._workflow_id, index)]


async def _produce_load(
    bench: Bench,
    workflow_id: str,
    duration_s: float,
    rate_per_s: float | None,
    batch: int,
    size: int,
) -> int:
    """Append to one stream for ``duration_s``; return how many records."""
    producer = bench.stream(workflow_id).producer(
        topic=EVENTS, producer_id="bench", attempt=1
    )
    pad = "x" * size
    sent = 0
    deadline = time.monotonic() + duration_s
    interval = None if rate_per_s is None else batch / rate_per_s
    while time.monotonic() < deadline:
        started = time.monotonic()
        stamp = _now()
        await producer.append(
            *({"i": sent + i, "t": stamp, "pad": pad} for i in range(batch))
        )
        sent += batch
        if interval is not None:
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))
    return sent


async def load(
    bench: Bench,
    *,
    streams: int,
    readers: int,
    duration_s: float,
    rate_per_s: float | None,
    batch: int,
    size: int,
    scenario: str,
) -> dict[str, Any]:
    ids = [f"bench-load-{uuid.uuid4().hex}" for _ in range(streams)]
    owners = [await bench.owner(workflow_id) for workflow_id in ids]
    # Readers start first, from the end, so they measure live delivery.
    reader_tasks: list[asyncio.Future[Any]] = []
    totals: dict[str, int] = {}

    async def follow(workflow_id: str, out: dict[str, Any]) -> None:
        stream = bench.stream(workflow_id)
        records = stream.read(after=await stream.latest())
        try:
            async for record in records:
                if record.kind is not RecordKind.DATA:
                    continue
                out["latencies"].append((_now() - record.value["t"]) / 1e6)
                out["bytes"] += len(record.value["pad"])
                out["count"] += 1
        finally:
            await records.aclose()

    outs: list[dict[str, Any]] = []
    for workflow_id in ids:
        for _ in range(readers):
            out: dict[str, Any] = {
                "id": workflow_id,
                "latencies": [],
                "bytes": 0,
                "count": 0,
            }
            outs.append(out)
            reader_tasks.append(asyncio.ensure_future(follow(workflow_id, out)))
    await asyncio.sleep(1.0)
    started = time.monotonic()
    sent_counts = await asyncio.gather(
        *(
            _produce_load(bench, workflow_id, duration_s, rate_per_s, batch, size)
            for workflow_id in ids
        )
    )
    for workflow_id, sent in zip(ids, sent_counts):
        totals[workflow_id] = sent
    # Readers run until they have every record, or a minute passes.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and any(
        out["count"] < totals[out["id"]] for out in outs
    ):
        await asyncio.sleep(0.1)
    complete = all(out["count"] >= totals[out["id"]] for out in outs)
    finished = time.monotonic()
    for task in reader_tasks:
        task.cancel()
    await asyncio.gather(*reader_tasks, return_exceptions=True)
    elapsed = finished - started
    latencies = [value for out in outs for value in out["latencies"]]
    received = sum(out["count"] for out in outs)
    received_bytes = sum(out["bytes"] for out in outs)
    expected = sum(sent_counts) * readers
    for owner in owners:
        await owner.signal(Owner.finish)
    return {
        "scenario": scenario,
        "streams": streams,
        "readers_per_stream": readers,
        "record_bytes": size,
        "batch": batch,
        "target_rate_per_stream": rate_per_s,
        "sent_records": sum(sent_counts),
        "delivered": received,
        "expected_deliveries": expected,
        "complete": complete and received == expected,
        "published_records_per_s": round(sum(sent_counts) / duration_s, 1),
        "delivered_records_per_s": round(received / elapsed, 1),
        "delivered_bytes_per_s": round(received_bytes / elapsed, 1),
        **_percentiles(latencies),
    }


def machine() -> dict[str, Any]:
    def run(*command: str) -> str:
        try:
            return subprocess.run(
                command, capture_output=True, text=True
            ).stdout.strip()
        except OSError:
            return ""

    return {
        "platform": platform.platform(),
        "cpu": run("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
        "cores": run("sysctl", "-n", "hw.ncpu"),
        "memory_bytes": run("sysctl", "-n", "hw.memsize"),
        "python": platform.python_version(),
    }


async def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scenario",
        choices=["perf04-activity", "perf04-workflow", "perf05a", "perf05b", "perf05c"],
    )
    parser.add_argument("--redis", required=True)
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--gap-ms", type=int, default=20)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--streams", type=int, default=1)
    parser.add_argument("--readers", type=int, default=1)
    parser.add_argument("--rate", type=float, default=None)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--size", type=int, default=100)
    args = parser.parse_args(argv)

    provider = RedisStreams(args.redis, key_prefix=f"bench-{uuid.uuid4().hex}")
    async with await WorkflowEnvironment.start_local() as env:
        config = env.client.config()
        config["plugins"] = [provider]
        client = Client(**config)
        task_queue = f"bench-{uuid.uuid4().hex}"
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Owner, WorkflowPublisher, ActivityPublisher],
            activities=[produce],
            max_cached_workflows=5000,
            max_concurrent_activities=1000,
        ):
            bench = Bench(client, provider, task_queue)
            if args.scenario == "perf04-activity":
                result = await perf04_activity(bench, args.count, args.gap_ms)
            elif args.scenario == "perf04-workflow":
                result = await perf04_workflow(bench, args.count, args.gap_ms)
            else:
                result = await load(
                    bench,
                    streams=args.streams,
                    readers=args.readers,
                    duration_s=args.duration,
                    rate_per_s=args.rate,
                    batch=args.batch,
                    size=args.size,
                    scenario=args.scenario,
                )
    await provider.close()
    info = await _redis_version(args.redis)
    print(json.dumps({"machine": machine(), "redis": info, "result": result}))


async def _redis_version(url: str) -> str:
    import redis.asyncio

    client = redis.asyncio.Redis.from_url(url)
    try:
        return str((await client.info("server"))["redis_version"])
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
