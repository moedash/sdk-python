"""Narrow the client-side run down to one reader, with logging on."""
from __future__ import annotations
import asyncio, json, logging, pathlib, sys, time, uuid
from pathlib import Path

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
for noisy in ("temporalio.bridge", "grpc", "asyncio"):
    logging.getLogger(noisy).setLevel(logging.INFO)

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.worker import Worker

sys.path.insert(0, str(Path(__file__).resolve().parent))
import provider_setup


@workflow.defn(name="AI198DebugRead", sandboxed=False)
class JustRead:
    @workflow.run
    async def run(self, want: int) -> list:
        import os
        inputs = streams.reader("inputs", type=dict)
        out = streams.writer("decisions")
        seen = []
        async for record in inputs:
            seen.append({"kind": record.kind.name, "value": record.value})
            if os.environ.get("AI198_PUBLISH"):
                await out.publish({"echo": record.value})
            if len(seen) >= want:
                break
        inputs.close()
        if os.environ.get("AI198_PUBLISH"):
            await out.finish()
        return seen


async def main() -> int:
    import os
    target, options = await provider_setup.open()
    backend = options["backend"]
    original_read = backend.read_after
    original_drain = None

    async def traced_read(key, cursor, *, max_records, block):
        records = await original_read(key, cursor, max_records=max_records, block=block)
        if records:
            print(f"TRACE read_after {key.stream_name} from={cursor} -> {len(records)}", flush=True)
        return records

    backend.read_after = traced_read
    client = await Client.connect(target, namespace="default")
    streams.configure(**options)
    uid = "ai198-debug-" + uuid.uuid4().hex
    import os
    cache = int(os.environ.get("AI198_CACHE", "100"))
    async with Worker(client, task_queue=uid, workflows=[JustRead], max_cached_workflows=cache, **streams.worker_options()):
        handle = await client.start_workflow(JustRead.run, 2, id=uid, task_queue=uid)
        p = await streams.producer(client, workflow_id=uid, stream="inputs", producer_id="m", attempt=1)
        await asyncio.sleep(float(os.environ.get("AI198_DELAY", "0")))
        await p.append({"id": "r1", "value": 1})
        await asyncio.sleep(float(os.environ.get("AI198_GAP", "0")))
        await p.append({"id": "r2", "value": 2})
        try:
            print("RESULT", json.dumps(await asyncio.wait_for(handle.result(), 40)))
        except Exception as err:
            print("FAILED", type(err).__name__, str(err)[:600])
        history = await handle.fetch_history()
        pathlib.Path("streams_demo/debug-history.json").write_text(history.to_json())
        for event in history.events:
            name = event.DESCRIPTOR.fields_by_name["event_type"].enum_type.values_by_number[event.event_type].name
            extra = ""
            if event.HasField("marker_recorded_event_attributes"):
                extra = event.marker_recorded_event_attributes.marker_name
            if event.HasField("workflow_task_failed_event_attributes"):
                extra = event.workflow_task_failed_event_attributes.failure.message[:200]
            print("EV", event.event_id, name.replace("EVENT_TYPE_", ""), extra)
    await provider_setup.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
