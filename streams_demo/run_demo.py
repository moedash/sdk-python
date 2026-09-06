"""Run the shared agent loop against whichever provider is configured.

Three cases, the same on both providers:

- read, decide, publish and an ordinary Activity in the same workflow task,
  with the smallest workflow cache the provider supports, so that as much of
  the run as it allows is rebuilt rather than remembered;
- a producer whose second attempt supersedes its first, which the reader has
  to report and the workflow has to act on;
- an outside consumer reading what the workflow published.

Byte-identical in both trees. ``provider_setup`` is what differs, and it is
the only import here that names a provider.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import time
import uuid
from typing import Any

from temporalio import streams
from temporalio.client import Client
from temporalio.worker import Worker

sys.path.insert(0, str(Path(__file__).resolve().parent))
from agent_loop import INPUTS, AgentLoop, record_decision  # noqa: E402
import provider_setup  # noqa: E402

DECISION_LIMIT = 8
EXPECTED_OUTPUT = 6


async def collect_output(client: Client, workflow_id: str, want: int) -> list[dict]:
    reader = await streams.consumer(client, workflow_id=workflow_id)
    seen: list[dict] = []
    async for record in reader.read(type=dict, topic="decisions"):
        seen.append({"kind": record.kind.name, "value": record.value})
        if len(seen) >= want:
            break
    return seen


async def main() -> int:
    out = Path(__file__).resolve().parent / f"results-{provider_setup.NAME}"
    out.mkdir(exist_ok=True)
    target, options = await provider_setup.open()
    client = await Client.connect(target, namespace="default")
    streams.configure(**options)

    uid = f"ai198-contract-{provider_setup.NAME}-" + uuid.uuid4().hex
    record: dict[str, Any] = {
        "provider": provider_setup.NAME,
        "workflow_id": uid,
        "target": target,
        "max_cached_workflows": provider_setup.WORKFLOW_CACHE,
    }

    async with Worker(
        client,
        task_queue=uid,
        workflows=[AgentLoop],
        activities=[record_decision],
        max_cached_workflows=provider_setup.WORKFLOW_CACHE,
        **streams.worker_options(),
    ):
        handle = await client.start_workflow(
            AgentLoop.run, DECISION_LIMIT, id=uid, task_queue=uid
        )
        output = asyncio.create_task(collect_output(client, uid, EXPECTED_OUTPUT))

        # The first attempt writes two records and then stops, as a failed
        # activity would. The second writes different inputs under the same
        # logical producer, which is what the reader has to report.
        first = await streams.producer(
            client, workflow_id=uid, stream=INPUTS, producer_id="model", attempt=1
        )
        await first.append({"id": "r1", "value": 1}, {"id": "r2", "value": 2})
        await asyncio.sleep(0.5)
        second = await streams.producer(
            client, workflow_id=uid, stream=INPUTS, producer_id="model", attempt=2
        )
        await second.append({"id": "r3", "value": 3}, {"id": "r4", "value": 4})
        await second.finish()

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            history = await handle.fetch_history()
            failed = [e for e in history.events if e.event_type == 9]
            if failed:
                record["outcome"] = "workflow_task_failed"
                record["failures"] = [
                    e.workflow_task_failed_event_attributes.failure.message
                    for e in failed
                ]
                break
            if any(e.event_type == 2 for e in history.events):
                record["outcome"] = "completed"
                record["trace"] = await handle.result()
                break
            await asyncio.sleep(0.1)
        else:
            record["outcome"] = "timed_out"

        try:
            record["observed_output"] = await asyncio.wait_for(output, timeout=20)
        except asyncio.TimeoutError:
            output.cancel()
            record["observed_output"] = "timed_out"

    history = await handle.fetch_history()
    record["history_events"] = len(history.events)
    (out / "history.json").write_text(history.to_json())
    await provider_setup.close()
    (out / "results.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))
    return 0 if record["outcome"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
