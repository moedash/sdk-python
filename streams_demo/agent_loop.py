"""One agent loop, written once, run on both stream providers.

Reads input, decides on it, publishes the decision, and runs an ordinary
Activity in the same workflow task. Also handles the two control records the
contract defines, so a retried producer and a finished topic are exercised
rather than described.

This file is byte-identical in the server-side tree and the client-side tree.
Only the worker that runs it differs, and only in which provider it is given.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import activity, streams, workflow

DECISIONS = "decisions"
INPUTS = "inputs"


@activity.defn(name="AI198RecordDecision")
async def record_decision(decision: dict[str, Any]) -> str:
    """An ordinary command in the same task as the publish."""
    return f"recorded:{decision['source']}:{decision['branch']}"


def decide(token: dict[str, Any]) -> dict[str, Any]:
    """The decision the workflow is here to make."""
    if token["value"] % 2 == 0:
        return {"source": token["id"], "branch": "even", "computed": token["value"] * 10}
    return {"source": token["id"], "branch": "odd", "computed": token["value"] + 100}


@workflow.defn(name="AI198StreamContractDemo", sandboxed=False)
class AgentLoop:
    """Read, decide, write, until the input topic finishes."""

    @workflow.run
    async def run(self, expected: int) -> list[dict[str, Any]]:
        inputs = streams.reader(
            INPUTS, type=dict, idle_timeout=timedelta(seconds=1)
        )
        decisions = streams.writer(DECISIONS)
        trace: list[dict[str, Any]] = []
        accepted = 0
        try:
            async for record in inputs:
                if record.kind is streams.RecordKind.SUPERSEDED:
                    # A newer attempt of the same producer started writing. The
                    # decisions already published stand, so the workflow says so
                    # rather than pretending they can be withdrawn.
                    trace.append(
                        {
                            "kind": "superseded",
                            "producer": record.producer,
                            "replaced": record.value.previous_attempt,
                            "attempt": record.value.attempt,
                        }
                    )
                    await decisions.publish(
                        {"retracting_attempt": record.value.previous_attempt}
                    )
                    continue
                if record.kind is streams.RecordKind.FINISH:
                    trace.append({"kind": "finish", "producer": record.producer})
                    continue
                decision = decide(record.value)
                await decisions.publish(decision)
                receipt = await workflow.execute_activity(
                    record_decision,
                    decision,
                    activity_id=f"decision-{decision['source']}",
                    start_to_close_timeout=timedelta(seconds=10),
                )
                trace.append(
                    {"kind": "decision", "value": decision, "receipt": receipt}
                )
                accepted += 1
                if accepted >= expected:
                    break
        finally:
            inputs.close()
        await decisions.finish()
        return trace
