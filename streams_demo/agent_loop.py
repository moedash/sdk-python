"""One agent loop, written once, run on every stream provider.

Reads input, decides on it, publishes the decision, and runs an ordinary
Activity in the same workflow task; the Activity reports on its own
workflow's stream in turn. Also handles the two control records the contract
defines, so a retried producer and a finished topic are exercised rather than
described.

This file is byte-identical in the server-side tree and the client-side tree.
Nothing in it names a provider: the workflow asks its runtime, the Activity
asks its context, and the process that runs them registered the provider once.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import activity, streams, workflow
from temporalio.streams import RecordKind

# Defined once and shared by the workflow, the Activity and the demo's
# reader, so the type each topic carries is stated in one place.
DECISIONS = streams.topic("decisions", dict[str, Any])
INPUTS = streams.topic("inputs", dict[str, Any])
RECEIPTS = streams.topic("receipts", dict[str, Any])


@activity.defn(name="RecordDecision")
async def record_decision(decision: dict[str, Any]) -> str:
    """An ordinary command in the same task as the publish.

    Appends its receipt onto its own workflow's stream too, under the
    Activity's own identity, so a reader outside sees the decision and the
    record of it side by side.
    """
    receipt = f"recorded:{decision['source']}:{decision['branch']}"
    await activity.stream_handle().producer(topic=RECEIPTS).append({"receipt": receipt})
    return receipt


def decide(token: dict[str, Any]) -> dict[str, Any]:
    """The decision the workflow is here to make."""
    if token["value"] % 2 == 0:
        return {
            "source": token["id"],
            "branch": "even",
            "computed": token["value"] * 10,
        }
    return {"source": token["id"], "branch": "odd", "computed": token["value"] + 100}


@workflow.defn(name="StreamContractDemo", sandboxed=False)
class AgentLoop:
    """Read, decide, write, until the producer says it has finished."""

    @workflow.run
    async def run(self, limit: int) -> list[dict[str, Any]]:
        """Decide on at most ``limit`` inputs, then return the trace."""
        inputs = workflow.stream_reader(INPUTS)
        decisions = workflow.stream_writer(DECISIONS)
        trace: list[dict[str, Any]] = []
        accepted = 0
        try:
            async for record in inputs:
                if record.kind is RecordKind.SUPERSEDED:
                    # A newer attempt of the same producer started writing. The
                    # decisions already published stand, so the workflow says so
                    # rather than pretending they can be withdrawn.
                    assert record.supersession is not None
                    trace.append(
                        {
                            "kind": "superseded",
                            "producer": record.producer_id,
                            "replaced": record.supersession.previous_attempt,
                            "attempt": record.supersession.attempt,
                        }
                    )
                    decisions.publish(
                        {"retracting_attempt": record.supersession.previous_attempt}
                    )
                    continue
                if record.kind is RecordKind.FINISH:
                    # The producer says it is done, which is what ends the loop.
                    # Counting decisions instead would leave the terminal record
                    # unread and let the workflow finish while its producer is
                    # still writing.
                    trace.append({"kind": "finish", "producer": record.producer_id})
                    break
                assert record.value is not None
                decision = decide(record.value)
                decisions.publish(decision)
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
                if accepted >= limit:
                    # A bound so a stuck producer cannot run this forever. The
                    # terminal record above is the ordinary way out.
                    break
        finally:
            inputs.close()
        decisions.finish()
        return trace
