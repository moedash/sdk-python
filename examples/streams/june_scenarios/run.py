r"""Run every June scenario, in order, on one provider.

    python -m examples.streams.june_scenarios.run native --address 127.0.0.1:7333
    python -m examples.streams.june_scenarios.run workflow_streams --address 127.0.0.1:7333
    python -m examples.streams.june_scenarios.run memory --address 127.0.0.1:7333
    python -m examples.streams.june_scenarios.run native --address 127.0.0.1:7333 --only s2,s5

Each scenario connects, runs and closes on its own, exactly as it does when
run as its own module, so one that a provider cannot serve prints why and
the next one starts clean. ``s8`` needs the server's Nexus HTTP ingress,
which ``--http`` names.
"""

from __future__ import annotations

import asyncio

from examples.streams.june_scenarios import (
    _common,
    s1_client_consumes,
    s2_standalone_streams,
    s3_workflow_producer,
    s4_workflow_as_generator,
    s5_activity_producers,
    s6_activity_as_generator,
    s7_workflow_consumer,
    s8_nexus_consumers,
)

SCENARIOS = {
    "s1": s1_client_consumes.run,
    "s2": s2_standalone_streams.run,
    "s3": s3_workflow_producer.run,
    "s4": s4_workflow_as_generator.run,
    "s5": s5_activity_producers.run,
    "s6": s6_activity_as_generator.run,
    "s7": s7_workflow_consumer.run,
    "s8": s8_nexus_consumers.run,
}


async def main() -> None:
    """Run the scenarios named by ``--only``, or all of them."""
    parser = _common.parser(__doc__ or "")
    parser.add_argument(
        "--only", default="", help="comma-separated scenarios, for example s1,s5"
    )
    args = parser.parse_args()
    chosen = [name for name in args.only.split(",") if name] or list(SCENARIOS)
    unknown = [name for name in chosen if name not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenarios {unknown}; choose from {list(SCENARIOS)}")
    for name in chosen:
        await SCENARIOS[name](args)


if __name__ == "__main__":
    asyncio.run(main())
