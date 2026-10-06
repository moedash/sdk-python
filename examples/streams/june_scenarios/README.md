# Roey's June scenarios, on the shipped surface

Every scenario in Roey's Notion page "Streaming Design Discussion Prep Notes"
(June 2, under "Streaming Links") mapped onto `temporalio.streams` as it
ships on this branch. Each file opens with his scenario heading, a status,
and one sentence why. Where his sketch uses a call shape we do not have, the
docstring shows his shape in one line and the code uses ours. Nothing here
reaches into private SDK code or adds a feature.

| Roey's scenario | File | Status | Note |
|---|---|---|---|
| Client starts and consume stream: primary and named | `s1_client_consumes.py` | implemented | Default topic with no name, a typed topic, `last=N`, `after=END`, `BEGINNING` on a moved floor (memory only) |
| Client starts and consume stream: standalone alt 1, 2, 3 | `s2_standalone_streams.py` | alts 1, 2 and 3 implemented | Alt 1 as a read that parks until `create_stream` and an append land (memory and Redis answer `StreamNotFoundError` rather than parking); alt 2 as `client.create_stream`, a policy floor, `close()` and `StreamClosedError`; alt 3 as a stream created first and passed into the workflow start as a `StreamRef`, opened in the activity with `activity.stream_handle(ref)`. A start that commits the stream with the workflow remains the design question. Workflow Streams declines |
| Workflow as Producer: as named handle | `s3_workflow_producer.py` | implemented | His turn loop with continue-as-new; the client follows the chain live |
| Workflow as Producer: as return type | `s4_workflow_as_generator.py` | emulated | Default-topic publishes plus `FINISH`, result from the workflow; the generator signature is sugar not built |
| Activity as Producer: as named handle | `s5_activity_producers.py` | implemented | Workflow topic (Path B), `scope="activity"`, standalone activity; all three on memory and Redis, the last two skipped on Workflow Streams |
| Activity as Producer: as return type | `s6_activity_as_generator.py` | emulated | Appends plus a heartbeat checkpoint; the retry resumes and readers see `SUPERSEDED` |
| Workflow as Consumer | `s7_workflow_consumer.py` | implemented; foreign stream unsupported | Own inbound topic across continue-as-new, handing over per batch with one producer per batch and carrying a checkpoint; runs on Workflow Streams and Redis, memory skips by design; reading a foreign stream from a workflow is rule 5 |
| Client as Consumer over Standalone Nexus | `s8_nexus_consumers.py` (a) | implemented | Activity reads through the `NexusStreams` front and resumes from a heartbeat cursor |
| Nexus operation handler | `s8_nexus_consumers.py` (b) | implemented | The operation returns `temporalio.streams.StreamRef`, taken from the producing workflow's handle, and the client opens it with `get_stream_handle(ref)` on a client whose provider is the front; a stream type of its own in the operation IDL is the nexgen follow-on |
| Workflow as Consumer over Nexus | `s8_nexus_consumers.py` docstring | unsupported by design | A workflow's reads ride its Workflow Task and never cross Nexus |

## Running

Each file runs on its own and takes the provider's name, the same way the
examples one directory up do. `run.py` runs them all in order:

```sh
python -m examples.streams.june_scenarios.run workflow_streams
python -m examples.streams.june_scenarios.run memory
python -m examples.streams.june_scenarios.run redis --redis redis://127.0.0.1:6379
python -m examples.streams.june_scenarios.s2_standalone_streams memory
```

`s5` (c) needs a server with standalone activities, and `s8` needs the
server's Nexus HTTP ingress (`--http`, default `http://127.0.0.1:7243`);
a dev server started with an HTTP port has both. `s8` creates and deletes
its own Nexus endpoint. `memory` is offered here, not in the parent
examples, because it is not replay-safe; these scenarios keep a warm cache.
`memory` and `redis` hold standalone streams, so `s2` runs on both; `redis`
runs with `--redis` naming a local Redis. Every scenario ran green on all
three providers against a dev server, apart from the refusals below.

A scenario a provider cannot serve says so in its output and moves on:
`s2` on `workflow_streams`, which keeps a stream inside a workflow's log,
`s5` (b) and (c) on `workflow_streams`, `s1` (d) on anything but `memory`,
and `s7` on `memory`, which keeps one topic across a chain rather than one
per run.
