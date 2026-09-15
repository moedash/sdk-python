# One workflow file, three stream providers

`agent_loop.py` and `run_demo.py` never name a provider. The provider is a
configuration choice, and this branch carries all three options plus the
Nexus front, so the same loop runs on each of them by changing one
environment variable.

The loop: an Activity streams records into the workflow's `inputs` stream,
the workflow reads them, publishes a decision per record onto `decisions`,
runs an ordinary Activity in the same task, and an outside consumer follows
the decisions.

## Today's Workflow Streams (Option 0)

Records ride the shipped feature's Signals and Updates and live in the
workflow's History. Nothing to deploy; Option 0's limits apply.

```sh
temporal server start-dev --headless &
STREAMS_PROVIDER=workflow_streams python streams_demo/run_demo.py
```

## Client-side (Redis)

The store is a Redis the process names; the workflow's own writes go through
the staged commit, so a failed Workflow Task leaks nothing.

```sh
redis-server --port 6399 &
temporal server start-dev --headless &
AI198_REDIS_URL=redis://127.0.0.1:6399 \
STREAMS_PROVIDER=redis python streams_demo/run_demo.py
```

## Server-side (native)

Streams live on the Temporal server; publishes are commands applied when the
task is accepted, and consumed ranges arrive on Workflow Tasks. Needs a
server built from `moedash/temporal` `moe/AI-198-server-side-streams`.

```sh
TEMPORAL_ADDRESS=127.0.0.1:7233 \
STREAMS_PROVIDER=native python streams_demo/run_demo.py
```

## The Nexus front (outside surface)

Outside producers and consumers can go through one Temporal-authenticated
endpoint instead of naming a store; the handler worker hides whichever
provider it is configured with. Workers still configure a storage provider,
because workflow reads and writes ride the Workflow Task.

```python
front = streams._provider.instance("nexus", endpoint=endpoint_id)
producer = await front.producer(None, workflow_id=wid, stream="inputs",
                                producer_id="model", attempt=1)
```

See `tests/streams/test_nexus_provider.py` for the endpoint setup and the
handler worker.

The conformance suite runs the same expectations on every provider:
`pytest tests/streams/` (in-memory, no server), plus
`STREAMS_LIVE=workflow_streams|nexus` for the live suites.

## The examples

`examples/streams/` is the same thing written as a worked example rather than
a measurement run: `agent.py` holds the workflow and activities, identical on
every provider, and `run.py` picks one. See its `configure_provider`, which
is the whole difference between the options.
