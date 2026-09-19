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
front = NexusStreams(endpoint=endpoint_id)
stream = front.get_stream_handle(client, workflow_id)
producer = stream.producer(topic="inputs", producer_id="model", attempt=1)
```

The endpoint has to exist and route to the handler worker's task queue, and
the provider takes its id rather than its name:

```sh
temporal operator nexus endpoint create --name streams-e2e \
    --target-task-queue streams-handlers-e2e
temporal operator nexus endpoint get --name streams-e2e -o json | jq -r .id
```

See `tests/streams/test_nexus_provider.py` for the handler worker, and
`examples/streams/run.py nexus --endpoint <id>` for the whole loop behind it.

The conformance suite runs the same expectations on every provider:
`pytest tests/streams/` (memory and Workflow Streams on the test server), plus
`STREAMS_LIVE=native|redis|nexus` for the suites that need a store.

## The examples

`examples/streams/` is the same thing written as a worked example rather than
a measurement run: `agent.py` holds the workflow and activities, identical on
every provider, and `run.py` picks one. See its `configure_provider`, which
is the whole difference between the options.
