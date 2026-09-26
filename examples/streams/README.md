# Streams, by path

One provider, registered once on the client. Workers built from that client
inherit it, and every context asks for its stream the same way. A topic is
defined once, with the type its records carry, and every context refers to
that definition, so no call names a type again.

```python
client = await Client.connect("localhost:7233", plugins=[provider])

INPUTS = streams.topic("inputs", Token)
DECISIONS = streams.topic("decisions", Decision)
```

| Path | Who | Call | Example file |
|---|---|---|---|
| A: the workflow publishes | workflow code | `workflow.stream_writer(PROGRESS).publish(Progress(...))`, then `.finish()` | `path_a_publish.py` |
| A: a backend follows | any process with a client | `stream = client.get_stream_handle(workflow_id)`, then `stream.read(topic=PROGRESS, after=await stream.latest(topic=PROGRESS))` | `path_a_publish.py` |
| B: an Activity produces | activity code | `await activity.stream_handle().producer(topic=INPUTS).append(Token(...))` | `path_b_produce.py` |
| B: a backend produces | any process with a client | `client.get_stream_handle(workflow_id).producer(topic=NOTES, producer_id=..., attempt=...)` | `path_b_produce.py` |
| B: a backend consumes | any process with a client | `client.get_stream_handle(workflow_id).read(topic=INPUTS)` | `path_b_produce.py` |
| C: the workflow consumes | workflow code | `async for record in workflow.stream_reader(COMMANDS)` | `path_c_consume.py` |

A plain string names a topic decided at runtime, with `result_type=` on the
call; the examples never need one.

`agent.py` and `run.py` compose all three paths in one agent, on every provider
and behind the Nexus front. `_setup.py` is the one place a store is named.

## Running

Each example takes the provider's name and runs against a dev server:

```sh
python -m examples.streams.path_a_publish workflow_streams
python -m examples.streams.path_a_publish native --address 127.0.0.1:7333
python -m examples.streams.path_a_publish redis --redis redis://127.0.0.1:6379
```

Swap `path_a_publish` for `path_b_produce`, `path_c_consume` or `run`. The
`native` provider needs a server built from the stream-carrying branch; the
`redis` provider needs a Redis to point at. `run.py` also takes `nexus`, with
an endpoint routed to the handler worker's task queue.

## Why the workflow's verbs differ

An Activity and a backend hold the same `StreamHandle`, with the same verbs,
because both act on the store at once: a producer's records are visible as
soon as the store accepts them, and a read follows the store live. Workflow
code gets two verbs of its own because its semantics differ. `publish` is
buffered and commits with the Workflow Task, so no reader can see a record
from a task that failed, and a `stream_reader` is an observation the SDK
records, so replay re-supplies the same records in the same order. That is
why `publish` is a plain call and the reader is an async iterator, and why
neither takes a client.
