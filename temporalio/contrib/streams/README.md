# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it, and so do its Activities and any client. Outside readers follow it from
a cursor. Records live in the application's Redis and never pass through
Temporal or its History, so publishing costs no Actions and no History
events.

## Install and register

```
pip install "temporalio[redis]"
```

Register the provider once, on the client. Every Worker built from that
client publishes through it.

```python
from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams

client = await Client.connect(
    "localhost:7233", plugins=[RedisStreams("redis://localhost:6379")]
)
```

Redis 7.0 or later is required. Use `maxmemory-policy noeviction`: an
evicting Redis drops whole stream keys under memory pressure, and the
provider logs a warning when it sees another policy.

## Path A: a Workflow publishes, a client reads

Define a topic once with the type its records decode to, publish from the
Workflow, and read from a client. A Workflow's own publish becomes visible
when its Workflow Task is accepted, and never if the task fails.

```python
PROGRESS = topic("progress", dict)

@workflow.defn
class ProcessOrder:
    @workflow.run
    async def run(self, order_id: str) -> str:
        progress = workflow_writer(PROGRESS)
        for step in ("reserved", "charged", "shipped"):
            progress.publish({"order": order_id, "step": step})
            await workflow.sleep(timedelta(milliseconds=100))
        progress.finish()
        return "done"

async for record in get_stream_handle(client, "order-1").read(topic=PROGRESS):
    if record.kind is RecordKind.DATA:
        print(record.value["step"])
```

The read ends when the Workflow's run chain closes. It follows
Continue-as-New, and `record.run_id` says which run published each record.
To resume later, keep the last `record.cursor` and pass it back as
`read(after=cursor)`.

## Path B: an Activity streams to its Workflow

An Activity writes to the stream of the Workflow that scheduled it, as
itself: its Activity id and its Temporal attempt. When a retry starts
writing, readers first see a `SUPERSEDED` record, so they can drop what the
failed attempt wrote.

```python
TOKENS = topic("tokens", str)

@activity.defn
async def generate(prompt: str) -> str:
    producer = activity_handle().producer(topic=TOKENS)
    ...
    await producer.append(*words)
    await producer.finish()

text = []
async for record in get_stream_handle(client, "answer-1").read(topic=TOKENS):
    if record.kind is RecordKind.SUPERSEDED:
        text.clear()
    elif record.kind is RecordKind.DATA:
        text.append(record.value)
```

Both samples run in full in `tests/contrib/streams/samples/`.

## What to know

- **Retries deduplicate.** An append that fails with
  `StreamOutcomeUnknownError` can be repeated on the same producer. The store
  returns the original position and writes nothing twice.
- **Retention.** A stream keeps records for `retention` (7 days by default,
  set on `RedisStreams`) and dies that long after its last write. A cursor
  past dropped records raises `StreamExpiredError`.
- **Closing.** Once the Workflow's run chain ends, appends are refused with
  `StreamClosedError`, after a short closing window.
- **Deleting.** Deleting a Workflow does not delete its streams.
  `RedisStreams.delete_workflow_streams` removes them at once.
- **Not in this release.** Reading a stream inside a Workflow, and streams
  owned by an Activity or by no one, raise `StreamUnsupportedError`.

## Further reading

- [Redis provider: guarantees and setup](docs/redis-guarantees.md)
- [Stream wire contract](docs/wire-contract.md)
