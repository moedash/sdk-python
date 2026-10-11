# Moving from Workflow Streams

> This package is experimental and may change in future versions.

`temporalio.contrib.workflow_streams` keeps a stream inside the Workflow:
publishes arrive as Signals, readers poll with Updates, and the records
live in the Workflow's state and History. `temporalio.contrib.streams`
keeps the records in your Redis instead. Publishing costs no Signals, and
records stay out of History. A Workflow Task that publishes adds one marker
event with a small manifest, far less than a Signal per batch. Readers do
not need a running Worker, and any number of readers can follow one stream.
This page maps one to the other.

## What maps

| Workflow Streams | Streams |
|---|---|
| `WorkflowStream()` in `@workflow.init` | Nothing to construct. Register `RedisStreams` once, on the client. |
| `stream.topic("events", type=E).publish(e)` in the Workflow | `workflow_writer(topic("events", E)).publish(e)` |
| `WorkflowStreamClient.from_within_activity()` and `topic(...).publish(...)` in an Activity | `activity_handle().producer(topic=...).append(...)` |
| `WorkflowStreamClient.create(client, workflow_id)` and `topic(...).publish(...)` | `get_stream_handle(client, workflow_id).producer(topic=..., producer_id=..., attempt=...).append(...)` |
| `client.subscribe(["events"], result_type=E)` | `get_stream_handle(client, workflow_id).read(topic=topic("events", E))` |
| Integer offsets, `from_offset=` | Opaque cursors: keep `record.cursor`, pass `after=cursor` |
| Publisher dedupe by publisher id and sequence | Producer id, attempt and sequence. A retry of the newest batch is written once. |
| `stream.continue_as_new(...)` and `prior_state` | Nothing. The stream follows the run chain. |
| `batch_interval`, `force_flush` | Each `append` is one atomic batch. A Workflow's publishes in one Workflow Task commit together. |
| `get_offset()` | `latest()` returns the newest cursor |

Differences to plan for:

- **Cursors are opaque.** Do not compare them or do arithmetic on them. A
  cursor belongs to one stream, and another stream refuses it.
- **Producers declare an attempt.** A producer outside an Activity passes
  `producer_id` and `attempt`, and raises the attempt when it restarts its
  work. A retry inside an attempt deduplicates. A new attempt makes readers
  see `SUPERSEDED` first.
- **A Workflow's publish is visible when its Workflow Task is accepted**,
  not when a Signal is processed.
- **Retention is time-based.** Records expire `retention` after they were
  written (7 days by default). There is no `truncate(up_to_offset)`.
- **Payload codec.** Workflow Streams runs the codec once per Signal or
  Update, on the batch. Streams runs it on each record body before storing
  it. Readers decode with the client's data converter as usual.

## What does not map

- **Reading inside the Workflow.** A Workflow cannot read a stream in this
  release (`workflow_reader` raises `StreamUnsupportedError`). If a
  Workflow reads its own Workflow Stream today, keep that part on Workflow
  Streams, or send the Workflow what it needs, for example as a Signal.
- **Activity-owned and standalone streams.** Only a Workflow owns a stream
  in this release.
- **A server-sent events bridge.** Streams ships none. Serve readers from
  your own endpoint with `get_stream_handle(...).read(...)`.
- **Records already published.** They stay where they are, in the
  Workflow's state and History. Nothing copies them to Redis. A reader that
  needs both must read the old stream to its end, then the new one.

## Moving a running application

1. Register `RedisStreams` on the client with `Client.connect`, and deploy
   Workers built from that client.
2. Move producers and readers of one topic together. A topic split across
   both libraries is two streams.
3. For Workflows already running, switch at a natural boundary, such as the
   next run after Continue-as-New, or let them finish on Workflow Streams.
   Every new publish point needs Workflow versioning, like any new command:
   a publish adds a marker to its Workflow Task, so a running Workflow that
   replays new code past it fails as nondeterministic. For example:

   ```python
   if workflow.patched("publish-to-streams"):
       workflow_writer(TOKENS).publish(token)
   else:
       await stream.topic("tokens").publish(token)
   ```
4. Remove `WorkflowStream` from a Workflow only after no reader needs its
   records.
