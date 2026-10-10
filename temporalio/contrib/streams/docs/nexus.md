# Streams over Nexus

> This package is experimental and may change in future versions.

A Nexus operation can return a stream instead of a single result. The
handler starts the operation, hands the caller a reference to a stream, and
the stream's producer keeps appending records to it. The caller Workflow
reads the records as they arrive. The operation completes when the stream
closes.

The records never pass through the caller's History. What reaches the
caller is progress: a small note that says the stream moved, with a position
and a counter. The caller then reads the records through the stream service
on the operation's endpoint.

## When to use it

Use a stream-returning operation when a caller in another namespace, or in
another team's service, needs a feed of results while the work runs. For
example a model's tokens, the steps of a long job, or messages for a chat
channel.

Inside one namespace you do not need Nexus. A Workflow or a client can read a
stream it knows the reference of directly, as in the
[package README](../README.md).

## Server setup

Streams over Nexus needs these dynamic config values on, for the namespaces
involved:

| Setting | Why |
|---|---|
| `history.enableChasm` | The stream notifier and the caller's operation run on CHASM. |
| `nexusoperation.enableChasmWorkflowOperations` and `nexusoperation.chasmWorkflowOperationsRolloutPercent: 100` | The caller's operation must use the CHASM path. Only that path accepts progress. |
| `nexusoperation.enableProgress` | The caller accepts progress deliveries. |
| `streamnotifier.enabled` | The server holds the callbacks of a stream and delivers its progress. |

Without progress, the operation still runs. The caller then gets the
completion only, and its reader sees every record at the end.

## The handler side

The handler names the stream and returns a `StreamOperationHandler`. Its
`open_stream` function usually starts the Workflow that produces the stream
and returns the stream's reference. The handler never appends; the producer
does.

```python
import nexusrpc
import nexusrpc.handler
import temporalio.nexus
from temporalio import workflow
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams.nexus import StreamOperationHandler, close_workflow_stream

MESSAGES = topic("messages", Message)

@nexusrpc.service
class ConversationService:
    respond: nexusrpc.Operation[str, str]

@workflow.defn
class Conversation:
    @workflow.run
    async def run(self, request: str) -> None:
        writer = workflow_writer(MESSAGES)
        for reply in make_replies(request):
            writer.publish(reply)
        writer.finish()
        await close_workflow_stream("done", topic=MESSAGES)

async def open_conversation(ctx, request: str) -> StreamRef:
    workflow_id = f"conversation-{ctx.request_id}"
    await temporalio.nexus.client().start_workflow(
        Conversation.run, request, id=workflow_id,
        task_queue=temporalio.nexus.info().task_queue,
    )
    return StreamRef.for_workflow(workflow_id, topic=MESSAGES.name)

@nexusrpc.handler.service_handler(service=ConversationService)
class ConversationHandler:
    @nexusrpc.handler.operation_handler
    def respond(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_conversation)
```

The handler's Worker carries the stream provider with its notify hook on, and
serves the stream service next to the handler, so the caller reads on the
same endpoint:

```python
from temporalio.contrib.streams.memory import MemoryStreams   # or RedisStreams(...)
from temporalio.contrib.streams.nexus import TemporalStreamsHandler

provider = MemoryStreams().notify_on_append()
worker = Worker(
    client,
    task_queue="conversations",
    workflows=[Conversation],
    nexus_service_handlers=[ConversationHandler(), TemporalStreamsHandler(provider)],
    plugins=[provider],
)
```

Without `notify_on_append()`, the operation still works, but the caller sees
no progress until the close.

Closing the stream completes every operation that handed it out:

- From the owning Workflow, `await close_workflow_stream(result, topic=...)`
  after its last publish. The close is recorded in History and replays.
- From outside a Workflow, for example an Activity or the process that sees
  the work end, `await provider.close_stream(client, ref, result)`.

`result` becomes the operation's result, for example a summary. A cancel
from the caller detaches its callback, and the stream keeps going for any
other caller. A start without a callback is refused.

What happens underneath:

1. The handler's start attaches the caller's callback to the stream's
   notifier on the server, and returns the stream reference in the
   operation token.
2. Each append notifies the notifier. Notifications are folded: at most one
   call is in flight, and the newest one wins.
3. The notifier delivers the latest notification to the caller as progress.
4. The close completes the caller's operation with the close result.

## The caller side

The caller Workflow starts the operation as usual and reads it with a
`StreamReader`:

```python
from temporalio.contrib.streams.nexus import StreamReader

handle = await nexus_client.start_operation(ConversationService.respond, request)
reader = StreamReader(handle, item_type=Message, endpoint=ENDPOINT)
while (batch := await reader.next()) is not None:
    for message in batch:
        ...
summary = await handle
```

A client generated from a service that declares the operation's stream
result has a `read_<op>_stream(handle)` method that builds the reader for
you, with the endpoint defaulting to the client's:

```python
reader = nexus_client.read_respond_stream(handle)
```

What `next()` does:

- It waits for progress, then reads every record after its cursor through
  the stream service on `endpoint`, and returns their bodies decoded to
  `item_type`. It never returns an empty list.
- It returns `None` once the operation completed and the last records are
  handed over. Every call after that returns `None` too.
- It hands over data records only. When a producer's earlier attempt was
  replaced by a newer one, the reader records that in
  `reader.supersessions`, in order, instead of returning it from `next()`.
  Each entry is a `ReadSupersession` with `index` (how many items had been
  handed over when it came), `cursor` and `supersession`:

  ```python
  if reader.supersessions:
      workflow.logger.warning("a producer restarted: %s", reader.supersessions[-1])
  ```

- A record whose body does not decode into `item_type` raises
  `StreamRecordError` (a `StreamError` with `.cursor`), after the records
  before it are handed over. The next call goes on past it, so the Workflow
  can catch it and keep reading.
- A failed read raises `temporalio.exceptions.NexusOperationError`. So does
  a failed operation, after the records it left are handed over.
- If the operation's token names no stream, it raises `ValueError`.

Bodies reach the Workflow decoded. The stream service decodes them with its
Worker's data converter, the read result crosses once encoded with that
Worker's payload codec, and the caller's codec decodes it. So the handler's
Worker and the caller's Worker must use the same payload codec.

Each read is a synchronous Nexus operation of the Workflow, so History
records it and a replay reads the same records. Between reads the reader
waits on the operation's progress, which is also on the handle:

```python
progress = await handle.progress()          # waits for the first progress
newer = await handle.progress(after_counter=progress.counter)
latest = handle.latest_progress             # or None
```

`progress()` returns `None` when the operation completes before newer
progress arrives. Progress comes from History, so a replay returns the same
values at the same points.

## Guarantees

- **Highest counter wins.** Each progress has a counter. The caller keeps
  the highest one and drops any lower or repeated one, so deliveries may
  arrive out of order.
- **A burst folds.** Progress that arrives while the caller is between
  Workflow Tasks waits for the next task. A burst of any size costs the
  caller at most two tasks: the one it rides, and one follow-up when newer
  progress folded in after that task was already scheduled. The Workflow
  then sees the highest counter of the burst. Intermediate counters may be
  skipped, which is why the reader reads records and does not count
  progress.
- **No History event per delivery.** Progress rides the caller's next
  Workflow Task scheduled event. History is still the record, so replay
  sees the same progress.
- **Completion never waits for progress.** The operation completes when the
  stream closes, whether or not a progress delivery is in flight. Progress
  for a closed operation is dropped.
- **Progress before the start is dropped.** The reader reads from its
  cursor when the start arrives, so nothing is lost.
- **Old receivers degrade.** A caller on a server without progress refuses
  it, and the notifier stops sending progress to that caller. The
  completion still arrives.

## Do not pass store claims across Nexus

The stream reference names a stream: its owner and its topic. It carries no
credential for the store. Do not put a Redis URL, a key prefix or any store
credential in the operation's input, result or token, and do not build a
caller that reads the handler's store directly. The caller reads through
the stream service on the handler's endpoint, so the handler keeps control
of who reads, and the caller needs no access to the handler's store.

## Sample

[`tests/contrib/streams/samples/nexus_slack_router.py`](../../../../tests/contrib/streams/samples/nexus_slack_router.py)
runs a handler that streams a conversation's messages and a caller Workflow
that reads them and files each one under its channel. Its test,
`tests/contrib/streams/test_nexus_samples.py`, runs it on a server with the
settings above and skips on a server without the stream notifier.
