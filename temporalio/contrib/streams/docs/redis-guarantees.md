# Redis provider: guarantees and setup

> This package is experimental and may change in future versions.

Stream records live in your Redis. Temporal never sees them, so what a
stream guarantees is what your Redis guarantees. This page says what the
provider promises, which Redis settings those promises depend on, and who
is responsible for what.

## What an acknowledged append means

An append returns once one Lua script has written the whole batch to the
stream's log. A batch is written in full or not at all, in order. A retry
of the newest batch on the same producer returns the original position and
writes nothing.

A Workflow's own publish is staged first and made visible only after
Temporal accepts the Workflow Task that published it. A task that fails
publishes nothing. While a task publishes, its progress depends on Redis:
if the stage call fails or outlasts the Workflow Task timeout, the task
fails and retries until Redis answers. Workflows that don't publish in that
task aren't affected.

A body larger than the data converter's external storage threshold is
stored as a claim, and the object behind it lives in your external store.
Stream retention doesn't expire that object, and the external store's own
lifecycle decides when it goes. If it goes first, a read of that record
raises `StreamRecordError`.

Promoting a large publish runs one script on Redis's single thread, which
reads the whole stage and writes every record. Thousands of large records
in one Workflow Task hold up every other client of that shard for the
script's run time.

How long an acknowledged record survives a failure depends on persistence:

| Redis setting | What an acknowledged record survives |
|---|---|
| `appendonly yes`, `appendfsync always` | A Redis process crash and a host power loss. Every write waits for an fsync. |
| `appendonly yes`, `appendfsync everysec` (Redis default for AOF) | A Redis process crash. A host power loss can lose about the last second of writes. |
| RDB snapshots only | Nothing written since the last snapshot. |
| No persistence | Nothing, after a restart. |

Replicas copy writes asynchronously. A failover can lose writes the primary
acknowledged but had not yet sent to the replica. The provider does not use
`WAIT`. If a failover must not lose acknowledged records, use a store with
a durable write log, or `appendfsync always` on a primary without automatic
failover. Amazon MemoryDB has a durable write log, but this release hasn't
run its tests against MemoryDB. After a failover that lost writes, a
reader's cursor can name an entry the new primary never had, and new
entries can get lower ids, so the reader skips them. Read again from
`latest()` after a known failover.

Recommended for production: `appendonly yes` with `appendfsync everysec` or
`always`, at least one replica, and the settings below.

## Settings the provider needs

- **Redis 7.0 or later.** The provider refuses an older server the first
  time it talks to it. Tested with Redis 7.0, 7.4, 8.10 and Valkey 8.
- **`maxmemory-policy noeviction`.** Under memory pressure, an evicting
  policy drops whole keys: a stream's records, its dedupe state, or a
  staged Workflow publish. If it drops a log's metadata, a reader can no
  longer tell that a cursor expired, and it reads on across the gap. The
  provider logs a warning when it can read another policy. It checks the
  policy, and the server version, once when it starts, so a later change
  goes unnoticed. With `noeviction`, a full Redis refuses writes instead,
  and producers get an error they can retry.
- **Enough memory for retention.** A stream keeps records for `retention`
  (7 days by default) after they were written. Size Redis for your write
  rate times the retention.

## Retention and cleanup

The provider cleans up after itself, with no job to run:

- Every write trims the log to `retention` and refreshes the log's expiry,
  so a stream disappears `retention` after its last write. That also covers
  Workflows that were terminated or timed out.
- Each log's metadata outlives the log by 30 days. That lets a reader tell
  a cursor whose records expired (`StreamExpiredError`) from a stream that
  never existed.
- A staged Workflow publish that is never made visible expires `retention`
  plus 30 days after it was staged.
- Each producer attempt keeps one dedupe field in the log's metadata. Once
  its last write is older than `retention`, the records it guards are gone,
  and writes to the topic drop it a few fields at a time.
- Deleting a Workflow does not delete its streams. To remove them at once,
  for example for a deletion request, call
  `RedisStreams.delete_workflow_streams(namespace, workflow_id)`. It walks
  the keyspace with `SCAN`, so run it rarely. Run it only after the chain
  closed and its producers stopped: it also removes the close flag and the
  dedupe state, so a late append writes a fresh log, and a Worker that
  hasn't promoted its last stage loses that output.

## When a Workflow closes

Redis cannot see a Workflow close, so the provider marks the stream closed
when someone notices: the Worker after a publishing Workflow's final
Workflow Task, a producer when it first writes, or a reader when its read
ends. After that, appends fail with `StreamClosedError`.

Between the Workflow's final Workflow Task and that mark there is a closing
window in which appends are still accepted. It is usually short. If the
Worker stops before it marks the stream, or the Workflow never published
itself, the window lasts until a reader or a producer notices. A producer
checks again at least once a minute, so a running producer notices within
a minute. Records written in the window stay readable.
Continue-as-New does not close a stream: the next run keeps publishing to
it.

## A Worker that stops

If a Worker stops after Temporal accepted a Workflow Task but before it
made that task's records visible, the records stay staged in Redis. The
next Worker that replays the Workflow makes them visible, and so does any
reader of the stream, after it checks the Workflow's History. Each record
becomes visible once. Two cases lose such output. A stage nobody promotes
within `retention` plus 30 days expires, and the promotion that finds it
gone logs a warning that names its token. A stage whose run History no
longer holds is never promoted, and it waits for that expiry.

A publishing Workflow can only move forward. Once it has published, only a
Worker with this release and the stream provider plugin can replay it, so
keep the plugin registered while such Workflows run, and don't roll a
fleet back to a release without them. Their next Workflow Task would fail
as nondeterministic until the rollback is undone.

## Access control

Temporal does not authorize or meter record traffic. Records go straight
between your applications and your Redis, so access is Redis's job.

Readers write too. A read marks the chain closed when it ends, and it
promotes or aborts the stages a stopped Worker left. So a reader needs the
same Redis rules as a writer, and the Temporal permissions to describe the
Workflow and read its History. A read holds one Redis connection while it
waits, so size the client's `max_connections` to the streams a process
follows.

Every key of a namespace's streams starts with
`<prefix>:{<namespace>:`, where `<prefix>` is the provider's `key_prefix`
(default `temporal-streams`). Each part is percent-encoded, so a namespace
`my-ns` gives keys under `temporal-streams:{my-ns:`. A Redis ACL user for
one namespace's applications:

```
ACL SETUSER streams-app on >secret resetkeys ~temporal-streams:{my-ns:* resetchannels -@all +evalsha +eval +script|load +multi +exec +xadd +xread +xrevrange +xrange +xtrim +xlen +hget +hset +hgetall +hincrby +hdel +hscan +rpush +lrange +exists +del +unlink +pexpire +pttl +time +info +config|get +ping +hello +client|setinfo
```

The conformance suite and a Workflow publish test run as a user with
exactly these rules, so a provider change that needs another command fails
the tests. The delete helper also needs `+scan`. `+config|get` is only used
to read `maxmemory-policy`, and the provider tolerates a server that refuses
it.

What crosses the wire:

- **Record bodies** go through your data converter's payload codec before
  they are stored, so an encrypting codec keeps them encrypted in Redis.
- **Workflow ids, topic names and producer ids** appear in key names and
  record fields in clear text. Do not put secrets in them.
- **Content hashes are not encrypted.** Each record carries the SHA-256 of
  its body as the converter produced it, before the codec, under the
  metadata key `temporal.io/content-hash`. Each producer's dedupe state in
  the log's metadata holds a SHA-256 digest of its newest batch, also taken
  before the codec. Neither reveals a body, but anyone who can read the
  keys can tell when two records carry the same value, and can confirm a
  guess of a value. If that matters for your data, add something unique,
  such as a random field, to each value.
- **Transport.** Use TLS (`rediss://`) to encrypt traffic to Redis.

## Redis Cluster

The provider supports Redis Cluster. Pass a `redis.asyncio.RedisCluster`
client; a URL string always makes a single-server client.

All keys of one run chain's streams share a hash tag, so every script and
transaction touches one slot. Different Workflows, and different run
chains of one Workflow id, spread across the cluster. The version check
and the `maxmemory-policy` check ask every primary, and
`delete_workflow_streams` scans every primary.

The conformance suite and the provider tests run against a Redis 7.4
cluster with three primaries. The cluster client also sends `CLUSTER SLOTS`
and `COMMAND` to discover the cluster, so an ACL user for it needs
`+cluster|slots +command` as well. The ACL above was tested on a single
server only.
