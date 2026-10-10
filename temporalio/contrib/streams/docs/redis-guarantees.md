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
publishes nothing.

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
a durable write log, such as Amazon MemoryDB, or `appendfsync always` on a
primary without automatic failover.

Recommended for production: `appendonly yes` with `appendfsync everysec` or
`always`, at least one replica, and the settings below.

## Settings the provider needs

- **Redis 7.0 or later.** The provider refuses an older server the first
  time it talks to it. Tested with Redis 7.0, 7.4, 8.10 and Valkey 8.
- **`maxmemory-policy noeviction`.** Under memory pressure, an evicting
  policy drops whole keys: a stream's records, its dedupe state, or a
  staged Workflow publish. The provider logs a warning when it can read
  another policy. With `noeviction`, a full Redis refuses writes instead,
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
- A staged Workflow publish that is never made visible expires with
  retention.
- Deleting a Workflow does not delete its streams. To remove them at once,
  for example for a deletion request, call
  `RedisStreams.delete_workflow_streams(namespace, workflow_id)`. It walks
  the keyspace with `SCAN`, so run it rarely.

## When a Workflow closes

Redis cannot see a Workflow close, so the provider marks the stream closed
when someone notices: the Worker after a publishing Workflow's final
Workflow Task, a producer when it first writes, or a reader when its read
ends. After that, appends fail with `StreamClosedError`.

Between the Workflow's final Workflow Task and that mark there is a closing
window in which appends are still accepted. It is usually short. If the
Worker stops before it marks the stream, the window lasts until a reader or
a new producer notices. Records written in the window stay readable.
Continue-as-New does not close a stream: the next run keeps publishing to
it.

## A Worker that stops

If a Worker stops after Temporal accepted a Workflow Task but before it
made that task's records visible, the records are not lost. They stay
staged in Redis. The next Worker that replays the Workflow makes them
visible, and so does any reader of the stream, after it checks the
Workflow's History. Each record becomes visible exactly once.

## Access control

Temporal does not authorize or meter record traffic. Records go straight
between your applications and your Redis, so access is Redis's job.

Every key of a namespace's streams starts with
`<prefix>:{<namespace>:`, where `<prefix>` is the provider's `key_prefix`
(default `temporal-streams`). Each part is percent-encoded, so a namespace
`my-ns` gives keys under `temporal-streams:{my-ns:`. A Redis ACL user for
one namespace's applications:

```
ACL SETUSER streams-app on >secret resetkeys ~temporal-streams:{my-ns:* resetchannels -@all +evalsha +eval +script|load +multi +exec +xadd +xread +xrevrange +xrange +xtrim +xlen +hget +hset +hgetall +hincrby +hdel +rpush +lrange +exists +del +unlink +pexpire +pttl +time +info +config|get +ping +hello +client|setinfo
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
