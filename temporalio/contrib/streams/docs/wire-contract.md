# Stream wire contract

> This package is experimental and may change in future versions.

This page is the contract another SDK, or a tool, follows to read and write
the same streams in Redis as Core's store, which every Core-based SDK uses.
Everything here is part of the stored format: change it and existing
streams stop working. The canonical scripts are in `crates/streams/lua/` of
the Core repository: `append.lua`, `stage.lua` and `promote.lua`, with the
trim and refresh they share in `keep.lua`.

## Redis

Redis 7.0 or later. Every write that must be atomic runs as one Lua script.

## Key layout

All keys of one Workflow's streams share a Redis Cluster hash tag, the
Workflow's run chain:

```
<prefix>:{<namespace>:<workflow id>:<first run id>}<suffix>
```

- `<prefix>` is the store's `key_prefix`, default `temporal-streams`.
- `<first run id>` is the first run of the Workflow's run chain
  (`first_run_id` from describing the Workflow, or
  `first_execution_run_id` inside it). Keying by the chain lets a reader
  follow Continue-as-New with no handoff.
- Each of `<prefix>`, `<namespace>`, `<workflow id>`, `<first run id>` and
  `<topic>` is percent-encoded as UTF-8, keeping only `A-Z a-z 0-9 - . _ ~`
  (Python's `urllib.parse.quote(part, safe="")`). So no part can contain
  `:`, `{` or `}`, and no two streams share a key.

| Suffix | Type | Holds |
|---|---|---|
| `:t:<topic>` | stream | The log of one topic |
| `:t:<topic>:meta` | hash | Dedupe state and the tombstone of that log |
| `:chain` | hash | `closed` = `1` once the run chain has ended |
| `:stage:<token>` | list | A staged Workflow publish: topic, record, topic, record, ... |
| `:stages` | hash | Every stage not yet promoted or aborted |

## Log entries

Each entry of a log has one field, `r`, whose value is the serialized
`temporal.sdk.streams.v1.StreamRecord` protobuf
(`crates/protos/protos/local/temporal/sdk/streams/v1/message.proto` in
Core). The package
`temporal.sdk.streams` is reserved for this envelope, so no other proto may
use it:

| Field | Number | Meaning |
|---|---|---|
| `body` | 1 | The published value as a `temporal.api.common.v1.Payload`, after the payload codec |
| `metadata` | 2 | Map of string to `Payload`, see below |
| `topic` | 3 | The topic name |
| `kind` | 4 | `STREAM_RECORD_KIND_DATA` (1) or `STREAM_RECORD_KIND_FINISH` (2). 0 reads as DATA. 3 is reserved, since readers synthesize `SUPERSEDED` with it |
| `producer_id` | 5 | Who wrote it; empty when the owning Workflow did. An Activity writes as `<activity id>@<scheduling run id>` |
| `attempt` | 6 | The producer's attempt; 0 for the owning Workflow |
| `sequence` | 7 | The record's position in its attempt, starting at 1; 0 for the owning Workflow |

Metadata keys, each a `Payload` with `encoding` = `binary/plain`:

- `temporal.io/content-hash`: the lowercase hex SHA-256 of the body's
  deterministic protobuf serialization, taken before the payload codec.
  Only the writer that made it compares it, since protobuf's deterministic
  serialization is stable within one implementation, not across languages.
- `temporal.io/run-id`: the run that published the record, on records the
  owning Workflow published.

The entry id that Redis assigns is the record's position. Readers never
see a `SUPERSEDED` record in the log: a reader synthesizes one when a
producer's newer attempt first appears.

## Appending a batch

One script, with keys `log`, `meta` and `chain`, does the following in
order:

1. Read the producer's high-water field from `meta`. The field name is
   `hw:<length of producer id in UTF-8 bytes>:<producer id>:<attempt>`. The
   value is `<first sequence>|<record count>|<first entry id>|<last entry
   id>|<digest>`, for the newest batch.
2. If the batch's first sequence equals the held one: with the same digest,
   return the held first and last entry ids and write nothing; with another
   digest, refuse (divergent). If the batch's first sequence is below the
   held one plus its record count, refuse (stale).
3. If `chain` has `closed`, refuse (closed).
4. If the log key is missing but `meta` has `last`, set `meta.trimmed` to
   `last`, because the log expired whole.
5. `XADD` each record with id `*` and field `r`.
6. Set the high-water field for this batch.
7. Trim and refresh, as below.

The digest is the lowercase hex SHA-256 over the batch's records before the
codec: for each record, its deterministic serialization's length as 8 bytes
big-endian, then the bytes. Because it is taken before the codec, a codec
that encrypts with a fresh nonce still deduplicates a retry. Only the
writer that made it compares it, as with the content hash.

Trim and refresh:

- `XREVRANGE log (<now - retention>-0 - COUNT 1` finds the newest record
  about to be trimmed. If there is one, `XTRIM log MINID <now - retention>-0`
  runs and `meta.trimmed` is set to that id. `now` is from `TIME`, in
  milliseconds.
- `PEXPIRE log <retention>`.
- `HINCRBY meta added <records>`, `HSET meta last <last entry id>`,
  `PEXPIRE meta <retention + 30 days>`.
- `HSCAN meta <meta.hwscan or 0> MATCH hw:* COUNT 50`, then `HDEL` each
  high-water field whose last entry id is older than `<now - retention>`,
  and `HSET meta hwscan <next cursor>`. A retry of a batch that old is
  accepted again, and its records were trimmed anyway.

Every writer of a chain must use the same retention. A writer with a
shorter one trims records that other writers' readers expect to keep, so
the shortest retention wins.

## A Workflow's own publish

1. Stage: one script runs `RPUSH stage <topic> <record> ...` (in chunks of
   1000 values, since Lua's `unpack` is bounded), sets
   `PEXPIRE stage <retention + 30 days>`, and sets
   `HSET stages <token> <run id>\x1f<history floor>\x1f<topic>\x1f...`.
   Topic names hold no control characters, so `\x1f` can't appear in one.
   It sets `PEXPIRE stages <retention + 30 days>` only when `PTTL stages`
   is lower, so the hash outlives every stage it lists.
2. Lang sends each record's topic, kind, body, content hash and
   plaintext size in the completion's output commit, and Core builds the
   stored record and the manifest from them. The manifest's fingerprint
   (version 2) covers each record's topic, kind, content hash and size, as
   length-prefixed fields, and never the run id, since a reset run replays
   the markers of its base run.
3. The Workflow Task's commit is recorded by Core in a
   `core_external_stream` marker whose output manifest carries the stage
   token.
4. Promote, once History shows that marker: one script first checks that
   every record's topic is among the topics it was given, and writes nothing
   if not. Then it deletes the `stages` field. If the stage still exists,
   it `XADD`s each record to its
   topic's log in order, trims and refreshes each log as above, and deletes
   the stage. Promoting twice does nothing. The script returns the number
   of records added, or -1 when the `stages` field existed but the stage
   was gone: retention dropped committed output, and Core logs a
   warning.
5. Abort, once History shows that the stage's commit cannot happen: delete
   the stage and its `stages` field. That is when the staging Workflow Task
   failed or timed out, when the run closed without the marker, or when a
   completion carries other markers at the same history floor and not this
   stage's token. Every commit of one attempt rides that attempt's
   completion, so such a stage belongs to an attempt that failed. A Worker
   applies that last rule only to a stage it held across an eviction of the
   run, since its own attempt may still be committing.
6. Repair: a reader settles the `stages` fields a stopped Worker left. It
   takes the stages pending at that moment by run start time and then
   history floor, and stops at the first stage History has not decided yet.
   A stage whose run History can't find is left to its expiry, since a
   replica behind a failover may not have the run yet. Order holds only
   among the stages one repair settles: a later run's Worker promotes its
   own output at once, so a stopped Worker's output that a reader repairs
   later lands after it (a known limit of DD-41).

## Cursors

A cursor token is `redis:<stream hash>:<entry id>`.

- `<stream hash>` is the first 8 lowercase hex characters of the SHA-256
  over namespace, owner kind (`workflow`), Workflow id and topic, each as
  UTF-8 bytes prefixed by its length as 8 bytes big-endian. The run id is
  not part of it, so a cursor stays valid across Continue-as-New. So it
  also can't tell two chains of one Workflow id apart.
- The empty token is `BEGINNING` and `$end` is `END`.

A reader refuses a token from another store or with another stream
hash, and resumes strictly after the entry id. It reports the cursor as
expired when `meta.trimmed` is newer than it, or, if the log is gone, when
`meta.last` is newer than it.

## Closing

Any party that sees the Workflow's run chain end (complete, fail, cancel,
terminate or time out, but not Continue-as-New) sets `HSET chain closed 1`
and `PEXPIRE chain <retention + 30 days>`.
