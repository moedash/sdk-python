"""Streams: a channel a workflow reads, decides on, and writes.

.. warning::
    This module is experimental and may change in future versions. The
    design is meant to be the shape that goes GA; the label is the SDK's
    release convention, not a licence to break it.

The contract, in five statements:

1. **A workflow publishes only to topics of its own stream, and it publishes
   transactionally.** :meth:`temporalio.workflow.StreamWriter.publish`
   returns at once. The record is visible when the Workflow Task is accepted,
   and never if the task fails, so no reader can see a decision the workflow
   did not commit.
2. **Reading is an observation, and the SDK records it.** What
   :class:`temporalio.workflow.StreamReader` handed to workflow code,
   including the boundary where it found nothing, is committed with the
   commands that reading produced. Recovery re-supplies the same records in
   the same order.
3. **Anything that does I/O publishes on its own account.** An activity or an
   outside process writes through a :class:`StreamProducer` with a producer
   id, an attempt and a sequence, and its records are visible as soon as the
   store accepts them. Those three let a reader tell a retry from a new
   generation. A retry that carries the same content is written once; one that
   carries different content at the same sequence is refused with
   :class:`StreamProducerError`, so a divergent retry is never dropped in
   silence.
4. **A cursor is opaque and belongs to its provider.** Hand it back to resume
   strictly after the record it names; :meth:`StreamHandle.latest` positions a
   follower. Do not compare two cursors or do arithmetic on one. A read with
   no cursor yet starts at :data:`BEGINNING`, at :data:`END`, or at the last
   ``N`` records with ``last=N``.
5. **A workflow addresses its streams relative to itself, by topic.** A topic
   can be written by the workflow and by outside producers, and read by the
   workflow and by outside consumers; which of those happen is the
   application's business. A topic is defined once with :func:`topic`, with
   the type its records decode to, and that definition is shared by the
   workflow, its activities and the backend; a plain string names a topic
   decided at runtime. A call that names no topic addresses the workflow's
   default topic, :data:`DEFAULT_TOPIC`.

A provider is an object, registered once as a plugin:
``Client.connect(plugins=[provider])``; workers built from that client inherit
it, and ``Worker(plugins=[provider])`` or ``Replayer(plugins=[provider])``
registers it on a worker alone. Each context then asks for its stream the
same way. Workflow code uses :func:`temporalio.workflow.stream_reader` and
:func:`temporalio.workflow.stream_writer`. An activity uses
:func:`temporalio.activity.stream_handle`, which is its own workflow pinned
to its run unless told otherwise. Any process holding a client uses
:meth:`temporalio.client.Client.get_stream_handle`, which mirrors
``get_workflow_handle``. The explicit form,
``provider.get_stream_handle(client, workflow_id)``, stays for a process that
talks to two stores. This module keeps the shared types, the errors and the
protocols a provider implements; nothing here that workflow code imports does
I/O.

A handle is bound to its client and provider, so a stream is handed to another
process as a :class:`StreamRef`: the owner and the topic as plain data, with
no cursor and no provider name. :meth:`StreamHandle.ref` makes one, the
default data converter carries it as JSON, and the receiver opens it with
``client.get_stream_handle(ref)`` or ``activity.stream_handle(ref)`` on
whatever provider its client has.

A stream can also stand alone, with an id of its own and no owner.
``client.create_stream(stream_id, retention=...)`` creates it with a retention
policy and returns its handle, ``client.get_stream_handle(stream_id=...)``
reaches an existing one, and the handle's ``close()`` seals it, after which
appends are refused with :class:`StreamClosedError` and the retained records
stay readable. A provider whose store cannot hold an ownerless stream raises
:class:`StreamUnsupportedError` for both.

What the contract does not promise: that a :attr:`RecordKind.FINISH` record
means the writing activity succeeded, that a superseded attempt's records can
be withdrawn, or that a stream outlives the retention its provider is
configured for. Reading somebody else's stream is out of scope for this
release.

The record on the wire is ``temporal.api.stream.v1.StreamRecord`` on every
provider, with the user's value in ``body`` as an ordinary payload, so a
reader in any language decodes the same bytes and a payload codec applies. A
provider owes that body what the SDK gives every payload it sends: it encodes
it through the client's data converter, so the codec and the
:class:`temporalio.converter.ExternalStorage` drivers apply, it takes the
retry fingerprint over the converted bytes before either runs and leaves the
plaintext hash on the record under :data:`CONTENT_HASH_KEY`, and it offloads a
workflow's own publish off the workflow thread. :func:`encode_body`,
:func:`decode_body` and :func:`content_fingerprint` are the shared code for
that; :class:`StreamProvider` states the rule.
"""

from __future__ import annotations

from temporalio.streams._body import (
    CONTENT_HASH_KEY,
    content_fingerprint,
    content_hash,
    decode_body,
    encode_body,
)
from temporalio.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.streams._provider import (
    ReadSource,
    StreamHandle,
    StreamProducer,
    StreamProvider,
    WorkflowStreamProvider,
    WriteSink,
)
from temporalio.streams._record import (
    BEGINNING,
    END,
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)
from temporalio.streams._ref import StreamOwnerKind, StreamRef
from temporalio.streams._topic import (
    DEFAULT_TOPIC,
    StreamTopic,
    resolve_topic,
    topic,
)

__all__ = [
    "BEGINNING",
    "CONTENT_HASH_KEY",
    "DEFAULT_TOPIC",
    "END",
    "Cursor",
    "ReadSource",
    "RecordKind",
    "StreamClosedError",
    "StreamCursorError",
    "StreamError",
    "StreamHandle",
    "StreamNotFoundError",
    "StreamOwnerKind",
    "StreamProducer",
    "StreamProducerError",
    "StreamProvider",
    "StreamRecord",
    "StreamRef",
    "StreamTopic",
    "StreamUnsupportedError",
    "Supersession",
    "WorkflowStreamProvider",
    "WriteSink",
    "content_fingerprint",
    "content_hash",
    "decode_body",
    "encode_body",
    "resolve_topic",
    "topic",
]
