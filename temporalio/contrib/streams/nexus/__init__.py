"""The stream service: streams over a Nexus endpoint.

.. warning::
    This package is experimental and may change in future versions.

A process that cannot reach a stream's store, or should not, appends to and
reads from it through a Nexus endpoint. The endpoint runs the
``temporal.sdk.streams.v1.TemporalStreams`` service over whichever provider
the endpoint's Worker carries, so callers never learn which store that is.

The service is defined once, in ``temporal_streams.nexusrpc.yaml`` next to
this file, and everything in :mod:`temporalio.contrib.streams.nexus._generated`
is generated from it by ``scripts/gen_nexus_streams_api.py``:

* :class:`TemporalStreams`, the service definition a handler implements and a
  Workflow's Nexus client calls.
* The models its operations take and return. :class:`StreamRef` is
  :class:`temporalio.contrib.streams.StreamRef` itself, so a reference a
  Workflow hands out is the value the service takes.
* :class:`TemporalStreamsHttpClient`, a caller for a process outside any
  Worker, posting to the endpoint over the Nexus HTTP ingress. It depends on
  nothing beyond the standard library.

:class:`TemporalStreamsHandler` serves the generated service over a
:class:`temporalio.contrib.streams.StreamProvider`.

A Nexus operation can also hand its caller a stream.
:class:`StreamOperationHandler` is such an operation: its start attaches the
caller to the stream's notifier on the server and puts the
:class:`StreamRef` in the operation token (:func:`stream_ref_from_token`).
A provider with :meth:`temporalio.contrib.streams.StreamProviderPlugin.notify_on_append`
on tells the notifier when the stream moves, which the caller sees as
operation progress, and
:meth:`temporalio.contrib.streams.StreamProviderPlugin.close_stream` completes
the operation, or :func:`close_workflow_stream` from the owning Workflow.
:class:`StreamNotifier` is the notifier client both use.

A caller Workflow reads such a stream with :class:`StreamReader`: it waits on
the operation's progress and reads through this service as Nexus operations
of the Workflow, so a replay hands over the same batches.
"""

from temporalio.contrib.streams import StreamRef
from temporalio.contrib.streams._notify import StreamNotifier
from temporalio.contrib.streams.nexus._generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    StreamCursor,
    TemporalStreams,
)
from temporalio.contrib.streams.nexus._generated.client import (
    HTTPStatusError,
    TemporalStreamsHttpClient,
)
from temporalio.contrib.streams.nexus._handler import TemporalStreamsHandler
from temporalio.contrib.streams.nexus._operation import (
    StreamOperationHandler,
    stream_ref_from_token,
)
from temporalio.contrib.streams.nexus._reader import (
    ReadSupersession,
    StreamReader,
    StreamRecordError,
)
from temporalio.contrib.streams.nexus._workflow import close_workflow_stream

__all__ = [
    "AppendInput",
    "AppendOutput",
    "HTTPStatusError",
    "ReadInput",
    "ReadOutput",
    "ReadSupersession",
    "RecordWire",
    "StreamCursor",
    "StreamNotifier",
    "StreamOperationHandler",
    "StreamReader",
    "StreamRecordError",
    "StreamRef",
    "TemporalStreams",
    "TemporalStreamsHandler",
    "TemporalStreamsHttpClient",
    "close_workflow_stream",
    "stream_ref_from_token",
]
