"""The producer wake path (P14).

The server-visible wakeup, used whenever no open Workflow Task can accept local
readiness. It carries no stream payload. It goes out as a notification on the
stream's channel when the server has channels, see :func:`send_wake` and
:func:`channel_for`, over the deprecated ``WakeWorkflowExecution`` call when it
has only that, and otherwise as the reserved Signal described below, which
carries only enough identity for Core to decide whether it is a wake, a stale
claim, or someone else's chain.

Two properties are load-bearing and neither is available from the public Signal
API, which is why this path does not reuse it:

**Codec bypass.** The envelope goes out through a raw ``SignalWorkflowExecution``
built with the protocol's own serialization rather than the user's
``DataConverter`` (ADR-025). Core is the component that must *read* this Signal,
and Core has no access to a user codec -- a codec that encrypts payloads would
make the envelope unreadable to the only reader that matters. Nothing user-owned
is in it, so bypassing the codec leaks nothing.

**Stable request ID.** The Temporal ``request_id`` is derived deterministically
from the wake's identity, so a producer retrying after an ambiguous failure sends
the identical request and the server deduplicates it. The public Signal path
generates a fresh UUID per attempt, which would turn every retry into a second
wake.
"""

from __future__ import annotations

import hashlib
import time
import uuid
import weakref
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, get_args
from urllib.parse import quote

import temporalio.api.common.v1
import temporalio.api.notification.v1
import temporalio.api.workflow.v1
import temporalio.api.workflowservice.v1
import temporalio.service
from temporalio.bridge.proto.external_stream.external_stream_pb2 import WakeSignal
from temporalio.contrib.external_workflow_streams._record import Offset

if TYPE_CHECKING:
    import temporalio.client
    from temporalio.contrib.external_workflow_streams._backend import StreamKey
    from temporalio.contrib.external_workflow_streams._producer import WorkflowChainKey

__all__ = [
    "WakeRequest",
    "WakeTransport",
    "channel_for",
    "new_sender_identity",
    "send_wake",
    "send_wake_signal",
    "server_has_channels",
    "wake_request_id",
]

WakeTransport = Literal["auto", "channel", "wake", "signal"]
"""How a wake reaches the server.

``"channel"`` notifies the stream's channel with ``NotifyChannel``. Every run
that subscribed to the channel is woken, and the only record is the
notification on the scheduled event of the Workflow Task it causes. ``"wake"``
uses the deprecated ``WakeWorkflowExecution`` call, addressed to one chain.
``"signal"`` uses the reserved Signal. ``"auto"`` tries them in that order and
steps down on ``UNIMPLEMENTED``, remembering per client what the server lacks.
"""

CHANNEL_PREFIX = "external-stream"
"""The first segment of every channel this package names.

Keeps these channels apart from any a Workflow subscribes to by hand under a
name that happens to look like a stream's.
"""


def channel_for(key: StreamKey) -> str:
    """The channel a stream's writers notify and its readers listen on.

    One formatting for both sides, so a producer process and the consuming
    Worker never disagree on the name. The namespace is left out because the
    server scopes a channel to the namespace of the call. Every segment is
    percent-encoded, so a Workflow ID or stream name containing ``/`` cannot
    collide with another stream's segments.
    """
    return "/".join(
        quote(part, safe="")
        for part in (
            CHANNEL_PREFIX,
            key.workflow_id,
            key.first_execution_run_id,
            key.direction.value,
            key.stream_name,
        )
    )


WAKE_SIGNAL_NAME = "__temporal_external_stream_wake"
"""Fixed, and versioned by the envelope rather than by the name.

Distinct from every ``__temporal_workflow_stream_*`` name already reserved by
``temporalio.contrib.workflow_streams`` (ADR-001), which is a different feature
that coexists with this one.
"""

WAKE_SIGNAL_MESSAGE_TYPE = "coresdk.external_stream.WakeSignal"
WAKE_SIGNAL_ENCODING = b"binary/protobuf"
WAKE_SIGNAL_ENVELOPE_VERSION = 1

UNPARKED_WAKE_GENERATION = 0
"""No confirmed park is known; ask for a Workflow Task anyway (ADR-023).

Not silence. The runtime rechecks every active subscription on wakeup regardless
of which stream the Signal named, so an unnecessary unparked wake costs at most
one empty Workflow Task -- while staying silent because no park was observed
would lose the record until something else happened to wake the Workflow.
"""

_REQUEST_ID_NAMESPACE = uuid.UUID("6f1f8a1e-6f26-5f3f-9f2b-4b0a2c7d8e10")
"""A fixed namespace, so the derivation is stable across processes and versions.

Deriving a UUID rather than passing the tuple through keeps the request ID
inside the length the server accepts while staying a pure function of its
inputs.
"""


def new_sender_identity(client_identity: str = "") -> str:
    """One sender instance's identity for unparked wakes.

    Unique per *sender*, not per client identity. Two Workers in one process
    share a ``Client`` and therefore one client identity, so deriving from that
    alone gives their first unparked wakes byte-identical request IDs -- the
    server deduplicates the second, no Workflow Task is created, and the Run
    stalls. That is precisely the loss the sender identity exists to prevent.

    Drawn once and held for the sender's lifetime, which is what "fixed across
    retries of that one attempt" requires: a value redrawn per attempt would
    turn the shutdown sweep's in-grace retry into a second wake.

    Random, which is safe here: the wake Signal is sent from the Worker's own
    event loop and never from Workflow code, so it is not replay-visible and
    cannot affect determinism. The client identity is kept as a prefix so a
    request ID stays traceable to a Worker in server-side logs.
    """
    return f"{client_identity}#{uuid.uuid4()}"


@dataclass(frozen=True)
class WakeRequest:
    """One wake attempt's complete identity.

    Everything the request ID is derived from lives here, so "the same attempt"
    is a property of a value rather than of how carefully a caller re-passed six
    arguments.
    """

    namespace: str
    workflow_id: str
    first_execution_run_id: str
    stream_name: str
    wait_id: int
    park_generation: int
    sender_identity: str = ""
    wake_counter: int = 0
    position: bytes = b""
    """The store position the wake reports, opaque to the server."""
    position_counter: int = 0
    """The store's order for ``position``; 0 when the store cannot give one.

    Orders positions within one pending burst of wakes, so the task that takes
    them sees the latest position. It is not a dedupe key: once a task has
    received a source's wake, the server accepts the next one whatever its
    counter, because only the receiver knows what it read.
    """
    channel: str = ""
    """The channel the stream's readers listen on, see :func:`channel_for`.

    Empty for a request composed by code that predates the channel, which the
    transport then wakes by chain. Not part of the request ID: the ID names the
    wake, and the channel is only where it is delivered.
    """

    @property
    def is_unparked(self) -> bool:
        """Whether this wake requests a task outside a confirmed park."""
        return self.park_generation == UNPARKED_WAKE_GENERATION

    def __post_init__(self) -> None:
        """Validate the identity required for server-side deduplication."""
        if self.is_unparked and not self.sender_identity:
            raise ValueError(
                "an unparked wake needs a sender identity. Generation 0 carries "
                "no attempt identity of its own, so without it two Workers "
                "shutting down at different times would derive the same request "
                "ID and the server would deduplicate the second wake away -- "
                "turning a correct retry mechanism into silent loss."
            )


def wake_request_id(request: WakeRequest) -> str:
    """The deterministic ``request_id`` for one wake attempt.

    A **parked** wake is identified by its generation: a generation is woken
    once, so every producer that races to wake it derives the same ID and the
    server keeps one.

    An **unparked** wake has no generation to identify it, so the derivation
    additionally includes the sender's identity -- unique per sender *instance*,
    see :py:func:`new_sender_identity` -- and a per-sender monotonic counter,
    both held fixed across retries of that one attempt. Two senders' unparked
    wakes must stay distinct even though neither knows about the other.
    """
    parts = [
        request.namespace,
        request.workflow_id,
        request.first_execution_run_id,
        request.stream_name,
        str(request.wait_id),
        str(request.park_generation),
    ]
    if request.is_unparked:
        parts += [request.sender_identity, str(request.wake_counter)]
    # Length-prefixed rather than delimiter-joined: a stream literally named
    # "a\x00b" must not collide with the pair ("a", "b").
    material = b"".join(
        len(encoded).to_bytes(4, "big") + encoded
        for encoded in (part.encode("utf-8") for part in parts)
    )
    return str(uuid.uuid5(_REQUEST_ID_NAMESPACE, hashlib.sha256(material).hexdigest()))


def wake_envelope(request: WakeRequest, *, producer_session_id: str = "") -> bytes:
    """The Signal's single argument, serialized at the protocol level."""
    return WakeSignal(
        envelope_version=WAKE_SIGNAL_ENVELOPE_VERSION,
        stream_name=request.stream_name,
        wait_id=request.wait_id,
        park_generation=request.park_generation,
        first_execution_run_id=request.first_execution_run_id,
        producer_session_id=producer_session_id,
    ).SerializeToString()


def build_signal_request(
    request: WakeRequest,
    *,
    identity: str,
    producer_session_id: str = "",
) -> temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest:
    """The raw request, built without touching the user's ``DataConverter``.

    Addressed to the Workflow ID with **no Run ID**, so it always lands on the
    current Run of the chain -- a wake sent while a Continue-As-New is in flight
    must reach the successor, not fail against a Run that has already closed.
    ``first_execution_run_id`` inside the envelope is what lets Core tell the
    chain apart from a reused Workflow ID.
    """
    return temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest(
        namespace=request.namespace,
        workflow_execution=temporalio.api.common.v1.WorkflowExecution(
            workflow_id=request.workflow_id
        ),
        signal_name=WAKE_SIGNAL_NAME,
        input=temporalio.api.common.v1.Payloads(
            payloads=[
                temporalio.api.common.v1.Payload(
                    metadata={
                        "encoding": WAKE_SIGNAL_ENCODING,
                        "messageType": WAKE_SIGNAL_MESSAGE_TYPE.encode(),
                    },
                    data=wake_envelope(
                        request, producer_session_id=producer_session_id
                    ),
                )
            ]
        ),
        identity=identity,
        request_id=wake_request_id(request),
    )


async def send_wake_signal(
    client: temporalio.client.Client,
    request: WakeRequest,
    *,
    producer_session_id: str = "",
) -> str:
    """Sends one wake and returns the request ID it was sent under.

    Retrying with the same :class:`WakeRequest` is safe and is the intended
    recovery from an ambiguous failure: the server deduplicates on the request
    ID, so the second attempt resolves the first rather than waking twice.
    """
    signal_request = build_signal_request(
        request,
        identity=client.service_client.config.identity,
        producer_session_id=producer_session_id,
    )
    await client.workflow_service.signal_workflow_execution(signal_request)
    return signal_request.request_id


_TRANSPORT_ORDER: tuple[str, ...] = ("channel", "wake", "signal")
"""What ``"auto"`` tries, in order.

Each step is taken by every server that takes the one before it, so stepping
down never loses a wake, only the folding and the History the later transport
costs.
"""

_CALL_NAMES = {"channel": "notify_channel", "wake": "wake_workflow_execution"}

_BEST_TRANSPORT: weakref.WeakKeyDictionary[Any, str] = weakref.WeakKeyDictionary()
"""Per service client, the first transport its server has not refused.

Keyed weakly by the client's service client so the answer lives as long as the
connection it describes, without adding state to the public client. Absent
means nothing has been learned yet and ``"auto"`` starts at the top.
"""


def _service_key(client: temporalio.client.Client) -> Any:
    return getattr(client, "service_client", None)


def _best_transport(client: temporalio.client.Client) -> str:
    key = _service_key(client)
    if key is None:
        return _TRANSPORT_ORDER[0]
    try:
        return _BEST_TRANSPORT.get(key, _TRANSPORT_ORDER[0])
    except TypeError:
        return _TRANSPORT_ORDER[0]


def _step_down(client: temporalio.client.Client, transport: str) -> str:
    """Remembers that ``transport`` is beyond this client's server.

    Returns the transport to try next. Only ever moves forward: a refusal of a
    later transport may already be on record, and going back would retry a
    call known to fail.
    """
    following = _TRANSPORT_ORDER[_TRANSPORT_ORDER.index(transport) + 1]
    key = _service_key(client)
    if key is not None:
        try:
            known = _TRANSPORT_ORDER.index(_best_transport(client))
            if known < _TRANSPORT_ORDER.index(following):
                _BEST_TRANSPORT[key] = following
        except TypeError:
            pass
    return following


PROBE_CHANNEL = f"{CHANNEL_PREFIX}/probe"
"""A channel nobody writes to, described to learn whether the server has any."""


async def server_has_channels(client: temporalio.client.Client) -> bool | None:
    """Whether this client's server implements notification channels.

    Asked with a ``DescribeChannel`` of a channel nobody writes to, because the
    one reader-side decision that depends on the answer is a command: a
    Workflow Task that subscribes to a channel on a server without them fails,
    and nothing in the wake path would ever tell the reader. A refusal is
    remembered the way a refused notify is, so the writers on the same client
    skip the call as well.

    Returns ``None`` when the server could not be asked, so the caller can ask
    again later rather than settle on an answer the server never gave.
    """
    if _best_transport(client) != "channel":
        return False
    service = getattr(client, "workflow_service", None)
    call = getattr(service, "describe_channel", None)
    if call is None:
        _step_down(client, "channel")
        return False
    try:
        await call(
            temporalio.api.workflowservice.v1.DescribeChannelRequest(
                namespace=client.namespace, channel=PROBE_CHANNEL
            )
        )
    except temporalio.service.RPCError as err:
        if err.status == temporalio.service.RPCStatusCode.UNIMPLEMENTED:
            _step_down(client, "channel")
            return False
        if err.status == temporalio.service.RPCStatusCode.NOT_FOUND:
            # The server knows the call and has never seen this channel.
            return True
        return None
    return True


def wake_transport_of(backend: object) -> WakeTransport:
    """The transport a backend asks for, defaulting for objects that predate it."""
    return getattr(backend, "wake_transport", "auto")


def wake_position(backend: object, offset: Offset | None) -> tuple[bytes, int]:
    """The position and counter a wake reports for ``offset`` in ``backend``.

    Without an offset the backend's own clock rule gives the counter, so a
    wake that reports no position still orders above every position the store
    handed out before now. A counter the backend cannot give is 0, which
    :func:`wake_call_counter` replaces with the clock.
    """
    if offset is None:
        counter_now = getattr(backend, "wake_counter_now", None)
        return b"", max(counter_now(), 0) if counter_now is not None else 0
    counter_for = getattr(backend, "wake_counter_for", None)
    counter = counter_for(offset) if counter_for is not None else 0
    return offset.token.encode("utf-8"), max(counter, 0)


def wake_call_counter(request: WakeRequest) -> int:
    """The counter the wake call carries for ``request``.

    The server folds a wake into one still pending for the same source when
    its counter is not above the pending one, so the counter decides which
    position the receiving task sees. The store's own order when it has one.
    Otherwise the wall clock in nanoseconds, which keeps one sender's wakes
    increasing but is not ordered across senders: a pending wake can then end
    up reporting an older position than another sender's. That costs latency
    only, because the task still reads from its own cursor and the watcher
    reports whatever it finds.
    """
    if request.position_counter > 0:
        return request.position_counter
    return time.time_ns()


def build_wake_request(
    request: WakeRequest, *, identity: str
) -> temporalio.api.workflowservice.v1.WakeWorkflowExecutionRequest:
    """The ``WakeWorkflowExecution`` request for one wake.

    ``run_id`` is the chain's first execution run, so the server follows the
    chain to its current run and answers ``NOT_FOUND`` once the chain has
    ended rather than waking a later chain that reused the Workflow ID.
    """
    return temporalio.api.workflowservice.v1.WakeWorkflowExecutionRequest(
        namespace=request.namespace,
        workflow_execution=temporalio.api.common.v1.WorkflowExecution(
            workflow_id=request.workflow_id,
            run_id=request.first_execution_run_id,
        ),
        wake=temporalio.api.workflow.v1.Wake(
            source=request.stream_name,
            position=request.position,
            counter=wake_call_counter(request),
        ),
        identity=identity,
    )


def build_notify_request(
    request: WakeRequest,
    *,
    identity: str,
    metadata: Mapping[str, temporalio.api.common.v1.Payload] | None = None,
) -> temporalio.api.workflowservice.v1.NotifyChannelRequest:
    """The ``NotifyChannel`` request for one wake.

    The server deduplicates a retried notification by ``request_id``, so the
    Signal's derivation serves here unchanged: two producers waking one parked
    generation send the identical request, and a sender retrying an attempt
    that may have landed sends it again rather than waking twice.
    """
    if not request.channel:
        raise ValueError(
            "a channel notification needs the stream's channel; compose the "
            "request with wake_request_for() or set WakeRequest.channel"
        )
    return temporalio.api.workflowservice.v1.NotifyChannelRequest(
        namespace=request.namespace,
        notification=temporalio.api.notification.v1.Notification(
            channel=request.channel,
            position=request.position,
            counter=wake_call_counter(request),
            metadata=dict(metadata or {}),
        ),
        identity=identity,
        request_id=wake_request_id(request),
    )


async def send_wake(
    client: temporalio.client.Client,
    request: WakeRequest,
    *,
    producer_session_id: str = "",
    transport: WakeTransport = "auto",
    metadata: Mapping[str, temporalio.api.common.v1.Payload] | None = None,
) -> str:
    """Sends one wake over ``transport`` and returns an identifier for it.

    The channel and the Signal return the request ID the wake went out under.
    The wake call has no request ID, since the server folds by counter
    instead, so it returns the run the wake was stored on.

    ``"auto"`` starts at the best transport this client's server is known to
    take and steps down on ``UNIMPLEMENTED``, remembering the step. A channel
    with no listeners is not a reason to step down: the server retains the
    notification and hands it to the next listener at registration, which is
    what closes the gap between a reader's last look at the store and its
    subscription taking effect.

    A ``NOT_FOUND`` from the wake call propagates as the ``RPCError`` it is,
    which is what a Signal to an ended Workflow raises as well, so callers keep
    one meaning for "the chain is over".

    Args:
        metadata: Payloads for the listeners, carried on the notification
            only. The channel already names the stream, so most wakes send
            none.

    Raises:
        temporalio.service.RPCError: The server refused the wake. With
            ``"channel"`` or ``"wake"`` this includes ``UNIMPLEMENTED``, since
            that transport was asked for explicitly.
    """
    # Checked against the alias rather than a literal tuple so an untyped caller
    # still gets a ValueError instead of a silent Signal.
    if transport not in get_args(WakeTransport):
        raise ValueError(f"unknown wake transport {transport!r}")
    attempt = _best_transport(client) if transport == "auto" else transport
    if attempt == "channel" and not request.channel and transport == "auto":
        # Composed without its channel by a caller that predates it. Nothing
        # about the server follows from that, so nothing is remembered.
        attempt = "wake"
    while attempt != "signal":
        service = getattr(client, "workflow_service", None)
        call = getattr(service, _CALL_NAMES[attempt], None)
        if call is None:
            # An SDK build whose generated service lacks the call can no more
            # send a wake than a server that lacks it can take one.
            if transport != "auto":
                raise RuntimeError(
                    f"the {attempt} transport was requested but this client has "
                    f"no {_CALL_NAMES[attempt]} call"
                )
            attempt = _step_down(client, attempt)
            continue
        identity = client.service_client.config.identity
        try:
            if attempt == "channel":
                await call(
                    build_notify_request(request, identity=identity, metadata=metadata)
                )
                return wake_request_id(request)
            response = await call(build_wake_request(request, identity=identity))
            return response.run_id
        except temporalio.service.RPCError as err:
            if (
                transport != "auto"
                or err.status != temporalio.service.RPCStatusCode.UNIMPLEMENTED
            ):
                raise
            attempt = _step_down(client, attempt)
    return await send_wake_signal(
        client, request, producer_session_id=producer_session_id
    )


def wake_request_for(
    workflow: WorkflowChainKey,
    *,
    stream_name: str,
    wait_id: int,
    park_generation: int | None,
    sender_identity: str,
    wake_counter: int = 0,
    position: bytes = b"",
    position_counter: int = 0,
) -> WakeRequest:
    """Builds the request from a chain key and an observed generation.

    ``park_generation=None`` -- no confirmed park for this subscription --
    becomes an unparked wake rather than no wake at all. The channel is the
    input stream's, which is the only direction a producer writes.
    """
    return WakeRequest(
        namespace=workflow.namespace,
        workflow_id=workflow.workflow_id,
        first_execution_run_id=workflow.first_execution_run_id,
        stream_name=stream_name,
        wait_id=wait_id,
        park_generation=(
            UNPARKED_WAKE_GENERATION if park_generation is None else park_generation
        ),
        sender_identity=sender_identity,
        wake_counter=wake_counter,
        position=position,
        position_counter=position_counter,
        channel=channel_for(workflow.stream_key(stream_name)),
    )
