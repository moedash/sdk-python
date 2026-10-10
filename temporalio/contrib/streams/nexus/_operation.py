"""A Nexus operation whose start hands its caller a stream.

The operation stays open while the stream runs. Its start attaches the
caller's callback to the stream's notifier on the server, so the notifier
tells the caller when the stream moves (Nexus operation progress) and
completes the operation when the stream closes. The caller learns which
stream it is from the operation token.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio.api.common.v1 import Callback
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import (
    AttachStreamCallbackRequest,
    DetachStreamCallbackRequest,
)
from temporalio.contrib.streams._notify import stream_reference
from temporalio.contrib.streams._ref import StreamRef

__all__ = ["StreamOperationHandler", "stream_ref_from_token"]

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

# The token's own format version, so a later release that carries more (or a
# typed start result instead) can tell an old token apart.
_TOKEN_VERSION = 1


def _encode_token(ref: StreamRef, attach_request_id: str) -> str:
    body = {
        "v": _TOKEN_VERSION,
        "ref": {
            "kind": ref.kind,
            "workflow_id": ref.workflow_id,
            "run_id": ref.run_id,
            "topic": ref.topic,
        },
        "attach": attach_request_id,
    }
    encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(encoded).decode().rstrip("=")


def _decode_token(token: str) -> tuple[StreamRef, str]:
    try:
        padded = token + "=" * (-len(token) % 4)
        body: Any = json.loads(base64.urlsafe_b64decode(padded))
        if not isinstance(body, dict) or body.get("v") != _TOKEN_VERSION:
            raise ValueError("unknown token version")
        ref: Any = body["ref"]
        attach: Any = body["attach"]
        if not isinstance(attach, str):
            raise ValueError("the attach request id is not a string")
        return (
            StreamRef(
                kind=ref["kind"],
                workflow_id=ref["workflow_id"],
                run_id=ref["run_id"],
                topic=ref["topic"],
            ),
            attach,
        )
    except (ValueError, TypeError, KeyError, binascii.Error) as error:
        raise ValueError(f"not a stream operation token: {error}") from None


def stream_ref_from_token(token: str) -> StreamRef:
    """The stream a stream-returning operation's token names.

    This is how a caller learns the stream: the start answers with the
    operation token, and the token carries the stream reference.

    Raises:
        ValueError: ``token`` did not come from :class:`StreamOperationHandler`.
    """
    return _decode_token(token)[0]


def _stream_reference(ref: StreamRef) -> StreamReference:
    return stream_reference(ref, ref.topic)


class StreamOperationHandler(
    nexusrpc.handler.OperationHandler[InputT, OutputT], Generic[InputT, OutputT]
):
    """A Nexus operation whose start hands its caller a stream.

    ``open_stream`` names the stream for a start: it creates or finds the
    stream and returns its reference. The start then attaches the caller's
    callback to that stream's notifier and answers asynchronously, with the
    reference in the operation token. From then on the server tells the
    caller each time the stream's producer notifies (see
    :class:`NotifyingStreamProvider`), and completes the operation with the
    close result when the stream closes. A cancel detaches the caller.

    The operation's output type is the close result's type. The records
    themselves are read through the stream service, not carried by the
    operation.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        open_stream: Callable[
            [nexusrpc.handler.StartOperationContext, InputT], Awaitable[StreamRef]
        ],
    ) -> None:
        """Hand out the stream ``open_stream`` names for each start."""
        self._open_stream = open_stream

    async def start(
        self, ctx: nexusrpc.handler.StartOperationContext, input: InputT
    ) -> nexusrpc.handler.StartOperationResultAsync:
        """Attach the caller to the stream and answer with its reference.

        Raises:
            nexusrpc.HandlerError: The caller sent no callback, so it could
                never hear about the stream.
        """
        if not ctx.callback_url:
            raise nexusrpc.HandlerError(
                "a stream-returning operation needs a caller callback; start it "
                "asynchronously",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
            )
        ref = await self._open_stream(ctx, input)
        client = temporalio.nexus.client()
        # Idempotent by request id, so a retried start attaches once.
        await client.workflow_service.attach_stream_callback(
            AttachStreamCallbackRequest(
                namespace=client.namespace,
                stream_ref=_stream_reference(ref),
                request_id=ctx.request_id,
                callback=Callback.Nexus(
                    url=ctx.callback_url, header=dict(ctx.callback_headers)
                ),
            )
        )
        return nexusrpc.handler.StartOperationResultAsync(
            token=_encode_token(ref, ctx.request_id)
        )

    async def cancel(
        self, ctx: nexusrpc.handler.CancelOperationContext, token: str
    ) -> None:
        """Detach the caller: the stream goes on, the operation ends.

        Raises:
            nexusrpc.HandlerError: ``token`` is not one this operation issued.
        """
        try:
            ref, attach_request_id = _decode_token(token)
        except ValueError as error:
            raise nexusrpc.HandlerError(
                str(error), type=nexusrpc.HandlerErrorType.BAD_REQUEST
            ) from None
        client = temporalio.nexus.client()
        await client.workflow_service.detach_stream_callback(
            DetachStreamCallbackRequest(
                namespace=client.namespace,
                stream_ref=_stream_reference(ref),
                request_id=attach_request_id,
            )
        )
