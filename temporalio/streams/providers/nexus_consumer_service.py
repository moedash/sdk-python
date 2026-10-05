r"""A standalone Nexus handler process that consumes a stream through its channel.

The Temporal frontend forwards a caller's ``StartOperation`` to an endpoint
whose target is an external URL, over the Nexus HTTP protocol. This module
serves that protocol with aiohttp for one :class:`nexusrpc.handler.Handler`,
start and cancel, and one more route the server's notification channels post
to, which it hands to the :class:`StreamConsumerOperation` instances it was
given. It is the shape the PoC demonstrates: a process with a URL of its own,
no worker, consuming a stream the server tells it about.

Run it with one command, against a server that serves channels and a stream
front endpoint named ``streams``::

    uv run python -m temporalio.streams.providers.nexus_consumer_service \\
        --address 127.0.0.1:7813 --http http://127.0.0.1:7823 \\
        --endpoint streams --port 8813 --register-endpoint consumers

``--register-endpoint`` creates a Nexus endpoint whose target is this process,
so a workflow calls ``CollectStream.collect`` on it with a ``StreamRef`` and
gets the stream's values back when the stream closes. The server has to allow
this host in ``callback.allowedAddresses``.

The wire details are the Nexus SDK's: a start is ``POST /{service}/{operation}``
with the input as the body, the caller's completion callback in the ``callback``
query parameter and its headers under the ``Nexus-Callback-`` prefix; an
asynchronous start answers ``201`` with the operation token; a cancel is
``POST /{service}/{operation}/cancel`` with the token in
``Nexus-Operation-Token`` and answers ``202``; a handler error answers the
status its type maps to with a JSON failure body. The body of a start is
turned into a payload the way the server does it, by content type, and a
synchronous result goes back the same way.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

import nexusrpc
import nexusrpc.handler
from aiohttp import web

import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams._record import RecordKind, StreamRecord
from temporalio.streams._ref import StreamRef
from temporalio.streams.providers.nexus import (
    NexusStreams,
    StreamConsumerOperation,
    _payload_content,
    stream_consumer_operation,
)

__all__ = [
    "CollectStream",
    "CollectStreamHandler",
    "NexusHttpService",
    "collect_values",
    "main",
]

logger = logging.getLogger(__name__)

_CALLBACK_PREFIX = "nexus-callback-"
_CONTENT_PREFIX = "content-"
_REQUEST_ID_HEADER = "nexus-request-id"
_REQUEST_TIMEOUT_HEADER = "request-timeout"
_OPERATION_TOKEN_HEADER = "nexus-operation-token"
_OPERATION_STATE_HEADER = "nexus-operation-state"
_RETRYABLE_HEADER = "nexus-request-retryable"
_CALLBACK_QUERY = "callback"
_TOKEN_QUERY = "token"
_DEFAULT_DELIVERIES_PATH = "/deliveries"

# The status each handler error type answers with, per the Nexus HTTP spec.
_ERROR_STATUS = {
    nexusrpc.HandlerErrorType.BAD_REQUEST: 400,
    nexusrpc.HandlerErrorType.UNAUTHENTICATED: 401,
    nexusrpc.HandlerErrorType.UNAUTHORIZED: 403,
    nexusrpc.HandlerErrorType.NOT_FOUND: 404,
    nexusrpc.HandlerErrorType.REQUEST_TIMEOUT: 408,
    nexusrpc.HandlerErrorType.CONFLICT: 409,
    nexusrpc.HandlerErrorType.RESOURCE_EXHAUSTED: 429,
    nexusrpc.HandlerErrorType.INTERNAL: 500,
    nexusrpc.HandlerErrorType.NOT_IMPLEMENTED: 501,
    nexusrpc.HandlerErrorType.UNAVAILABLE: 503,
    nexusrpc.HandlerErrorType.UPSTREAM_TIMEOUT: 520,
}
_OPERATION_FAILED_STATUS = 424

_DURATION = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")
_UNIT_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


class _NeverCancelled(nexusrpc.handler.OperationTaskCancellation):
    """The task cancellation of a request no worker is going to cancel."""

    def is_cancelled(self) -> bool:
        return False

    def cancellation_reason(self) -> str | None:
        return None

    def wait_until_cancelled_sync(self, timeout: float | None = None) -> bool:
        del timeout
        return False

    async def wait_until_cancelled(self) -> None:
        await asyncio.Event().wait()


def _deadline(timeout: str | None) -> datetime | None:
    """The request deadline a Go duration string in ``Request-Timeout`` names."""
    if not timeout:
        return None
    seconds = 0.0
    matched = False
    for amount, unit in _DURATION.findall(timeout):
        seconds += float(amount) * _UNIT_SECONDS[unit]
        matched = True
    if not matched:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _media_type(content_type: str) -> tuple[str, dict[str, str]]:
    media, _, rest = content_type.partition(";")
    params: dict[str, str] = {}
    for part in rest.split(";"):
        key, _, value = part.strip().partition("=")
        if key:
            params[key.strip().lower()] = value.strip().strip('"')
    return media.strip().lower(), params


def _payload_from_content(headers: Mapping[str, str], body: bytes) -> Payload:
    """The payload a start body is, mapped the way the server maps Nexus content."""
    content = {
        key[len(_CONTENT_PREFIX) :]: value
        for key, value in headers.items()
        if key.startswith(_CONTENT_PREFIX)
    }
    content_type = content.pop("type", "")
    content.pop("length", None)
    if not content_type:
        if not content and not body:
            return Payload(metadata={"encoding": b"binary/null"})
        return _unknown(content, body)
    media, params = _media_type(content_type)
    if media == "application/x-temporal-payload":
        return Payload.FromString(body)
    if media == "application/json":
        if params.get("format") == "protobuf" and params.get("message-type"):
            return Payload(
                metadata={
                    "encoding": b"json/protobuf",
                    "messageType": params["message-type"].encode(),
                },
                data=body,
            )
        return Payload(metadata={"encoding": b"json/plain"}, data=body)
    if media == "application/x-protobuf" and params.get("message-type"):
        return Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": params["message-type"].encode(),
            },
            data=body,
        )
    if media == "application/octet-stream":
        return Payload(metadata={"encoding": b"binary/plain"}, data=body)
    content["type"] = content_type
    return _unknown(content, body)


def _unknown(content: Mapping[str, str], body: bytes) -> Payload:
    metadata = {key: value.encode() for key, value in content.items()}
    metadata["encoding"] = b"unknown/nexus-content"
    return Payload(metadata=metadata, data=body)


class _PayloadSerializer:
    """Hands a start's payload to the handler through the data converter."""

    def __init__(
        self, converter: temporalio.converter.DataConverter, payload: Payload
    ) -> None:
        self._converter = converter
        self._payload = payload

    async def serialize(self, value: Any) -> nexusrpc.Content:
        del value
        raise NotImplementedError("the service serializes results itself")

    async def deserialize(
        self, content: nexusrpc.Content, as_type: type[Any] | None = None
    ) -> Any:
        del content
        payload = self._payload
        if self._converter.payload_codec is not None:
            [payload] = await self._converter.payload_codec.decode([payload])
        try:
            [value] = self._converter.payload_converter.from_payloads(
                [payload], [as_type] if as_type is not None else None
            )
        except Exception as error:
            raise nexusrpc.HandlerError(
                f"invalid operation input: {error}",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                retryable_override=False,
            ) from error
        return value


def _failure(error: nexusrpc.HandlerError) -> web.Response:
    headers = {}
    if error.retryable_override is not None:
        headers[_RETRYABLE_HEADER] = "true" if error.retryable_override else "false"
    error_type = (
        error.type
        if isinstance(error.type, nexusrpc.HandlerErrorType)
        else nexusrpc.HandlerErrorType.UNKNOWN
    )
    return web.json_response(
        {"message": error.message},
        status=_ERROR_STATUS.get(error_type, 500),
        headers=headers,
    )


class NexusHttpService:
    """Serves one Nexus handler over HTTP, plus the route channel deliveries reach.

    ``handler`` is the Nexus SDK's dispatcher over the service handlers the
    process hosts. ``consumers`` are the stream consumer operations among
    them; a delivery is offered to each until one holds the operation it
    names. ``data_converter`` turns start bodies into inputs and results
    into bodies; the caller's converter when a client is at hand.

    .. warning::
       This API is experimental and unstable.
    """

    def __init__(
        self,
        handler: nexusrpc.handler.Handler,
        consumers: Sequence[StreamConsumerOperation[Any]],
        *,
        data_converter: temporalio.converter.DataConverter | None = None,
        deliveries_path: str = _DEFAULT_DELIVERIES_PATH,
    ) -> None:
        """Serve ``handler`` and route deliveries at ``deliveries_path`` to ``consumers``."""
        if deliveries_path.count("/") != 1 or not deliveries_path.startswith("/"):
            raise ValueError(
                "deliveries_path must be one path segment, so it cannot be mistaken "
                "for a start request"
            )
        self._handler = handler
        self._consumers = list(consumers)
        self._converter = data_converter or temporalio.converter.DataConverter.default
        self._deliveries_path = deliveries_path

    @property
    def deliveries_path(self) -> str:
        """The route the channel's deliveries are served at."""
        return self._deliveries_path

    def application(self) -> web.Application:
        """The aiohttp application serving the routes; run it with the usual runner."""
        app = web.Application()
        app.add_routes(
            [
                web.post(self._deliveries_path, self._deliver),
                web.post("/{service}/{operation}", self._start),
                web.post("/{service}/{operation}/cancel", self._cancel),
            ]
        )
        return app

    async def _start(self, request: web.Request) -> web.Response:
        headers = {key.lower(): value for key, value in request.headers.items()}
        body = await request.read()
        ctx = nexusrpc.handler.StartOperationContext(
            service=request.match_info["service"],
            operation=request.match_info["operation"],
            headers={
                key: value
                for key, value in headers.items()
                if not key.startswith((_CONTENT_PREFIX, _CALLBACK_PREFIX))
            },
            request_id=headers.get(_REQUEST_ID_HEADER) or uuid.uuid4().hex,
            callback_url=request.query.get(_CALLBACK_QUERY),
            callback_headers={
                key[len(_CALLBACK_PREFIX) :]: value
                for key, value in headers.items()
                if key.startswith(_CALLBACK_PREFIX)
            },
            task_cancellation=_NeverCancelled(),
            request_deadline=_deadline(headers.get(_REQUEST_TIMEOUT_HEADER)),
        )
        input = nexusrpc.LazyValue(
            serializer=_PayloadSerializer(
                self._converter, _payload_from_content(headers, body)
            ),
            headers={},
            stream=None,
        )
        try:
            result = await self._handler.start_operation(ctx, input)
        except nexusrpc.HandlerError as error:
            return _failure(error)
        except nexusrpc.OperationError as error:
            return web.json_response(
                {"message": str(error)},
                status=_OPERATION_FAILED_STATUS,
                headers={_OPERATION_STATE_HEADER: error.state.value},
            )
        if isinstance(result, nexusrpc.handler.StartOperationResultAsync):
            return web.json_response(
                {"token": result.token, "state": "running"}, status=201
            )
        [payload] = self._converter.payload_converter.to_payloads([result.value])
        if self._converter.payload_codec is not None:
            [payload] = await self._converter.payload_codec.encode([payload])
        content, data = _payload_content(payload)
        return web.Response(status=200, body=data, headers=content)

    async def _cancel(self, request: web.Request) -> web.Response:
        headers = {key.lower(): value for key, value in request.headers.items()}
        token = headers.get(_OPERATION_TOKEN_HEADER) or request.query.get(_TOKEN_QUERY)
        if not token:
            return _failure(
                nexusrpc.HandlerError(
                    "missing operation token",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                )
            )
        ctx = nexusrpc.handler.CancelOperationContext(
            service=request.match_info["service"],
            operation=request.match_info["operation"],
            headers={
                key: value
                for key, value in headers.items()
                if key != _OPERATION_TOKEN_HEADER
            },
            task_cancellation=_NeverCancelled(),
        )
        try:
            await self._handler.cancel_operation(ctx, token)
        except nexusrpc.HandlerError as error:
            return _failure(error)
        return web.Response(status=202)

    async def _deliver(self, request: web.Request) -> web.Response:
        body = await request.read()
        headers = dict(request.headers)
        try:
            for consumer in self._consumers:
                if (await consumer.deliver(headers, body)).known:
                    break
        except Exception as error:
            # A 5xx makes the server retry the delivery, which is what a
            # failed read needs.
            logger.exception("a channel delivery failed")
            return web.json_response({"message": str(error)}, status=500)
        return web.Response(status=200)


# ---------------------------------------------------------------------------
# The demo service: collect a stream's values into the operation's result.
# ---------------------------------------------------------------------------


def collect_values(record: StreamRecord[Any], values: list[Any]) -> list[Any]:
    """The demo's consume function: keep every published value, in order."""
    if record.kind is RecordKind.DATA:
        values.append(record.value)
    return values


@nexusrpc.service
class CollectStream:
    """Consumes the stream a ref names and answers with its values when it closes."""

    collect: nexusrpc.Operation[StreamRef, list[Any]]


@nexusrpc.handler.service_handler(service=CollectStream)
class CollectStreamHandler:
    """Serves :class:`CollectStream` out of one consumer operation."""

    def __init__(self, consumer: StreamConsumerOperation[list[Any]]) -> None:
        """Serve ``collect`` with ``consumer``, which the deliveries also reach."""
        self._consumer = consumer

    @nexusrpc.handler.operation_handler
    def collect(self) -> nexusrpc.handler.OperationHandler[StreamRef, list[Any]]:
        """The consumer operation, as the operation handler factory hands it out."""
        return self._consumer


async def _register_endpoint(client: Client, name: str, url: str) -> None:
    try:
        await client.operator_service.create_nexus_endpoint(
            CreateNexusEndpointRequest(
                spec=EndpointSpec(
                    name=name,
                    target=EndpointTarget(external=EndpointTarget.External(url=url)),
                )
            )
        )
        logger.info("registered nexus endpoint %s -> %s", name, url)
    except RPCError as error:
        if error.status is not RPCStatusCode.ALREADY_EXISTS:
            raise
        logger.info("nexus endpoint %s exists; leaving it as it is", name)


async def main(argv: Sequence[str] | None = None) -> None:
    """Serve the demo consumer until interrupted; see the module docstring."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "--address", default="127.0.0.1:7233", help="the server's gRPC address"
    )
    parser.add_argument("--namespace", default="default")
    parser.add_argument(
        "--http",
        default="http://127.0.0.1:7243",
        help="the server's Nexus HTTP ingress, where the stream front is read",
    )
    parser.add_argument(
        "--endpoint", required=True, help="the stream front's Nexus endpoint name"
    )
    parser.add_argument("--host", default="127.0.0.1", help="the interface to serve on")
    parser.add_argument("--port", type=int, default=8813, help="the port to serve on")
    parser.add_argument(
        "--listener-url",
        default=None,
        help="the URL the server posts deliveries to; defaults to this process's route",
    )
    parser.add_argument(
        "--register-endpoint",
        default=None,
        metavar="NAME",
        help="create a Nexus endpoint of this name whose target is this process",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    base_url = f"http://{args.host}:{args.port}"
    client = await Client.connect(
        args.address,
        namespace=args.namespace,
        plugins=[NexusStreams(endpoint=args.endpoint, http_address=args.http)],
    )
    consumer = stream_consumer_operation(
        collect_values,
        initial=list,
        listener_url=args.listener_url or base_url + _DEFAULT_DELIVERIES_PATH,
        client=client,
    )
    service = NexusHttpService(
        nexusrpc.handler.Handler([CollectStreamHandler(consumer)]),
        [consumer],
        data_converter=client.data_converter,
    )
    runner = web.AppRunner(service.application())
    await runner.setup()
    try:
        await web.TCPSite(runner, args.host, args.port).start()
        if args.register_endpoint:
            await _register_endpoint(client, args.register_endpoint, base_url)
        logger.info(
            "serving %s at %s, deliveries at %s",
            CollectStream.__name__,
            base_url,
            consumer._listener_url,  # pyright: ignore[reportPrivateUsage]
        )
        await asyncio.Event().wait()
    finally:
        await consumer.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
