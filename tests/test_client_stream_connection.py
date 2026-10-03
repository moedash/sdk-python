"""The stream channel is the client's connection, opened again with ``grpcio``.

sdk-core does not know the stream service, so its channel cannot be shared.
What can be shared is the configuration: these pin that a TLS-configured
client yields a secure channel with the same material, that an API key rides
as the bearer header along with the client's other headers, and that the
client's ``retry_config`` is what the shared stream client retries under.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import grpc
import grpc.aio
import pytest

import temporalio.api.streamservice.v1 as stream
from temporalio import client_stream
from temporalio.client_stream import Connection, StreamClient
from temporalio.service import (
    ConnectConfig,
    HttpConnectProxyConfig,
    KeepAliveConfig,
    RetryConfig,
    TLSConfig,
    __version__,
)

# The name the vendored stubs call the service by, server-internal as it is.
_SERVICE = "temporal.server.chasm.lib.stream.proto.v1.StreamService"


def _fake_client(config: ConnectConfig, namespace: str = "ns") -> Any:
    return SimpleNamespace(
        service_client=SimpleNamespace(config=config), namespace=namespace
    )


def test_a_plaintext_client_yields_an_insecure_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: dict[str, Any] = {}

    def insecure_channel(target: str, **kwargs: Any) -> str:
        opened.update(target=target, **kwargs)
        return "channel"

    monkeypatch.setattr(grpc.aio, "insecure_channel", insecure_channel)
    connection = Connection.from_config(ConnectConfig(target_host="localhost:7233"))
    assert not connection.secure
    assert connection.channel() == "channel"
    assert opened["target"] == "localhost:7233"
    keep_alive = KeepAliveConfig.default
    assert ("grpc.keepalive_time_ms", keep_alive.interval_millis) in opened["options"]
    assert ("grpc.keepalive_timeout_ms", keep_alive.timeout_millis) in opened["options"]


def test_a_tls_client_yields_a_secure_channel_with_its_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    made: dict[str, Any] = {}
    opened: dict[str, Any] = {}

    def ssl_channel_credentials(**kwargs: Any) -> str:
        made.update(kwargs)
        return "credentials"

    def secure_channel(target: str, credentials: Any, **kwargs: Any) -> str:
        opened.update(target=target, credentials=credentials, **kwargs)
        return "channel"

    monkeypatch.setattr(grpc, "ssl_channel_credentials", ssl_channel_credentials)
    monkeypatch.setattr(grpc.aio, "secure_channel", secure_channel)

    connection = Connection.from_config(
        ConnectConfig(
            target_host="cloud.example:7233",
            tls=TLSConfig(
                server_root_ca_cert=b"root",
                client_cert=b"cert",
                client_private_key=b"key",
                domain="cloud.example",
                verification_server_name="pinned.test",
            ),
        )
    )
    assert connection.secure
    assert connection.channel() == "channel"
    assert made == {
        "root_certificates": b"root",
        "private_key": b"key",
        "certificate_chain": b"cert",
    }
    assert opened["target"] == "cloud.example:7233"
    assert opened["credentials"] == "credentials"
    assert ("grpc.ssl_target_name_override", "pinned.test") in opened["options"]
    assert ("grpc.default_authority", "cloud.example") in opened["options"]


def test_tls_is_on_by_default_with_an_api_key_and_off_when_refused() -> None:
    with_key = Connection.from_config(
        ConnectConfig(target_host="cloud.example:7233", api_key="secret")
    )
    assert with_key.secure
    assert with_key.server_root_ca_cert is None, "system roots"
    refused = Connection.from_config(
        ConnectConfig(target_host="localhost:7233", api_key="secret", tls=False)
    )
    assert not refused.secure
    assert ("authorization", "Bearer secret") in refused.headers


def test_a_scheme_in_the_target_decides_and_is_dropped() -> None:
    connection = Connection.from_config(
        ConnectConfig(target_host="https://cloud.example:7233")
    )
    assert connection.secure
    assert connection.target == "cloud.example:7233"


def test_a_proxy_and_the_clients_own_authorization_carry_over() -> None:
    connection = Connection.from_config(
        ConnectConfig(
            target_host="localhost:7233",
            api_key="ignored",
            tls=False,
            rpc_metadata={"Authorization": "Custom token", "x-tenant": "t1"},
            http_connect_proxy_config=HttpConnectProxyConfig(
                target_host="proxy:3128", basic_auth=("user", "pass")
            ),
        )
    )
    # Core leaves a caller's own authorization header alone.
    assert ("Authorization", "Custom token") in connection.headers
    assert ("x-tenant", "t1") in connection.headers
    assert not any(key == "authorization" for key, _ in connection.headers)
    assert connection.http_proxy == "http://user:pass@proxy:3128"


class _Recorder:
    """Answers describe and keeps the headers it arrived with."""

    def __init__(self) -> None:
        self.metadata: dict[str, str | bytes] = {}

    async def describe(
        self, _request: Any, context: Any
    ) -> stream.DescribeStreamResponse:
        self.metadata = dict(context.invocation_metadata() or ())
        return stream.DescribeStreamResponse(
            frontend_response=stream.DescribeStreamOutput(
                state=stream.StreamState(head_offset=3)
            )
        )

    def register(self, server: grpc.aio.Server) -> None:
        # The generated registration wants the whole servicer; one method is
        # enough to see the headers.
        handler: Any = grpc.unary_unary_rpc_method_handler(
            self.describe,
            request_deserializer=stream.DescribeStreamRequest.FromString,
            response_serializer=stream.DescribeStreamResponse.SerializeToString,
        )
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    _SERVICE, {"DescribeStream": handler}
                ),
            )
        )


async def test_an_api_key_client_sends_the_header_on_every_call() -> None:
    recorder = _Recorder()
    server = grpc.aio.server()
    recorder.register(server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    config = ConnectConfig(
        target_host=f"127.0.0.1:{port}",
        api_key="secret",
        tls=False,
        rpc_metadata={"x-tenant": "t1"},
    )
    streams = StreamClient.for_connection(Connection.from_config(config), "ns")
    try:
        state = await streams.get("s").describe()
        assert state.head_offset == 3
        assert recorder.metadata["authorization"] == "Bearer secret"
        assert recorder.metadata["x-tenant"] == "t1"
        assert recorder.metadata["client-name"] == "temporal-python"
        assert recorder.metadata["client-version"] == __version__
    finally:
        await streams.close()
        await server.stop(None)


async def test_the_shared_client_retries_under_the_clients_config() -> None:
    retry = RetryConfig(max_retries=3)
    client = _fake_client(
        ConnectConfig(target_host="localhost:7233", retry_config=retry), "ns"
    )
    try:
        shared = client_stream.shared_client(client)
        assert shared._retry_config is retry
        assert shared is client_stream.shared_client(client), "one channel per key"
        # A client with other credentials to the same host is not the same channel.
        other = _fake_client(
            ConnectConfig(target_host="localhost:7233", api_key="k", tls=False), "ns"
        )
        assert client_stream.shared_client(other) is not shared
        assert client_stream.shared_key(other) != client_stream.shared_key(client)
    finally:
        await client_stream.close_shared_clients()
