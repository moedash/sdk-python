"""A store configuration registers as a client plugin, and Core connects to it."""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from temporalio import activity
from temporalio.bridge.proto.streams import (
    LatestRequest,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.client import Client
from temporalio.contrib.streams import StreamError, StreamNotFoundError
from temporalio.contrib.streams._plugin import _StreamsInterceptor, call
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.worker import Worker
from tests.contrib.streams._support import connect_with


async def test_one_store_per_client(client: Client):
    first, second = MemoryStreams(), MemoryStreams()
    config = client.config()
    config["plugins"] = [first, second]
    with pytest.raises(ValueError, match="is already registered"):
        Client(**config)
    # The same instance twice is one store, not two.
    config["plugins"] = [first, first]
    Client(**config)


async def test_a_worker_takes_the_store_of_its_client(client: Client):
    store = MemoryStreams()
    streams_client = await connect_with(client, store)
    worker = Worker(streams_client, task_queue=str(uuid.uuid4()), activities=[_noop])
    interceptors = worker.config(active_config=True).get("interceptors", [])
    assert len([i for i in interceptors if isinstance(i, _StreamsInterceptor)]) == 1
    assert store._temporal_stream_store is not None
    # The store on the client and the Worker still adds one interceptor.
    with pytest.warns(UserWarning, match="same plugin type"):
        worker = Worker(
            streams_client,
            task_queue=str(uuid.uuid4()),
            activities=[_noop],
            plugins=[store],
        )
    interceptors = worker.config(active_config=True).get("interceptors", [])
    assert len([i for i in interceptors if isinstance(i, _StreamsInterceptor)]) == 1


async def test_a_store_on_the_worker_alone_is_refused(client: Client):
    with pytest.raises(ValueError, match="registered on the Worker's client"):
        Worker(client, task_queue="tq", activities=[_noop], plugins=[MemoryStreams()])
    # A client made without connecting has not connected its store either.
    config = client.config()
    config["plugins"] = [MemoryStreams()]
    with pytest.raises(ValueError, match="registered on the Worker's client"):
        Worker(Client(**config), task_queue="tq", activities=[_noop])


async def test_a_client_made_without_connecting_connects_its_store_on_use(
    client: Client,
):
    store = MemoryStreams()
    config = client.config()
    config["plugins"] = [store]
    service = await store._service_for(Client(**config))
    # An owner that does not exist is Core's answer, so the call reached the store.
    stream = StreamAddress(
        namespace=client.namespace,
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id=f"no-such-owner-{uuid.uuid4()}",
        topic="out",
    )
    with pytest.raises(StreamNotFoundError):
        await call(service.latest(LatestRequest(stream=stream)))


async def test_a_store_core_cannot_reach_fails_the_connect(client: Client):
    with pytest.raises(StreamError):
        await connect_with(
            client,
            RedisStreams("redis://127.0.0.1:1", response_timeout=timedelta(seconds=1)),
        )


def test_redis_settings_reach_core():
    store = RedisStreams(
        ["redis://a:6379", "redis://b:6379"],
        cluster=True,
        key_prefix="app",
        retention=timedelta(hours=2),
        blocking_reads_per_node=4,
        response_timeout=timedelta(seconds=3),
    )
    config = store._config.redis
    assert list(config.urls) == ["redis://a:6379", "redis://b:6379"]
    assert config.cluster
    assert config.key_prefix == "app"
    assert config.retention.ToTimedelta() == timedelta(hours=2)
    assert config.blocking_reads_per_node == 4
    assert config.response_timeout.ToTimedelta() == timedelta(seconds=3)
    # Unset settings leave Core's defaults.
    defaults = RedisStreams("redis://a:6379")._config.redis
    assert defaults.blocking_reads_per_node == 0
    assert not defaults.HasField("response_timeout")


def test_redis_setting_mistakes_are_value_errors():
    with pytest.raises(ValueError, match="needs a URL"):
        RedisStreams([])
    with pytest.raises(ValueError, match="key_prefix"):
        RedisStreams("redis://a", key_prefix="")
    with pytest.raises(ValueError, match="retention"):
        RedisStreams("redis://a", retention=timedelta(microseconds=10))


async def test_deleting_needs_a_connected_store(client: Client):
    store = MemoryStreams()
    with pytest.raises(ValueError, match="no client has connected"):
        await store.delete_workflow_streams(client.namespace, "wf")
    await connect_with(client, store)
    assert await store.delete_workflow_streams(client.namespace, "wf") == 0


@activity.defn
async def _noop() -> None:
    pass
