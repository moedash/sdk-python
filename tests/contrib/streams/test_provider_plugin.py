"""A provider registers as a plugin, once per client or Worker."""

from __future__ import annotations

import pytest

from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.streams import StreamHandle, StreamProviderPlugin, StreamRef
from temporalio.contrib.streams._plugin import _StreamsInterceptor
from temporalio.worker import Worker


class _StubProvider(StreamProviderPlugin):
    def __init__(self, name: str) -> None:
        super().__init__(name)

    def get_stream_handle(self, client: Client, ref: StreamRef) -> StreamHandle:
        raise NotImplementedError

    async def close(self) -> None:
        pass


async def test_one_provider_per_client(client: Client):
    first, second = _StubProvider("first"), _StubProvider("second")
    config = client.config()
    config["plugins"] = [first, second]
    with pytest.raises(ValueError, match="is already registered"):
        Client(**config)
    # The same instance twice is one provider, not two.
    config["plugins"] = [first, first]
    Client(**config)


class _OtherStubProvider(_StubProvider):
    pass


async def test_one_provider_per_worker(client: Client):
    first, second = _StubProvider("first"), _OtherStubProvider("second")
    config = client.config()
    config["plugins"] = [first]
    with pytest.raises(ValueError, match="is already registered"):
        Worker(Client(**config), task_queue="tq", activities=[_noop], plugins=[second])
    # The provider on the client and the Worker adds one interceptor.
    with pytest.warns(UserWarning, match="same plugin type"):
        worker = Worker(
            Client(**config), task_queue="tq", activities=[_noop], plugins=[first]
        )
    interceptors = worker.config(active_config=True).get("interceptors", [])
    assert len([i for i in interceptors if isinstance(i, _StreamsInterceptor)]) == 1


@activity.defn
async def _noop() -> None:
    pass
