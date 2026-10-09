"""The publish samples run end to end on Redis and the dev server."""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams.samples import (
    path_a_workflow_publish,
    path_b_activity_publish,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("STREAMS_REDIS_URL"),
    reason="set STREAMS_REDIS_URL to run the stream samples",
)


def client_with(client: Client, provider: RedisStreams) -> Client:
    config = client.config()
    config["plugins"] = [provider]
    return Client(**config)


async def test_path_a_a_client_reads_what_a_workflow_published(client: Client):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"sample-{uuid.uuid4().hex}"
    )
    steps = await path_a_workflow_publish.main(
        client_with(client, provider), f"sample-a-{uuid.uuid4().hex}"
    )
    assert steps == ["reserved", "charged", "shipped"]
    await provider.close()


async def test_path_b_a_reader_drops_what_a_failed_attempt_wrote(client: Client):
    provider = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"sample-{uuid.uuid4().hex}"
    )
    text = await path_b_activity_publish.main(
        client_with(client, provider), f"sample-b-{uuid.uuid4().hex}"
    )
    assert text == "an answer to streams"
    await provider.close()
