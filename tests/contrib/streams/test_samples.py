"""The publish samples run end to end on Redis and the dev server."""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.samples import (
    path_a_workflow_publish,
    path_b_activity_publish,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("STREAMS_REDIS_URL"),
    reason="set STREAMS_REDIS_URL to run the stream samples",
)


async def test_path_a_a_client_reads_what_a_workflow_published(client: Client):
    store = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"sample-{uuid.uuid4().hex}"
    )
    # Ids of their own, so runs that share a dev server never collide.
    steps = await path_a_workflow_publish.main(
        await connect_with(client, store),
        f"sample-a-{uuid.uuid4().hex}",
        workflow_id=f"order-{uuid.uuid4().hex}",
    )
    assert steps == ["reserved", "charged", "shipped"]


async def test_path_b_a_reader_drops_what_a_failed_attempt_wrote(client: Client):
    store = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"sample-{uuid.uuid4().hex}"
    )
    text = await path_b_activity_publish.main(
        await connect_with(client, store),
        f"sample-b-{uuid.uuid4().hex}",
        workflow_id=f"answer-{uuid.uuid4().hex}",
    )
    assert text == "an answer to streams"
