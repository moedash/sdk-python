"""The Redis provider works as a user with exactly the documented ACL."""

from __future__ import annotations

import os
import uuid
from datetime import timedelta

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._redis_acl import documented_user
from tests.helpers import new_worker

pytestmark = pytest.mark.skipif(
    not os.environ.get("STREAMS_REDIS_URL"),
    reason="set STREAMS_REDIS_URL to run the Redis ACL test",
)

EVENTS = topic("events", dict)


@workflow.defn
class PublishTwice:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        await workflow.sleep(timedelta(milliseconds=10))
        workflow_writer(EVENTS).publish({"n": 2})


async def test_publish_read_and_delete_as_the_documented_user(client: Client):
    prefix = f"test-acl-{uuid.uuid4().hex}"
    url = os.environ["STREAMS_REDIS_URL"]
    # The page says the delete helper also needs SCAN.
    async with documented_user(url, client.namespace, prefix, "+scan") as acl_url:
        provider = RedisStreams(acl_url, key_prefix=prefix)
        config = client.config()
        config["plugins"] = [provider]
        streams_client = Client(**config)
        workflow_id = f"redis-acl-{uuid.uuid4().hex}"
        async with new_worker(streams_client, PublishTwice) as worker:
            handle = await streams_client.start_workflow(
                PublishTwice.run, id=workflow_id, task_queue=worker.task_queue
            )
            stream = provider.get_stream_handle(
                client, StreamRef.for_workflow(workflow_id)
            )
            producer = stream.producer(topic=EVENTS, producer_id="p", attempt=1)
            await producer.append({"n": "outside"})
            await handle.result()
        values = [r.value async for r in stream.read(topic=EVENTS)]
        assert {"n": 1} in values and {"n": 2} in values
        assert {"n": "outside"} in values
        assert await provider.delete_workflow_streams(client.namespace, workflow_id) > 0
        await provider.close()
