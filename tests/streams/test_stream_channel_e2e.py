"""A native stream notifies the channel named by the stream, on a live server.

Every append and the close of a native stream notify the channel
:func:`temporalio.client.stream_channel` derives from the stream's ref, so a
client follows a stream the way it follows an external one: by polling the
channel or registering a callback on it. The workflow's own consumption of a
stream is untouched by this and is covered elsewhere. Needs a server on which
streams drive channels, named with ``-E host:port``; skipped otherwise.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import (
    Callback,
    ChannelAddress,
    ChannelKind,
    Client,
    stream_channel,
)
from temporalio.common import Execution, ExecutionType
from temporalio.service import RPCError
from temporalio.streams import StreamRef, topic
from temporalio.streams.providers.native import NativeStreams
from tests.helpers import assert_eventually, new_worker

OUT = topic("out", dict)

pytestmark = pytest.mark.needs_stream_channel_server


async def _native(client: Client, provider: NativeStreams) -> Client:
    """A client on the same server as ``client`` with the native provider on it."""
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=[provider],
    )


def _closed(client: Client, notification: Any) -> bool:
    if "closed" not in notification.metadata:
        return False
    return client.data_converter.payload_converter.from_payload(
        notification.metadata["closed"], bool
    )


async def test_a_standalone_stream_notifies_the_channel_named_by_its_id(
    client: Client,
):
    provider = NativeStreams()
    native = await _native(client, provider)
    stream_id = f"chan-{uuid.uuid4().hex[:8]}"
    try:
        created = await native.create_stream(stream_id)
        address = stream_channel(created.ref(topic=OUT))
        # A standalone stream's topics share one stream, so the topic is not in
        # the name and the channel is an independent one.
        assert address == ChannelAddress(f"stream/{stream_id}", None)
        producer = created.producer(topic=OUT, producer_id="writer", attempt=1)

        async def changes(count: int) -> list[Any]:
            polled = await native.poll_channel(address.channel, wait=False)
            assert [n.counter for n in polled] == list(range(1, count + 1))
            return polled

        # A standalone stream reaches its channel through a task of its own
        # that hands over the latest change, so the notifications trail the
        # appends and a burst arrives as its newest change. Each append waits
        # for its notification so that every change is seen.
        await producer.append({"n": 1})
        await assert_eventually(lambda: changes(1))
        await producer.append({"n": 2})
        polled = await assert_eventually(lambda: changes(2))
        for notification in polled:
            assert notification.channel == address.channel
            assert notification.linked_to is None
            # The position is the head after the change, as a native cursor
            # names one: the stream id in place of a run, then the offset.
            assert notification.position.decode().startswith(f"{stream_id}:")
            assert not _closed(native, notification)
        await created.close()
        [third] = await native.poll_channel(
            address.channel, after_counter=2, wait=timedelta(seconds=10)
        )
        assert third.counter == 3
        assert _closed(native, third)
        description = await native.describe_channel(address.channel)
        assert description.kind == ChannelKind.INDEPENDENT
        assert description.latest is not None and description.latest.counter == 3
        assert description.retained_count == 3
        # A callback registers on the derived name like on any channel.
        callback = Callback(url="http://localhost:1/never-called", headers={})
        listener_id = await native.register_channel_listener(address.channel, callback)
        description = await native.describe_channel(address.channel)
        assert [listener.callback for listener in description.listeners] == [callback]
        await native.unregister_channel_listener(address.channel, listener_id)
    finally:
        await provider.close()


@workflow.defn
class WriteOnNudge:
    """Writes one record at the start and one more per nudge, until ``rounds``."""

    def __init__(self) -> None:
        self._nudges = 0
        self._written = 0

    @workflow.run
    async def run(self, rounds: int) -> int:
        writer = workflow.stream_writer(OUT)
        writer.publish({"n": 0})
        while self._written < rounds:
            await workflow.wait_condition(lambda: self._nudges > self._written)
            self._written += 1
            writer.publish({"n": self._written})
        return self._written

    @workflow.signal
    def nudge(self) -> None:
        self._nudges += 1


async def test_a_workflow_stream_notifies_the_channel_linked_to_its_owner(
    client: Client,
):
    provider = NativeStreams()
    native = await _native(client, provider)
    worker = new_worker(native, WriteOnNudge)
    running = asyncio.create_task(worker.run())
    # More rounds than nudges: the linked ring dies with the run, so the run
    # stays open until the test has read it and is ended by hand.
    handle = await native.start_workflow(
        WriteOnNudge.run, 10, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
    )
    try:
        address = stream_channel(StreamRef.for_workflow(handle.id, topic=OUT))
        assert address == ChannelAddress("stream/out", Execution.workflow(handle.id))
        assert address.workflow_id == handle.id
        assert address == stream_channel(
            native.get_stream_handle(handle.id).ref(topic=OUT)
        )

        async def changes(count: int) -> list[Any]:
            polled = await native.poll_channel(
                address.channel, workflow_id=address.workflow_id, wait=False
            )
            assert [n.counter for n in polled] == list(range(1, count + 1))
            return polled

        # The first task's publish is one append, so one notification.
        await assert_eventually(lambda: changes(1), timeout=timedelta(seconds=30))
        await handle.signal(WriteOnNudge.nudge)
        await assert_eventually(lambda: changes(2), timeout=timedelta(seconds=30))
        await handle.signal(WriteOnNudge.nudge)
        polled = await assert_eventually(
            lambda: changes(3), timeout=timedelta(seconds=30)
        )
        run_id = handle.first_execution_run_id
        for notification in polled:
            assert notification.linked_to is not None
            assert notification.linked_to.business_id == handle.id
            assert notification.position.decode().startswith(f"{run_id}:")
        description = await native.describe_channel(
            address.channel, workflow_id=address.workflow_id
        )
        assert description.kind == ChannelKind.LINKED
        assert description.latest is not None and description.latest.counter == 3
    finally:
        with contextlib.suppress(RPCError):
            await handle.terminate()
        try:
            await asyncio.wait_for(worker.shutdown(), 15)
        except asyncio.TimeoutError:
            running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await provider.close()


_released: dict[str, bool] = {}
"""Activity ids the test has let go, read by the activity in the same process."""


@activity.defn
async def write_then_wait() -> str:
    """Writes one record to the activity's own stream and lingers until released."""
    activity_id = activity.info().activity_id
    producer = activity.stream_handle().producer(topic=OUT)
    await producer.append({"n": 1})
    while not _released.get(activity_id):
        activity.heartbeat()
        await asyncio.sleep(0.2)
    return activity_id


@pytest.mark.needs_execution_server
async def test_a_standalone_activity_stream_notifies_the_channel_linked_to_it(
    client: Client,
):
    provider = NativeStreams()
    native = await _native(client, provider)
    activity_id = f"act-{uuid.uuid4().hex[:8]}"
    async with new_worker(native, activities=[write_then_wait]) as worker:
        handle = await native.start_activity(
            write_then_wait,
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=60),
        )
        try:
            # A standalone activity is an execution of its own, so its stream
            # notifies a channel linked to it, under the topic's name alone.
            address = stream_channel(StreamRef.for_activity(activity_id, topic=OUT))
            assert address == ChannelAddress(
                "stream/out", Execution.activity(activity_id)
            )
            assert address.workflow_id is None

            async def changes(count: int) -> list[Any]:
                polled = await native.poll_channel(
                    address.channel, execution=address.execution, wait=False
                )
                assert [n.counter for n in polled] == list(range(1, count + 1))
                return polled

            [first] = await assert_eventually(
                lambda: changes(1), timeout=timedelta(seconds=30)
            )
            assert first.linked_to is not None
            assert (first.linked_to.type, first.linked_to.business_id) == (
                ExecutionType.ACTIVITY,
                activity_id,
            )
            description = await native.describe_channel(
                address.channel, execution=address.execution
            )
            assert description.kind == ChannelKind.LINKED
            assert description.linked_to is not None
            assert description.linked_to.business_id == activity_id
        finally:
            _released[activity_id] = True
            with contextlib.suppress(Exception):
                await asyncio.wait_for(handle.result(), 30)
            await provider.close()
