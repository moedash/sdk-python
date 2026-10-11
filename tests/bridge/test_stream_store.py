"""Core's in-process stream service, reached through the bridge."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any, cast

import pytest

import temporalio.bridge.client
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import (
    AppendRecord,
    AppendRequest,
    LatestRequest,
    MemoryStoreConfig,
    NamedProducer,
    ReadRequest,
    StreamAddress,
    StreamFailureKind,
    StreamOwnerKind,
    StreamStoreConfig,
)
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.bridge.streams import StreamCallFailure, StreamStore
from temporalio.bridge.streams_generated import StreamService
from temporalio.client import Client


async def memory_service(client: Client) -> StreamService:
    bridge = cast(Any, client.service_client)
    connected: temporalio.bridge.client.Client = await bridge._connected_client()
    store = await StreamStore.connect(
        connected, StreamStoreConfig(memory=MemoryStoreConfig())
    )
    return StreamService(store)


async def running_owner(client: Client) -> str:
    # The service asks the server about the owner, so it has to exist. No Worker runs it.
    workflow_id = f"stream-owner-{uuid.uuid4()}"
    await client.start_workflow(
        "StreamOwner", id=workflow_id, task_queue=f"unpolled-{uuid.uuid4()}"
    )
    return workflow_id


def address(client: Client, workflow_id: str) -> StreamAddress:
    return StreamAddress(
        namespace=client.namespace,
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id=workflow_id,
        topic="out",
    )


def append_request(
    stream: StreamAddress, *bodies: bytes, sequence: int = 1
) -> AppendRequest:
    return AppendRequest(
        stream=stream,
        named=NamedProducer(producer_id="p", attempt=1),
        sequence=sequence,
        records=[
            AppendRecord(
                kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
                body=Payload(data=body),
                content_hash=hashlib.sha256(body).digest(),
            )
            for body in bodies
        ],
    )


async def test_an_append_reads_back_through_the_generated_stub(client: Client):
    service = await memory_service(client)
    stream = address(client, await running_owner(client))

    appended = await service.append(append_request(stream, b"a", b"b"))
    read = await service.read(ReadRequest(stream=stream))
    latest = await service.latest(LatestRequest(stream=stream))

    assert [record.stored.body.data for record in read.records] == [b"a", b"b"]
    assert read.records[0].cursor == appended.first_cursor
    assert read.records[-1].cursor == appended.last_cursor
    assert read.cursor == appended.last_cursor
    assert latest.cursor == appended.last_cursor


async def test_a_refused_call_crosses_as_core_s_failure(client: Client):
    service = await memory_service(client)
    stream = address(client, await running_owner(client))
    await service.append(append_request(stream, b"a"))

    # The same sequence with other content is a divergent retry.
    with pytest.raises(StreamCallFailure) as refused:
        await service.append(append_request(stream, b"other"))

    assert (
        refused.value.failure.kind
        == StreamFailureKind.STREAM_FAILURE_KIND_PRODUCER_DIVERGENT
    )
    assert refused.value.failure.message


async def test_a_stream_without_an_owner_is_not_found(client: Client):
    service = await memory_service(client)
    stream = address(client, f"no-such-owner-{uuid.uuid4()}")

    with pytest.raises(StreamCallFailure) as refused:
        await service.append(append_request(stream, b"a"))

    assert refused.value.failure.kind == StreamFailureKind.STREAM_FAILURE_KIND_NOT_FOUND
