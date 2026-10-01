"""The native provider stamps every body with the hash of its plaintext.

The server deduplicates a producer's repeat on that hash when it is present,
so a codec that encrypts with a fresh nonce per call cannot turn a retry into
a divergent write. These pin where the stamp is taken: over the body as the
payload converter produced it, before the codec on the outside path and before
the worker's payload pass on the workflow path.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

import pytest

import temporalio.converter
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import StreamRecord
from temporalio.bridge.proto.workflow_completion import WorkflowActivationCompletion
from temporalio.bridge.worker import encode_completion
from temporalio.client_stream import Appended
from temporalio.converter import (
    DataConverter,
    PayloadCodec,
    StorageDriverWorkflowInfo,
)
from temporalio.streams import CONTENT_HASH_KEY
from temporalio.streams.providers import native
from temporalio.streams.providers.native import NativeProducer


class _NonceCodec(PayloadCodec):
    """Encodes to something different every time, like a nonce-based cipher."""

    def __init__(self) -> None:
        self.calls = 0

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.calls += 1
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=f"{self.calls}:".encode() + p.data,
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload(data=p.data.split(b":", 1)[1]) for p in payloads]


class _Handle:
    """Records what a producer appends and answers as the server would."""

    def __init__(self) -> None:
        self.owner_run_id = "run"
        self.appended: list[StreamRecord] = []

    async def append(self, *records: StreamRecord, **_: Any) -> Appended:
        self.appended.extend(records)
        return Appended(first_offset=0, next_offset=len(records), count=len(records))


def _plaintext_hash(value: Any) -> bytes:
    payload = temporalio.converter.default().payload_converter.to_payloads([value])[0]
    return hashlib.sha256(payload.SerializeToString()).hexdigest().encode()


def _target(_run_id: str) -> StorageDriverWorkflowInfo:
    return StorageDriverWorkflowInfo(namespace="ns", id="wf")


async def test_an_outside_append_is_stamped_before_the_codec() -> None:
    handle: Any = _Handle()
    converter = DataConverter(payload_codec=_NonceCodec())
    producer: NativeProducer[Any] = NativeProducer(
        handle, None, converter, _target, "t", "p", 1
    )

    await producer.append({"n": 1})
    await producer.append({"n": 1})

    first, second = handle.appended
    # The bodies went out differently encoded and the stamps agree anyway.
    assert first.body.data != second.body.data
    assert first.metadata[CONTENT_HASH_KEY].data == _plaintext_hash({"n": 1})
    assert second.metadata[CONTENT_HASH_KEY].data == _plaintext_hash({"n": 1})
    assert first.metadata[CONTENT_HASH_KEY].metadata["encoding"] == b"binary/plain"


async def test_a_finish_record_carries_no_stamp() -> None:
    handle: Any = _Handle()
    producer: NativeProducer[Any] = NativeProducer(
        handle, None, DataConverter.default, _target, "t", "p", 1
    )
    await producer.finish()
    (record,) = handle.appended
    assert CONTENT_HASH_KEY not in record.metadata


def test_a_workflow_publish_is_stamped_on_the_workflow_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged: list[StreamRecord] = []

    def stage(records: Sequence[StreamRecord], *, stream_name: str) -> None:
        assert stream_name == "t"
        staged.extend(records)

    monkeypatch.setattr(workflow, "_append_stream_records", stage)
    converter = temporalio.converter.default().payload_converter
    body = converter.to_payloads([{"n": 2}])[0]
    record = StreamRecord(topic="t", body=body)

    native._NativeWriteSink("t").publish(record)

    (stamped,) = staged
    assert stamped.metadata[CONTENT_HASH_KEY].data == _plaintext_hash({"n": 2})
    # Two publishes of the same value stamp the same, which is what replay
    # reissues.
    native._NativeWriteSink("t").publish(StreamRecord(topic="t", body=body))
    assert staged[1].metadata[CONTENT_HASH_KEY] == stamped.metadata[CONTENT_HASH_KEY]


async def test_the_workers_payload_pass_leaves_the_stamp_alone() -> None:
    """The codec and the store rewrite the body and the other metadata, never the hash.

    The server reads the declared hash as sent and refuses a value that is not
    hex, so a codec that touched it would fail the workflow's own append.
    """
    converter = DataConverter(payload_codec=_NonceCodec())
    body = converter.payload_converter.to_payloads([{"n": 3}])[0]
    record = native._fingerprint(StreamRecord(topic="t", body=body))
    record.metadata["note"].CopyFrom(Payload(data=b"plain"))
    completion = WorkflowActivationCompletion()
    command = completion.successful.commands.add()
    command.append_stream_records.stream_name = "t"
    command.append_stream_records.records.append(record)

    await encode_completion(
        completion, converter, encode_headers=False, storage_concurrency_limit=1
    )

    sent = completion.successful.commands[0].append_stream_records.records[0]
    assert sent.body.metadata["encoding"] == b"binary/nonce"
    assert sent.metadata["note"].metadata["encoding"] == b"binary/nonce"
    assert sent.metadata[CONTENT_HASH_KEY].data == _plaintext_hash({"n": 3})
    assert sent.metadata[CONTENT_HASH_KEY].metadata["encoding"] == b"binary/plain"
