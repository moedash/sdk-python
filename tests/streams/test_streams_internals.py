"""Unit tests for the pieces under ``temporalio.streams`` that no provider owns.

The wire format, the supersession policy, the store key, the cursor prefix and
the plugin registration are shared by every provider and implemented once, so
they are tested once, here, against the private modules. What a provider owes
is in ``test_streams_conformance``; keeping the two apart is what makes that
file answerable by a new provider.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Sequence

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.client import ClientConfig
from temporalio.converter import (
    DataConverter,
    ExternalStorage,
    PayloadCodec,
    StorageDriver,
    StorageDriverClaim,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
)
from temporalio.streams import (
    BEGINNING,
    CONTENT_HASH_KEY,
    Cursor,
    RecordKind,
    StreamCursorError,
    StreamRef,
    Supersession,
    _ids,
    _wire,
    content_fingerprint,
    content_hash,
    decode_body,
    encode_body,
)
from temporalio.streams._policy import AttemptTracker
from temporalio.streams.providers.memory import MemoryStreams
from temporalio.worker import ReplayerConfig, WorkerConfig


def test_record_roundtrips_through_the_wire():
    converter = DataConverter.default.payload_converter
    wire = _wire.to_wire(
        converter,
        topic="decisions",
        kind=RecordKind.DATA,
        value={"n": 1},
        producer_id="model",
        attempt=3,
        sequence=7,
    )
    parsed = _wire.WireRecord.FromString(wire.SerializeToString())
    record = _wire.from_wire(converter, Cursor("memory:0"), parsed, dict)
    assert (
        record.kind,
        record.topic,
        record.producer_id,
        record.attempt,
        record.sequence,
        record.value,
    ) == (RecordKind.DATA, "decisions", "model", 3, 7, {"n": 1})
    assert record.supersession is None
    finish = _wire.to_wire(converter, topic="decisions", kind=RecordKind.FINISH)
    assert not finish.HasField("body")
    assert _wire.from_wire(converter, Cursor("memory:1"), finish, dict).value is None


def test_a_stored_supersession_is_not_a_record():
    converter = DataConverter.default.payload_converter
    wire = _wire.WireRecord(topic="t", kind=int(RecordKind.SUPERSEDED))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="synthesized"):
        _wire.from_wire(converter, Cursor("memory:0"), wire, None)


def test_an_unset_kind_is_read_as_data():
    converter = DataConverter.default.payload_converter
    wire = _wire.WireRecord(topic="t", body=converter.to_payloads([{"n": 1}])[0])
    record = _wire.from_wire(converter, Cursor("memory:0"), wire, dict)
    assert record.kind is RecordKind.DATA
    assert record.value == {"n": 1}


def test_supersession_is_synthesized_from_observations():
    attempts = AttemptTracker()
    assert attempts.note("model", 1, topic="t", previous=BEGINNING) is None
    superseded = attempts.note("model", 2, topic="t", previous=Cursor("memory:0"))
    assert superseded is not None
    assert superseded.kind is RecordKind.SUPERSEDED
    assert superseded.supersession == Supersession("model", 1, 2)
    assert superseded.value is None
    # Positioned before the triggering record, so a resume after it delivers
    # that record next.
    assert superseded.cursor == Cursor("memory:0")
    # The same attempt again is not a new generation.
    assert attempts.note("model", 2, topic="t", previous=Cursor("memory:1")) is None


def test_topic_keys_cannot_collide():
    # A colon in a workflow id must not make two addresses one key.
    assert _ids.topic_key("a:b", "c") != _ids.topic_key("a", "b:c")
    assert _ids.topic_key("a%3Ab", "c") != _ids.topic_key("a:b", "c")
    assert _ids.topic_key("wf", "inputs") == "wf:inputs"


def test_cursors_name_their_provider():
    assert _wire.cursor_position(BEGINNING, provider="memory") is None
    assert _wire.cursor_position(Cursor("memory:42"), provider="memory") == "42"
    with pytest.raises(StreamCursorError):
        _wire.cursor_position(Cursor("redis:1700000000000-0"), provider="memory")


def test_registering_a_provider_twice_is_refused():
    # There is one slot on each of the three, and a user who passes a provider
    # by hand and a provider plugin, or two provider plugins, meant both.
    first, second = MemoryStreams(), MemoryStreams()
    with pytest.raises(ValueError, match="already registered"):
        second.configure_client(ClientConfig(stream_provider=first))  # type: ignore[typeddict-item]
    with pytest.raises(ValueError, match="already registered"):
        second.configure_worker(WorkerConfig(stream_provider=first))  # type: ignore[typeddict-item]
    with pytest.raises(ValueError, match="already registered"):
        second.configure_replayer(ReplayerConfig(stream_provider=first))  # type: ignore[typeddict-item]


def test_registering_the_same_provider_twice_is_fine():
    # A worker built from a client that already carries the plugin configures
    # it again with the same object, which is not a conflict.
    provider = MemoryStreams()
    config = provider.configure_client(ClientConfig(stream_provider=provider))  # type: ignore[typeddict-item]
    assert config.get("stream_provider") is provider
    assert provider.configure_client(ClientConfig()).get("stream_provider") is provider  # type: ignore[typeddict-item]


class _HoldEverything(StorageDriver):
    """A driver that keeps every payload it is handed, in memory."""

    def __init__(self) -> None:
        self.held: list[bytes] = []

    def name(self) -> str:
        return "hold"

    async def store(
        self, context: StorageDriverStoreContext, payloads: Sequence[Payload]
    ) -> list[StorageDriverClaim]:
        claims = []
        for payload in payloads:
            claims.append(StorageDriverClaim(claim_data={"i": str(len(self.held))}))
            self.held.append(payload.SerializeToString())
        return claims

    async def retrieve(
        self,
        context: StorageDriverRetrieveContext,
        claims: Sequence[StorageDriverClaim],
    ) -> list[Payload]:
        return [Payload.FromString(self.held[int(c.claim_data["i"])]) for c in claims]


class _NonceCodec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=os.urandom(8) + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[8:]) for p in payloads]


async def test_encode_body_stamps_the_plaintext_hash_and_offloads_the_body():
    driver = _HoldEverything()
    converter = dataclasses.replace(
        DataConverter.default,
        payload_codec=_NonceCodec(),
        external_storage=ExternalStorage(drivers=[driver], payload_size_threshold=0),
    )
    wire = _wire.to_wire(
        converter.payload_converter, topic="t", kind=RecordKind.DATA, value={"n": 1}
    )
    plaintext = Payload()
    plaintext.CopyFrom(wire.body)

    await encode_body(converter, wire)
    # The hash is over what the converter produced, not over what the codec
    # or the driver made of it, and it rides the record where the store can
    # read it without the plaintext.
    stamped = wire.metadata[CONTENT_HASH_KEY]
    assert stamped.metadata["encoding"] == b"binary/plain"
    assert stamped.data.decode() == content_hash(plaintext)
    assert len(stamped.data) == 64
    # With a threshold of zero the body was offloaded: the record holds the
    # claim and the driver holds the coded payload.
    assert wire.body != plaintext
    assert len(wire.body.external_payloads) == 1
    assert len(driver.held) == 1

    await decode_body(converter, wire)
    assert wire.body == plaintext
    assert wire.metadata[CONTENT_HASH_KEY] == stamped

    # A record without a body has nothing to hash or offload.
    finish = _wire.to_wire(
        converter.payload_converter, topic="t", kind=RecordKind.FINISH
    )
    await encode_body(converter, finish)
    assert CONTENT_HASH_KEY not in finish.metadata
    assert len(driver.held) == 1


async def test_content_fingerprint_is_taken_before_the_codec():
    converter = dataclasses.replace(DataConverter.default, payload_codec=_NonceCodec())
    plain = converter.payload_converter

    def batch(*values: dict) -> list[_wire.WireRecord]:
        return [
            _wire.to_wire(plain, topic="t", kind=RecordKind.DATA, value=v, sequence=i)
            for i, v in enumerate(values, 1)
        ]

    first, retry = batch({"n": 1}, {"n": 2}), batch({"n": 1}, {"n": 2})
    before = content_fingerprint(first)
    assert before == content_fingerprint(retry)
    # Different content, and the same content split differently, both differ.
    assert before != content_fingerprint(batch({"n": 1}, {"n": 3}))
    assert before != content_fingerprint(batch({"n": 1}) + batch({"n": 2}))

    for record in first + retry:
        await encode_body(converter, record)
    # The codec made the two batches' bytes differ; the identity taken first
    # is what lets a store still recognise the retry.
    assert first[0].body != retry[0].body
    assert content_fingerprint(first) != content_fingerprint(retry)
    # Decoding gives the converted bodies back; the hash stays stamped on the
    # record, which is why the identity is taken before encoding, not after.
    for record in first:
        await decode_body(converter, record)
    assert [r.body for r in first] == [r.body for r in batch({"n": 1}, {"n": 2})]
    assert all(CONTENT_HASH_KEY in r.metadata for r in first)


async def test_a_stream_ref_names_one_owner_and_travels_as_json():
    workflow = StreamRef.for_workflow("wf", run_id="r", topic="out")
    activity = StreamRef.for_activity("act", workflow_id="wf", topic="progress")
    standalone = StreamRef.for_standalone("shared")
    assert workflow == StreamRef("workflow", "out", workflow_id="wf", run_id="r")
    assert activity.kind == "activity" and activity.activity_id == "act"
    assert standalone == StreamRef("standalone", "output", stream_id="shared")
    assert standalone.with_topic("x").topic == "x"

    for bad in (
        dict(kind="workflow"),
        dict(kind="workflow", workflow_id="wf", stream_id="s"),
        dict(kind="activity", workflow_id="wf"),
        dict(kind="standalone", stream_id="s", workflow_id="wf"),
        dict(kind="standalone"),
        dict(kind="nexus", stream_id="s"),
        dict(kind="workflow", workflow_id="wf", topic=""),
    ):
        with pytest.raises(ValueError):
            StreamRef(**bad)  # type: ignore[arg-type]

    converter = DataConverter.default
    for ref in (workflow, activity, standalone):
        [carried] = await converter.decode(await converter.encode([ref]), [StreamRef])
        assert carried == ref
    payload = (await converter.encode([standalone]))[0]
    assert payload.metadata["encoding"] == b"json/plain"
