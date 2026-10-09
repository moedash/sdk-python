"""Unit tests for the shared pieces no provider implements.

Topics, refs, cursors, the body helpers, the wire conversion and the
supersession tracker. None of these needs a server.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    BEGINNING,
    CONTENT_HASH_KEY,
    Cursor,
    RecordKind,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamUnsupportedError,
    Supersession,
    content_fingerprint,
    content_hash,
    decode_body,
    encode_body,
)
from temporalio.contrib.streams._policy import AttemptTracker
from temporalio.contrib.streams._wire import RecordDecoder, from_wire, to_wire
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.converter import DataConverter, PayloadCodec


def test_every_condition_is_a_stream_error():
    for error in (
        StreamClosedError,
        StreamCursorError,
        StreamExpiredError,
        StreamNotFoundError,
        StreamOutcomeUnknownError,
        StreamProducerError,
        StreamUnsupportedError,
    ):
        assert issubclass(error, StreamError)
    # Expired is a kind of invalid cursor, so a caller that only resets on a
    # bad cursor still catches it, and one that cares can tell them apart.
    assert issubclass(StreamExpiredError, StreamCursorError)
    # Refused and unknown are different answers to a retrying caller.
    assert not issubclass(StreamOutcomeUnknownError, StreamProducerError)
    assert not issubclass(StreamProducerError, StreamOutcomeUnknownError)


def test_the_envelope_keeps_its_field_numbers():
    # The bytes are the stored format; a renumbered field would make every
    # retained record unreadable.
    fields = {f.name: f.number for f in WireRecord.DESCRIPTOR.fields}
    assert fields == {
        "body": 1,
        "metadata": 2,
        "topic": 3,
        "kind": 4,
        "producer_id": 5,
        "attempt": 6,
        "sequence": 7,
    }
    assert WireRecord.DESCRIPTOR.full_name == "temporal.sdk.streams.v1.StreamRecord"
    kinds = WireRecord.DESCRIPTOR.fields_by_name["kind"].enum_type
    assert kinds is not None
    assert {v.name: v.number for v in kinds.values} == {
        "STREAM_RECORD_KIND_UNSPECIFIED": 0,
        "STREAM_RECORD_KIND_DATA": 1,
        "STREAM_RECORD_KIND_FINISH": 2,
    }
    for kind in (RecordKind.UNSPECIFIED, RecordKind.DATA, RecordKind.FINISH):
        assert int(kind) in kinds.values_by_number


def test_a_record_round_trips_through_the_wire():
    converter = DataConverter.default.payload_converter
    wire = to_wire(
        converter,
        topic="out",
        kind=RecordKind.DATA,
        value={"n": 1},
        producer_id="p",
        attempt=2,
        sequence=3,
    )
    parsed = WireRecord.FromString(wire.SerializeToString())
    record = from_wire(converter, Cursor("c"), parsed, dict)
    assert record.kind is RecordKind.DATA
    assert record.value == {"n": 1}
    assert (record.producer_id, record.attempt, record.sequence) == ("p", 2, 3)

    finish = to_wire(converter, topic="out", kind=RecordKind.FINISH)
    assert not finish.HasField("body")
    assert from_wire(converter, Cursor("c"), finish, None).kind is RecordKind.FINISH

    unset = WireRecord(topic="out", body=converter.to_payloads([1])[0])
    assert from_wire(converter, Cursor("c"), unset, int).kind is RecordKind.DATA


def test_no_writer_stores_a_synthesized_kind():
    converter = DataConverter.default.payload_converter
    for kind in (RecordKind.SUPERSEDED, RecordKind.UNSPECIFIED):
        with pytest.raises(ValueError):
            to_wire(converter, topic="out", kind=kind)


def test_a_raw_value_passes_through_untouched():
    converter = DataConverter.default.payload_converter
    raw = Payload(metadata={"encoding": b"binary/custom"}, data=b"\x00\x01")
    wire = to_wire(converter, topic="out", kind=RecordKind.DATA, value=RawValue(raw))
    assert wire.body == raw
    record = from_wire(converter, Cursor("c"), wire, RawValue)
    assert isinstance(record.value, RawValue)
    assert record.value.payload == raw


def test_supersession_is_synthesized_from_observations():
    attempts = AttemptTracker()
    assert attempts.note("model", 1, topic="t", previous=BEGINNING) is None
    assert attempts.note("model", 1, topic="t", previous=Cursor("c1")) is None
    superseded = attempts.note("model", 2, topic="t", previous=Cursor("c2"))
    assert superseded is not None
    assert superseded.kind is RecordKind.SUPERSEDED
    assert superseded.cursor == Cursor("c2")
    assert superseded.supersession == Supersession("model", 1, 2)
    assert attempts.note("model", 2, topic="t", previous=Cursor("c3")) is None
    # Without an id or an attempt there is no generation to compare.
    assert attempts.note("", 5, topic="t", previous=Cursor("c4")) is None
    assert attempts.note("other", 0, topic="t", previous=Cursor("c4")) is None


def test_the_decoder_skips_a_record_it_cannot_decode():
    said: list[str] = []
    converter = DataConverter.default.payload_converter
    decoder = RecordDecoder(converter, int, after=BEGINNING, warn=said.append)
    bad = WireRecord(
        topic="t",
        kind=RecordKind.DATA.value,  # type: ignore[arg-type]
        body=Payload(metadata={"encoding": b"json/plain"}, data=b"{not json"),
    )
    assert decoder.decode(Cursor("c1"), bad) == []
    assert said and "c1" in said[0]
    good = to_wire(converter, topic="t", kind=RecordKind.DATA, value=3)
    assert [r.value for r in decoder.decode(Cursor("c2"), good)] == [3]


def test_the_content_hash_is_the_plaintext_payload_hash():
    converter = DataConverter.default.payload_converter
    one = converter.to_payloads([{"n": 1}])[0]
    assert content_hash(one) == content_hash(converter.to_payloads([{"n": 1}])[0])
    assert content_hash(one) != content_hash(converter.to_payloads([{"n": 2}])[0])
    assert len(content_hash(one)) == 64


def test_the_fingerprint_is_length_delimited_over_the_batch():
    converter = DataConverter.default.payload_converter

    def records(*values: str) -> list[WireRecord]:
        return [
            to_wire(converter, topic="t", kind=RecordKind.DATA, value=v) for v in values
        ]

    assert content_fingerprint(records("a", "b")) == content_fingerprint(
        records("a", "b")
    )
    assert content_fingerprint(records("a", "b")) != content_fingerprint(
        records("b", "a")
    )
    assert content_fingerprint(records("ab")) != content_fingerprint(records("a", "b"))


class NonceCodec(PayloadCodec):
    """Encrypts with a fresh nonce per call, so two encodings never match."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=uuid.uuid4().bytes + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[16:]) for p in payloads]


async def test_the_body_is_hashed_before_the_codec_and_decoded_after():
    converter = DataConverter(payload_codec=NonceCodec())
    plain = to_wire(
        converter.payload_converter, topic="t", kind=RecordKind.DATA, value={"n": 1}
    )
    expected = content_hash(plain.body)

    first = WireRecord()
    first.CopyFrom(plain)
    second = WireRecord()
    second.CopyFrom(plain)
    await encode_body(converter, first)
    await encode_body(converter, second)
    # The codec made the stored bytes differ, and the hash did not move.
    assert first.body != second.body
    assert first.body.metadata["encoding"] == b"binary/nonce"
    for stamped in (first, second):
        assert stamped.metadata[CONTENT_HASH_KEY].data.decode() == expected
        assert stamped.metadata[CONTENT_HASH_KEY].metadata["encoding"] == (
            b"binary/plain"
        )

    await decode_body(converter, first)
    assert first.body == plain.body

    finish = to_wire(converter.payload_converter, topic="t", kind=RecordKind.FINISH)
    await encode_body(converter, finish)
    assert CONTENT_HASH_KEY not in finish.metadata
