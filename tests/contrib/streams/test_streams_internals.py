"""Unit tests for the shared pieces no provider implements.

Topics, refs, cursors, the body helpers, the wire conversion and the
supersession tracker. None of these needs a server.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import pytest

from temporalio import activity
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    BEGINNING,
    CONTENT_HASH_KEY,
    DEFAULT_TOPIC,
    END,
    Cursor,
    RecordKind,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamHandle,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamProviderPlugin,
    StreamRef,
    StreamTopic,
    StreamUnsupportedError,
    Supersession,
    content_fingerprint,
    content_hash,
    decode_body,
    encode_body,
    resolve_topic,
    topic,
)
from temporalio.contrib.streams._cursor import (
    cursor_position,
    mint_cursor,
    stream_hash,
)
from temporalio.contrib.streams._plugin import _StreamsInterceptor
from temporalio.contrib.streams._policy import AttemptTracker
from temporalio.contrib.streams._wire import RecordDecoder, from_wire, to_wire
from temporalio.contrib.streams.proto.v1 import StreamRecord as WireRecord
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import Worker

OUT = topic("out", dict)


def test_a_definition_carries_its_name_and_type():
    assert OUT == StreamTopic("out", dict)
    assert resolve_topic(OUT) == ("out", dict)
    assert resolve_topic("free", int) == ("free", int)
    assert resolve_topic(None) == (DEFAULT_TOPIC, None)


def test_topic_mistakes_are_value_errors():
    with pytest.raises(ValueError):
        topic("")
    with pytest.raises(ValueError):
        resolve_topic("")
    with pytest.raises(ValueError, match="already carries its type"):
        resolve_topic(OUT, dict)


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


def test_a_ref_names_a_workflow_stream():
    ref = StreamRef.for_workflow("wf", topic=OUT)
    assert ref == StreamRef("workflow", "wf", topic="out")
    assert StreamRef.for_workflow("wf").topic == DEFAULT_TOPIC
    assert ref.with_topic(None).topic == DEFAULT_TOPIC
    assert ref.with_topic("other").workflow_id == "wf"


def test_a_ref_refuses_owner_kinds_this_release_lacks():
    for kind in ("activity", "standalone"):
        with pytest.raises(StreamUnsupportedError, match="only Workflow-owned"):
            StreamRef(kind, "x")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown"):
        StreamRef("nexus", "x")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        StreamRef("workflow", "")
    with pytest.raises(ValueError):
        StreamRef("workflow", "wf", topic="")


def test_a_ref_round_trips_through_the_default_converter():
    ref = StreamRef.for_workflow("wf", run_id="run", topic=OUT)
    converter = DataConverter.default.payload_converter
    payload = converter.to_payloads([ref])[0]
    assert converter.from_payloads([payload], [StreamRef])[0] == ref


def test_a_cursor_is_bound_to_its_provider_and_stream():
    one = stream_hash("ns", StreamRef.for_workflow("wf", topic="a"))
    cursor = mint_cursor("memory", one, "7")
    assert cursor_position(cursor, provider="memory", stream=one) == "7"
    assert cursor_position(BEGINNING, provider="memory", stream=one) is None

    with pytest.raises(StreamCursorError, match="minted by the 'memory' provider"):
        cursor_position(cursor, provider="redis", stream=one)
    for other in (
        StreamRef.for_workflow("wf", topic="b"),
        StreamRef.for_workflow("wf2", topic="a"),
    ):
        with pytest.raises(StreamCursorError, match="another stream"):
            cursor_position(cursor, provider="memory", stream=stream_hash("ns", other))
    with pytest.raises(StreamCursorError, match="another stream"):
        cursor_position(
            cursor,
            provider="memory",
            stream=stream_hash("ns2", StreamRef.for_workflow("wf", topic="a")),
        )
    for token in ("garbage", "memory:", f"memory:{one}:"):
        with pytest.raises(StreamCursorError):
            cursor_position(Cursor(token), provider="memory", stream=one)
    with pytest.raises(StreamCursorError, match="END"):
        cursor_position(END, provider="memory", stream=one)


def test_the_stream_hash_follows_the_chain_not_the_run():
    pinned = StreamRef.for_workflow("wf", run_id="r1", topic="a")
    follower = StreamRef.for_workflow("wf", topic="a")
    assert stream_hash("ns", pinned) == stream_hash("ns", follower)
    assert len(stream_hash("ns", pinned)) == 8


def test_the_stream_hash_is_length_delimited():
    assert stream_hash("ns", StreamRef("workflow", "a:b", topic="c")) != stream_hash(
        "ns", StreamRef("workflow", "a", topic="b:c")
    )


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


def test_an_attempt_that_goes_backwards_is_reported():
    said: list[str] = []
    attempts = AttemptTracker(said.append)
    assert attempts.note("model", 2, topic="t", previous=BEGINNING) is None
    assert attempts.note("model", 1, topic="t", previous=Cursor("c3")) is None
    assert len(said) == 1
    assert "attempt 1" in said[0] and "behind attempt 2" in said[0]
    assert "'model'" in said[0]


def test_a_repeat_of_the_current_attempt_is_not_reported():
    said: list[str] = []
    attempts = AttemptTracker(said.append)
    attempts.note("model", 1, topic="t", previous=BEGINNING)
    assert attempts.note("model", 1, topic="t", previous=Cursor("c1")) is None
    assert said == []


def test_the_decoder_reports_a_backwards_attempt():
    said: list[str] = []
    converter = DataConverter.default.payload_converter
    decoder = RecordDecoder(converter, int, after=BEGINNING, warn=said.append)

    def record(attempt: int) -> WireRecord:
        return to_wire(
            converter,
            topic="t",
            kind=RecordKind.DATA,
            value=attempt,
            producer_id="model",
            attempt=attempt,
            sequence=1,
        )

    decoder.decode(Cursor("c1"), record(2))
    out = decoder.decode(Cursor("c2"), record(1))
    # Still delivered, because dropping it would hide what the store holds.
    assert [r.value for r in out] == [1]
    assert len(said) == 1 and "behind attempt 2" in said[0]


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
