"""The stream service bindings generated from its Nexus contract.

``scripts/gen_nexus_streams_api.py --check`` keeps the bindings in step with
the contract; these pin what the rest of the SDK relies on from them.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypeVar

import nexusrpc
import pytest

import temporalio.contrib.streams
from temporalio.contrib.streams import StreamRef
from temporalio.contrib.streams.nexus import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
)
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError

T = TypeVar("T")

_converter = DataConverter.default.payload_converter


def _to_json(value: object) -> dict[str, Any]:
    return json.loads(_converter.to_payloads([value])[0].data)


def _from_json(document: Mapping[str, Any], type_hint: type[T]) -> T:
    payload = _converter.to_payloads([document])[0]
    return _converter.from_payloads([payload], [type_hint])[0]


def test_the_service_definition_carries_the_contract_operations():
    definition = nexusrpc.get_service_definition(TemporalStreams)
    assert definition is not None
    assert definition.name == "temporal.sdk.streams.v1.TemporalStreams"
    operations = {
        operation.name: (operation.input_type, operation.output_type)
        for operation in definition.operation_definitions.values()
    }
    assert operations == {
        "append": (AppendInput, AppendOutput),
        "read": (ReadInput, ReadOutput),
    }


def test_the_contract_ships_beside_the_bindings():
    contract = (
        Path(temporalio.contrib.streams.__file__).parent
        / "nexus"
        / "temporal_streams.nexusrpc.yaml"
    )
    assert "x-nexus-stream-ref: { py: temporalio.contrib.streams.StreamRef }" in (
        contract.read_text()
    )


def test_the_stream_reference_is_the_sdk_type():
    from temporalio.contrib.streams.nexus import StreamRef as ServiceStreamRef

    assert ServiceStreamRef is StreamRef


def test_an_append_crosses_as_the_contract_spells_it():
    ref = StreamRef.for_workflow("wf-1", topic="tokens")
    append = AppendInput(
        stream=ref,
        producer_id="model",
        attempt=2,
        sequence=5,
        payloads=[b"\x0a\x01x", b""],
    )
    document = _to_json(append)
    assert document == {
        "stream": {
            "kind": "workflow",
            "workflow_id": "wf-1",
            "topic": "tokens",
        },
        "producer_id": "model",
        "attempt": 2,
        "sequence": 5,
        "payloads": [base64.b64encode(b"\x0a\x01x").decode(), ""],
    }
    decoded = _from_json(document, AppendInput)
    assert decoded == append
    assert type(decoded.stream) is StreamRef


def test_a_read_answer_decodes_with_its_cursors_and_records():
    document = {
        "records": [{"token": "memory:abcd1234:0", "record": "CgF0"}],
        "next_token": "memory:abcd1234:0",
        "done": False,
        "added_later": True,
    }
    answer = _from_json(document, ReadOutput)
    assert isinstance(answer, ReadOutput)
    assert answer.records == [
        RecordWire(token="memory:abcd1234:0", record=b"\x0a\x01t")
    ]
    assert answer.next_token == "memory:abcd1234:0"
    assert answer.done is False


def test_an_input_with_an_unknown_member_is_refused():
    document = _to_json(ReadInput(stream=StreamRef.for_workflow("wf-1")))
    document["max_record"] = 5
    # The type a Nexus Worker answers BAD_REQUEST for.
    with pytest.raises(ApplicationError) as refused:
        _from_json(document, ReadInput)
    assert refused.value.type == "PayloadValidationError"
    assert "max_record" in str(refused.value.details)


def test_a_reference_this_release_cannot_open_is_refused_at_decode():
    document = _to_json(ReadInput(stream=StreamRef.for_workflow("wf-1")))
    stream = document["stream"]
    assert isinstance(stream, dict)
    stream["kind"] = "activity"
    with pytest.raises(Exception, match="activity"):
        _from_json(document, ReadInput)
