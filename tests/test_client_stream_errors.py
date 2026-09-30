"""What a refused stream call raises.

The service carries no typed detail for its refusals, so a typed one arrives
as a ``FAILED_PRECONDITION`` with a reason token at the front of the message.
These pin the mapping: each token to its error, the phrases an older server
sends to the same errors, and an unrelated message on the same code to a
plain ``RPCError`` that keeps the code.
"""

from __future__ import annotations

import grpc
import pytest

from temporalio.client_stream import translate_error
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import (
    StreamCursorError,
    StreamNotFoundError,
    StreamProducerError,
)


@pytest.mark.parametrize(
    "details",
    [
        "STREAM_PRODUCER_CONFLICT: producer sequence 3 already used with different content",
        "STREAM_PRODUCER_STALE_SEQUENCE: stale producer sequence 2, last accepted for "
        'producer "p" is 3',
    ],
)
def test_a_producer_refusal_is_a_producer_error(details: str) -> None:
    error = translate_error(grpc.StatusCode.FAILED_PRECONDITION, details)
    assert isinstance(error, StreamProducerError)
    assert str(error) == details


def test_an_older_servers_producer_refusal_is_a_producer_error() -> None:
    # Before the tokens the refusal was an INVALID_ARGUMENT with the phrase.
    details = 'stale producer sequence 2, last accepted for producer "p" is 3'
    error = translate_error(grpc.StatusCode.INVALID_ARGUMENT, details)
    assert isinstance(error, StreamProducerError)


@pytest.mark.parametrize(
    "details",
    [
        "STREAM_CURSOR_BELOW_FLOOR: offset 2 is below the stream's floor of 5",
        "offset 2 is below the stream's floor of 5",
    ],
)
def test_a_read_below_the_floor_is_a_cursor_error(details: str) -> None:
    error = translate_error(grpc.StatusCode.FAILED_PRECONDITION, details)
    assert isinstance(error, StreamCursorError)
    assert str(error) == details


def test_not_found_is_a_not_found_error() -> None:
    error = translate_error(grpc.StatusCode.NOT_FOUND, "no stream with id 's'")
    assert isinstance(error, StreamNotFoundError)


def test_an_unrelated_message_keeps_its_code() -> None:
    # The same codes with another message are the server's ordinary refusals.
    for code, expected in [
        (grpc.StatusCode.INVALID_ARGUMENT, RPCStatusCode.INVALID_ARGUMENT),
        (grpc.StatusCode.FAILED_PRECONDITION, RPCStatusCode.FAILED_PRECONDITION),
        (grpc.StatusCode.UNAVAILABLE, RPCStatusCode.UNAVAILABLE),
    ]:
        error = translate_error(code, "stream is closed", b"raw")
        assert type(error) is RPCError
        assert error.status == expected
        assert error.raw_grpc_status == b"raw"
        assert str(error) == "stream is closed"


def test_a_token_needs_its_code_and_its_separator() -> None:
    # A token on another code is prose, and so is one with no ": " after it.
    error = translate_error(
        grpc.StatusCode.INVALID_ARGUMENT, "STREAM_PRODUCER_CONFLICT: elsewhere"
    )
    assert type(error) is RPCError
    error = translate_error(
        grpc.StatusCode.FAILED_PRECONDITION, "STREAM_CURSOR_BELOW_FLOOR"
    )
    assert type(error) is RPCError
    error = translate_error(
        grpc.StatusCode.FAILED_PRECONDITION, "STREAM_SOMETHING_ELSE: unknown token"
    )
    assert type(error) is RPCError


def test_an_empty_message_reads_as_the_code() -> None:
    error = translate_error(grpc.StatusCode.UNAVAILABLE, "")
    assert str(error) == "UNAVAILABLE"
