"""The error family, Core's failure kinds, and the cursors a read starts from."""

from __future__ import annotations

from temporalio.bridge.proto.streams import StreamFailure, StreamFailureKind
from temporalio.contrib.streams import (
    BEGINNING,
    END,
    Cursor,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRecordError,
    StreamRefusedError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._errors import error_from_failure


def test_every_condition_is_a_stream_error():
    for error in (
        StreamClosedError,
        StreamCursorError,
        StreamExpiredError,
        StreamNotFoundError,
        StreamOutcomeUnknownError,
        StreamProducerError,
        StreamRecordError,
        StreamUnsupportedError,
    ):
        assert issubclass(error, StreamError)
    # Expired is a kind of invalid cursor, so a caller that only resets on a
    # bad cursor still catches it, and one that cares can tell them apart.
    assert issubclass(StreamExpiredError, StreamCursorError)
    # Refused and unknown are different answers to a retrying caller.
    assert not issubclass(StreamOutcomeUnknownError, StreamProducerError)
    assert not issubclass(StreamProducerError, StreamOutcomeUnknownError)


def test_refused_and_storage_failures_are_families_of_their_own():
    # A refused write did not happen; a storage failure may have.
    assert issubclass(StreamProducerError, StreamRefusedError)
    assert issubclass(StreamClosedError, StreamRefusedError)
    assert issubclass(StreamOutcomeUnknownError, StreamStorageError)
    assert not issubclass(StreamRefusedError, StreamStorageError)
    assert not issubclass(StreamStorageError, StreamRefusedError)


def test_each_core_failure_kind_raises_its_error():
    expected = {
        StreamFailureKind.STREAM_FAILURE_KIND_PRODUCER_DIVERGENT: StreamProducerError,
        StreamFailureKind.STREAM_FAILURE_KIND_PRODUCER_STALE: StreamProducerError,
        StreamFailureKind.STREAM_FAILURE_KIND_OUTCOME_UNKNOWN: StreamOutcomeUnknownError,
        StreamFailureKind.STREAM_FAILURE_KIND_REFUSED: StreamRefusedError,
        StreamFailureKind.STREAM_FAILURE_KIND_CLOSED: StreamClosedError,
        StreamFailureKind.STREAM_FAILURE_KIND_CURSOR: StreamCursorError,
        StreamFailureKind.STREAM_FAILURE_KIND_EXPIRED: StreamExpiredError,
        StreamFailureKind.STREAM_FAILURE_KIND_NOT_FOUND: StreamNotFoundError,
        StreamFailureKind.STREAM_FAILURE_KIND_RECORD: StreamRecordError,
        StreamFailureKind.STREAM_FAILURE_KIND_STORAGE: StreamStorageError,
        StreamFailureKind.STREAM_FAILURE_KIND_UNSUPPORTED: StreamUnsupportedError,
    }
    # Every kind Core defines has its error, so a new kind fails here first.
    assert set(expected) == set(StreamFailureKind.values()) - {
        StreamFailureKind.STREAM_FAILURE_KIND_UNSPECIFIED
    }
    for kind, error_type in expected.items():
        error = error_from_failure(StreamFailure(kind=kind, message="why"))
        assert type(error) is error_type
        assert str(error) == "why"


def test_a_record_failure_carries_the_cursor_to_resume_past():
    error = error_from_failure(
        StreamFailure(
            kind=StreamFailureKind.STREAM_FAILURE_KIND_RECORD,
            message="bad record",
            cursor="memory:abc:7",
        )
    )
    assert isinstance(error, StreamRecordError)
    assert error.cursor == Cursor("memory:abc:7")


def test_a_kind_this_release_does_not_know_is_a_storage_error():
    for kind in (
        StreamFailureKind.STREAM_FAILURE_KIND_UNSPECIFIED,
        StreamFailureKind.ValueType(99),
    ):
        error = error_from_failure(StreamFailure(kind=kind, message="new"))
        assert type(error) is StreamStorageError


def test_the_start_cursors_are_the_tokens_core_reads():
    # An empty `after` starts at the oldest record, and `$end` at what comes next.
    assert BEGINNING.token == ""
    assert END.token == "$end"
    assert str(Cursor("memory:abc:7")) == "memory:abc:7"
