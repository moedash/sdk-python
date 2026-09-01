import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from temporalio.api.common.v1 import message_pb2 as _message_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StreamState(_message.Message):
    __slots__ = ("head_offset", "base_offset", "last_txn_id", "closed", "close_reason", "owner_epoch", "bucket_size", "collection_id", "producers", "consumers", "lifecycle", "redirect_run_id", "close_time")
    class ProducersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ProducerCursor
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ProducerCursor, _Mapping]] = ...) -> None: ...
    class ConsumersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ConsumerCursor
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ConsumerCursor, _Mapping]] = ...) -> None: ...
    HEAD_OFFSET_FIELD_NUMBER: _ClassVar[int]
    BASE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LAST_TXN_ID_FIELD_NUMBER: _ClassVar[int]
    CLOSED_FIELD_NUMBER: _ClassVar[int]
    CLOSE_REASON_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    BUCKET_SIZE_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCERS_FIELD_NUMBER: _ClassVar[int]
    CONSUMERS_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_FIELD_NUMBER: _ClassVar[int]
    REDIRECT_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CLOSE_TIME_FIELD_NUMBER: _ClassVar[int]
    head_offset: int
    base_offset: int
    last_txn_id: int
    closed: bool
    close_reason: _message_pb2.Payload
    owner_epoch: int
    bucket_size: int
    collection_id: str
    producers: _containers.MessageMap[str, ProducerCursor]
    consumers: _containers.MessageMap[str, ConsumerCursor]
    lifecycle: StreamLifecycle
    redirect_run_id: str
    close_time: _timestamp_pb2.Timestamp
    def __init__(self, head_offset: _Optional[int] = ..., base_offset: _Optional[int] = ..., last_txn_id: _Optional[int] = ..., closed: bool = ..., close_reason: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ..., owner_epoch: _Optional[int] = ..., bucket_size: _Optional[int] = ..., collection_id: _Optional[str] = ..., producers: _Optional[_Mapping[str, ProducerCursor]] = ..., consumers: _Optional[_Mapping[str, ConsumerCursor]] = ..., lifecycle: _Optional[_Union[StreamLifecycle, _Mapping]] = ..., redirect_run_id: _Optional[str] = ..., close_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ProducerCursor(_message.Message):
    __slots__ = ("seq", "first_offset", "count", "content_hash", "fenced")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    FIRST_OFFSET_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    FENCED_FIELD_NUMBER: _ClassVar[int]
    seq: int
    first_offset: int
    count: int
    content_hash: bytes
    fenced: bool
    def __init__(self, seq: _Optional[int] = ..., first_offset: _Optional[int] = ..., count: _Optional[int] = ..., content_hash: _Optional[bytes] = ..., fenced: bool = ...) -> None: ...

class ConsumerCursor(_message.Message):
    __slots__ = ("workflow_id", "run_id", "offset", "active", "external")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    run_id: str
    offset: int
    active: bool
    external: bool
    def __init__(self, workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., offset: _Optional[int] = ..., active: bool = ..., external: bool = ...) -> None: ...

class WorkflowStreamCursor(_message.Message):
    __slots__ = ("stream_id", "collection_id", "bucket_size", "offset", "known_head", "external", "pending_from", "pending_to", "has_pending")
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_SIZE_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    KNOWN_HEAD_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_FIELD_NUMBER: _ClassVar[int]
    PENDING_FROM_FIELD_NUMBER: _ClassVar[int]
    PENDING_TO_FIELD_NUMBER: _ClassVar[int]
    HAS_PENDING_FIELD_NUMBER: _ClassVar[int]
    stream_id: str
    collection_id: str
    bucket_size: int
    offset: int
    known_head: int
    external: bool
    pending_from: int
    pending_to: int
    has_pending: bool
    def __init__(self, stream_id: _Optional[str] = ..., collection_id: _Optional[str] = ..., bucket_size: _Optional[int] = ..., offset: _Optional[int] = ..., known_head: _Optional[int] = ..., external: bool = ..., pending_from: _Optional[int] = ..., pending_to: _Optional[int] = ..., has_pending: bool = ...) -> None: ...

class StreamLifecycle(_message.Message):
    __slots__ = ("retention", "max_items")
    RETENTION_FIELD_NUMBER: _ClassVar[int]
    MAX_ITEMS_FIELD_NUMBER: _ClassVar[int]
    retention: _duration_pb2.Duration
    max_items: int
    def __init__(self, retention: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., max_items: _Optional[int] = ...) -> None: ...
