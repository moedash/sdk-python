from temporalio.api.common.v1 import message_pb2 as _message_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StreamMessageKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STREAM_MESSAGE_KIND_UNSPECIFIED: _ClassVar[StreamMessageKind]
    STREAM_MESSAGE_KIND_DATA: _ClassVar[StreamMessageKind]
    STREAM_MESSAGE_KIND_FLUSH: _ClassVar[StreamMessageKind]
STREAM_MESSAGE_KIND_UNSPECIFIED: StreamMessageKind
STREAM_MESSAGE_KIND_DATA: StreamMessageKind
STREAM_MESSAGE_KIND_FLUSH: StreamMessageKind

class StreamMessage(_message.Message):
    __slots__ = ("body", "metadata", "topic", "topic_sequence", "kind", "offset")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    BODY_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    TOPIC_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    body: _message_pb2.Payload
    metadata: _containers.MessageMap[str, _message_pb2.Payload]
    topic: str
    topic_sequence: int
    kind: StreamMessageKind
    offset: int
    def __init__(self, body: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ..., metadata: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., topic: _Optional[str] = ..., topic_sequence: _Optional[int] = ..., kind: _Optional[_Union[StreamMessageKind, str]] = ..., offset: _Optional[int] = ...) -> None: ...

class StreamMessageBatch(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[StreamMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[StreamMessage, _Mapping]]] = ...) -> None: ...
