from temporalio.api.streamservice.v1 import message_pb2 as _message_pb2
from temporalio.api.streamservice.v1 import stream_state_pb2 as _stream_state_pb2
from temporalio.api.common.v1 import message_pb2 as _message_pb2_1
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateStreamInput(_message.Message):
    __slots__ = ("namespace", "stream_id", "lifecycle")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    lifecycle: _stream_state_pb2.StreamLifecycle
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ..., lifecycle: _Optional[_Union[_stream_state_pb2.StreamLifecycle, _Mapping]] = ...) -> None: ...

class CreateStreamOutput(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class AddMessagesInput(_message.Message):
    __slots__ = ("namespace", "stream_id", "run_id", "messages", "producer_id", "sequence", "expected_offset", "use_expected_offset", "owner_epoch")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_OFFSET_FIELD_NUMBER: _ClassVar[int]
    USE_EXPECTED_OFFSET_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    run_id: str
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.StreamMessage]
    producer_id: str
    sequence: int
    expected_offset: int
    use_expected_offset: bool
    owner_epoch: int
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ..., run_id: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[_message_pb2.StreamMessage, _Mapping]]] = ..., producer_id: _Optional[str] = ..., sequence: _Optional[int] = ..., expected_offset: _Optional[int] = ..., use_expected_offset: _Optional[bool] = ..., owner_epoch: _Optional[int] = ...) -> None: ...

class AddMessagesOutput(_message.Message):
    __slots__ = ("first_offset", "next_offset", "count", "deduplicated")
    FIRST_OFFSET_FIELD_NUMBER: _ClassVar[int]
    NEXT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    DEDUPLICATED_FIELD_NUMBER: _ClassVar[int]
    first_offset: int
    next_offset: int
    count: int
    deduplicated: bool
    def __init__(self, first_offset: _Optional[int] = ..., next_offset: _Optional[int] = ..., count: _Optional[int] = ..., deduplicated: _Optional[bool] = ...) -> None: ...

class FinishWritingInput(_message.Message):
    __slots__ = ("namespace", "stream_id", "producer_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    producer_id: str
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ..., producer_id: _Optional[str] = ...) -> None: ...

class FinishWritingOutput(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SubscribeWorkflowInput(_message.Message):
    __slots__ = ("namespace", "workflow_id", "stream_name", "stream_id", "start_offset")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    START_OFFSET_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    stream_name: str
    stream_id: str
    start_offset: int
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., stream_name: _Optional[str] = ..., stream_id: _Optional[str] = ..., start_offset: _Optional[int] = ...) -> None: ...

class SubscribeWorkflowOutput(_message.Message):
    __slots__ = ("start_offset",)
    START_OFFSET_FIELD_NUMBER: _ClassVar[int]
    start_offset: int
    def __init__(self, start_offset: _Optional[int] = ...) -> None: ...

class PollMessagesInput(_message.Message):
    __slots__ = ("namespace", "stream_id", "run_id", "from_offset", "max_messages", "topics", "wait_new_messages")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_OFFSET_FIELD_NUMBER: _ClassVar[int]
    MAX_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    WAIT_NEW_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    run_id: str
    from_offset: int
    max_messages: int
    topics: _containers.RepeatedScalarFieldContainer[str]
    wait_new_messages: bool
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ..., run_id: _Optional[str] = ..., from_offset: _Optional[int] = ..., max_messages: _Optional[int] = ..., topics: _Optional[_Iterable[str]] = ..., wait_new_messages: _Optional[bool] = ...) -> None: ...

class PollMessagesOutput(_message.Message):
    __slots__ = ("messages", "next_offset", "head_offset", "closed", "close_reason")
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    NEXT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    HEAD_OFFSET_FIELD_NUMBER: _ClassVar[int]
    CLOSED_FIELD_NUMBER: _ClassVar[int]
    CLOSE_REASON_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.StreamMessage]
    next_offset: int
    head_offset: int
    closed: bool
    close_reason: _message_pb2_1.Payload
    def __init__(self, messages: _Optional[_Iterable[_Union[_message_pb2.StreamMessage, _Mapping]]] = ..., next_offset: _Optional[int] = ..., head_offset: _Optional[int] = ..., closed: _Optional[bool] = ..., close_reason: _Optional[_Union[_message_pb2_1.Payload, _Mapping]] = ...) -> None: ...

class DescribeStreamInput(_message.Message):
    __slots__ = ("namespace", "stream_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ...) -> None: ...

class PollWorkflowMessagesInput(_message.Message):
    __slots__ = ("namespace", "workflow_id", "stream_name", "from_offset", "max_messages", "topics", "wait_new_messages")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    FROM_OFFSET_FIELD_NUMBER: _ClassVar[int]
    MAX_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    WAIT_NEW_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    stream_name: str
    from_offset: int
    max_messages: int
    topics: _containers.RepeatedScalarFieldContainer[str]
    wait_new_messages: bool
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., stream_name: _Optional[str] = ..., from_offset: _Optional[int] = ..., max_messages: _Optional[int] = ..., topics: _Optional[_Iterable[str]] = ..., wait_new_messages: _Optional[bool] = ...) -> None: ...

class DescribeWorkflowStreamInput(_message.Message):
    __slots__ = ("namespace", "workflow_id", "stream_name")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    stream_name: str
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., stream_name: _Optional[str] = ...) -> None: ...

class AddWorkflowMessagesInput(_message.Message):
    __slots__ = ("namespace", "workflow_id", "stream_name", "messages", "producer_id", "sequence")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    stream_name: str
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.StreamMessage]
    producer_id: str
    sequence: int
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., stream_name: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[_message_pb2.StreamMessage, _Mapping]]] = ..., producer_id: _Optional[str] = ..., sequence: _Optional[int] = ...) -> None: ...

class DescribeStreamOutput(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: _stream_state_pb2.StreamState
    def __init__(self, state: _Optional[_Union[_stream_state_pb2.StreamState, _Mapping]] = ...) -> None: ...

class CloseStreamInput(_message.Message):
    __slots__ = ("namespace", "stream_id", "reason")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    reason: _message_pb2_1.Payload
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ..., reason: _Optional[_Union[_message_pb2_1.Payload, _Mapping]] = ...) -> None: ...

class CloseStreamOutput(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TruncateStreamInput(_message.Message):
    __slots__ = ("namespace", "stream_id", "new_base_offset")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_BASE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    new_base_offset: int
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ..., new_base_offset: _Optional[int] = ...) -> None: ...

class TruncateStreamOutput(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteStreamInput(_message.Message):
    __slots__ = ("namespace", "stream_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    stream_id: str
    def __init__(self, namespace: _Optional[str] = ..., stream_id: _Optional[str] = ...) -> None: ...

class DeleteStreamOutput(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CreateStreamRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: CreateStreamInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[CreateStreamInput, _Mapping]] = ...) -> None: ...

class CreateStreamResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: CreateStreamOutput
    def __init__(self, frontend_response: _Optional[_Union[CreateStreamOutput, _Mapping]] = ...) -> None: ...

class AddMessagesRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: AddMessagesInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[AddMessagesInput, _Mapping]] = ...) -> None: ...

class AddMessagesResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: AddMessagesOutput
    def __init__(self, frontend_response: _Optional[_Union[AddMessagesOutput, _Mapping]] = ...) -> None: ...

class FinishWritingRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: FinishWritingInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[FinishWritingInput, _Mapping]] = ...) -> None: ...

class FinishWritingResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: FinishWritingOutput
    def __init__(self, frontend_response: _Optional[_Union[FinishWritingOutput, _Mapping]] = ...) -> None: ...

class SubscribeWorkflowRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: SubscribeWorkflowInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[SubscribeWorkflowInput, _Mapping]] = ...) -> None: ...

class SubscribeWorkflowResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: SubscribeWorkflowOutput
    def __init__(self, frontend_response: _Optional[_Union[SubscribeWorkflowOutput, _Mapping]] = ...) -> None: ...

class PollMessagesRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: PollMessagesInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[PollMessagesInput, _Mapping]] = ...) -> None: ...

class PollMessagesResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: PollMessagesOutput
    def __init__(self, frontend_response: _Optional[_Union[PollMessagesOutput, _Mapping]] = ...) -> None: ...

class DescribeStreamRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: DescribeStreamInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[DescribeStreamInput, _Mapping]] = ...) -> None: ...

class DescribeStreamResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: DescribeStreamOutput
    def __init__(self, frontend_response: _Optional[_Union[DescribeStreamOutput, _Mapping]] = ...) -> None: ...

class PollWorkflowMessagesRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: PollWorkflowMessagesInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[PollWorkflowMessagesInput, _Mapping]] = ...) -> None: ...

class PollWorkflowMessagesResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: PollMessagesOutput
    def __init__(self, frontend_response: _Optional[_Union[PollMessagesOutput, _Mapping]] = ...) -> None: ...

class DescribeWorkflowStreamRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: DescribeWorkflowStreamInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[DescribeWorkflowStreamInput, _Mapping]] = ...) -> None: ...

class DescribeWorkflowStreamResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: DescribeStreamOutput
    def __init__(self, frontend_response: _Optional[_Union[DescribeStreamOutput, _Mapping]] = ...) -> None: ...

class AddWorkflowMessagesRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: AddWorkflowMessagesInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[AddWorkflowMessagesInput, _Mapping]] = ...) -> None: ...

class AddWorkflowMessagesResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: AddMessagesOutput
    def __init__(self, frontend_response: _Optional[_Union[AddMessagesOutput, _Mapping]] = ...) -> None: ...

class CloseStreamRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: CloseStreamInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[CloseStreamInput, _Mapping]] = ...) -> None: ...

class CloseStreamResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: CloseStreamOutput
    def __init__(self, frontend_response: _Optional[_Union[CloseStreamOutput, _Mapping]] = ...) -> None: ...

class TruncateStreamRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: TruncateStreamInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[TruncateStreamInput, _Mapping]] = ...) -> None: ...

class TruncateStreamResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: TruncateStreamOutput
    def __init__(self, frontend_response: _Optional[_Union[TruncateStreamOutput, _Mapping]] = ...) -> None: ...

class ListStreamsInput(_message.Message):
    __slots__ = ("namespace", "page_size", "next_page_token", "query")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    page_size: int
    next_page_token: bytes
    query: str
    def __init__(self, namespace: _Optional[str] = ..., page_size: _Optional[int] = ..., next_page_token: _Optional[bytes] = ..., query: _Optional[str] = ...) -> None: ...

class StreamListEntry(_message.Message):
    __slots__ = ("stream_id", "run_id")
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    stream_id: str
    run_id: str
    def __init__(self, stream_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class ListStreamsOutput(_message.Message):
    __slots__ = ("streams", "next_page_token")
    STREAMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    streams: _containers.RepeatedCompositeFieldContainer[StreamListEntry]
    next_page_token: bytes
    def __init__(self, streams: _Optional[_Iterable[_Union[StreamListEntry, _Mapping]]] = ..., next_page_token: _Optional[bytes] = ...) -> None: ...

class ListStreamsRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: ListStreamsInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[ListStreamsInput, _Mapping]] = ...) -> None: ...

class ListStreamsResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: ListStreamsOutput
    def __init__(self, frontend_response: _Optional[_Union[ListStreamsOutput, _Mapping]] = ...) -> None: ...

class DeleteStreamRequest(_message.Message):
    __slots__ = ("namespace_id", "frontend_request")
    NAMESPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FRONTEND_REQUEST_FIELD_NUMBER: _ClassVar[int]
    namespace_id: str
    frontend_request: DeleteStreamInput
    def __init__(self, namespace_id: _Optional[str] = ..., frontend_request: _Optional[_Union[DeleteStreamInput, _Mapping]] = ...) -> None: ...

class DeleteStreamResponse(_message.Message):
    __slots__ = ("frontend_response",)
    FRONTEND_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    frontend_response: DeleteStreamOutput
    def __init__(self, frontend_response: _Optional[_Union[DeleteStreamOutput, _Mapping]] = ...) -> None: ...
