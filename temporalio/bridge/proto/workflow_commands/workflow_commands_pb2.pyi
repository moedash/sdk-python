import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from temporalio.api.common.v1 import message_pb2 as _message_pb2
from temporalio.api.enums.v1 import workflow_pb2 as _workflow_pb2
from temporalio.api.failure.v1 import message_pb2 as _message_pb2_1
from temporalio.api.sdk.v1 import user_metadata_pb2 as _user_metadata_pb2
from temporalio.api.stream.v1 import message_pb2 as _message_pb2_1_1
from temporalio.api.sdk.v1 import event_group_marker_pb2 as _event_group_marker_pb2
from temporalio.bridge.proto.child_workflow import child_workflow_pb2 as _child_workflow_pb2
from temporalio.bridge.proto.nexus import nexus_pb2 as _nexus_pb2
from temporalio.bridge.proto.common import common_pb2 as _common_pb2
from temporalio.bridge.proto.external_data import external_data_pb2 as _external_data_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ActivityCancellationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRY_CANCEL: _ClassVar[ActivityCancellationType]
    WAIT_CANCELLATION_COMPLETED: _ClassVar[ActivityCancellationType]
    ABANDON: _ClassVar[ActivityCancellationType]
TRY_CANCEL: ActivityCancellationType
WAIT_CANCELLATION_COMPLETED: ActivityCancellationType
ABANDON: ActivityCancellationType

class WorkflowCommand(_message.Message):
    __slots__ = ("user_metadata", "event_group_markers", "start_timer", "schedule_activity", "respond_to_query", "request_cancel_activity", "cancel_timer", "complete_workflow_execution", "fail_workflow_execution", "continue_as_new_workflow_execution", "cancel_workflow_execution", "set_patch_marker", "start_child_workflow_execution", "cancel_child_workflow_execution", "request_cancel_external_workflow_execution", "signal_external_workflow_execution", "cancel_signal_workflow", "schedule_local_activity", "request_cancel_local_activity", "upsert_workflow_search_attributes", "modify_workflow_properties", "update_response", "schedule_nexus_operation", "request_cancel_nexus_operation", "workflow_stream_progress", "workflow_stream_quiescent", "external_stream_park_result", "external_stream_finalized", "workflow_output_stream_commit", "workflow_output_stream_buffered", "subscribe_stream", "add_stream_messages")
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    EVENT_GROUP_MARKERS_FIELD_NUMBER: _ClassVar[int]
    START_TIMER_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    RESPOND_TO_QUERY_FIELD_NUMBER: _ClassVar[int]
    REQUEST_CANCEL_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    CANCEL_TIMER_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    FAIL_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    CANCEL_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    SET_PATCH_MARKER_FIELD_NUMBER: _ClassVar[int]
    START_CHILD_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    CANCEL_CHILD_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    CANCEL_SIGNAL_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_LOCAL_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    REQUEST_CANCEL_LOCAL_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    MODIFY_WORKFLOW_PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    UPDATE_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_NEXUS_OPERATION_FIELD_NUMBER: _ClassVar[int]
    REQUEST_CANCEL_NEXUS_OPERATION_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_STREAM_PROGRESS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_STREAM_QUIESCENT_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_STREAM_PARK_RESULT_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_STREAM_FINALIZED_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_OUTPUT_STREAM_COMMIT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_OUTPUT_STREAM_BUFFERED_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIBE_STREAM_FIELD_NUMBER: _ClassVar[int]
    ADD_STREAM_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    user_metadata: _user_metadata_pb2.UserMetadata
    event_group_markers: _containers.RepeatedCompositeFieldContainer[_event_group_marker_pb2.EventGroupMarker]
    start_timer: StartTimer
    schedule_activity: ScheduleActivity
    respond_to_query: QueryResult
    request_cancel_activity: RequestCancelActivity
    cancel_timer: CancelTimer
    complete_workflow_execution: CompleteWorkflowExecution
    fail_workflow_execution: FailWorkflowExecution
    continue_as_new_workflow_execution: ContinueAsNewWorkflowExecution
    cancel_workflow_execution: CancelWorkflowExecution
    set_patch_marker: SetPatchMarker
    start_child_workflow_execution: StartChildWorkflowExecution
    cancel_child_workflow_execution: CancelChildWorkflowExecution
    request_cancel_external_workflow_execution: RequestCancelExternalWorkflowExecution
    signal_external_workflow_execution: SignalExternalWorkflowExecution
    cancel_signal_workflow: CancelSignalWorkflow
    schedule_local_activity: ScheduleLocalActivity
    request_cancel_local_activity: RequestCancelLocalActivity
    upsert_workflow_search_attributes: UpsertWorkflowSearchAttributes
    modify_workflow_properties: ModifyWorkflowProperties
    update_response: UpdateResponse
    schedule_nexus_operation: ScheduleNexusOperation
    request_cancel_nexus_operation: RequestCancelNexusOperation
    workflow_stream_progress: WorkflowStreamProgress
    workflow_stream_quiescent: WorkflowStreamQuiescent
    external_stream_park_result: ExternalStreamParkResult
    external_stream_finalized: ExternalStreamFinalized
    workflow_output_stream_commit: WorkflowOutputStreamCommit
    workflow_output_stream_buffered: WorkflowOutputStreamBuffered
    subscribe_stream: SubscribeStream
    add_stream_messages: AddStreamMessages
    def __init__(self, user_metadata: _Optional[_Union[_user_metadata_pb2.UserMetadata, _Mapping]] = ..., event_group_markers: _Optional[_Iterable[_Union[_event_group_marker_pb2.EventGroupMarker, _Mapping]]] = ..., start_timer: _Optional[_Union[StartTimer, _Mapping]] = ..., schedule_activity: _Optional[_Union[ScheduleActivity, _Mapping]] = ..., respond_to_query: _Optional[_Union[QueryResult, _Mapping]] = ..., request_cancel_activity: _Optional[_Union[RequestCancelActivity, _Mapping]] = ..., cancel_timer: _Optional[_Union[CancelTimer, _Mapping]] = ..., complete_workflow_execution: _Optional[_Union[CompleteWorkflowExecution, _Mapping]] = ..., fail_workflow_execution: _Optional[_Union[FailWorkflowExecution, _Mapping]] = ..., continue_as_new_workflow_execution: _Optional[_Union[ContinueAsNewWorkflowExecution, _Mapping]] = ..., cancel_workflow_execution: _Optional[_Union[CancelWorkflowExecution, _Mapping]] = ..., set_patch_marker: _Optional[_Union[SetPatchMarker, _Mapping]] = ..., start_child_workflow_execution: _Optional[_Union[StartChildWorkflowExecution, _Mapping]] = ..., cancel_child_workflow_execution: _Optional[_Union[CancelChildWorkflowExecution, _Mapping]] = ..., request_cancel_external_workflow_execution: _Optional[_Union[RequestCancelExternalWorkflowExecution, _Mapping]] = ..., signal_external_workflow_execution: _Optional[_Union[SignalExternalWorkflowExecution, _Mapping]] = ..., cancel_signal_workflow: _Optional[_Union[CancelSignalWorkflow, _Mapping]] = ..., schedule_local_activity: _Optional[_Union[ScheduleLocalActivity, _Mapping]] = ..., request_cancel_local_activity: _Optional[_Union[RequestCancelLocalActivity, _Mapping]] = ..., upsert_workflow_search_attributes: _Optional[_Union[UpsertWorkflowSearchAttributes, _Mapping]] = ..., modify_workflow_properties: _Optional[_Union[ModifyWorkflowProperties, _Mapping]] = ..., update_response: _Optional[_Union[UpdateResponse, _Mapping]] = ..., schedule_nexus_operation: _Optional[_Union[ScheduleNexusOperation, _Mapping]] = ..., request_cancel_nexus_operation: _Optional[_Union[RequestCancelNexusOperation, _Mapping]] = ..., workflow_stream_progress: _Optional[_Union[WorkflowStreamProgress, _Mapping]] = ..., workflow_stream_quiescent: _Optional[_Union[WorkflowStreamQuiescent, _Mapping]] = ..., external_stream_park_result: _Optional[_Union[ExternalStreamParkResult, _Mapping]] = ..., external_stream_finalized: _Optional[_Union[ExternalStreamFinalized, _Mapping]] = ..., workflow_output_stream_commit: _Optional[_Union[WorkflowOutputStreamCommit, _Mapping]] = ..., workflow_output_stream_buffered: _Optional[_Union[WorkflowOutputStreamBuffered, _Mapping]] = ..., subscribe_stream: _Optional[_Union[SubscribeStream, _Mapping]] = ..., add_stream_messages: _Optional[_Union[AddStreamMessages, _Mapping]] = ...) -> None: ...

class WorkflowStreamProgress(_message.Message):
    __slots__ = ("observation_delta", "request_rollover")
    OBSERVATION_DELTA_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ROLLOVER_FIELD_NUMBER: _ClassVar[int]
    observation_delta: bytes
    request_rollover: bool
    def __init__(self, observation_delta: _Optional[bytes] = ..., request_rollover: _Optional[bool] = ...) -> None: ...

class WorkflowStreamQuiescent(_message.Message):
    __slots__ = ("quiescence_generation", "waits", "idle_timeout")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    WAITS_FIELD_NUMBER: _ClassVar[int]
    IDLE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    waits: _containers.RepeatedCompositeFieldContainer[ExternalStreamWait]
    idle_timeout: _duration_pb2.Duration
    def __init__(self, quiescence_generation: _Optional[int] = ..., waits: _Optional[_Iterable[_Union[ExternalStreamWait, _Mapping]]] = ..., idle_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class ExternalStreamWait(_message.Message):
    __slots__ = ("wait_id", "generation", "immediately_parkable")
    WAIT_ID_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    IMMEDIATELY_PARKABLE_FIELD_NUMBER: _ClassVar[int]
    wait_id: int
    generation: int
    immediately_parkable: bool
    def __init__(self, wait_id: _Optional[int] = ..., generation: _Optional[int] = ..., immediately_parkable: _Optional[bool] = ...) -> None: ...

class ExternalStreamParkResult(_message.Message):
    __slots__ = ("quiescence_generation", "confirmed", "became_ready", "final_observation_delta")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_FIELD_NUMBER: _ClassVar[int]
    BECAME_READY_FIELD_NUMBER: _ClassVar[int]
    FINAL_OBSERVATION_DELTA_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    confirmed: ParkSetConfirmed
    became_ready: StreamSetBecameReady
    final_observation_delta: bytes
    def __init__(self, quiescence_generation: _Optional[int] = ..., confirmed: _Optional[_Union[ParkSetConfirmed, _Mapping]] = ..., became_ready: _Optional[_Union[StreamSetBecameReady, _Mapping]] = ..., final_observation_delta: _Optional[bytes] = ...) -> None: ...

class ParkSetConfirmed(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StreamSetBecameReady(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ExternalStreamFinalized(_message.Message):
    __slots__ = ("quiescence_generation", "final_observation_delta")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    FINAL_OBSERVATION_DELTA_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    final_observation_delta: bytes
    def __init__(self, quiescence_generation: _Optional[int] = ..., final_observation_delta: _Optional[bytes] = ...) -> None: ...

class WorkflowOutputStreamCommit(_message.Message):
    __slots__ = ("manifest", "request_rollover")
    MANIFEST_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ROLLOVER_FIELD_NUMBER: _ClassVar[int]
    manifest: _external_data_pb2.ExternalOutputStreamManifest
    request_rollover: bool
    def __init__(self, manifest: _Optional[_Union[_external_data_pb2.ExternalOutputStreamManifest, _Mapping]] = ..., request_rollover: _Optional[bool] = ...) -> None: ...

class WorkflowOutputStreamBuffered(_message.Message):
    __slots__ = ("max_publish_latency",)
    MAX_PUBLISH_LATENCY_FIELD_NUMBER: _ClassVar[int]
    max_publish_latency: _duration_pb2.Duration
    def __init__(self, max_publish_latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class AddStreamMessages(_message.Message):
    __slots__ = ("stream_id", "messages")
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    stream_id: str
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2_1_1.StreamMessage]
    def __init__(self, stream_id: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[_message_pb2_1_1.StreamMessage, _Mapping]]] = ...) -> None: ...

class SubscribeStream(_message.Message):
    __slots__ = ("stream_id", "start_offset")
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    START_OFFSET_FIELD_NUMBER: _ClassVar[int]
    stream_id: str
    start_offset: int
    def __init__(self, stream_id: _Optional[str] = ..., start_offset: _Optional[int] = ...) -> None: ...

class StartTimer(_message.Message):
    __slots__ = ("seq", "start_to_fire_timeout")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    START_TO_FIRE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    seq: int
    start_to_fire_timeout: _duration_pb2.Duration
    def __init__(self, seq: _Optional[int] = ..., start_to_fire_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class CancelTimer(_message.Message):
    __slots__ = ("seq",)
    SEQ_FIELD_NUMBER: _ClassVar[int]
    seq: int
    def __init__(self, seq: _Optional[int] = ...) -> None: ...

class ScheduleActivity(_message.Message):
    __slots__ = ("seq", "activity_id", "activity_type", "task_queue", "headers", "arguments", "schedule_to_close_timeout", "schedule_to_start_timeout", "start_to_close_timeout", "heartbeat_timeout", "retry_policy", "cancellation_type", "do_not_eagerly_execute", "versioning_intent", "priority")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    SEQ_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TO_CLOSE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TO_START_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    START_TO_CLOSE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    DO_NOT_EAGERLY_EXECUTE_FIELD_NUMBER: _ClassVar[int]
    VERSIONING_INTENT_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    seq: int
    activity_id: str
    activity_type: str
    task_queue: str
    headers: _containers.MessageMap[str, _message_pb2.Payload]
    arguments: _containers.RepeatedCompositeFieldContainer[_message_pb2.Payload]
    schedule_to_close_timeout: _duration_pb2.Duration
    schedule_to_start_timeout: _duration_pb2.Duration
    start_to_close_timeout: _duration_pb2.Duration
    heartbeat_timeout: _duration_pb2.Duration
    retry_policy: _message_pb2.RetryPolicy
    cancellation_type: ActivityCancellationType
    do_not_eagerly_execute: bool
    versioning_intent: _common_pb2.VersioningIntent
    priority: _message_pb2.Priority
    def __init__(self, seq: _Optional[int] = ..., activity_id: _Optional[str] = ..., activity_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., headers: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., arguments: _Optional[_Iterable[_Union[_message_pb2.Payload, _Mapping]]] = ..., schedule_to_close_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., schedule_to_start_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., start_to_close_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., heartbeat_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., retry_policy: _Optional[_Union[_message_pb2.RetryPolicy, _Mapping]] = ..., cancellation_type: _Optional[_Union[ActivityCancellationType, str]] = ..., do_not_eagerly_execute: _Optional[bool] = ..., versioning_intent: _Optional[_Union[_common_pb2.VersioningIntent, str]] = ..., priority: _Optional[_Union[_message_pb2.Priority, _Mapping]] = ...) -> None: ...

class ScheduleLocalActivity(_message.Message):
    __slots__ = ("seq", "activity_id", "activity_type", "attempt", "original_schedule_time", "headers", "arguments", "schedule_to_close_timeout", "schedule_to_start_timeout", "start_to_close_timeout", "retry_policy", "local_retry_threshold", "cancellation_type")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    SEQ_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_SCHEDULE_TIME_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TO_CLOSE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TO_START_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    START_TO_CLOSE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    LOCAL_RETRY_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    seq: int
    activity_id: str
    activity_type: str
    attempt: int
    original_schedule_time: _timestamp_pb2.Timestamp
    headers: _containers.MessageMap[str, _message_pb2.Payload]
    arguments: _containers.RepeatedCompositeFieldContainer[_message_pb2.Payload]
    schedule_to_close_timeout: _duration_pb2.Duration
    schedule_to_start_timeout: _duration_pb2.Duration
    start_to_close_timeout: _duration_pb2.Duration
    retry_policy: _message_pb2.RetryPolicy
    local_retry_threshold: _duration_pb2.Duration
    cancellation_type: ActivityCancellationType
    def __init__(self, seq: _Optional[int] = ..., activity_id: _Optional[str] = ..., activity_type: _Optional[str] = ..., attempt: _Optional[int] = ..., original_schedule_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., headers: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., arguments: _Optional[_Iterable[_Union[_message_pb2.Payload, _Mapping]]] = ..., schedule_to_close_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., schedule_to_start_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., start_to_close_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., retry_policy: _Optional[_Union[_message_pb2.RetryPolicy, _Mapping]] = ..., local_retry_threshold: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., cancellation_type: _Optional[_Union[ActivityCancellationType, str]] = ...) -> None: ...

class RequestCancelActivity(_message.Message):
    __slots__ = ("seq",)
    SEQ_FIELD_NUMBER: _ClassVar[int]
    seq: int
    def __init__(self, seq: _Optional[int] = ...) -> None: ...

class RequestCancelLocalActivity(_message.Message):
    __slots__ = ("seq",)
    SEQ_FIELD_NUMBER: _ClassVar[int]
    seq: int
    def __init__(self, seq: _Optional[int] = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ("query_id", "succeeded", "failed")
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    succeeded: QuerySuccess
    failed: _message_pb2_1.Failure
    def __init__(self, query_id: _Optional[str] = ..., succeeded: _Optional[_Union[QuerySuccess, _Mapping]] = ..., failed: _Optional[_Union[_message_pb2_1.Failure, _Mapping]] = ...) -> None: ...

class QuerySuccess(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: _message_pb2.Payload
    def __init__(self, response: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...

class CompleteWorkflowExecution(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: _message_pb2.Payload
    def __init__(self, result: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...

class FailWorkflowExecution(_message.Message):
    __slots__ = ("failure",)
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    failure: _message_pb2_1.Failure
    def __init__(self, failure: _Optional[_Union[_message_pb2_1.Failure, _Mapping]] = ...) -> None: ...

class ContinueAsNewWorkflowExecution(_message.Message):
    __slots__ = ("workflow_type", "task_queue", "arguments", "workflow_run_timeout", "workflow_task_timeout", "memo", "headers", "search_attributes", "retry_policy", "versioning_intent", "initial_versioning_behavior", "backoff_start_interval")
    class MemoEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_RUN_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TASK_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    MEMO_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    SEARCH_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    VERSIONING_INTENT_FIELD_NUMBER: _ClassVar[int]
    INITIAL_VERSIONING_BEHAVIOR_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_START_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    workflow_type: str
    task_queue: str
    arguments: _containers.RepeatedCompositeFieldContainer[_message_pb2.Payload]
    workflow_run_timeout: _duration_pb2.Duration
    workflow_task_timeout: _duration_pb2.Duration
    memo: _containers.MessageMap[str, _message_pb2.Payload]
    headers: _containers.MessageMap[str, _message_pb2.Payload]
    search_attributes: _message_pb2.SearchAttributes
    retry_policy: _message_pb2.RetryPolicy
    versioning_intent: _common_pb2.VersioningIntent
    initial_versioning_behavior: _workflow_pb2.ContinueAsNewVersioningBehavior
    backoff_start_interval: _duration_pb2.Duration
    def __init__(self, workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., arguments: _Optional[_Iterable[_Union[_message_pb2.Payload, _Mapping]]] = ..., workflow_run_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., workflow_task_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., memo: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., headers: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., search_attributes: _Optional[_Union[_message_pb2.SearchAttributes, _Mapping]] = ..., retry_policy: _Optional[_Union[_message_pb2.RetryPolicy, _Mapping]] = ..., versioning_intent: _Optional[_Union[_common_pb2.VersioningIntent, str]] = ..., initial_versioning_behavior: _Optional[_Union[_workflow_pb2.ContinueAsNewVersioningBehavior, str]] = ..., backoff_start_interval: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class CancelWorkflowExecution(_message.Message):
    __slots__ = ("details",)
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    details: _message_pb2.Payloads
    def __init__(self, details: _Optional[_Union[_message_pb2.Payloads, _Mapping]] = ...) -> None: ...

class SetPatchMarker(_message.Message):
    __slots__ = ("patch_id", "deprecated")
    PATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DEPRECATED_FIELD_NUMBER: _ClassVar[int]
    patch_id: str
    deprecated: bool
    def __init__(self, patch_id: _Optional[str] = ..., deprecated: _Optional[bool] = ...) -> None: ...

class StartChildWorkflowExecution(_message.Message):
    __slots__ = ("seq", "namespace", "workflow_id", "workflow_type", "task_queue", "input", "workflow_execution_timeout", "workflow_run_timeout", "workflow_task_timeout", "parent_close_policy", "workflow_id_reuse_policy", "retry_policy", "cron_schedule", "headers", "memo", "search_attributes", "cancellation_type", "versioning_intent", "priority")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    class MemoEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    SEQ_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_EXECUTION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_RUN_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TASK_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    PARENT_CLOSE_POLICY_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_REUSE_POLICY_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    MEMO_FIELD_NUMBER: _ClassVar[int]
    SEARCH_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSIONING_INTENT_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    seq: int
    namespace: str
    workflow_id: str
    workflow_type: str
    task_queue: str
    input: _containers.RepeatedCompositeFieldContainer[_message_pb2.Payload]
    workflow_execution_timeout: _duration_pb2.Duration
    workflow_run_timeout: _duration_pb2.Duration
    workflow_task_timeout: _duration_pb2.Duration
    parent_close_policy: _child_workflow_pb2.ParentClosePolicy
    workflow_id_reuse_policy: _workflow_pb2.WorkflowIdReusePolicy
    retry_policy: _message_pb2.RetryPolicy
    cron_schedule: str
    headers: _containers.MessageMap[str, _message_pb2.Payload]
    memo: _containers.MessageMap[str, _message_pb2.Payload]
    search_attributes: _message_pb2.SearchAttributes
    cancellation_type: _child_workflow_pb2.ChildWorkflowCancellationType
    versioning_intent: _common_pb2.VersioningIntent
    priority: _message_pb2.Priority
    def __init__(self, seq: _Optional[int] = ..., namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., input: _Optional[_Iterable[_Union[_message_pb2.Payload, _Mapping]]] = ..., workflow_execution_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., workflow_run_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., workflow_task_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., parent_close_policy: _Optional[_Union[_child_workflow_pb2.ParentClosePolicy, str]] = ..., workflow_id_reuse_policy: _Optional[_Union[_workflow_pb2.WorkflowIdReusePolicy, str]] = ..., retry_policy: _Optional[_Union[_message_pb2.RetryPolicy, _Mapping]] = ..., cron_schedule: _Optional[str] = ..., headers: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., memo: _Optional[_Mapping[str, _message_pb2.Payload]] = ..., search_attributes: _Optional[_Union[_message_pb2.SearchAttributes, _Mapping]] = ..., cancellation_type: _Optional[_Union[_child_workflow_pb2.ChildWorkflowCancellationType, str]] = ..., versioning_intent: _Optional[_Union[_common_pb2.VersioningIntent, str]] = ..., priority: _Optional[_Union[_message_pb2.Priority, _Mapping]] = ...) -> None: ...

class CancelChildWorkflowExecution(_message.Message):
    __slots__ = ("child_workflow_seq", "reason")
    CHILD_WORKFLOW_SEQ_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    child_workflow_seq: int
    reason: str
    def __init__(self, child_workflow_seq: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class RequestCancelExternalWorkflowExecution(_message.Message):
    __slots__ = ("seq", "workflow_execution", "reason")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    seq: int
    workflow_execution: _common_pb2.NamespacedWorkflowExecution
    reason: str
    def __init__(self, seq: _Optional[int] = ..., workflow_execution: _Optional[_Union[_common_pb2.NamespacedWorkflowExecution, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class SignalExternalWorkflowExecution(_message.Message):
    __slots__ = ("seq", "workflow_execution", "child_workflow_id", "signal_name", "args", "headers")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...
    SEQ_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    CHILD_WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    seq: int
    workflow_execution: _common_pb2.NamespacedWorkflowExecution
    child_workflow_id: str
    signal_name: str
    args: _containers.RepeatedCompositeFieldContainer[_message_pb2.Payload]
    headers: _containers.MessageMap[str, _message_pb2.Payload]
    def __init__(self, seq: _Optional[int] = ..., workflow_execution: _Optional[_Union[_common_pb2.NamespacedWorkflowExecution, _Mapping]] = ..., child_workflow_id: _Optional[str] = ..., signal_name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[_message_pb2.Payload, _Mapping]]] = ..., headers: _Optional[_Mapping[str, _message_pb2.Payload]] = ...) -> None: ...

class CancelSignalWorkflow(_message.Message):
    __slots__ = ("seq",)
    SEQ_FIELD_NUMBER: _ClassVar[int]
    seq: int
    def __init__(self, seq: _Optional[int] = ...) -> None: ...

class UpsertWorkflowSearchAttributes(_message.Message):
    __slots__ = ("search_attributes",)
    SEARCH_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    search_attributes: _message_pb2.SearchAttributes
    def __init__(self, search_attributes: _Optional[_Union[_message_pb2.SearchAttributes, _Mapping]] = ...) -> None: ...

class ModifyWorkflowProperties(_message.Message):
    __slots__ = ("upserted_memo",)
    UPSERTED_MEMO_FIELD_NUMBER: _ClassVar[int]
    upserted_memo: _message_pb2.Memo
    def __init__(self, upserted_memo: _Optional[_Union[_message_pb2.Memo, _Mapping]] = ...) -> None: ...

class UpdateResponse(_message.Message):
    __slots__ = ("protocol_instance_id", "accepted", "rejected", "completed")
    PROTOCOL_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    REJECTED_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    protocol_instance_id: str
    accepted: _empty_pb2.Empty
    rejected: _message_pb2_1.Failure
    completed: _message_pb2.Payload
    def __init__(self, protocol_instance_id: _Optional[str] = ..., accepted: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., rejected: _Optional[_Union[_message_pb2_1.Failure, _Mapping]] = ..., completed: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ...) -> None: ...

class ScheduleNexusOperation(_message.Message):
    __slots__ = ("seq", "endpoint", "service", "operation", "input", "schedule_to_close_timeout", "nexus_header", "cancellation_type", "schedule_to_start_timeout", "start_to_close_timeout")
    class NexusHeaderEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SEQ_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TO_CLOSE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    NEXUS_HEADER_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TO_START_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    START_TO_CLOSE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    seq: int
    endpoint: str
    service: str
    operation: str
    input: _message_pb2.Payload
    schedule_to_close_timeout: _duration_pb2.Duration
    nexus_header: _containers.ScalarMap[str, str]
    cancellation_type: _nexus_pb2.NexusOperationCancellationType
    schedule_to_start_timeout: _duration_pb2.Duration
    start_to_close_timeout: _duration_pb2.Duration
    def __init__(self, seq: _Optional[int] = ..., endpoint: _Optional[str] = ..., service: _Optional[str] = ..., operation: _Optional[str] = ..., input: _Optional[_Union[_message_pb2.Payload, _Mapping]] = ..., schedule_to_close_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., nexus_header: _Optional[_Mapping[str, str]] = ..., cancellation_type: _Optional[_Union[_nexus_pb2.NexusOperationCancellationType, str]] = ..., schedule_to_start_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., start_to_close_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class RequestCancelNexusOperation(_message.Message):
    __slots__ = ("seq",)
    SEQ_FIELD_NUMBER: _ClassVar[int]
    seq: int
    def __init__(self, seq: _Optional[int] = ...) -> None: ...
