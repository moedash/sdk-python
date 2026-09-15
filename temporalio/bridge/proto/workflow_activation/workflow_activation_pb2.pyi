import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from temporalio.api.failure.v1 import message_pb2 as _message_pb2
from temporalio.api.update.v1 import message_pb2 as _message_pb2_1
from temporalio.api.common.v1 import message_pb2 as _message_pb2_1_1
from temporalio.api.stream.v1 import message_pb2 as _message_pb2_1_1_1
from temporalio.api.enums.v1 import workflow_pb2 as _workflow_pb2
from temporalio.bridge.proto.activity_result import activity_result_pb2 as _activity_result_pb2
from temporalio.bridge.proto.child_workflow import child_workflow_pb2 as _child_workflow_pb2
from temporalio.bridge.proto.common import common_pb2 as _common_pb2
from temporalio.bridge.proto.nexus import nexus_pb2 as _nexus_pb2
from temporalio.bridge.proto.external_data import external_data_pb2 as _external_data_pb2
from temporalio.bridge.proto.workflow_commands import workflow_commands_pb2 as _workflow_commands_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkflowActivation(_message.Message):
    __slots__ = ("run_id", "timestamp", "is_replaying", "history_length", "jobs", "available_internal_flags", "history_size_bytes", "continue_as_new_suggested", "deployment_version_for_current_task", "last_sdk_version", "suggest_continue_as_new_reasons", "target_worker_deployment_version_changed", "history_floor_event_id")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    IS_REPLAYING_FIELD_NUMBER: _ClassVar[int]
    HISTORY_LENGTH_FIELD_NUMBER: _ClassVar[int]
    JOBS_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_INTERNAL_FLAGS_FIELD_NUMBER: _ClassVar[int]
    HISTORY_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_SUGGESTED_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_VERSION_FOR_CURRENT_TASK_FIELD_NUMBER: _ClassVar[int]
    LAST_SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    SUGGEST_CONTINUE_AS_NEW_REASONS_FIELD_NUMBER: _ClassVar[int]
    TARGET_WORKER_DEPLOYMENT_VERSION_CHANGED_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FLOOR_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    timestamp: _timestamp_pb2.Timestamp
    is_replaying: bool
    history_length: int
    jobs: _containers.RepeatedCompositeFieldContainer[WorkflowActivationJob]
    available_internal_flags: _containers.RepeatedScalarFieldContainer[int]
    history_size_bytes: int
    continue_as_new_suggested: bool
    deployment_version_for_current_task: _common_pb2.WorkerDeploymentVersion
    last_sdk_version: str
    suggest_continue_as_new_reasons: _containers.RepeatedScalarFieldContainer[_workflow_pb2.SuggestContinueAsNewReason]
    target_worker_deployment_version_changed: bool
    history_floor_event_id: int
    def __init__(self, run_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., is_replaying: _Optional[bool] = ..., history_length: _Optional[int] = ..., jobs: _Optional[_Iterable[_Union[WorkflowActivationJob, _Mapping]]] = ..., available_internal_flags: _Optional[_Iterable[int]] = ..., history_size_bytes: _Optional[int] = ..., continue_as_new_suggested: _Optional[bool] = ..., deployment_version_for_current_task: _Optional[_Union[_common_pb2.WorkerDeploymentVersion, _Mapping]] = ..., last_sdk_version: _Optional[str] = ..., suggest_continue_as_new_reasons: _Optional[_Iterable[_Union[_workflow_pb2.SuggestContinueAsNewReason, str]]] = ..., target_worker_deployment_version_changed: _Optional[bool] = ..., history_floor_event_id: _Optional[int] = ...) -> None: ...

class WorkflowActivationJob(_message.Message):
    __slots__ = ("initialize_workflow", "fire_timer", "update_random_seed", "query_workflow", "cancel_workflow", "signal_workflow", "resolve_activity", "notify_has_patch", "resolve_child_workflow_execution_start", "resolve_child_workflow_execution", "resolve_signal_external_workflow", "resolve_request_cancel_external_workflow", "do_update", "resolve_nexus_operation_start", "resolve_nexus_operation", "resolve_external_stream_waits", "prepare_external_stream_park", "replay_external_streams", "finalize_external_streams", "deliver_stream_messages", "remove_from_cache")
    INITIALIZE_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    FIRE_TIMER_FIELD_NUMBER: _ClassVar[int]
    UPDATE_RANDOM_SEED_FIELD_NUMBER: _ClassVar[int]
    QUERY_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    CANCEL_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    NOTIFY_HAS_PATCH_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_CHILD_WORKFLOW_EXECUTION_START_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_CHILD_WORKFLOW_EXECUTION_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_SIGNAL_EXTERNAL_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    DO_UPDATE_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_NEXUS_OPERATION_START_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_NEXUS_OPERATION_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_EXTERNAL_STREAM_WAITS_FIELD_NUMBER: _ClassVar[int]
    PREPARE_EXTERNAL_STREAM_PARK_FIELD_NUMBER: _ClassVar[int]
    REPLAY_EXTERNAL_STREAMS_FIELD_NUMBER: _ClassVar[int]
    FINALIZE_EXTERNAL_STREAMS_FIELD_NUMBER: _ClassVar[int]
    DELIVER_STREAM_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FROM_CACHE_FIELD_NUMBER: _ClassVar[int]
    initialize_workflow: InitializeWorkflow
    fire_timer: FireTimer
    update_random_seed: UpdateRandomSeed
    query_workflow: QueryWorkflow
    cancel_workflow: CancelWorkflow
    signal_workflow: SignalWorkflow
    resolve_activity: ResolveActivity
    notify_has_patch: NotifyHasPatch
    resolve_child_workflow_execution_start: ResolveChildWorkflowExecutionStart
    resolve_child_workflow_execution: ResolveChildWorkflowExecution
    resolve_signal_external_workflow: ResolveSignalExternalWorkflow
    resolve_request_cancel_external_workflow: ResolveRequestCancelExternalWorkflow
    do_update: DoUpdate
    resolve_nexus_operation_start: ResolveNexusOperationStart
    resolve_nexus_operation: ResolveNexusOperation
    resolve_external_stream_waits: ResolveExternalStreamWaits
    prepare_external_stream_park: PrepareExternalStreamPark
    replay_external_streams: ReplayExternalStreams
    finalize_external_streams: FinalizeExternalStreams
    deliver_stream_messages: DeliverStreamMessages
    remove_from_cache: RemoveFromCache
    def __init__(self, initialize_workflow: _Optional[_Union[InitializeWorkflow, _Mapping]] = ..., fire_timer: _Optional[_Union[FireTimer, _Mapping]] = ..., update_random_seed: _Optional[_Union[UpdateRandomSeed, _Mapping]] = ..., query_workflow: _Optional[_Union[QueryWorkflow, _Mapping]] = ..., cancel_workflow: _Optional[_Union[CancelWorkflow, _Mapping]] = ..., signal_workflow: _Optional[_Union[SignalWorkflow, _Mapping]] = ..., resolve_activity: _Optional[_Union[ResolveActivity, _Mapping]] = ..., notify_has_patch: _Optional[_Union[NotifyHasPatch, _Mapping]] = ..., resolve_child_workflow_execution_start: _Optional[_Union[ResolveChildWorkflowExecutionStart, _Mapping]] = ..., resolve_child_workflow_execution: _Optional[_Union[ResolveChildWorkflowExecution, _Mapping]] = ..., resolve_signal_external_workflow: _Optional[_Union[ResolveSignalExternalWorkflow, _Mapping]] = ..., resolve_request_cancel_external_workflow: _Optional[_Union[ResolveRequestCancelExternalWorkflow, _Mapping]] = ..., do_update: _Optional[_Union[DoUpdate, _Mapping]] = ..., resolve_nexus_operation_start: _Optional[_Union[ResolveNexusOperationStart, _Mapping]] = ..., resolve_nexus_operation: _Optional[_Union[ResolveNexusOperation, _Mapping]] = ..., resolve_external_stream_waits: _Optional[_Union[ResolveExternalStreamWaits, _Mapping]] = ..., prepare_external_stream_park: _Optional[_Union[PrepareExternalStreamPark, _Mapping]] = ..., replay_external_streams: _Optional[_Union[ReplayExternalStreams, _Mapping]] = ..., finalize_external_streams: _Optional[_Union[FinalizeExternalStreams, _Mapping]] = ..., deliver_stream_messages: _Optional[_Union[DeliverStreamMessages, _Mapping]] = ..., remove_from_cache: _Optional[_Union[RemoveFromCache, _Mapping]] = ...) -> None: ...

class ResolveExternalStreamWaits(_message.Message):
    __slots__ = ("quiescence_generation", "ready_hints")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    READY_HINTS_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    ready_hints: _containers.RepeatedCompositeFieldContainer[_workflow_commands_pb2.ExternalStreamWait]
    def __init__(self, quiescence_generation: _Optional[int] = ..., ready_hints: _Optional[_Iterable[_Union[_workflow_commands_pb2.ExternalStreamWait, _Mapping]]] = ...) -> None: ...

class PrepareExternalStreamPark(_message.Message):
    __slots__ = ("quiescence_generation", "waits", "reason")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    WAITS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    waits: _containers.RepeatedCompositeFieldContainer[_workflow_commands_pb2.ExternalStreamWait]
    reason: _external_data_pb2.ParkReason
    def __init__(self, quiescence_generation: _Optional[int] = ..., waits: _Optional[_Iterable[_Union[_workflow_commands_pb2.ExternalStreamWait, _Mapping]]] = ..., reason: _Optional[_Union[_external_data_pb2.ParkReason, str]] = ...) -> None: ...

class ReplayExternalStreams(_message.Message):
    __slots__ = ("quiescence_generation", "waits", "replay_annotation", "terminal_boundary", "output")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    WAITS_FIELD_NUMBER: _ClassVar[int]
    REPLAY_ANNOTATION_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_BOUNDARY_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    waits: _containers.RepeatedCompositeFieldContainer[_workflow_commands_pb2.ExternalStreamWait]
    replay_annotation: bytes
    terminal_boundary: _external_data_pb2.ParkReason
    output: _external_data_pb2.ExternalOutputStreamManifest
    def __init__(self, quiescence_generation: _Optional[int] = ..., waits: _Optional[_Iterable[_Union[_workflow_commands_pb2.ExternalStreamWait, _Mapping]]] = ..., replay_annotation: _Optional[bytes] = ..., terminal_boundary: _Optional[_Union[_external_data_pb2.ParkReason, str]] = ..., output: _Optional[_Union[_external_data_pb2.ExternalOutputStreamManifest, _Mapping]] = ...) -> None: ...

class FinalizeExternalStreams(_message.Message):
    __slots__ = ("quiescence_generation", "waits", "reason")
    QUIESCENCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    WAITS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    quiescence_generation: int
    waits: _containers.RepeatedCompositeFieldContainer[_workflow_commands_pb2.ExternalStreamWait]
    reason: _external_data_pb2.ParkReason
    def __init__(self, quiescence_generation: _Optional[int] = ..., waits: _Optional[_Iterable[_Union[_workflow_commands_pb2.ExternalStreamWait, _Mapping]]] = ..., reason: _Optional[_Union[_external_data_pb2.ParkReason, str]] = ...) -> None: ...

class DeliverStreamMessages(_message.Message):
    __slots__ = ("stream_id", "from_offset", "to_offset", "messages")
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_OFFSET_FIELD_NUMBER: _ClassVar[int]
    TO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    stream_id: str
    from_offset: int
    to_offset: int
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2_1_1_1.StreamMessage]
    def __init__(self, stream_id: _Optional[str] = ..., from_offset: _Optional[int] = ..., to_offset: _Optional[int] = ..., messages: _Optional[_Iterable[_Union[_message_pb2_1_1_1.StreamMessage, _Mapping]]] = ...) -> None: ...

class InitializeWorkflow(_message.Message):
    __slots__ = ("workflow_type", "workflow_id", "arguments", "randomness_seed", "headers", "identity", "parent_workflow_info", "workflow_execution_timeout", "workflow_run_timeout", "workflow_task_timeout", "continued_from_execution_run_id", "continued_initiator", "continued_failure", "last_completion_result", "first_execution_run_id", "retry_policy", "attempt", "cron_schedule", "workflow_execution_expiration_time", "cron_schedule_to_schedule_interval", "memo", "search_attributes", "start_time", "root_workflow", "priority", "original_execution_run_id")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2_1_1.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2_1_1.Payload, _Mapping]] = ...) -> None: ...
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    RANDOMNESS_SEED_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    PARENT_WORKFLOW_INFO_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_EXECUTION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_RUN_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TASK_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_FROM_EXECUTION_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_INITIATOR_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_FAILURE_FIELD_NUMBER: _ClassVar[int]
    LAST_COMPLETION_RESULT_FIELD_NUMBER: _ClassVar[int]
    FIRST_EXECUTION_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_EXECUTION_EXPIRATION_TIME_FIELD_NUMBER: _ClassVar[int]
    CRON_SCHEDULE_TO_SCHEDULE_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    MEMO_FIELD_NUMBER: _ClassVar[int]
    SEARCH_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    ROOT_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_EXECUTION_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_type: str
    workflow_id: str
    arguments: _containers.RepeatedCompositeFieldContainer[_message_pb2_1_1.Payload]
    randomness_seed: int
    headers: _containers.MessageMap[str, _message_pb2_1_1.Payload]
    identity: str
    parent_workflow_info: _common_pb2.NamespacedWorkflowExecution
    workflow_execution_timeout: _duration_pb2.Duration
    workflow_run_timeout: _duration_pb2.Duration
    workflow_task_timeout: _duration_pb2.Duration
    continued_from_execution_run_id: str
    continued_initiator: _workflow_pb2.ContinueAsNewInitiator
    continued_failure: _message_pb2.Failure
    last_completion_result: _message_pb2_1_1.Payloads
    first_execution_run_id: str
    retry_policy: _message_pb2_1_1.RetryPolicy
    attempt: int
    cron_schedule: str
    workflow_execution_expiration_time: _timestamp_pb2.Timestamp
    cron_schedule_to_schedule_interval: _duration_pb2.Duration
    memo: _message_pb2_1_1.Memo
    search_attributes: _message_pb2_1_1.SearchAttributes
    start_time: _timestamp_pb2.Timestamp
    root_workflow: _message_pb2_1_1.WorkflowExecution
    priority: _message_pb2_1_1.Priority
    original_execution_run_id: str
    def __init__(self, workflow_type: _Optional[str] = ..., workflow_id: _Optional[str] = ..., arguments: _Optional[_Iterable[_Union[_message_pb2_1_1.Payload, _Mapping]]] = ..., randomness_seed: _Optional[int] = ..., headers: _Optional[_Mapping[str, _message_pb2_1_1.Payload]] = ..., identity: _Optional[str] = ..., parent_workflow_info: _Optional[_Union[_common_pb2.NamespacedWorkflowExecution, _Mapping]] = ..., workflow_execution_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., workflow_run_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., workflow_task_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., continued_from_execution_run_id: _Optional[str] = ..., continued_initiator: _Optional[_Union[_workflow_pb2.ContinueAsNewInitiator, str]] = ..., continued_failure: _Optional[_Union[_message_pb2.Failure, _Mapping]] = ..., last_completion_result: _Optional[_Union[_message_pb2_1_1.Payloads, _Mapping]] = ..., first_execution_run_id: _Optional[str] = ..., retry_policy: _Optional[_Union[_message_pb2_1_1.RetryPolicy, _Mapping]] = ..., attempt: _Optional[int] = ..., cron_schedule: _Optional[str] = ..., workflow_execution_expiration_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cron_schedule_to_schedule_interval: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., memo: _Optional[_Union[_message_pb2_1_1.Memo, _Mapping]] = ..., search_attributes: _Optional[_Union[_message_pb2_1_1.SearchAttributes, _Mapping]] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., root_workflow: _Optional[_Union[_message_pb2_1_1.WorkflowExecution, _Mapping]] = ..., priority: _Optional[_Union[_message_pb2_1_1.Priority, _Mapping]] = ..., original_execution_run_id: _Optional[str] = ...) -> None: ...

class FireTimer(_message.Message):
    __slots__ = ("seq",)
    SEQ_FIELD_NUMBER: _ClassVar[int]
    seq: int
    def __init__(self, seq: _Optional[int] = ...) -> None: ...

class ResolveActivity(_message.Message):
    __slots__ = ("seq", "result", "is_local")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    IS_LOCAL_FIELD_NUMBER: _ClassVar[int]
    seq: int
    result: _activity_result_pb2.ActivityResolution
    is_local: bool
    def __init__(self, seq: _Optional[int] = ..., result: _Optional[_Union[_activity_result_pb2.ActivityResolution, _Mapping]] = ..., is_local: _Optional[bool] = ...) -> None: ...

class ResolveChildWorkflowExecutionStart(_message.Message):
    __slots__ = ("seq", "succeeded", "failed", "cancelled")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    seq: int
    succeeded: ResolveChildWorkflowExecutionStartSuccess
    failed: ResolveChildWorkflowExecutionStartFailure
    cancelled: ResolveChildWorkflowExecutionStartCancelled
    def __init__(self, seq: _Optional[int] = ..., succeeded: _Optional[_Union[ResolveChildWorkflowExecutionStartSuccess, _Mapping]] = ..., failed: _Optional[_Union[ResolveChildWorkflowExecutionStartFailure, _Mapping]] = ..., cancelled: _Optional[_Union[ResolveChildWorkflowExecutionStartCancelled, _Mapping]] = ...) -> None: ...

class ResolveChildWorkflowExecutionStartSuccess(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class ResolveChildWorkflowExecutionStartFailure(_message.Message):
    __slots__ = ("workflow_id", "workflow_type", "cause")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    workflow_type: str
    cause: _child_workflow_pb2.StartChildWorkflowExecutionFailedCause
    def __init__(self, workflow_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., cause: _Optional[_Union[_child_workflow_pb2.StartChildWorkflowExecutionFailedCause, str]] = ...) -> None: ...

class ResolveChildWorkflowExecutionStartCancelled(_message.Message):
    __slots__ = ("failure",)
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    failure: _message_pb2.Failure
    def __init__(self, failure: _Optional[_Union[_message_pb2.Failure, _Mapping]] = ...) -> None: ...

class ResolveChildWorkflowExecution(_message.Message):
    __slots__ = ("seq", "result")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    seq: int
    result: _child_workflow_pb2.ChildWorkflowResult
    def __init__(self, seq: _Optional[int] = ..., result: _Optional[_Union[_child_workflow_pb2.ChildWorkflowResult, _Mapping]] = ...) -> None: ...

class UpdateRandomSeed(_message.Message):
    __slots__ = ("randomness_seed",)
    RANDOMNESS_SEED_FIELD_NUMBER: _ClassVar[int]
    randomness_seed: int
    def __init__(self, randomness_seed: _Optional[int] = ...) -> None: ...

class QueryWorkflow(_message.Message):
    __slots__ = ("query_id", "query_type", "arguments", "headers")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2_1_1.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2_1_1.Payload, _Mapping]] = ...) -> None: ...
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    query_type: str
    arguments: _containers.RepeatedCompositeFieldContainer[_message_pb2_1_1.Payload]
    headers: _containers.MessageMap[str, _message_pb2_1_1.Payload]
    def __init__(self, query_id: _Optional[str] = ..., query_type: _Optional[str] = ..., arguments: _Optional[_Iterable[_Union[_message_pb2_1_1.Payload, _Mapping]]] = ..., headers: _Optional[_Mapping[str, _message_pb2_1_1.Payload]] = ...) -> None: ...

class CancelWorkflow(_message.Message):
    __slots__ = ("reason",)
    REASON_FIELD_NUMBER: _ClassVar[int]
    reason: str
    def __init__(self, reason: _Optional[str] = ...) -> None: ...

class SignalWorkflow(_message.Message):
    __slots__ = ("signal_name", "input", "identity", "headers", "originating_event_id")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2_1_1.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2_1_1.Payload, _Mapping]] = ...) -> None: ...
    SIGNAL_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    ORIGINATING_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    signal_name: str
    input: _containers.RepeatedCompositeFieldContainer[_message_pb2_1_1.Payload]
    identity: str
    headers: _containers.MessageMap[str, _message_pb2_1_1.Payload]
    originating_event_id: int
    def __init__(self, signal_name: _Optional[str] = ..., input: _Optional[_Iterable[_Union[_message_pb2_1_1.Payload, _Mapping]]] = ..., identity: _Optional[str] = ..., headers: _Optional[_Mapping[str, _message_pb2_1_1.Payload]] = ..., originating_event_id: _Optional[int] = ...) -> None: ...

class NotifyHasPatch(_message.Message):
    __slots__ = ("patch_id",)
    PATCH_ID_FIELD_NUMBER: _ClassVar[int]
    patch_id: str
    def __init__(self, patch_id: _Optional[str] = ...) -> None: ...

class ResolveSignalExternalWorkflow(_message.Message):
    __slots__ = ("seq", "failure")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    seq: int
    failure: _message_pb2.Failure
    def __init__(self, seq: _Optional[int] = ..., failure: _Optional[_Union[_message_pb2.Failure, _Mapping]] = ...) -> None: ...

class ResolveRequestCancelExternalWorkflow(_message.Message):
    __slots__ = ("seq", "failure")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    seq: int
    failure: _message_pb2.Failure
    def __init__(self, seq: _Optional[int] = ..., failure: _Optional[_Union[_message_pb2.Failure, _Mapping]] = ...) -> None: ...

class DoUpdate(_message.Message):
    __slots__ = ("id", "protocol_instance_id", "name", "input", "headers", "meta", "run_validator")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _message_pb2_1_1.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_message_pb2_1_1.Payload, _Mapping]] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    RUN_VALIDATOR_FIELD_NUMBER: _ClassVar[int]
    id: str
    protocol_instance_id: str
    name: str
    input: _containers.RepeatedCompositeFieldContainer[_message_pb2_1_1.Payload]
    headers: _containers.MessageMap[str, _message_pb2_1_1.Payload]
    meta: _message_pb2_1.Meta
    run_validator: bool
    def __init__(self, id: _Optional[str] = ..., protocol_instance_id: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Iterable[_Union[_message_pb2_1_1.Payload, _Mapping]]] = ..., headers: _Optional[_Mapping[str, _message_pb2_1_1.Payload]] = ..., meta: _Optional[_Union[_message_pb2_1.Meta, _Mapping]] = ..., run_validator: _Optional[bool] = ...) -> None: ...

class ResolveNexusOperationStart(_message.Message):
    __slots__ = ("seq", "operation_token", "started_sync", "failed")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    OPERATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STARTED_SYNC_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    seq: int
    operation_token: str
    started_sync: bool
    failed: _message_pb2.Failure
    def __init__(self, seq: _Optional[int] = ..., operation_token: _Optional[str] = ..., started_sync: _Optional[bool] = ..., failed: _Optional[_Union[_message_pb2.Failure, _Mapping]] = ...) -> None: ...

class ResolveNexusOperation(_message.Message):
    __slots__ = ("seq", "result")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    seq: int
    result: _nexus_pb2.NexusOperationResult
    def __init__(self, seq: _Optional[int] = ..., result: _Optional[_Union[_nexus_pb2.NexusOperationResult, _Mapping]] = ...) -> None: ...

class RemoveFromCache(_message.Message):
    __slots__ = ("message", "reason")
    class EvictionReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNSPECIFIED: _ClassVar[RemoveFromCache.EvictionReason]
        CACHE_FULL: _ClassVar[RemoveFromCache.EvictionReason]
        CACHE_MISS: _ClassVar[RemoveFromCache.EvictionReason]
        NONDETERMINISM: _ClassVar[RemoveFromCache.EvictionReason]
        LANG_FAIL: _ClassVar[RemoveFromCache.EvictionReason]
        LANG_REQUESTED: _ClassVar[RemoveFromCache.EvictionReason]
        TASK_NOT_FOUND: _ClassVar[RemoveFromCache.EvictionReason]
        UNHANDLED_COMMAND: _ClassVar[RemoveFromCache.EvictionReason]
        FATAL: _ClassVar[RemoveFromCache.EvictionReason]
        PAGINATION_OR_HISTORY_FETCH: _ClassVar[RemoveFromCache.EvictionReason]
        WORKFLOW_EXECUTION_ENDING: _ClassVar[RemoveFromCache.EvictionReason]
    UNSPECIFIED: RemoveFromCache.EvictionReason
    CACHE_FULL: RemoveFromCache.EvictionReason
    CACHE_MISS: RemoveFromCache.EvictionReason
    NONDETERMINISM: RemoveFromCache.EvictionReason
    LANG_FAIL: RemoveFromCache.EvictionReason
    LANG_REQUESTED: RemoveFromCache.EvictionReason
    TASK_NOT_FOUND: RemoveFromCache.EvictionReason
    UNHANDLED_COMMAND: RemoveFromCache.EvictionReason
    FATAL: RemoveFromCache.EvictionReason
    PAGINATION_OR_HISTORY_FETCH: RemoveFromCache.EvictionReason
    WORKFLOW_EXECUTION_ENDING: RemoveFromCache.EvictionReason
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    message: str
    reason: RemoveFromCache.EvictionReason
    def __init__(self, message: _Optional[str] = ..., reason: _Optional[_Union[RemoveFromCache.EvictionReason, str]] = ...) -> None: ...
