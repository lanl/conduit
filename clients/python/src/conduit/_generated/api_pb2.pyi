import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryOperation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AND: _ClassVar[QueryOperation]
    OR: _ClassVar[QueryOperation]

class LeaseType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SOURCE: _ClassVar[LeaseType]
    DESTINATION: _ClassVar[LeaseType]

class DeprecatedAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COPY: _ClassVar[DeprecatedAction]
    MOVE: _ClassVar[DeprecatedAction]
    RECURSIVE_COPY: _ClassVar[DeprecatedAction]
    RECURSIVE_MOVE: _ClassVar[DeprecatedAction]

class SchedulerCommand(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NONE: _ClassVar[SchedulerCommand]
    VALIDATION: _ClassVar[SchedulerCommand]
    SETUP: _ClassVar[SchedulerCommand]
    TRANSFER: _ClassVar[SchedulerCommand]
    TEARDOWN: _ClassVar[SchedulerCommand]

class TransferState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSFER_NONE: _ClassVar[TransferState]
    TRANSFER_ERROR: _ClassVar[TransferState]
    TRANSFER_ABORT: _ClassVar[TransferState]
    TRANSFER_ABORTED: _ClassVar[TransferState]
    TRANSFER_INIT: _ClassVar[TransferState]
    TRANSFER_INIT_COMPLETE: _ClassVar[TransferState]
    TRANSFER_VALIDATION_READY: _ClassVar[TransferState]
    TRANSFER_VALIDATION_SUBMITTED: _ClassVar[TransferState]
    TRANSFER_VALIDATING: _ClassVar[TransferState]
    TRANSFER_VALIDATION_COMPLETE: _ClassVar[TransferState]
    TRANSFER_WAITING_FOR_LEASE: _ClassVar[TransferState]
    TRANSFER_LEASE_ACQUIRED: _ClassVar[TransferState]
    TRANSFER_SETUP_READY: _ClassVar[TransferState]
    TRANSFER_SETUP_SUBMITTED: _ClassVar[TransferState]
    TRANSFER_SETUP: _ClassVar[TransferState]
    TRANSFER_SETUP_COMPLETE: _ClassVar[TransferState]
    TRANSFER_DATA_READY: _ClassVar[TransferState]
    TRANSFER_DATA_SUBMITTED: _ClassVar[TransferState]
    TRANSFER_DATA_TRANSFERRING: _ClassVar[TransferState]
    TRANSFER_DATA_COMPLETE: _ClassVar[TransferState]
    TRANSFER_TEARDOWN_READY: _ClassVar[TransferState]
    TRANSFER_TEARDOWN_SUBMITTED: _ClassVar[TransferState]
    TRANSFER_TEARDOWN: _ClassVar[TransferState]
    TRANSFER_TEARDOWN_COMPLETE: _ClassVar[TransferState]
    TRANSFER_FINALIZED: _ClassVar[TransferState]

class TransferArchiveState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSFER_ARCHIVE_NONE: _ClassVar[TransferArchiveState]
    TRANSFER_ARCHIVE_ERROR: _ClassVar[TransferArchiveState]
    TRANSFER_ARCHIVE_INIT: _ClassVar[TransferArchiveState]
    TRANSFER_ARCHIVE_READY: _ClassVar[TransferArchiveState]
    TRANSFER_ARCHIVE_SUBMITTED: _ClassVar[TransferArchiveState]
    TRANSFER_ARCHIVE: _ClassVar[TransferArchiveState]
    TRANSFER_ARCHIVE_COMPLETE: _ClassVar[TransferArchiveState]

class Error(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR_NONE: _ClassVar[Error]
    ERROR_PERMISSIONS: _ClassVar[Error]
    ERROR_NETWORK: _ClassVar[Error]
    ERROR_AUTH: _ClassVar[Error]
    ERROR_PFTOOL_FAILED: _ClassVar[Error]
    ERROR_LEASE_EXPIRED: _ClassVar[Error]
    ERROR_ETCD_CONNECTION: _ClassVar[Error]
    ERROR_ETCD_INTERNAL: _ClassVar[Error]
    ERROR_SCHEDULER: _ClassVar[Error]
    ERROR_SCHEDULER_CONNECTION: _ClassVar[Error]
    ERROR_SCHEDULER_AUTH: _ClassVar[Error]
    ERROR_CONDUIT_INTERNAL: _ClassVar[Error]
    ERROR_NO_TRANSFERS_FOR_USER: _ClassVar[Error]
    ERROR_RENAME_FAILED: _ClassVar[Error]
    ERROR_SYMLINK_FAILED: _ClassVar[Error]
    ERROR_MOUNTINFO_FAILED: _ClassVar[Error]
    ERROR_MKDIR_FAILED: _ClassVar[Error]
    ERROR_LEASE_ERROR: _ClassVar[Error]
    ERROR_ABORTED: _ClassVar[Error]
    ERROR_REMOVE_FAILED: _ClassVar[Error]
    ERROR_STAT_FAILED: _ClassVar[Error]
    ERROR_CLEANUP_ERROR: _ClassVar[Error]
    ERROR_VALIDATION: _ClassVar[Error]
    ERROR_DATA_CHANGED: _ClassVar[Error]
    ERROR_FILE_NOT_EXIST: _ClassVar[Error]
    ERROR_INVALID_REGEX: _ClassVar[Error]
    ERROR_INVALID_CONDUIT_CONFIG: _ClassVar[Error]
    ERROR_INVALID_INPUT: _ClassVar[Error]
    ERROR_MAX_ARG_STRLEN: _ClassVar[Error]
    ERROR_FTA_PLUGIN_FAILED: _ClassVar[Error]
    ERROR_CHTIME_FAILED: _ClassVar[Error]

class ArchiveState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ARCHIVE_NONE: _ClassVar[ArchiveState]
    ARCHIVE_ERROR: _ClassVar[ArchiveState]
    ARCHIVE_READY: _ClassVar[ArchiveState]
    ARCHIVE_SUBMIT: _ClassVar[ArchiveState]
    ARCHIVE_COMPLETE: _ClassVar[ArchiveState]

class DestInfo(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEST_NONE: _ClassVar[DestInfo]
    DEST_NOT_EXIST: _ClassVar[DestInfo]
    DEST_NOT_DIR: _ClassVar[DestInfo]
    DEST_IS_DIR: _ClassVar[DestInfo]

class KrbCacheType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    KRB_NONE: _ClassVar[KrbCacheType]
    API: _ClassVar[KrbCacheType]
    DIR: _ClassVar[KrbCacheType]
    FILE: _ClassVar[KrbCacheType]
    KCM: _ClassVar[KrbCacheType]
    KEYRING: _ClassVar[KrbCacheType]
    MEMORY: _ClassVar[KrbCacheType]
    MSLSA: _ClassVar[KrbCacheType]

class ServerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SERVER_UNKNOWN: _ClassVar[ServerState]
    SERVER_STARTING: _ClassVar[ServerState]
    SERVER_RUNNING: _ClassVar[ServerState]
    SERVER_STOPPING: _ClassVar[ServerState]
    SERVER_STOPPED: _ClassVar[ServerState]
    SERVER_ERROR: _ClassVar[ServerState]
    SERVER_DRAIN_INIT: _ClassVar[ServerState]
    SERVER_DRAINING: _ClassVar[ServerState]

class ServerControlAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SERVER_CONTROL_NONE: _ClassVar[ServerControlAction]
    SERVER_CONTROL_STATUS: _ClassVar[ServerControlAction]
    SERVER_CONTROL_STOP: _ClassVar[ServerControlAction]
    SERVER_CONTROL_START: _ClassVar[ServerControlAction]
    SERVER_CONTROL_DRAIN: _ClassVar[ServerControlAction]

class JobType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HEAD: _ClassVar[JobType]
    ALLOCATE: _ClassVar[JobType]
AND: QueryOperation
OR: QueryOperation
SOURCE: LeaseType
DESTINATION: LeaseType
COPY: DeprecatedAction
MOVE: DeprecatedAction
RECURSIVE_COPY: DeprecatedAction
RECURSIVE_MOVE: DeprecatedAction
NONE: SchedulerCommand
VALIDATION: SchedulerCommand
SETUP: SchedulerCommand
TRANSFER: SchedulerCommand
TEARDOWN: SchedulerCommand
TRANSFER_NONE: TransferState
TRANSFER_ERROR: TransferState
TRANSFER_ABORT: TransferState
TRANSFER_ABORTED: TransferState
TRANSFER_INIT: TransferState
TRANSFER_INIT_COMPLETE: TransferState
TRANSFER_VALIDATION_READY: TransferState
TRANSFER_VALIDATION_SUBMITTED: TransferState
TRANSFER_VALIDATING: TransferState
TRANSFER_VALIDATION_COMPLETE: TransferState
TRANSFER_WAITING_FOR_LEASE: TransferState
TRANSFER_LEASE_ACQUIRED: TransferState
TRANSFER_SETUP_READY: TransferState
TRANSFER_SETUP_SUBMITTED: TransferState
TRANSFER_SETUP: TransferState
TRANSFER_SETUP_COMPLETE: TransferState
TRANSFER_DATA_READY: TransferState
TRANSFER_DATA_SUBMITTED: TransferState
TRANSFER_DATA_TRANSFERRING: TransferState
TRANSFER_DATA_COMPLETE: TransferState
TRANSFER_TEARDOWN_READY: TransferState
TRANSFER_TEARDOWN_SUBMITTED: TransferState
TRANSFER_TEARDOWN: TransferState
TRANSFER_TEARDOWN_COMPLETE: TransferState
TRANSFER_FINALIZED: TransferState
TRANSFER_ARCHIVE_NONE: TransferArchiveState
TRANSFER_ARCHIVE_ERROR: TransferArchiveState
TRANSFER_ARCHIVE_INIT: TransferArchiveState
TRANSFER_ARCHIVE_READY: TransferArchiveState
TRANSFER_ARCHIVE_SUBMITTED: TransferArchiveState
TRANSFER_ARCHIVE: TransferArchiveState
TRANSFER_ARCHIVE_COMPLETE: TransferArchiveState
ERROR_NONE: Error
ERROR_PERMISSIONS: Error
ERROR_NETWORK: Error
ERROR_AUTH: Error
ERROR_PFTOOL_FAILED: Error
ERROR_LEASE_EXPIRED: Error
ERROR_ETCD_CONNECTION: Error
ERROR_ETCD_INTERNAL: Error
ERROR_SCHEDULER: Error
ERROR_SCHEDULER_CONNECTION: Error
ERROR_SCHEDULER_AUTH: Error
ERROR_CONDUIT_INTERNAL: Error
ERROR_NO_TRANSFERS_FOR_USER: Error
ERROR_RENAME_FAILED: Error
ERROR_SYMLINK_FAILED: Error
ERROR_MOUNTINFO_FAILED: Error
ERROR_MKDIR_FAILED: Error
ERROR_LEASE_ERROR: Error
ERROR_ABORTED: Error
ERROR_REMOVE_FAILED: Error
ERROR_STAT_FAILED: Error
ERROR_CLEANUP_ERROR: Error
ERROR_VALIDATION: Error
ERROR_DATA_CHANGED: Error
ERROR_FILE_NOT_EXIST: Error
ERROR_INVALID_REGEX: Error
ERROR_INVALID_CONDUIT_CONFIG: Error
ERROR_INVALID_INPUT: Error
ERROR_MAX_ARG_STRLEN: Error
ERROR_FTA_PLUGIN_FAILED: Error
ERROR_CHTIME_FAILED: Error
ARCHIVE_NONE: ArchiveState
ARCHIVE_ERROR: ArchiveState
ARCHIVE_READY: ArchiveState
ARCHIVE_SUBMIT: ArchiveState
ARCHIVE_COMPLETE: ArchiveState
DEST_NONE: DestInfo
DEST_NOT_EXIST: DestInfo
DEST_NOT_DIR: DestInfo
DEST_IS_DIR: DestInfo
KRB_NONE: KrbCacheType
API: KrbCacheType
DIR: KrbCacheType
FILE: KrbCacheType
KCM: KrbCacheType
KEYRING: KrbCacheType
MEMORY: KrbCacheType
MSLSA: KrbCacheType
SERVER_UNKNOWN: ServerState
SERVER_STARTING: ServerState
SERVER_RUNNING: ServerState
SERVER_STOPPING: ServerState
SERVER_STOPPED: ServerState
SERVER_ERROR: ServerState
SERVER_DRAIN_INIT: ServerState
SERVER_DRAINING: ServerState
SERVER_CONTROL_NONE: ServerControlAction
SERVER_CONTROL_STATUS: ServerControlAction
SERVER_CONTROL_STOP: ServerControlAction
SERVER_CONTROL_START: ServerControlAction
SERVER_CONTROL_DRAIN: ServerControlAction
HEAD: JobType
ALLOCATE: JobType

class MultiTransferDetails(_message.Message):
    __slots__ = ("details",)
    class DetailsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TransferDetails
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TransferDetails, _Mapping]] = ...) -> None: ...
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    details: _containers.MessageMap[str, TransferDetails]
    def __init__(self, details: _Optional[_Mapping[str, TransferDetails]] = ...) -> None: ...

class VersionInfo(_message.Message):
    __slots__ = ("version", "modified", "time")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    version: str
    modified: bool
    time: str
    def __init__(self, version: _Optional[str] = ..., modified: bool = ..., time: _Optional[str] = ...) -> None: ...

class TransferIds(_message.Message):
    __slots__ = ("value", "user")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    value: _containers.RepeatedScalarFieldContainer[str]
    user: str
    def __init__(self, value: _Optional[_Iterable[str]] = ..., user: _Optional[str] = ...) -> None: ...

class QueryOptions(_message.Message):
    __slots__ = ("queryMap", "queryOperation", "user")
    class QueryMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    QUERYMAP_FIELD_NUMBER: _ClassVar[int]
    QUERYOPERATION_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    queryMap: _containers.ScalarMap[str, str]
    queryOperation: QueryOperation
    user: str
    def __init__(self, queryMap: _Optional[_Mapping[str, str]] = ..., queryOperation: _Optional[_Union[QueryOperation, str]] = ..., user: _Optional[str] = ...) -> None: ...

class CertResponse(_message.Message):
    __slots__ = ("cert",)
    CERT_FIELD_NUMBER: _ClassVar[int]
    cert: bytes
    def __init__(self, cert: _Optional[bytes] = ...) -> None: ...

class CertRequest(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: str
    def __init__(self, user: _Optional[str] = ...) -> None: ...

class TransferDetails(_message.Message):
    __slots__ = ("transferID", "deprecatedAction", "source", "destination", "leases", "user", "startTime", "endTime", "createdTime", "state", "error", "errorMessage", "dataTransferred", "filesTransferred", "filesChunks", "bandwidth", "directoriesTransferred", "schedulerNodes", "active", "comment", "pausedState", "expiry", "archiveState", "warnings", "destInfo", "validationOnly", "pluginData", "pluginStatus", "priority", "action", "options")
    class OptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _any_pb2.Any
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
    TRANSFERID_FIELD_NUMBER: _ClassVar[int]
    DEPRECATEDACTION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    LEASES_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    STARTTIME_FIELD_NUMBER: _ClassVar[int]
    ENDTIME_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    DATATRANSFERRED_FIELD_NUMBER: _ClassVar[int]
    FILESTRANSFERRED_FIELD_NUMBER: _ClassVar[int]
    FILESCHUNKS_FIELD_NUMBER: _ClassVar[int]
    BANDWIDTH_FIELD_NUMBER: _ClassVar[int]
    DIRECTORIESTRANSFERRED_FIELD_NUMBER: _ClassVar[int]
    SCHEDULERNODES_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    PAUSEDSTATE_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    ARCHIVESTATE_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    DESTINFO_FIELD_NUMBER: _ClassVar[int]
    VALIDATIONONLY_FIELD_NUMBER: _ClassVar[int]
    PLUGINDATA_FIELD_NUMBER: _ClassVar[int]
    PLUGINSTATUS_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    transferID: str
    deprecatedAction: DeprecatedAction
    source: _containers.RepeatedScalarFieldContainer[str]
    destination: str
    leases: Leases
    user: str
    startTime: _timestamp_pb2.Timestamp
    endTime: _timestamp_pb2.Timestamp
    createdTime: _timestamp_pb2.Timestamp
    state: TransferState
    error: Error
    errorMessage: str
    dataTransferred: str
    filesTransferred: int
    filesChunks: int
    bandwidth: str
    directoriesTransferred: int
    schedulerNodes: SchedulerNodes
    active: bool
    comment: str
    pausedState: TransferState
    expiry: _timestamp_pb2.Timestamp
    archiveState: ArchiveState
    warnings: _containers.RepeatedScalarFieldContainer[str]
    destInfo: DestInfo
    validationOnly: bool
    pluginData: bytes
    pluginStatus: str
    priority: int
    action: str
    options: _containers.MessageMap[str, _any_pb2.Any]
    def __init__(self, transferID: _Optional[str] = ..., deprecatedAction: _Optional[_Union[DeprecatedAction, str]] = ..., source: _Optional[_Iterable[str]] = ..., destination: _Optional[str] = ..., leases: _Optional[_Union[Leases, _Mapping]] = ..., user: _Optional[str] = ..., startTime: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., endTime: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., createdTime: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., state: _Optional[_Union[TransferState, str]] = ..., error: _Optional[_Union[Error, str]] = ..., errorMessage: _Optional[str] = ..., dataTransferred: _Optional[str] = ..., filesTransferred: _Optional[int] = ..., filesChunks: _Optional[int] = ..., bandwidth: _Optional[str] = ..., directoriesTransferred: _Optional[int] = ..., schedulerNodes: _Optional[_Union[SchedulerNodes, _Mapping]] = ..., active: bool = ..., comment: _Optional[str] = ..., pausedState: _Optional[_Union[TransferState, str]] = ..., expiry: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., archiveState: _Optional[_Union[ArchiveState, str]] = ..., warnings: _Optional[_Iterable[str]] = ..., destInfo: _Optional[_Union[DestInfo, str]] = ..., validationOnly: bool = ..., pluginData: _Optional[bytes] = ..., pluginStatus: _Optional[str] = ..., priority: _Optional[int] = ..., action: _Optional[str] = ..., options: _Optional[_Mapping[str, _any_pb2.Any]] = ...) -> None: ...

class TransferRequest(_message.Message):
    __slots__ = ("user", "deprecatedAction", "source", "destination", "comment", "pausedState", "action", "options")
    class OptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _any_pb2.Any
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
    USER_FIELD_NUMBER: _ClassVar[int]
    DEPRECATEDACTION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    PAUSEDSTATE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    user: str
    deprecatedAction: DeprecatedAction
    source: _containers.RepeatedScalarFieldContainer[str]
    destination: str
    comment: str
    pausedState: TransferState
    action: str
    options: _containers.MessageMap[str, _any_pb2.Any]
    def __init__(self, user: _Optional[str] = ..., deprecatedAction: _Optional[_Union[DeprecatedAction, str]] = ..., source: _Optional[_Iterable[str]] = ..., destination: _Optional[str] = ..., comment: _Optional[str] = ..., pausedState: _Optional[_Union[TransferState, str]] = ..., action: _Optional[str] = ..., options: _Optional[_Mapping[str, _any_pb2.Any]] = ...) -> None: ...

class PauseRequest(_message.Message):
    __slots__ = ("transferID", "pausedState")
    TRANSFERID_FIELD_NUMBER: _ClassVar[int]
    PAUSEDSTATE_FIELD_NUMBER: _ClassVar[int]
    transferID: str
    pausedState: TransferState
    def __init__(self, transferID: _Optional[str] = ..., pausedState: _Optional[_Union[TransferState, str]] = ...) -> None: ...

class SchedulerNodes(_message.Message):
    __slots__ = ("validation", "setup", "transfer", "teardown")
    VALIDATION_FIELD_NUMBER: _ClassVar[int]
    SETUP_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    TEARDOWN_FIELD_NUMBER: _ClassVar[int]
    validation: str
    setup: str
    transfer: str
    teardown: str
    def __init__(self, validation: _Optional[str] = ..., setup: _Optional[str] = ..., transfer: _Optional[str] = ..., teardown: _Optional[str] = ...) -> None: ...

class Leases(_message.Message):
    __slots__ = ("source", "destination")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    source: _containers.RepeatedScalarFieldContainer[str]
    destination: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, source: _Optional[_Iterable[str]] = ..., destination: _Optional[_Iterable[str]] = ...) -> None: ...

class ServerControlRequest(_message.Message):
    __slots__ = ("action",)
    ACTION_FIELD_NUMBER: _ClassVar[int]
    action: ServerControlAction
    def __init__(self, action: _Optional[_Union[ServerControlAction, str]] = ...) -> None: ...

class ServerControlResponse(_message.Message):
    __slots__ = ("serverState",)
    SERVERSTATE_FIELD_NUMBER: _ClassVar[int]
    serverState: ServerState
    def __init__(self, serverState: _Optional[_Union[ServerState, str]] = ...) -> None: ...

class NodeStatus(_message.Message):
    __slots__ = ("jobs", "availableMemory")
    class JobsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: JobInfo
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[JobInfo, _Mapping]] = ...) -> None: ...
    JOBS_FIELD_NUMBER: _ClassVar[int]
    AVAILABLEMEMORY_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.MessageMap[str, JobInfo]
    availableMemory: int
    def __init__(self, jobs: _Optional[_Mapping[str, JobInfo]] = ..., availableMemory: _Optional[int] = ...) -> None: ...

class JobRequest(_message.Message):
    __slots__ = ("cmd", "type", "transferID", "nodes", "existingJobs")
    class ExistingJobsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: JobInfo
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[JobInfo, _Mapping]] = ...) -> None: ...
    CMD_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TRANSFERID_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    EXISTINGJOBS_FIELD_NUMBER: _ClassVar[int]
    cmd: SchedulerCommand
    type: JobType
    transferID: str
    nodes: _containers.RepeatedScalarFieldContainer[str]
    existingJobs: _containers.MessageMap[str, JobInfo]
    def __init__(self, cmd: _Optional[_Union[SchedulerCommand, str]] = ..., type: _Optional[_Union[JobType, str]] = ..., transferID: _Optional[str] = ..., nodes: _Optional[_Iterable[str]] = ..., existingJobs: _Optional[_Mapping[str, JobInfo]] = ...) -> None: ...

class JobInfo(_message.Message):
    __slots__ = ("actions",)
    class ActionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: bool
        def __init__(self, key: _Optional[int] = ..., value: bool = ...) -> None: ...
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.ScalarMap[int, bool]
    def __init__(self, actions: _Optional[_Mapping[int, bool]] = ...) -> None: ...

class SchedulerInfoResponse(_message.Message):
    __slots__ = ("schedulers",)
    class SchedulersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: SchedulerStatus
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[SchedulerStatus, _Mapping]] = ...) -> None: ...
    SCHEDULERS_FIELD_NUMBER: _ClassVar[int]
    schedulers: _containers.MessageMap[str, SchedulerStatus]
    def __init__(self, schedulers: _Optional[_Mapping[str, SchedulerStatus]] = ...) -> None: ...

class SchedulerStatus(_message.Message):
    __slots__ = ("nodes",)
    class NodesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: NodeStatus
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[NodeStatus, _Mapping]] = ...) -> None: ...
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.MessageMap[str, NodeStatus]
    def __init__(self, nodes: _Optional[_Mapping[str, NodeStatus]] = ...) -> None: ...

class ErrantPathsResponse(_message.Message):
    __slots__ = ("paths",)
    class PathsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _timestamp_pb2.Timestamp
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    PATHS_FIELD_NUMBER: _ClassVar[int]
    paths: _containers.MessageMap[str, _timestamp_pb2.Timestamp]
    def __init__(self, paths: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ...) -> None: ...

class PurgeErrantPathRequest(_message.Message):
    __slots__ = ("paths", "user")
    PATHS_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    paths: _containers.RepeatedScalarFieldContainer[str]
    user: str
    def __init__(self, paths: _Optional[_Iterable[str]] = ..., user: _Optional[str] = ...) -> None: ...

class ErrantPathsRequest(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: str
    def __init__(self, user: _Optional[str] = ...) -> None: ...
