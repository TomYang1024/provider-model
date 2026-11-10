"""Agent Tool Calling Infrastructure：工具的定义、发现、调用与治理。

与 Provider SDK、Agent Runtime 解耦：核心包不依赖任何 Provider SDK 或具体 Runtime；
和 ``provider_sdk`` 之间的数据转换在可选的 :mod:`agent_tools.integrations.provider_sdk` 中
（需要安装 ``agent-tools[provider-sdk]``）。

HTTP 工具适配器在 :mod:`agent_tools.adapters.http` 中（需要 ``agent-tools[http]``）；
MCP Client 适配器在 :mod:`agent_tools.adapters.mcp` 与 :mod:`agent_tools.integrations.mcp`
中（需要 ``agent-tools[mcp]``）。它们都不会被本包自动导入。

P3 扩展能力（熔断、人工审批、结果缓存与调用去重、进程隔离、超大输出钩子）全部默认关闭，
需要宿主显式配置；见 README 的“P3 扩展能力”。

导入本包没有任何副作用：不会配置 OpenTelemetry Provider、导出器或日志，也不会启动子进程。
"""

from .binding import PythonTarget, ToolBinding
from .executor import ExecutionMode, ExecutorConfig, ToolExecutor
from .governance.approval import ApprovalDecision, ApprovalHandler, ApprovalRequest
from .governance.circuit import (
    CircuitBreaker,
    CircuitBreakerConfig,
    InProcessCircuitBreaker,
    default_failure_classifier,
)
from .governance.ratelimit import RateLimiter, TokenBucketRateLimiter
from .governance.reuse import InMemoryResultStore, ResultStore
from .observability.audit import AuditEvent, AuditSink
from .protocol import (
    ApprovalPolicy,
    ApprovalRejectedError,
    ApprovalRequiredError,
    CachePolicy,
    CircuitOpenError,
    ConcurrencyLimitedError,
    ContentBlock,
    DedupePolicy,
    ExecutorClosedError,
    HttpTarget,
    ImageBlock,
    InvalidArgumentsError,
    JsonBlock,
    McpTarget,
    OutputLimitError,
    OutputLimits,
    OutputValidationError,
    PermissionDeniedError,
    Principal,
    RateLimit,
    RateLimitedError,
    RegistryConflictError,
    RegistryError,
    RetryPolicy,
    SecretRef,
    TextBlock,
    ToolAnnotations,
    ToolCancelledError,
    ToolContext,
    ToolDescriptor,
    ToolDisabledError,
    ToolError,
    ToolErrorInfo,
    ToolExecutionError,
    ToolExecutionResult,
    ToolInfraError,
    ToolInvocation,
    ToolListing,
    ToolNotFoundError,
    ToolOutput,
    ToolPolicy,
    ToolSpec,
    ToolTimeoutError,
    ToolVersionNotFoundError,
    UpstreamError,
    new_request_id,
)
from .registry import RegistrySnapshot, ToolRegistry, model_alias
from .runtime.isolation import ProcessIsolation, ResourceLimits, StdioIsolation
from .security.authz import Authorizer, AuthzDecision, ScopeAuthorizer
from .security.net import NetworkPolicy
from .security.oversize import OversizeHandler
from .security.result_filter import ResultFilter
from .security.secrets import MappingSecretProvider, SecretProvider
from .version import __version__

__all__ = [
    "ApprovalDecision",
    "ApprovalHandler",
    "ApprovalPolicy",
    "ApprovalRejectedError",
    "ApprovalRequest",
    "ApprovalRequiredError",
    "AuditEvent",
    "AuditSink",
    "Authorizer",
    "AuthzDecision",
    "CachePolicy",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "ConcurrencyLimitedError",
    "ContentBlock",
    "DedupePolicy",
    "ExecutionMode",
    "ExecutorClosedError",
    "ExecutorConfig",
    "HttpTarget",
    "ImageBlock",
    "InMemoryResultStore",
    "InProcessCircuitBreaker",
    "InvalidArgumentsError",
    "JsonBlock",
    "MappingSecretProvider",
    "McpTarget",
    "NetworkPolicy",
    "OutputLimitError",
    "OutputLimits",
    "OutputValidationError",
    "OversizeHandler",
    "PermissionDeniedError",
    "Principal",
    "ProcessIsolation",
    "PythonTarget",
    "RateLimit",
    "RateLimitedError",
    "RateLimiter",
    "RegistryConflictError",
    "RegistryError",
    "RegistrySnapshot",
    "ResourceLimits",
    "ResultFilter",
    "ResultStore",
    "RetryPolicy",
    "ScopeAuthorizer",
    "SecretProvider",
    "SecretRef",
    "StdioIsolation",
    "TextBlock",
    "TokenBucketRateLimiter",
    "ToolAnnotations",
    "ToolBinding",
    "ToolCancelledError",
    "ToolContext",
    "ToolDescriptor",
    "ToolDisabledError",
    "ToolError",
    "ToolErrorInfo",
    "ToolExecutionError",
    "ToolExecutionResult",
    "ToolExecutor",
    "ToolInfraError",
    "ToolInvocation",
    "ToolListing",
    "ToolNotFoundError",
    "ToolOutput",
    "ToolPolicy",
    "ToolRegistry",
    "ToolSpec",
    "ToolTimeoutError",
    "ToolVersionNotFoundError",
    "UpstreamError",
    "__version__",
    "default_failure_classifier",
    "model_alias",
    "new_request_id",
]
