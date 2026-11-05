"""Provider SDK：DeepSeek、GLM、Kimi、Qwen 的统一异步接口。

导入本包没有任何副作用：不会安装全局 OpenTelemetry Provider、导出器或日志配置。
"""

from . import exceptions as errors
from .adapters import (
    BaseProviderAdapter,
    DeepSeekAdapter,
    GLMAdapter,
    KimiAdapter,
    OpenAICompatibleAdapter,
    QwenAdapter,
)
from .client import ProviderSDK
from .config import LoggingConfig, ObservabilityConfig, SDKConfig
from .exceptions import (
    AuthenticationError,
    ContextLengthExceededError,
    InvalidRequestError,
    ProviderConnectionError,
    ProviderError,
    ProviderNotFoundError,
    ProviderServerError,
    ProviderTimeoutError,
    RateLimitError,
    StreamClosedError,
    UnsupportedCapabilityError,
)
from .registry import ProviderRegistry
from .schemas import (
    ChatRequest,
    ChatResponse,
    ContentPart,
    ImagePart,
    Message,
    MessageEndEvent,
    MessageStartEvent,
    ModelCapabilities,
    ProviderConfig,
    ReasoningConfig,
    ReasoningDeltaEvent,
    RequestContext,
    StreamEvent,
    TextDeltaEvent,
    TextPart,
    TimeoutConfig,
    TokenUsage,
    ToolCall,
    ToolCallDeltaEvent,
    ToolCallEndEvent,
    ToolCallStartEvent,
    ToolChoiceFunction,
    ToolDefinition,
    ToolResult,
    TraceMetadata,
    UsageEvent,
)
from .stream import ChatStream
from .version import __version__

# 兼容别名
ProviderClient = ProviderSDK

__all__ = [
    "AuthenticationError",
    "BaseProviderAdapter",
    "ChatRequest",
    "ChatResponse",
    "ChatStream",
    "ContentPart",
    "ContextLengthExceededError",
    "DeepSeekAdapter",
    "GLMAdapter",
    "ImagePart",
    "InvalidRequestError",
    "KimiAdapter",
    "LoggingConfig",
    "Message",
    "MessageEndEvent",
    "MessageStartEvent",
    "ModelCapabilities",
    "ObservabilityConfig",
    "OpenAICompatibleAdapter",
    "ProviderClient",
    "ProviderConfig",
    "ProviderConnectionError",
    "ProviderError",
    "ProviderNotFoundError",
    "ProviderRegistry",
    "ProviderSDK",
    "ProviderServerError",
    "ProviderTimeoutError",
    "QwenAdapter",
    "RateLimitError",
    "ReasoningConfig",
    "ReasoningDeltaEvent",
    "RequestContext",
    "SDKConfig",
    "StreamClosedError",
    "StreamEvent",
    "TextDeltaEvent",
    "TextPart",
    "TimeoutConfig",
    "TokenUsage",
    "ToolCall",
    "ToolCallDeltaEvent",
    "ToolCallEndEvent",
    "ToolCallStartEvent",
    "ToolChoiceFunction",
    "ToolDefinition",
    "ToolResult",
    "TraceMetadata",
    "UnsupportedCapabilityError",
    "UsageEvent",
    "__version__",
    "errors",
]
