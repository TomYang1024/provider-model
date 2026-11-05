"""与 Provider 无关的数据模型（Pydantic v2）。"""

from .context import RequestContext, TokenUsage, TraceMetadata, UsageSource, new_request_id
from .message import (
    ContentPart,
    ImagePart,
    Message,
    Role,
    TextPart,
    ToolCall,
    ToolChoice,
    ToolChoiceFunction,
    ToolDefinition,
    ToolResult,
)
from .provider import ModelCapabilities, ProviderConfig, TimeoutConfig
from .request import ChatRequest, Operation, ReasoningConfig, ResponseFormat
from .response import ChatResponse, FinishReason
from .stream import (
    MessageEndEvent,
    MessageStartEvent,
    ReasoningDeltaEvent,
    StreamEvent,
    TextDeltaEvent,
    ToolCallDeltaEvent,
    ToolCallEndEvent,
    ToolCallStartEvent,
    UsageEvent,
)

__all__ = [
    "ChatRequest",
    "ChatResponse",
    "ContentPart",
    "FinishReason",
    "ImagePart",
    "Message",
    "MessageEndEvent",
    "MessageStartEvent",
    "ModelCapabilities",
    "Operation",
    "ProviderConfig",
    "ReasoningConfig",
    "ReasoningDeltaEvent",
    "RequestContext",
    "ResponseFormat",
    "Role",
    "StreamEvent",
    "TextDeltaEvent",
    "TextPart",
    "TimeoutConfig",
    "TokenUsage",
    "ToolCall",
    "ToolCallDeltaEvent",
    "ToolCallEndEvent",
    "ToolCallStartEvent",
    "ToolChoice",
    "ToolChoiceFunction",
    "ToolDefinition",
    "ToolResult",
    "TraceMetadata",
    "UsageEvent",
    "UsageSource",
    "new_request_id",
]
