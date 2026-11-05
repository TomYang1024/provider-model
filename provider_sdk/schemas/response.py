"""归一化后的响应模型。"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .context import TokenUsage, TraceMetadata
from .message import Message, ToolCall

# 统一的结束原因；Provider 的非标准取值归为 "unknown"
FinishReason = Literal["stop", "length", "tool_calls", "content_filter", "error", "unknown"]


class ChatResponse(BaseModel):
    """``chat()`` / ``messages()``（以及 ``message_end`` 事件）的统一返回结构，与 Provider 无关。"""

    model_config = ConfigDict(extra="forbid")

    id: str | None = Field(default=None, description="Provider 返回的响应 ID（如果有）。")
    request_id: str
    provider: str
    #: 调用方请求的模型 ID。
    model: str
    #: Provider 返回的模型 ID（可能不同，例如带日期的快照版本）。
    response_model: str | None = None
    message: Message
    finish_reason: FinishReason = "unknown"
    #: Provider 原始的 finish_reason，保留用于排查非标准取值。
    raw_finish_reason: str | None = None
    usage: TokenUsage = Field(default_factory=TokenUsage.unknown)
    latency_ms: float | None = None
    trace: TraceMetadata = Field(default_factory=TraceMetadata)
    #: 白名单方式保留的、可 JSON 序列化的 Provider 扩展字段（如 ``system_fingerprint``）。
    provider_extensions: dict[str, Any] = Field(default_factory=dict)

    @property
    def text(self) -> str:
        return self.message.text

    @property
    def reasoning(self) -> str | None:
        return self.message.reasoning_content

    @property
    def tool_calls(self) -> list[ToolCall]:
        return list(self.message.tool_calls or [])
