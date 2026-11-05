"""HTTP 请求体模型。响应直接复用 SDK 模型（``ChatResponse``）。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from provider_sdk.schemas import (
    Message,
    ReasoningConfig,
    ResponseFormat,
    ToolChoice,
    ToolDefinition,
)


class ClientContext(BaseModel):
    """调用方元数据：只写入日志（绝不用于指标标签，也不发送给 Provider）。"""

    model_config = ConfigDict(extra="forbid")

    user_id: str | None = Field(default=None, max_length=256)
    tenant_id: str | None = Field(default=None, max_length=256)
    session_id: str | None = Field(default=None, max_length=256)
    tags: dict[str, str] = Field(default_factory=dict, max_length=32)


class _Common(BaseModel):
    """各接口共用的请求字段；extra="forbid" 拒绝未知字段，避免拼写错误被静默忽略。"""

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1, max_length=64)
    model: str = Field(min_length=1, max_length=128)
    temperature: float | None = Field(default=None, ge=0, le=2)
    top_p: float | None = Field(default=None, gt=0, le=1)
    max_tokens: int | None = Field(default=None, gt=0)
    stop: list[str] | None = Field(default=None, max_length=16)
    seed: int | None = None
    response_format: ResponseFormat | None = None
    reasoning: ReasoningConfig | None = None
    provider_options: dict[str, Any] = Field(default_factory=dict)
    timeout: float | None = Field(default=None, gt=0)
    context: ClientContext = Field(default_factory=ClientContext)

    def sdk_options(self) -> dict[str, Any]:
        """转换为 SDK 的关键字参数。

        排除由路由显式传递的字段，避免重复传参；嵌套模型保持对象形式传给 SDK。
        """
        return self.model_dump(
            exclude={
                "provider",
                "model",
                "context",
                "prompt",
                "system",
                "messages",
                "tools",
                "tool_choice",
                "parallel_tool_calls",
            },
            exclude_none=True,
        ) | {"response_format": self.response_format, "reasoning": self.reasoning}


class ChatHTTPRequest(_Common):
    prompt: str = Field(min_length=1)
    system: str | None = None


class MessagesHTTPRequest(_Common):
    # 限制消息数和工具数，防止超大请求消耗网关资源
    messages: list[Message] = Field(min_length=1, max_length=2000)
    tools: list[ToolDefinition] | None = Field(default=None, max_length=128)
    tool_choice: ToolChoice | None = None
    parallel_tool_calls: bool | None = None
