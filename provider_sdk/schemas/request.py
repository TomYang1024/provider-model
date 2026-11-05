"""归一化后的请求模型。"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .context import RequestContext
from .message import Message, ToolChoice, ToolDefinition

Operation = Literal["chat", "messages", "stream"]

#: SDK 自己设置的标准参数；``provider_options`` 不允许覆盖它们，
#: 否则 ``extra_body`` 可能悄悄替换掉模型、消息或 stream 标志。
RESERVED_PARAMS = frozenset(
    {
        "model",
        "messages",
        "stream",
        "stream_options",
        "tools",
        "tool_choice",
        "temperature",
        "top_p",
        "max_tokens",
        "max_completion_tokens",
        "stop",
        "seed",
        "response_format",
        "n",
    }
)


class ReasoningConfig(BaseModel):
    """与 Provider 无关的推理（思考）配置。

    如何映射到各 Provider 由适配器决定，见 ``docs/providers.md``。
    不支持的选项会抛出 ``UnsupportedCapabilityError``，而不是被静默忽略。
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    #: 推理深度（Provider 提供 ``reasoning_effort`` 时可用：DeepSeek、Kimi K3）。
    effort: Literal["low", "high", "max"] | None = None
    #: 思考 Token 上限（Provider 提供预算参数时可用：Qwen）。
    budget_tokens: int | None = Field(default=None, gt=0)


class ResponseFormat(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["text", "json_object"] = "text"


class ChatRequest(BaseModel):
    """一次完整描述的模型调用。"""

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1, max_length=64)
    model: str = Field(min_length=1, max_length=128)
    messages: list[Message] = Field(min_length=1)
    tools: list[ToolDefinition] | None = None
    tool_choice: ToolChoice | None = None
    parallel_tool_calls: bool | None = None
    temperature: float | None = Field(default=None, ge=0, le=2)
    top_p: float | None = Field(default=None, gt=0, le=1)
    max_tokens: int | None = Field(default=None, gt=0)
    stop: list[str] | None = Field(default=None, max_length=16)
    seed: int | None = None
    response_format: ResponseFormat | None = None
    reasoning: ReasoningConfig | None = None
    #: Provider 特有参数，作为额外字段合并到 JSON 请求体中。
    provider_options: dict[str, Any] = Field(default_factory=dict)
    #: 本次调用的总截止时间（秒），覆盖 Provider 默认值。
    timeout: float | None = Field(default=None, gt=0, le=3600)
    context: RequestContext = Field(default_factory=RequestContext)
    operation: Operation = "messages"

    @field_validator("provider_options")
    @classmethod
    def _no_reserved(cls, value: dict[str, Any]) -> dict[str, Any]:
        clash = RESERVED_PARAMS.intersection(value)
        if clash:
            raise ValueError(
                f"provider_options may not override standard parameters: {sorted(clash)}"
            )
        return value

    @property
    def stream(self) -> bool:
        return self.operation == "stream"
