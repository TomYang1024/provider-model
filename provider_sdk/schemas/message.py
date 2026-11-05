"""消息、内容片段（ContentPart）和工具相关的数据模型。"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# 支持的四种消息角色
Role = Literal["system", "user", "assistant", "tool"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


class TextPart(_Model):
    type: Literal["text"] = "text"
    text: str


class ImagePart(_Model):
    """图片输入。``url`` 可以是 ``https://`` 地址或 ``data:`` URI（Base64）。"""

    type: Literal["image_url"] = "image_url"
    url: str
    detail: Literal["auto", "low", "high"] | None = None


# 多模态内容片段：按 type 字段区分的可判别联合类型
ContentPart = Annotated[TextPart | ImagePart, Field(discriminator="type")]


class ToolCall(_Model):
    """模型请求的一次工具调用。

    ``arguments`` 保留模型生成的原始 JSON 字符串，因为模型偶尔会输出非法 JSON；
    需要时调用 :meth:`parsed_arguments` 解析，由调用方决定如何处理解析失败。
    """

    id: str
    name: str
    arguments: str = ""
    type: Literal["function"] = "function"

    def parsed_arguments(self) -> dict[str, Any]:
        """解析参数为 dict；空参数返回 {}，非法 JSON 或非对象时抛出 ValueError。"""
        if not self.arguments.strip():
            return {}
        value = json.loads(self.arguments)
        if not isinstance(value, dict):
            raise ValueError("tool call arguments must be a JSON object")
        return value


class ToolDefinition(_Model):
    """模型可以调用的函数工具定义（参数使用 JSON Schema 描述）。"""

    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_\-]+$")
    description: str | None = None
    parameters: dict[str, Any] = Field(default_factory=lambda: {"type": "object", "properties": {}})
    strict: bool | None = None


class ToolChoiceFunction(_Model):
    type: Literal["function"] = "function"
    name: str


# 工具选择策略：自动 / 不调用 / 必须调用 / 指定某个函数
ToolChoice = Literal["auto", "none", "required"] | ToolChoiceFunction


class Message(_Model):
    """与 Provider 无关的一条对话消息。"""

    role: Role
    content: str | list[ContentPart] | None = None
    name: str | None = None
    #: assistant 消息：模型请求的工具调用。
    tool_calls: list[ToolCall] | None = None
    #: tool 消息：本条结果对应的工具调用 ID。
    tool_call_id: str | None = None
    #: assistant 消息：模型返回的推理 / 思考内容。
    reasoning_content: str | None = None

    @model_validator(mode="after")
    def _check_role_fields(self) -> Message:
        """校验字段与角色的组合是否合法，尽早发现构造错误的消息。"""
        if self.role == "tool":
            if not self.tool_call_id:
                raise ValueError("tool messages require tool_call_id")
        elif self.tool_call_id is not None:
            raise ValueError("tool_call_id is only valid on tool messages")
        if self.tool_calls and self.role != "assistant":
            raise ValueError("tool_calls are only valid on assistant messages")
        if self.reasoning_content is not None and self.role != "assistant":
            raise ValueError("reasoning_content is only valid on assistant messages")
        if self.role in ("system", "user", "tool") and self.content is None:
            raise ValueError(f"{self.role} messages require content")
        if self.role == "assistant" and self.content is None and not self.tool_calls:
            raise ValueError("assistant messages require content or tool_calls")
        return self

    # -- 便捷构造方法 --------------------------------------------------------------
    @classmethod
    def system(cls, content: str) -> Message:
        return cls(role="system", content=content)

    @classmethod
    def user(cls, content: str | list[ContentPart]) -> Message:
        return cls(role="user", content=content)

    @classmethod
    def assistant(
        cls, content: str | None = None, tool_calls: list[ToolCall] | None = None
    ) -> Message:
        return cls(role="assistant", content=content, tool_calls=tool_calls)

    @property
    def text(self) -> str:
        """消息的拼接文本（跳过图片片段）。"""
        if self.content is None:
            return ""
        if isinstance(self.content, str):
            return self.content
        return "".join(p.text for p in self.content if isinstance(p, TextPart))

    def has_images(self) -> bool:
        return isinstance(self.content, list) and any(
            isinstance(p, ImagePart) for p in self.content
        )


class ToolResult(_Model):
    """工具执行结果，会被转换为 ``tool`` 角色的消息。

    SDK 自身从不执行工具：由调用方执行工具，并通过此模型把结果传回。
    """

    tool_call_id: str
    content: str | list[ContentPart]
    is_error: bool = False

    def to_message(self) -> Message:
        content = self.content
        # 执行失败的结果加上前缀，让模型能够识别并据此调整后续行为
        if self.is_error and isinstance(content, str):
            content = f"[tool error] {content}"
        return Message(role="tool", tool_call_id=self.tool_call_id, content=content)
