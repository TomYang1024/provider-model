"""工具调用请求、输出内容块和执行结果的数据契约（全部可序列化）。"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

__all__ = [
    "ContentBlock",
    "ImageBlock",
    "JsonBlock",
    "TextBlock",
    "ToolErrorInfo",
    "ToolExecutionResult",
    "ToolInvocation",
    "ToolOutput",
]


class _Frozen(BaseModel):
    """契约模型的公共配置：禁止未知字段、不可变。"""

    model_config = ConfigDict(extra="forbid", frozen=True)


class TextBlock(_Frozen):
    """文本内容块；超过大小限制时会在 UTF-8 字符边界处截断，并把 ``truncated`` 置为 True。"""

    type: Literal["text"] = "text"
    text: str
    truncated: bool = False


class JsonBlock(_Frozen):
    """JSON 内容块；超过大小限制时**不会**截断，而是整体报错（截断会破坏结构）。"""

    type: Literal["json"] = "json"
    data: JsonValue


class ImageBlock(_Frozen):
    """图片内容块：``data_b64``（Base64 数据）与 ``url`` 二选一。"""

    type: Literal["image"] = "image"
    mime_type: str = Field(pattern=r"^image/(png|jpeg|gif|webp)$")
    data_b64: str | None = None
    url: str | None = Field(default=None, max_length=2048)

    @model_validator(mode="after")
    def _exactly_one_source(self) -> ImageBlock:
        """校验 ``data_b64`` 与 ``url`` 恰好提供一个。

        Raises:
            ValueError: 两者都提供或都未提供。
        """
        if (self.data_b64 is None) == (self.url is None):
            raise ValueError("exactly one of data_b64 or url must be set")
        return self


#: 按 ``type`` 字段区分的内容块联合类型。
ContentBlock = Annotated[TextBlock | JsonBlock | ImageBlock, Field(discriminator="type")]


class ToolOutput(_Frozen):
    """适配器返回的归一化输出。"""

    content: list[ContentBlock] = Field(default_factory=list)
    #: 结构化结果（工具返回 dict 或 Pydantic 模型时填充），便于宿主直接使用。
    structured: dict[str, JsonValue] | None = None


class ToolErrorInfo(_Frozen):
    """可以安全展示给模型或客户端的错误信息。"""

    type: str
    code: str
    message: str
    retryable: bool = False


class ToolInvocation(_Frozen):
    """一次工具调用请求，一般由模型返回的 ``ToolCall`` 转换而来。"""

    #: 调用 ID（通常是模型返回的 ``ToolCall.id``）；不保证跨轮次唯一。
    call_id: str = Field(min_length=1, max_length=256)
    #: 模型侧别名或内部名。
    tool: str = Field(min_length=1, max_length=256)
    #: 只有宿主可以指定；``None`` 表示使用当前启用的版本。
    version: str | None = Field(default=None, max_length=64)
    #: 参数；字符串时由执行器按 JSON 解析（空字符串视为 ``{}``）。
    arguments: dict[str, Any] | str = Field(default_factory=dict)


class ToolExecutionResult(_Frozen):
    """一次工具调用的结果。工具层面的失败也以结果的形式返回（``status="error"``）。"""

    call_id: str
    #: 实际执行的内部名；解析失败时为调用方传入的名字。
    tool: str
    #: 模型侧别名；解析失败时为调用方传入的名字。
    alias: str
    version: str | None = None
    status: Literal["success", "error"]
    content: list[ContentBlock] = Field(default_factory=list)
    structured: dict[str, JsonValue] | None = None
    error: ToolErrorInfo | None = None
    #: 工具输出一律视为不可信内容，宿主据此决定如何交给模型。
    trust: Literal["untrusted"] = "untrusted"
    attempts: int = Field(default=0, ge=0)
    duration_ms: float = Field(default=0.0, ge=0)
    trace_id: str | None = None
    span_id: str | None = None
    #: 结果不是本次执行产生的，而是复用的（P3）：``cache`` 表示来自结果缓存（工具没有再次执行），
    #: ``dedupe`` 表示同一逻辑调用的重复提交、返回了第一次执行的结果。``None`` 表示本次实际执行。
    reused: Literal["cache", "dedupe"] | None = None

    @property
    def ok(self) -> bool:
        """调用是否成功。"""
        return self.status == "success"

    def to_model_text(self) -> str:
        """生成回传给模型的文本。

        成功时拼接所有文本块和 JSON 块（JSON 紧凑序列化），图片用占位符表示；
        失败时只包含错误码和安全摘要，不包含任何内部细节。

        Returns:
            可以直接作为 ``tool`` 消息内容的字符串。
        """
        if self.error is not None:
            return f"[{self.error.code}] {self.error.message}"
        parts: list[str] = []
        for block in self.content:
            if isinstance(block, TextBlock):
                parts.append(block.text)
            elif isinstance(block, JsonBlock):
                parts.append(json.dumps(block.data, ensure_ascii=False, separators=(",", ":")))
            else:
                parts.append("[image]")
        return "\n".join(parts)
