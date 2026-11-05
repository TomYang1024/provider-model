"""流式事件协议。

单个流的事件顺序（``*`` = 零个或多个）::

    message_start
    ( reasoning_delta | text_delta
      | tool_call_start tool_call_delta* tool_call_end )*
    usage
    message_end

协议保证：

* ``message_start`` 总是第一个事件，且只在 Provider 接受请求（收到 HTTP 响应头）后发出。
  在此之前的错误由 ``open_stream()`` / 第一次 ``__anext__`` 直接抛出，不会产生任何事件，
  因此调用方可以安全重试。
* 每个 ``tool_call_start`` 都恰好对应一个 ``index`` 相同的 ``tool_call_end``；
  不同工具调用的 delta 可能交错出现。
* ``message_end`` 之前恰好有一个 ``usage`` 事件；Provider 未返回用量时
  ``usage.source`` 为 ``unknown``。
* ``message_end`` 是*唯一*的成功终态，并且总是最后一个事件。
* 失败时迭代器抛出 :class:`~provider_sdk.ProviderError`，不会发出 ``message_end``。
  提前关闭（break + ``aclose()``）、被取消或超时的流同样不会发出 ``message_end``。
* ``seq`` 从 0 开始，每个事件递增 1。
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from .context import TokenUsage, TraceMetadata
from .message import ToolCall
from .response import ChatResponse, FinishReason


class _Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq: int = 0


class MessageStartEvent(_Event):
    type: Literal["message_start"] = "message_start"
    request_id: str
    provider: str
    model: str
    trace: TraceMetadata = Field(default_factory=TraceMetadata)


class TextDeltaEvent(_Event):
    type: Literal["text_delta"] = "text_delta"
    text: str


class ReasoningDeltaEvent(_Event):
    type: Literal["reasoning_delta"] = "reasoning_delta"
    text: str


class ToolCallStartEvent(_Event):
    type: Literal["tool_call_start"] = "tool_call_start"
    index: int
    id: str
    name: str


class ToolCallDeltaEvent(_Event):
    type: Literal["tool_call_delta"] = "tool_call_delta"
    index: int
    arguments_delta: str


class ToolCallEndEvent(_Event):
    type: Literal["tool_call_end"] = "tool_call_end"
    index: int
    tool_call: ToolCall


class UsageEvent(_Event):
    type: Literal["usage"] = "usage"
    usage: TokenUsage


class MessageEndEvent(_Event):
    type: Literal["message_end"] = "message_end"
    finish_reason: FinishReason
    #: 聚合后的完整响应（与 ``messages()`` 返回结构相同）。
    response: ChatResponse


# 按 type 字段区分的可判别联合类型，便于 JSON 反序列化和模式匹配
StreamEvent = Annotated[
    MessageStartEvent
    | TextDeltaEvent
    | ReasoningDeltaEvent
    | ToolCallStartEvent
    | ToolCallDeltaEvent
    | ToolCallEndEvent
    | UsageEvent
    | MessageEndEvent,
    Field(discriminator="type"),
]

#: 内容事件：其中第一个到达的事件用于计算首 Token 时间（TTFT）。
CONTENT_EVENT_TYPES = frozenset({"text_delta", "reasoning_delta", "tool_call_start"})
