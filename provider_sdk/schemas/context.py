"""请求上下文、Trace 元数据和 Token 用量模型。"""

from __future__ import annotations

import re
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# request_id 只允许安全字符，防止日志注入和超长 ID
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


def new_request_id() -> str:
    return f"req_{uuid.uuid4().hex}"


class RequestContext(BaseModel):
    """调用方提供的请求级上下文。

    ``request_id`` 用于关联日志、Span 和响应。``user_id`` / ``tenant_id``
    只记录在日志中（绝不作为指标标签），也绝不会发送给 Provider。
    """

    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(default_factory=new_request_id)
    user_id: str | None = Field(default=None, max_length=256)
    tenant_id: str | None = Field(default=None, max_length=256)
    session_id: str | None = Field(default=None, max_length=256)
    #: 写入日志的少量自由标签（数量有上限）。
    tags: dict[str, str] = Field(default_factory=dict, max_length=32)

    @field_validator("request_id")
    @classmethod
    def _valid_request_id(cls, value: str) -> str:
        if not _REQUEST_ID_RE.match(value):
            raise ValueError("request_id must match [A-Za-z0-9._:-]{1,128}")
        return value


class TraceMetadata(BaseModel):
    """SDK 请求 Span 的 W3C 标识（十六进制编码）。"""

    model_config = ConfigDict(extra="forbid")

    trace_id: str | None = None
    span_id: str | None = None
    sampled: bool = False


UsageSource = Literal["provider", "estimated", "unknown"]


class TokenUsage(BaseModel):
    """Token 用量。

    ``source`` 表示数字的可信程度：

    * ``provider``  - Provider API 返回（权威数据）。
    * ``estimated`` - 本地启发式估算（不可用于计费）。
    * ``unknown``   - Provider 未返回用量，各 Token 字段为 None（绝不伪造）。
    """

    model_config = ConfigDict(extra="forbid")

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    reasoning_tokens: int | None = None
    cached_input_tokens: int | None = None
    source: UsageSource = "unknown"

    @classmethod
    def unknown(cls) -> TokenUsage:
        return cls(source="unknown")
