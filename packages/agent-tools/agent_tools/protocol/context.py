"""工具调用的执行上下文：调用方身份、权限、请求 ID 和截止时间。"""

from __future__ import annotations

import re
import time
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = ["Principal", "ToolContext", "new_request_id"]

# 与 provider_sdk.RequestContext.request_id 的规则相同，便于两边日志直接关联
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


def new_request_id() -> str:
    """生成一个新的请求 ID。

    Returns:
        形如 ``req_<32 位十六进制>`` 的字符串。
    """
    return f"req_{uuid.uuid4().hex}"


class Principal(BaseModel):
    """调用方身份与权限。由宿主在完成身份认证后构造，基础设施不负责认证。"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    user_id: str | None = Field(default=None, max_length=256)
    tenant_id: str | None = Field(default=None, max_length=256)
    scopes: frozenset[str] = frozenset()


class ToolContext(BaseModel):
    """一次（批）工具调用的上下文，不可变。

    ``deadline`` 是 ``time.monotonic()`` 的绝对时刻，只在同一个进程内有意义；
    需要跨进程传递时请换算成剩余秒数。Trace 上下文通过 OpenTelemetry 的 contextvars
    隐式传递，不放在字段里。
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: str = Field(default_factory=new_request_id)
    principal: Principal = Field(default_factory=Principal)
    deadline: float | None = None
    session_id: str | None = Field(default=None, max_length=256)
    #: 只写入日志和 Span 的少量标签；不作为指标标签，也不传给工具。
    attributes: dict[str, str] = Field(default_factory=dict, max_length=32)

    @field_validator("request_id")
    @classmethod
    def _valid_request_id(cls, value: str) -> str:
        """校验请求 ID 只包含安全字符，防止日志注入和超长 ID。

        Args:
            value: 待校验的请求 ID。

        Returns:
            原样返回通过校验的请求 ID。

        Raises:
            ValueError: 包含不允许的字符或长度超过 128。
        """
        if not _REQUEST_ID_RE.match(value):
            raise ValueError("request_id must match [A-Za-z0-9._:-]{1,128}")
        return value

    @classmethod
    def with_timeout(
        cls,
        timeout_s: float,
        *,
        principal: Principal | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
        now: float | None = None,
    ) -> ToolContext:
        """便捷构造：从“剩余秒数”换算出绝对截止时间。

        Args:
            timeout_s: 从现在起允许的总时长（秒）。
            principal: 调用方身份；不传时为匿名、无权限的调用方。
            request_id: 请求 ID；不传时自动生成。
            session_id: 会话 ID。
            now: 当前的 monotonic 时刻；不传时使用 ``time.monotonic()``（测试可以注入）。

        Returns:
            设置了 ``deadline`` 的上下文。
        """
        base = time.monotonic() if now is None else now
        return cls(
            request_id=request_id or new_request_id(),
            principal=principal or Principal(),
            deadline=base + timeout_s,
            session_id=session_id,
        )

    def remaining(self, now: float) -> float | None:
        """计算距离截止时间还剩多少秒。

        Args:
            now: 当前的 monotonic 时刻。

        Returns:
            剩余秒数（可能为负数）；没有设置截止时间时返回 ``None``。
        """
        if self.deadline is None:
            return None
        return self.deadline - now
