"""月之暗面 Kimi 适配器（https://platform.kimi.com/docs/api/chat）。

2026-11-05 对照官方文档核实：

* OpenAI 兼容地址 ``https://api.moonshot.cn/v1``。
* K3 系列（``kimi-k3.1-pro``、``kimi-k3``）始终思考，深度通过 ``reasoning_effort``
  （``low``/``high``/``max``）控制，没有 ``thinking`` 参数。
  ``kimi-k2.6`` 通过 ``thinking: {"type": ...}`` 切换；``kimi-k2.7-code`` 必须开启思考。
* 多轮对话中必须原样回传 assistant 的 ``reasoning_content``，因此默认
  ``send_reasoning_content=True``。
* ``tool_choice`` 支持 ``auto`` / ``none`` / ``required`` / 指定函数。
* 流式 Usage 需要 ``stream_options.include_usage``；最后一个 chunk 的 ``choices``
  为空并在顶层携带 ``usage``（旧版模型放在 ``choices[0].usage`` 中，仍兼容解析）。
* 错误体：``error.message``，以及可选的 ``type`` / ``code``。
"""

from __future__ import annotations

from typing import Any, ClassVar

from .. import exceptions as errors
from ..schemas.provider import ModelCapabilities
from ..schemas.request import ChatRequest
from .openai_compatible import (
    CONTENT_FILTERED,
    QUOTA,
    ErrorRule,
    OpenAICompatibleAdapter,
    _ErrorInfo,
)

# Kimi 通过 error.type 区分错误类别
_RULES: dict[str, ErrorRule] = {
    "exceeded_current_quota_error": QUOTA,  # 额度用尽：不可重试
    "content_filter": CONTENT_FILTERED,  # 内容安全拦截
    "engine_overloaded_error": ErrorRule(errors.ProviderServerError),  # 引擎过载：可重试
    "rate_limit_reached_error": ErrorRule(errors.RateLimitError),  # 限流：可重试
    "invalid_authentication_error": ErrorRule(errors.AuthenticationError),
}


class KimiAdapter(OpenAICompatibleAdapter):
    adapter_type: ClassVar[str] = "kimi"
    supports_reasoning_toggle: ClassVar[bool] = True
    supports_reasoning_effort: ClassVar[bool] = True

    def apply_reasoning(
        self, request: ChatRequest, caps: ModelCapabilities, body: dict[str, Any]
    ) -> None:
        # K3 系列为 reasoning_always_on：只发送 reasoning_effort，不发送 thinking
        self.apply_thinking_object(request, caps, body)

    def classify_error(self, info: _ErrorInfo) -> ErrorRule | None:
        return _RULES.get(info.type or "")
