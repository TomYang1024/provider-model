"""智谱 GLM 适配器（https://docs.bigmodel.cn）。

2026-11-05 对照官方文档核实：

* OpenAI 兼容地址 ``https://open.bigmodel.cn/api/paas/v4/``。
* 思考模式（GLM-4.5 及以上）通过 ``thinking: {"type": "enabled" | "disabled"}`` 切换
  （默认开启）；GLM-5.3 始终思考。思考内容在 ``reasoning_content`` 中返回。
  通过 ``provider_options`` 设置 ``thinking.clear_thinking=false`` 可保留历史推理，
  此时必须回传 reasoning_content（需设置 ``send_reasoning_content=True``）。
* ``tool_choice`` 只接受 ``auto``。
* 流式 Usage 在 chunk 顶层返回，无需 ``stream_options``。
* 业务错误码在 ``error.code`` 中返回（如 1113 欠费、1261 Prompt 超长、
  1301 内容安全、1302/1303 限流）。
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

# GLM 业务错误码 -> SDK 异常规则
_AUTH = ErrorRule(errors.AuthenticationError)
_RULES: dict[str, ErrorRule] = {
    # 1000-1004：认证相关（API Key 无效、过期等）
    **{code: _AUTH for code in ("1000", "1001", "1002", "1003", "1004")},
    "1113": QUOTA,  # 余额不足 / 欠费：不可重试
    "1261": ErrorRule(errors.ContextLengthExceededError),  # Prompt 超长
    "1301": CONTENT_FILTERED,  # 内容安全拦截
    "1302": ErrorRule(errors.RateLimitError),  # 并发过高
    "1303": ErrorRule(errors.RateLimitError),  # 频率过高
    "1305": ErrorRule(errors.ProviderServerError),  # 服务过载
}


class GLMAdapter(OpenAICompatibleAdapter):
    adapter_type: ClassVar[str] = "glm"
    supports_reasoning_toggle: ClassVar[bool] = True

    def apply_reasoning(
        self, request: ChatRequest, caps: ModelCapabilities, body: dict[str, Any]
    ) -> None:
        # GLM 使用 thinking 对象格式；不支持 effort（基类校验会拒绝）
        self.apply_thinking_object(request, caps, body)

    def classify_error(self, info: _ErrorInfo) -> ErrorRule | None:
        # GLM 的业务错误码是字符串形式的数字，未知错误码交给通用规则
        return _RULES.get(info.code or "")
