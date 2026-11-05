"""阿里云百炼（DashScope）Qwen 适配器。

文档：https://help.aliyun.com/zh/model-studio/ （OpenAI 兼容模式），2026-11-05 核实：

* 地址 ``https://dashscope.aliyuncs.com/compatible-mode/v1``（旧域名，仍可用）。
  阿里云现推荐工作空间域名
  ``https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1``，
  通过 ``QWEN_BASE_URL`` 设置。API Key 与地域绑定（地域不匹配返回 401）。
* 混合思考通过请求体 ``enable_thinking`` 切换，思考预算通过 ``thinking_budget`` 设置，
  思考内容在 ``reasoning_content`` 中返回。商业版模型支持非流式思考；
  部分开源模型仅支持流式（Provider 返回 400，映射为 ``InvalidRequestError``）。
* 流式 Usage 通过 ``stream_options.include_usage`` 获取（在 ``choices`` 为空的最后一个 chunk 中）。
* 错误码在 ``error.code`` 中，如 ``InvalidApiKey``、``Throttling``、``Arrearage``、
  ``data_inspection_failed``。
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


class QwenAdapter(OpenAICompatibleAdapter):
    adapter_type: ClassVar[str] = "qwen"
    supports_reasoning_toggle: ClassVar[bool] = True
    supports_reasoning_budget: ClassVar[bool] = True

    def apply_reasoning(
        self, request: ChatRequest, caps: ModelCapabilities, body: dict[str, Any]
    ) -> None:
        # 未指定推理配置时不发送，沿用模型默认值（不同 Qwen 模型默认值不同）
        if request.reasoning is None or not caps.reasoning:
            return
        body["enable_thinking"] = request.reasoning.enabled
        if request.reasoning.enabled and request.reasoning.budget_tokens:
            body["thinking_budget"] = request.reasoning.budget_tokens

    def classify_error(self, info: _ErrorInfo) -> ErrorRule | None:
        # Qwen 错误码大小写不统一（InvalidApiKey / invalid_api_key），统一转小写比较
        code = (info.code or "").lower()
        if code in {"invalidapikey", "accessdenied"}:
            return ErrorRule(errors.AuthenticationError)
        if code.startswith("throttling"):
            return ErrorRule(errors.RateLimitError)
        if code == "arrearage":  # 账户欠费
            return QUOTA
        if code == "data_inspection_failed":  # 内容安全拦截
            return CONTENT_FILTERED
        return None
