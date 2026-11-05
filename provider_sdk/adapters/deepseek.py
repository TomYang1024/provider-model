"""DeepSeek 适配器（https://api-docs.deepseek.com）。

2026-11-05 对照官方文档核实：

* OpenAI 兼容的 Chat Completions，地址 ``https://api.deepseek.com``。
* 模型 ``deepseek-flash`` / ``deepseek-v4-pro``（1M 上下文，支持工具调用和 JSON 输出）。
  混合思考，**默认开启**，通过 ``thinking: {"type": "enabled"|"disabled"}`` 切换；
  推理深度通过 ``reasoning_effort``（``low``/``high``/``max``）控制。
* 思考内容通过 ``reasoning_content`` 返回。请求带 ``tools`` 时，历史 assistant 消息中的
  ``reasoning_content`` **必须**回传（否则返回 HTTP 400），
  因此默认 ``send_reasoning_content=True``。
* 缓存统计：``prompt_cache_hit_tokens`` -> ``cached_input_tokens``。
* 错误：402 余额不足、422 参数错误、503 服务繁忙。
"""

from __future__ import annotations

from typing import Any, ClassVar

from ..schemas.provider import ModelCapabilities
from ..schemas.request import ChatRequest
from .openai_compatible import OpenAICompatibleAdapter


class DeepSeekAdapter(OpenAICompatibleAdapter):
    adapter_type: ClassVar[str] = "deepseek"
    supports_reasoning_toggle: ClassVar[bool] = True
    supports_reasoning_effort: ClassVar[bool] = True

    def apply_reasoning(
        self, request: ChatRequest, caps: ModelCapabilities, body: dict[str, Any]
    ) -> None:
        # DeepSeek 使用 thinking 对象 + reasoning_effort 格式
        self.apply_thinking_object(request, caps, body)
