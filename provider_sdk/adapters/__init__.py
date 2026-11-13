"""Provider 适配器。

所有适配器都经由 LiteLLM 发起调用。四个内置适配器是 :class:`OpenAICompatibleAdapter`
的平级子类；:class:`LiteLLMAdapter` 用于接入 LiteLLM 支持的其他 Provider。
"""

from .base import AdapterStream, BaseProviderAdapter
from .deepseek import DeepSeekAdapter
from .glm import GLMAdapter
from .kimi import KimiAdapter
from .litellm_adapter import LiteLLMAdapter
from .openai_compatible import OpenAICompatibleAdapter, OpenAICompatibleStream
from .qwen import QwenAdapter

# 注册表默认可用的适配器类型：类型键 -> 适配器类
BUILTIN_ADAPTERS: dict[str, type[BaseProviderAdapter]] = {
    OpenAICompatibleAdapter.adapter_type: OpenAICompatibleAdapter,
    DeepSeekAdapter.adapter_type: DeepSeekAdapter,
    GLMAdapter.adapter_type: GLMAdapter,
    KimiAdapter.adapter_type: KimiAdapter,
    QwenAdapter.adapter_type: QwenAdapter,
    LiteLLMAdapter.adapter_type: LiteLLMAdapter,
}

__all__ = [
    "BUILTIN_ADAPTERS",
    "AdapterStream",
    "BaseProviderAdapter",
    "DeepSeekAdapter",
    "GLMAdapter",
    "KimiAdapter",
    "LiteLLMAdapter",
    "OpenAICompatibleAdapter",
    "OpenAICompatibleStream",
    "QwenAdapter",
]
