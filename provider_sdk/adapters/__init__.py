"""Provider 适配器。四个内置适配器都是 :class:`OpenAICompatibleAdapter` 的平级子类。"""

from .base import AdapterStream, BaseProviderAdapter
from .deepseek import DeepSeekAdapter
from .glm import GLMAdapter
from .kimi import KimiAdapter
from .openai_compatible import OpenAICompatibleAdapter, OpenAICompatibleStream
from .qwen import QwenAdapter

# 注册表默认可用的适配器类型：类型键 -> 适配器类
BUILTIN_ADAPTERS: dict[str, type[BaseProviderAdapter]] = {
    OpenAICompatibleAdapter.adapter_type: OpenAICompatibleAdapter,
    DeepSeekAdapter.adapter_type: DeepSeekAdapter,
    GLMAdapter.adapter_type: GLMAdapter,
    KimiAdapter.adapter_type: KimiAdapter,
    QwenAdapter.adapter_type: QwenAdapter,
}

__all__ = [
    "BUILTIN_ADAPTERS",
    "AdapterStream",
    "BaseProviderAdapter",
    "DeepSeekAdapter",
    "GLMAdapter",
    "KimiAdapter",
    "OpenAICompatibleAdapter",
    "OpenAICompatibleStream",
    "QwenAdapter",
]
