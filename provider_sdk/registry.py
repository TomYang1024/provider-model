"""Provider 注册表。

维护 Provider 名称到配置和适配器实例的映射。新增 Provider 只需注册一个
:class:`ProviderConfig`（如果协议不兼容 OpenAI，再注册一个适配器类型），
Client 本身的代码无需任何修改。
"""

from __future__ import annotations

import asyncio
import contextlib

import httpx2
from opentelemetry import trace

from . import exceptions as errors
from .adapters import BUILTIN_ADAPTERS, BaseProviderAdapter
from .schemas.provider import ProviderConfig


class ProviderRegistry:
    """Provider 注册表：配置在注册时保存，适配器在首次使用时才创建（懒加载）。"""

    def __init__(self, *, tracer: trace.Tracer | None = None) -> None:
        # 适配器类型键 -> 适配器类（内置类型 + 自定义注册的类型）
        self._adapter_types: dict[str, type[BaseProviderAdapter]] = dict(BUILTIN_ADAPTERS)
        # Provider 名称 -> 配置
        self._configs: dict[str, ProviderConfig] = {}
        # Provider 名称 -> 已创建的适配器实例（持有连接池）
        self._adapters: dict[str, BaseProviderAdapter] = {}
        # Provider 名称 -> 注入的自定义传输层（测试 / 自定义 TLS），None 表示默认
        self._transports: dict[str, httpx2.AsyncBaseTransport | None] = {}
        self._tracer = tracer or trace.get_tracer("provider_sdk")

    def bind_tracer(self, tracer: trace.Tracer) -> None:
        self._tracer = tracer

    # -- 适配器类型 ---------------------------------------------------------------------
    def register_adapter_type(
        self, key: str, adapter_cls: type[BaseProviderAdapter], *, replace: bool = False
    ) -> None:
        if key in self._adapter_types and not replace:
            raise ValueError(f"adapter type {key!r} already registered")
        self._adapter_types[key] = adapter_cls

    # -- Provider 管理 ------------------------------------------------------------------
    def register(
        self,
        config: ProviderConfig,
        *,
        transport: httpx2.AsyncBaseTransport | None = None,
        replace: bool = False,
    ) -> None:
        """注册一个 Provider。``transport`` 用于覆盖 HTTP 传输层（测试场景）。"""
        if config.name in self._configs and not replace:
            raise ValueError(f"provider {config.name!r} already registered")
        if config.adapter not in self._adapter_types:
            raise ValueError(
                f"provider {config.name!r} uses unknown adapter type {config.adapter!r}"
            )
        old = self._adapters.pop(config.name, None)
        if old is not None:
            # 替换已有 Provider 时，在后台关闭旧适配器的连接池；新适配器下次使用时懒创建
            with contextlib.suppress(RuntimeError):  # 没有运行中的事件循环：交给 GC 回收
                asyncio.get_running_loop().create_task(old.aclose())
        self._configs[config.name] = config
        self._transports[config.name] = transport

    def enable(self, name: str, enabled: bool = True) -> None:
        # 配置对象不可原地修改，复制一份新配置；已创建的适配器保留，禁用期间不会被使用
        cfg = self.config(name)
        self._configs[name] = cfg.model_copy(update={"enabled": enabled})

    def disable(self, name: str) -> None:
        self.enable(name, False)

    def config(self, name: str) -> ProviderConfig:
        try:
            return self._configs[name]
        except KeyError:
            raise errors.ProviderNotFoundError(
                f"unknown provider {name!r}", provider=name
            ) from None

    def names(self, *, enabled_only: bool = True) -> list[str]:
        return [n for n, c in self._configs.items() if c.enabled or not enabled_only]

    def get(self, name: str) -> BaseProviderAdapter:
        """返回已启用 Provider 的适配器（首次调用时创建）。"""
        cfg = self.config(name)
        if not cfg.enabled:
            raise errors.ProviderNotFoundError(
                f"provider {name!r} is disabled", provider=name, error_code="provider_disabled"
            )
        adapter = self._adapters.get(name)
        if adapter is None:
            adapter_cls = self._adapter_types[cfg.adapter]
            # 适配器构造失败（如缺少 API Key）转换为请求级错误，而不是让进程崩溃
            try:
                adapter = adapter_cls(cfg, tracer=self._tracer, transport=self._transports[name])
            except ValueError as exc:
                raise errors.InvalidRequestError(
                    str(exc), provider=name, error_code="provider_misconfigured"
                ) from exc
            self._adapters[name] = adapter
        return adapter

    def known_model(self, provider: str, model: str) -> bool:
        """模型是否在配置中登记（决定指标中使用模型 ID 还是 _other）。"""
        cfg = self._configs.get(provider)
        return cfg is not None and cfg.is_known_model(model)

    async def aclose(self) -> None:
        """并发关闭所有适配器的连接池。"""
        adapters = list(self._adapters.values())
        self._adapters.clear()
        # 单个适配器关闭失败不影响其他适配器；但取消等 BaseException 需要继续抛出
        results = await asyncio.gather(*(a.aclose() for a in adapters), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, Exception):
                raise result
