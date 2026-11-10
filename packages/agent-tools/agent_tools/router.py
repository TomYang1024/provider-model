"""路由：在给定的注册表快照上解析调用，并选择对应类型的适配器。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from .adapters.base import ToolAdapter
from .binding import ToolBinding
from .protocol.errors import ToolInfraError
from .protocol.invocation import ToolInvocation
from .registry import RegistrySnapshot

__all__ = ["Route", "ToolRouter"]


@dataclass(frozen=True, slots=True)
class Route:
    """一次调用的路由结果。

    Attributes:
        binding: 解析到的工具绑定（具体版本）。
        alias: 模型侧别名。
        adapter: 负责执行的适配器。
    """

    binding: ToolBinding
    alias: str
    adapter: ToolAdapter


class ToolRouter:
    """无状态路由器：P0 只按 ``ToolSpec.kind`` 选择适配器。

    多实例选择、按租户路由等能力在后续阶段扩展，不改变 :meth:`route` 的契约。
    """

    def __init__(self, adapters: Mapping[str, ToolAdapter]) -> None:
        """创建路由器。

        Args:
            adapters: ``kind`` → 适配器。
        """
        self._adapters = MappingProxyType(dict(adapters))

    @property
    def adapters(self) -> Mapping[str, ToolAdapter]:
        """已配置的适配器（只读）。"""
        return self._adapters

    def route(self, snapshot: RegistrySnapshot, invocation: ToolInvocation) -> Route:
        """解析调用对应的工具和适配器。

        Args:
            snapshot: 本次（批）执行使用的注册表快照。
            invocation: 调用请求。

        Returns:
            路由结果。

        Raises:
            ToolNotFoundError: 工具不存在。
            ToolDisabledError: 工具已停用。
            ToolVersionNotFoundError: 指定的版本不存在。
            ToolInfraError: 没有为该工具类型配置适配器（属于宿主配置错误）。
        """
        binding = snapshot.resolve(invocation.tool, invocation.version)
        adapter = self._adapters.get(binding.spec.kind)
        if adapter is None:
            raise ToolInfraError(
                f"no adapter configured for tool kind {binding.spec.kind!r}",
                code="adapter_unavailable",
            )
        return Route(binding=binding, alias=snapshot.alias_of(binding.spec.name), adapter=adapter)
