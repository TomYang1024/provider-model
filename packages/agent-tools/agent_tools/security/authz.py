"""授权：决定某个调用方能否看到、能否调用某个工具。

列出工具与执行工具使用同一个 :class:`Authorizer`。列表过滤只是为了减少模型看到的工具，
真正的权限控制在执行时：即使模型调用了一个没有出现在列表里的工具，也会被拒绝。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..protocol.context import Principal
from ..protocol.spec import ToolSpec

__all__ = ["Authorizer", "AuthzDecision", "ScopeAuthorizer"]


@dataclass(frozen=True, slots=True)
class AuthzDecision:
    """授权决定。

    Attributes:
        allowed: 是否允许。
        reason: 低基数的原因代码（例如 ``public``、``missing_scopes``），用于日志和后续审计；
            不会返回给模型。
    """

    allowed: bool
    reason: str


class Authorizer(Protocol):
    """授权协议。宿主可以提供自己的实现（例如对接权限系统）。

    实现必须是同步、无 I/O 的快速判断，因为列出工具时会对每个工具调用一次。
    需要查询外部系统的复杂授权，应由宿主预先把结果放进 ``Principal.scopes``。
    """

    def authorize(self, principal: Principal, spec: ToolSpec) -> AuthzDecision:
        """判断调用方能否使用某个工具。

        Args:
            principal: 调用方身份与权限。
            spec: 工具规格（具体版本）。

        Returns:
            授权决定。
        """
        ...


class ScopeAuthorizer:
    """默认实现：``public`` 工具对所有调用方开放；``scoped`` 工具要求调用方拥有全部所需权限。"""

    def authorize(self, principal: Principal, spec: ToolSpec) -> AuthzDecision:
        """按工具的访问声明和调用方的 ``scopes`` 做判断。

        Args:
            principal: 调用方身份与权限。
            spec: 工具规格。

        Returns:
            授权决定。
        """
        policy = spec.policy
        if policy.access == "public":
            return AuthzDecision(True, "public")
        if policy.required_scopes <= principal.scopes:
            return AuthzDecision(True, "scopes_satisfied")
        return AuthzDecision(False, "missing_scopes")
