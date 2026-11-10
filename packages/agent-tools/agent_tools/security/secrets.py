"""密钥：``SecretProvider`` 接口，以及本次调用中已知密钥值的精确清除。

工具规格里只保存 :class:`~agent_tools.protocol.spec.SecretRef`（密钥名），真正的值在调用时
由宿主实现的 :class:`SecretProvider` 提供，并登记到本次调用的 :class:`CallSecrets` 中。

精确清除能保证的范围：通过 ``SecretProvider`` 取得、登记到本次调用的密钥值，会在本次调用
产生的日志、Span 属性、审计事件，以及返回给模型的结果中，按原值被替换为
``[REDACTED_SECRET]``。它**不能**识别经过编码、拆分或变形后的密钥，也不能识别下游自己
返回的其他敏感数据；基于规则的脱敏（``observability.redaction``）同样只是尽力而为。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from ..protocol.context import ToolContext

__all__ = ["REDACTED_SECRET", "CallSecrets", "MappingSecretProvider", "SecretProvider"]

#: 替换已知密钥值时使用的标记。
REDACTED_SECRET = "[REDACTED_SECRET]"  # noqa: S105 - 替换标记，不是密钥
#: 短于这个长度的值不做精确替换：太短的值会误伤正常文本，且本身就不应作为密钥使用。
_MIN_SECRET_LEN = 6


class SecretProvider(Protocol):
    """由宿主实现的密钥提供者（例如对接 Vault、KMS、环境变量）。"""

    async def get_secret(self, name: str, ctx: ToolContext) -> str:
        """取得密钥值。可以根据 ``ctx.principal.tenant_id`` 返回租户专属的密钥。

        Args:
            name: ``SecretRef.name``。
            ctx: 调用上下文。

        Returns:
            密钥值。

        Raises:
            KeyError: 密钥不存在（适配器会转换为 ``secret_unavailable`` 错误，
                不会泄露名字以外的信息）。
        """
        ...


class MappingSecretProvider:
    """从内存字典读取密钥的简单实现，适合测试和本地开发；生产环境请对接专门的密钥系统。"""

    def __init__(self, secrets: Mapping[str, str]) -> None:
        """创建提供者。

        Args:
            secrets: 密钥名 → 值。
        """
        self._secrets = dict(secrets)

    async def get_secret(self, name: str, ctx: ToolContext) -> str:
        """按名字查找密钥（不区分租户）。

        Args:
            name: 密钥名。
            ctx: 调用上下文（未使用）。

        Returns:
            密钥值。

        Raises:
            KeyError: 密钥不存在。
        """
        return self._secrets[name]


class CallSecrets:
    """一次调用中已知的密钥值集合，用于精确清除。只在事件循环线程中使用。"""

    def __init__(self) -> None:
        """创建空集合；每次调用一个实例，跨该调用的所有尝试共享。"""
        self._values: set[str] = set()

    def add(self, value: str) -> None:
        """登记一个本次调用使用的密钥值。

        Args:
            value: 密钥值；短于 6 个字符的值不会被登记（见模块说明）。
        """
        if len(value) >= _MIN_SECRET_LEN:
            self._values.add(value)

    def __bool__(self) -> bool:
        """是否登记过任何密钥值。"""
        return bool(self._values)

    def scrub_text(self, text: str) -> str:
        """把文本中的已知密钥值替换为 ``[REDACTED_SECRET]``。

        先替换更长的值，避免一个密钥是另一个密钥的子串时只替换了一部分。

        Args:
            text: 原始文本。

        Returns:
            替换后的文本。
        """
        for value in sorted(self._values, key=len, reverse=True):
            if value in text:
                text = text.replace(value, REDACTED_SECRET)
        return text

    def scrub_json(self, value: Any) -> Any:
        """递归替换 JSON 数据中字符串（包括对象的键）里出现的已知密钥值。

        Args:
            value: JSON 数据。

        Returns:
            替换后的新数据（原数据不变）。
        """
        if not self._values:
            return value
        if isinstance(value, str):
            return self.scrub_text(value)
        if isinstance(value, list):
            return [self.scrub_json(v) for v in value]
        if isinstance(value, dict):
            return {self.scrub_text(str(k)): self.scrub_json(v) for k, v in value.items()}
        return value
