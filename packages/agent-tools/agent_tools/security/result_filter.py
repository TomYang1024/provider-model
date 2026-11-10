"""``ResultFilter``：宿主对**返回给模型的结果**做最后的过滤或改写。

与日志脱敏的区别：

* 日志脱敏（``log_mode`` 为 ``redacted`` / ``full`` 时）只作用于日志、Span 属性的副本，
  **不改变**返回给调用方的结果；
* ``ResultFilter`` 改变的是**结果本身**，例如去掉某些字段、屏蔽内部链接、按租户裁剪数据。

执行顺序：输出 Schema 校验 → 输出大小限制 → 清除本次调用的已知密钥值 → ``ResultFilter``
→ 再次检查输出大小限制。过滤器抛出异常时调用失败（返回 ``internal`` 错误），**不会**把
未经过滤的内容交给模型（失败即关闭）。
"""

from __future__ import annotations

from typing import Protocol

from ..protocol.context import ToolContext
from ..protocol.invocation import ToolOutput
from ..protocol.spec import ToolSpec

__all__ = ["ResultFilter"]


class ResultFilter(Protocol):
    """结果过滤器协议（同步、无 I/O 的快速处理）。"""

    def filter(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
        """过滤或改写一次成功调用的输出。失败的调用不会经过过滤器。

        Args:
            spec: 工具规格。
            output: 已经过大小限制和密钥清除的输出。
            ctx: 调用上下文。

        Returns:
            新的输出（结果之后还会再做一次大小限制检查）。
        """
        ...
