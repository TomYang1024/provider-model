"""超大输出处理钩子 ``on_oversize``（P3，可选，默认不配置）。

默认行为不变：输出超过 ``OutputLimits``（JSON、图片、合计超限）时调用失败，返回
``OutputLimitError``；文本超限只截断，不会触发钩子。

配置了 :class:`OversizeHandler` 后，执行器在输出超限时调用它，让宿主把输出改写成更小的形式
（例如摘要、分页提示、只保留前 N 条记录）。处理流程::

    输出 Schema 校验（完整数据）→ 大小限制 ──超限──► 清除已知密钥值 → on_oversize（限时）
                                                         │
                       ┌─────────────────────────────────┘
                       ▼
    输出 Schema 校验（声明了 Schema 时）→ 大小限制 → 清除已知密钥值 → ResultFilter → 大小限制

* 输入：工具规格、**已经清除本次调用已知密钥值**的完整输出、触发的 ``OutputLimitError`` 和
  调用上下文；
* 输出：新的 :class:`~agent_tools.protocol.invocation.ToolOutput`。它会**再次**经过输出 Schema
  校验、大小限制、密钥清除和 ``ResultFilter``，钩子不能借此绕过任何限制；处理后仍然超限时
  返回 ``OutputLimitError(output_too_large_after_hook)``；
* 超时（``ExecutorConfig.oversize_timeout_s``）、抛出异常或返回非 ``ToolOutput``：返回原来的
  ``OutputLimitError``（安全失败），异常类型只写本地日志；
* 调用方取消：``CancelledError`` 原样传播。

局限：钩子拿到的是已经完整读入内存的输出。HTTP 适配器在流式读取时已按
``max_response_bytes`` 中断；MCP 适配器则由 SDK 先把整个响应读入内存（可以用
``McpServerConfig.max_response_bytes`` 或 stdio 隔离启动器的行长度上限在传输层限制）。
钩子只能改写结果，**不能**减少传输阶段的内存占用。
"""

from __future__ import annotations

from typing import Protocol

from ..protocol.context import ToolContext
from ..protocol.errors import OutputLimitError
from ..protocol.invocation import ToolOutput
from ..protocol.spec import ToolSpec

__all__ = ["OversizeHandler"]


class OversizeHandler(Protocol):
    """宿主实现的超大输出处理器。"""

    async def on_oversize(
        self, spec: ToolSpec, output: ToolOutput, error: OutputLimitError, ctx: ToolContext
    ) -> ToolOutput:
        """把超限的输出改写成更小的输出。

        Args:
            spec: 工具规格。
            output: 完整的原始输出（已清除本次调用的已知密钥值，仍是不可信内容）。
            error: 触发钩子的大小限制错误（说明哪一项超限）。
            ctx: 调用上下文。

        Returns:
            改写后的输出（之后还会再次经过校验与限制）。

        Raises:
            Exception: 视为处理失败，调用返回原来的 ``OutputLimitError``。
        """
        ...
