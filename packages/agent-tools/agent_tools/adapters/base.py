"""适配器协议。适配器只做协议转换与错误映射，不负责重试、超时、鉴权、限流或日志。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..binding import ToolBinding
from ..protocol.context import ToolContext
from ..protocol.invocation import ToolOutput
from ..runtime.lease import ExecutionLease
from ..security.secrets import CallSecrets

__all__ = ["AttemptScope", "ToolAdapter"]


@dataclass(slots=True)
class AttemptScope:
    """单次尝试的执行范围，由执行器传给适配器。

    Attributes:
        attempt: 尝试序号，从 1 开始；重试时递增。
        deadline: 本次尝试的截止时刻（monotonic），执行器已经用 ``asyncio.timeout`` 强制执行。
        lease: 本次调用的执行租约；后台工作在超时后仍会继续时，适配器用它推迟额度的归还。
        started: 工具是否已经真正开始执行。适配器在拿到执行资源（例如线程槽位）、即将
            运行工具代码时调用 :meth:`mark_started`。执行器据此区分“工具超时”
            （``ToolTimeoutError``）与“超时前一直没拿到执行资源”（``ConcurrencyLimitedError``）。
        secrets: 本次**调用**（跨所有尝试共享）使用过的密钥值。适配器从 ``SecretProvider``
            取得密钥后必须登记到这里，执行器据此在日志、Span、审计和结果中做精确清除。
        idempotency_key: 本次调用的幂等键（同一调用的所有重试相同）。只有下游明确支持时，
            适配器才应该发送它；发送它本身不能保证幂等。
    """

    attempt: int
    deadline: float
    lease: ExecutionLease
    started: bool = False
    secrets: CallSecrets = field(default_factory=CallSecrets)
    idempotency_key: str | None = None

    def mark_started(self) -> None:
        """标记工具已经开始执行（只能在事件循环线程中调用）。"""
        self.started = True


class ToolAdapter(Protocol):
    """各类工具（Python、HTTP、MCP）适配器的统一协议。"""

    #: 对应 ``ToolSpec.kind``。
    kind: str

    async def invoke(
        self,
        binding: ToolBinding,
        args: Mapping[str, Any],
        ctx: ToolContext,
        attempt: AttemptScope,
    ) -> ToolOutput:
        """执行一次工具调用。

        取消语义：执行器在超时或调用方取消时会取消本协程，实现必须让
        ``asyncio.CancelledError`` 原样传播，不得吞掉；已经发生的副作用无法撤回。

        Args:
            binding: 已解析的工具绑定。
            args: 已通过校验的参数。
            ctx: 调用上下文。
            attempt: 本次尝试的执行范围。

        Returns:
            归一化后的工具输出。

        Raises:
            ToolError: 工具层面的失败（由适配器把底层异常映射成统一错误）。
        """
        ...

    async def aclose(self) -> None:
        """释放适配器持有的资源（连接池、会话等）。"""
        ...
