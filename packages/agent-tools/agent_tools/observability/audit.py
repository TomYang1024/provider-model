"""工具授权决定和执行结果的审计契约。

审计事件与日志的区别：审计**不采样**，由宿主的 :class:`AuditSink` 负责持久化和访问控制；
事件只包含元数据，**不包含**参数、结果内容或凭证（工具名在解析前是模型给出的名字，已截断）。

写入时机与故障策略（``ExecutorConfig.audit_failure_mode``）：

* ``decision``：工具级授权、参数级授权各写一条，发生在**工具执行之前**。写入失败或超时时，
  ``fail_open`` 继续执行；``fail_closed`` 拒绝执行（返回 ``audit_unavailable``）。
* ``approval``（P3）：需要人工审批的工具，审批结束后（批准、拒绝、超时、处理器失败、
  没有处理器）写一条，发生在工具执行之前，故障策略与 ``decision`` 相同。``approver`` 和
  ``reason`` 来自审批处理器；参数只以哈希的形式出现在审批请求中，不进入审计事件。
* ``result``：调用结束后写一条。此时工具**已经执行**，写入失败只能计数并记录日志，
  无法撤回已经发生的副作用，结果照常返回（两种策略相同）。结果是复用的（缓存命中或去重重放）
  时 ``reused`` 标明来源。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

__all__ = ["AuditEvent", "AuditSink"]


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """一次授权决定或执行结果。

    Attributes:
        kind: ``decision``（授权决定）、``approval``（人工审批，P3）或 ``result``（执行结果）。
        request_id: 请求 ID。
        call_id: 调用 ID。
        tool: 工具内部名（解析失败时为模型给出的名字，截断到 64 个字符）。
        tenant_id: 租户 ID。
        outcome: ``decision`` 为 ``allowed`` / ``denied``；``approval`` 为 ``approved`` /
            ``rejected`` / ``timeout`` / ``error`` / ``unavailable``；``result`` 为
            ``success`` / ``error``。
        reason: 授权原因代码（低基数，例如 ``missing_scopes``、``arguments``）；审批事件为
            审批处理器给出的原因（截断到 256 个字符）。
        error_type: ``result`` 失败时的错误类型。
        error_code: ``result`` 失败时的错误码。
        attempts: ``result`` 的尝试次数。
        approver: ``approval`` 的审批人标识（P3）。
        reused: ``result`` 复用的来源：``cache`` / ``dedupe``（P3）；实际执行时为 ``None``。
    """

    kind: Literal["decision", "approval", "result"]
    request_id: str
    call_id: str
    tool: str
    tenant_id: str | None
    outcome: str
    reason: str | None = None
    error_type: str | None = None
    error_code: str | None = None
    attempts: int | None = None
    approver: str | None = None
    reused: str | None = None


class AuditSink(Protocol):
    """由宿主实现的异步审计接收端。"""

    async def emit(self, event: AuditEvent) -> None:
        """写入一条审计事件。

        执行器为每次写入设置 ``audit_timeout_s`` 超时；抛出的 ``Exception`` 或超时按故障策略
        处理。实现不应吞掉 ``asyncio.CancelledError``。

        Args:
            event: 审计事件。
        """
        ...
