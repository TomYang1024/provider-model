"""截止时间与单次尝试超时的计算。

公式::

    remaining       = ctx.deadline - now                （没有 deadline 时视为无穷大）
    attempt_timeout = min(policy.timeout_s, remaining)
    开始前 remaining <= 0  → ToolTimeoutError(deadline_exceeded)，不调用适配器

排队等待并发额度的时间同样计入 ``ctx.deadline``。P0 不重试，因此没有退避计算。
"""

from __future__ import annotations

from dataclasses import dataclass

from ..protocol.context import ToolContext
from ..protocol.errors import ToolTimeoutError

__all__ = ["AttemptWindow", "attempt_window", "wait_budget"]


@dataclass(frozen=True, slots=True)
class AttemptWindow:
    """单次尝试的时间窗口。

    Attributes:
        timeout_s: 本次尝试允许的时长（秒）。
        deadline: 本次尝试的截止时刻（monotonic）。
        bounded_by_deadline: 是否由 ``ctx.deadline`` 而不是工具自身的超时决定，
            用于区分超时错误码 ``deadline_exceeded`` 与 ``attempt_timeout``。
    """

    timeout_s: float
    deadline: float
    bounded_by_deadline: bool


def attempt_window(ctx: ToolContext, tool_timeout_s: float, now: float) -> AttemptWindow:
    """计算单次尝试的时间窗口。

    Args:
        ctx: 调用上下文。
        tool_timeout_s: 工具策略中的单次超时（秒）。
        now: 当前的 monotonic 时刻。

    Returns:
        时间窗口。

    Raises:
        ToolTimeoutError: 整体截止时间已经过去（``deadline_exceeded``，不可重试）。
    """
    remaining = ctx.remaining(now)
    if remaining is not None and remaining <= 0:
        raise ToolTimeoutError(
            "deadline exceeded before the tool started", code="deadline_exceeded", retryable=False
        )
    if remaining is not None and remaining < tool_timeout_s:
        return AttemptWindow(remaining, now + remaining, bounded_by_deadline=True)
    return AttemptWindow(tool_timeout_s, now + tool_timeout_s, bounded_by_deadline=False)


def wait_budget(ctx: ToolContext, tool_timeout_s: float, now: float) -> float:
    """计算排队等待并发额度的最长时间。

    有整体截止时间时以它为准；没有时以工具的单次超时为上限，避免无限期排队。

    Args:
        ctx: 调用上下文。
        tool_timeout_s: 工具策略中的单次超时（秒）。
        now: 当前的 monotonic 时刻。

    Returns:
        最长等待秒数（不小于 0）。
    """
    remaining = ctx.remaining(now)
    budget = tool_timeout_s if remaining is None else remaining
    return max(budget, 0.0)
