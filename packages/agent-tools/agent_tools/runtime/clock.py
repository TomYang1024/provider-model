"""可注入的时钟，便于测试截止时间相关的逻辑。"""

from __future__ import annotations

import time
from typing import Protocol

__all__ = ["Clock", "MonotonicClock"]


class Clock(Protocol):
    """单调时钟协议。返回值必须和 ``ToolContext.deadline`` 使用同一个时间基准。"""

    def monotonic(self) -> float:
        """返回当前的单调时刻（秒）。"""
        ...


class MonotonicClock:
    """默认实现：``time.monotonic()``，与 asyncio 事件循环默认使用的时钟一致。"""

    def monotonic(self) -> float:
        """返回 ``time.monotonic()`` 的当前值。"""
        return time.monotonic()
