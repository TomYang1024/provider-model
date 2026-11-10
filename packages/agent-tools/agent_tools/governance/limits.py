"""进程内的并发额度：全局在途上限 + 单工具并发上限。

拿到的额度登记在 :class:`~agent_tools.runtime.lease.ExecutionLease` 中，由租约决定何时归还：
通常在调用结束时归还；同步工具超时后线程仍在运行时，推迟到线程结束后归还。
"""

from __future__ import annotations

import asyncio

from ..protocol.errors import ConcurrencyLimitedError
from ..runtime.lease import ExecutionLease

__all__ = ["ConcurrencyLimiter"]


class ConcurrencyLimiter:
    """全局与单工具两级并发控制（只在当前进程内有效）。"""

    def __init__(self, max_inflight: int) -> None:
        """创建限制器。

        Args:
            max_inflight: 全局同时在途的调用上限。

        Raises:
            ValueError: ``max_inflight`` 不是正数。
        """
        if max_inflight <= 0:
            raise ValueError("max_inflight must be positive")
        self._max_inflight = max_inflight
        self._global = asyncio.Semaphore(max_inflight)
        self._global_used = 0
        self._per_tool: dict[str, tuple[int, asyncio.Semaphore]] = {}

    @property
    def inflight(self) -> int:
        """当前占用的全局额度数（包括等待孤儿线程结束而未归还的额度）。"""
        return self._global_used

    async def acquire(
        self, key: str, max_concurrency: int | None, *, wait_s: float, lease: ExecutionLease
    ) -> None:
        """获取全局额度和单工具额度，并登记到租约中。

        两级额度在同一个超时时间内获取；任一级超时都会归还已经拿到的额度。
        等待过程中被取消时同样会归还，``CancelledError`` 原样传播。

        Args:
            key: 单工具额度的键（工具名 + 版本）。
            max_concurrency: 单工具上限；``None`` 表示只受全局上限约束。
            wait_s: 最长等待时间（秒）。
            lease: 本次调用的执行租约。

        Raises:
            ConcurrencyLimitedError: 在 ``wait_s`` 内没有拿到全部额度。
            asyncio.CancelledError: 等待期间被取消。
        """
        tool_sem = self._tool_semaphore(key, max_concurrency)
        got_global = False
        got_tool = False
        try:
            async with asyncio.timeout(wait_s):
                await self._global.acquire()
                got_global = True
                self._global_used += 1
                if tool_sem is not None:
                    await tool_sem.acquire()
                    got_tool = True
        except TimeoutError:
            self._rollback(got_global, tool_sem if got_tool else None)
            raise ConcurrencyLimitedError("too many concurrent tool calls") from None
        except BaseException:
            self._rollback(got_global, tool_sem if got_tool else None)
            raise
        lease.add(self._release_global)
        if tool_sem is not None:
            lease.add(tool_sem.release)

    def _tool_semaphore(self, key: str, limit: int | None) -> asyncio.Semaphore | None:
        """获取（必要时创建）单工具信号量。

        Args:
            key: 工具名 + 版本。
            limit: 并发上限；``None`` 表示不限制。

        Returns:
            信号量；不限制时返回 ``None``。
        """
        if limit is None:
            return None
        existing = self._per_tool.get(key)
        if existing is not None and existing[0] == limit:
            return existing[1]
        # 同一个 key 的上限只会在重新注册版本时变化；旧信号量仍由在途调用归还，互不影响
        sem = asyncio.Semaphore(limit)
        self._per_tool[key] = (limit, sem)
        return sem

    def _release_global(self) -> None:
        """归还一份全局额度。"""
        self._global_used -= 1
        self._global.release()

    def _rollback(self, got_global: bool, tool_sem: asyncio.Semaphore | None) -> None:
        """获取失败时归还已经拿到的额度。

        Args:
            got_global: 是否已经拿到全局额度。
            tool_sem: 已经拿到的单工具信号量；没拿到时为 ``None``。
        """
        if tool_sem is not None:
            tool_sem.release()
        if got_global:
            self._release_global()
