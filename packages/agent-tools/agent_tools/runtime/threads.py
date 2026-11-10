"""运行同步 Python 工具的专用、有上限的线程池。

关键限制：Python 无法安全地强制终止线程。超时或取消后，调用会立即结束，但线程会继续运行
到函数返回为止。为了防止这类“孤儿线程”无限堆积，线程槽位要等线程**真正结束**才归还；
同时执行器的其他额度也会通过 :class:`~agent_tools.runtime.lease.ExecutionLease` 推迟归还。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import contextvars
import functools
import threading
from collections.abc import Callable, Mapping
from typing import Any

from .lease import ExecutionLease

__all__ = ["BoundedThreadRunner"]


class BoundedThreadRunner:
    """有上限的线程池：槽位数等于线程数，所以提交的任务不会在线程池内部排队。"""

    def __init__(
        self,
        max_workers: int,
        *,
        on_orphan_change: Callable[[int], None] | None = None,
        on_orphan_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        """创建线程池。

        Args:
            max_workers: 最大线程数，同时也是槽位数。
            on_orphan_change: 孤儿线程数量变化时的回调，参数为变化量（+1 / -1），
                在事件循环线程中调用。用于上报指标。
            on_orphan_error: 孤儿线程最终抛出异常时的回调（结果已被丢弃，只能记录）。

        Raises:
            ValueError: ``max_workers`` 不是正数。
        """
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        self._max_workers = max_workers
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="agent-tools"
        )
        self._slots = asyncio.Semaphore(max_workers)
        self._in_use = 0
        self._orphans: set[concurrent.futures.Future[Any]] = set()
        self._on_orphan_change = on_orphan_change
        self._on_orphan_error = on_orphan_error

    @property
    def orphaned(self) -> int:
        """已经超时或被取消、但线程仍在运行的任务数。"""
        return len(self._orphans)

    @property
    def available_slots(self) -> int:
        """当前空闲的线程槽位数（用于测试和诊断）。"""
        return self._max_workers - self._in_use

    async def run(
        self,
        fn: Callable[..., Any],
        kwargs: Mapping[str, Any],
        *,
        lease: ExecutionLease,
        cancel_event: threading.Event | None = None,
        on_started: Callable[[], None] | None = None,
    ) -> Any:
        """在线程池中运行同步函数，并等待它返回。

        等待空闲线程槽位没有单独的超时：由调用方外层的 ``asyncio.timeout`` 统一限制，
        调用方可以通过 ``on_started`` 是否被调用来判断超时发生在开始之前还是之后。

        当前协程被取消（包括外层 ``asyncio.timeout`` 到期）时：立即向上抛出
        ``CancelledError``；如果线程已经开始运行，它会继续运行到结束，此时会设置
        ``cancel_event``（若提供），把该任务计为孤儿线程，并让槽位和 ``lease`` 中的
        额度等到线程结束后再归还。

        Args:
            fn: 同步函数（宿主信任的代码）。
            kwargs: 关键字参数。
            lease: 本次调用的执行租约，额度会被推迟到线程结束后归还。
            cancel_event: 协作式取消事件；超时或取消时会被设置，工具可以自行检查后提前退出。
            on_started: 拿到线程槽位、任务已提交到线程时调用（在事件循环线程中）。

        Returns:
            函数的返回值。

        Raises:
            asyncio.CancelledError: 当前协程被取消（包括等待线程槽位期间）。
            Exception: 函数自身抛出的异常（原样传递，由适配器负责转换）。
        """
        await self._slots.acquire()
        self._in_use += 1

        loop = asyncio.get_running_loop()
        # 复制当前 contextvars，让工具代码在线程中也能看到当前 Span（Trace 关联）
        context = contextvars.copy_context()
        try:
            call: Callable[[], Any] = functools.partial(fn, **kwargs)
            future = self._pool.submit(context.run, call)
        except BaseException:
            self._in_use -= 1
            self._slots.release()
            raise
        future.add_done_callback(lambda f: self._on_done(loop, f))
        lease.hold_until(future)
        if on_started is not None:
            on_started()

        try:
            return await asyncio.wrap_future(future)
        except BaseException:
            # 超时、取消（或线程抛出异常）。只有线程还没结束时才是孤儿线程：
            # 结果将被丢弃，但槽位和额度要等它真正结束才归还
            if not future.done():
                if cancel_event is not None:
                    cancel_event.set()
                self._orphans.add(future)
                self._notify_orphan_change(+1)
            raise

    def shutdown(self) -> None:
        """关闭线程池：不再接受新任务，取消排队中的任务。

        正在运行的线程无法被终止，会继续运行到结束；本方法不会等待它们。
        """
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _on_done(
        self, loop: asyncio.AbstractEventLoop, future: concurrent.futures.Future[Any]
    ) -> None:
        """线程任务结束时的回调（在工作线程中执行）：把收尾工作调度回事件循环线程。

        Args:
            loop: 提交任务时所在的事件循环。
            future: 已经结束的线程任务。
        """
        try:
            loop.call_soon_threadsafe(self._finish, future)
        except RuntimeError:
            # 事件循环已关闭：没有协程会再等待槽位，直接在当前线程收尾
            self._finish(future)

    def _finish(self, future: concurrent.futures.Future[Any]) -> None:
        """归还线程槽位；如果是孤儿线程，更新计数并记录它最终的异常。

        Args:
            future: 已经结束的线程任务。
        """
        self._in_use -= 1
        self._slots.release()
        if future in self._orphans:
            self._orphans.discard(future)
            self._notify_orphan_change(-1)
            if not future.cancelled():
                exc = future.exception()
                if exc is not None and self._on_orphan_error is not None:
                    self._on_orphan_error(exc)

    def _notify_orphan_change(self, delta: int) -> None:
        """调用孤儿线程数量变化的回调；回调自身出错时忽略（只影响遥测）。

        Args:
            delta: 变化量。
        """
        if self._on_orphan_change is None:
            return
        # 遥测回调失败不能影响线程收尾
        with contextlib.suppress(Exception):
            self._on_orphan_change(delta)
