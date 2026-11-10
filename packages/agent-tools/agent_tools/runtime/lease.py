"""执行租约：管理一次调用占用的并发额度何时归还。

默认情况下，调用结束（成功、失败、超时或取消）时立即归还额度。但同步工具在线程里运行，
超时或取消后线程**仍会继续执行**，此时如果立即归还额度，超时的任务会越积越多、耗尽线程。
因此适配器可以调用 :meth:`ExecutionLease.hold_until`，把归还时机推迟到线程真正结束。

一次调用可能有多次尝试（P1 重试），每次超时的同步尝试都可能留下一个仍在运行的线程。
额度只有在“调用已经结束”**并且**“所有被登记的后台任务都已结束”之后才归还。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Callable
from typing import Any

__all__ = ["ExecutionLease"]


class ExecutionLease:
    """一次调用持有的全部额度（全局并发、单工具并发等）的归还器。

    线程安全：:meth:`hold_until` 注册的回调会在工作线程中触发，计数和归还都会被调度回
    事件循环线程执行，因为 ``asyncio.Semaphore.release`` 不是线程安全的。
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """创建租约。

        Args:
            loop: 持有这些额度的事件循环；归还操作会在该循环上执行。
        """
        self._loop = loop
        self._releasers: list[Callable[[], None]] = []
        self._pending = 0
        self._finished = False
        self._released = False
        self._lock = threading.Lock()

    @property
    def held(self) -> bool:
        """是否还有被登记、尚未结束的后台任务。"""
        return self._pending > 0

    @property
    def released(self) -> bool:
        """额度是否已经全部归还。"""
        return self._released

    def add(self, release: Callable[[], None]) -> None:
        """登记一个归还函数。

        Args:
            release: 归还某一份额度的函数，会在事件循环线程中被调用且只调用一次。
        """
        self._releasers.append(release)

    def hold_until(self, future: concurrent.futures.Future[Any]) -> None:
        """把额度的归还推迟到 ``future`` 完成（无论成功、失败还是被取消）。

        可以多次调用（每次尝试一个后台任务）；所有被登记的任务都结束、且 :meth:`finish`
        已经被调用之后，才会归还额度。必须在事件循环线程中调用。

        Args:
            future: 后台线程任务对应的 Future。
        """
        self._pending += 1
        future.add_done_callback(lambda _f: self._schedule(self._background_done))

    def finish(self) -> None:
        """调用结束时调用：如果没有仍在运行的后台任务，就立即归还额度。"""
        self._finished = True
        self._maybe_release()

    def _background_done(self) -> None:
        """一个后台任务结束（在事件循环线程中执行）：减少计数并尝试归还。"""
        self._pending -= 1
        self._maybe_release()

    def _maybe_release(self) -> None:
        """调用已结束且没有在运行的后台任务时，归还额度。"""
        if self._finished and self._pending == 0:
            self._release_now()

    def _schedule(self, fn: Callable[[], None]) -> None:
        """从任意线程把 ``fn`` 调度到事件循环线程执行。

        Args:
            fn: 需要在事件循环线程中执行的函数。
        """
        try:
            self._loop.call_soon_threadsafe(fn)
        except RuntimeError:
            # 事件循环已经关闭：不会再有任何协程等待这些信号量，直接执行即可
            fn()

    def _release_now(self) -> None:
        """执行全部归还函数；重复调用是安全的（只会归还一次）。"""
        with self._lock:
            if self._released:
                return
            self._released = True
            releasers, self._releasers = self._releasers, []
        for release in releasers:
            release()
