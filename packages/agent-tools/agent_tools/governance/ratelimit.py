"""速率限制：协议与进程内的令牌桶实现。

行为约定：

* 按 ``(工具名, 租户)`` 分别计数，不同租户互不影响；没有租户的调用共用一个桶；
* 每次尝试（包括重试）消耗一个令牌；
* 没有令牌时：如果下一个令牌在 ``min(RateLimit.max_wait_s, 剩余截止时间)`` 之内，就预留
  一个令牌并等待；否则立即抛出 ``RateLimitedError``（``retry_after`` 为预计等待时间）；
* 等待期间被取消时，预留的令牌会退还，``CancelledError`` 原样传播；
* 令牌在执行之后**不会**退还（限制的是请求速率，不是并发数）。

进程内实现只对当前进程有效；多实例部署需要宿主提供共享状态的实现（例如基于 Redis）。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from ..protocol.errors import RateLimitedError
from ..protocol.spec import RateLimit
from ..runtime.clock import Clock, MonotonicClock

__all__ = ["RateLimiter", "TokenBucketRateLimiter"]


class RateLimiter(Protocol):
    """速率限制协议。宿主可以提供共享状态的实现（例如基于 Redis），以便在多个实例间统一限流。"""

    async def acquire(self, key: str, limit: RateLimit, *, max_wait_s: float) -> None:
        """取得一次调用的许可。

        Args:
            key: 计数键（执行器使用 ``工具名|租户``）。
            limit: 速率限制。
            max_wait_s: 调用方允许的最长等待（已经考虑了截止时间）。

        Raises:
            RateLimitedError: 在允许的等待时间内拿不到许可。
            asyncio.CancelledError: 等待期间被取消。
        """
        ...


@dataclass
class _Bucket:
    """一个令牌桶的状态。"""

    tokens: float
    updated: float
    capacity: float
    rate: float


class TokenBucketRateLimiter:
    """进程内的令牌桶限流器（只在事件循环线程中使用）。"""

    def __init__(
        self,
        *,
        clock: Clock | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_keys: int = 10_000,
    ) -> None:
        """创建限流器。

        Args:
            clock: 单调时钟（测试可以注入）。
            sleep: 等待函数（测试可以注入）。
            max_keys: 桶的数量上限；超过时清理已经回满的空闲桶，防止租户很多时内存持续增长。
        """
        self._clock = clock or MonotonicClock()
        self._sleep = sleep
        self._max_keys = max_keys
        self._buckets: dict[str, _Bucket] = {}

    async def acquire(self, key: str, limit: RateLimit, *, max_wait_s: float) -> None:
        """取得一个令牌，必要时在允许的范围内等待。

        Args:
            key: 计数键。
            limit: 速率限制。
            max_wait_s: 调用方允许的最长等待（秒）。

        Raises:
            RateLimitedError: 在允许的等待时间内拿不到令牌。
            asyncio.CancelledError: 等待期间被取消（预留的令牌会退还）。
        """
        capacity = float(limit.burst or limit.requests)
        rate = limit.requests / limit.per_s
        bucket = self._refill(key, capacity, rate)
        if bucket.tokens >= 1:
            bucket.tokens -= 1
            return
        wait = (1 - bucket.tokens) / rate
        if wait > min(limit.max_wait_s, max(max_wait_s, 0.0)):
            raise RateLimitedError("tool rate limit exceeded", retry_after=round(wait, 3))
        # 先预留令牌再等待，保证多个等待者按到达顺序排队，不会在醒来后互相争抢
        bucket.tokens -= 1
        try:
            await self._sleep(wait)
        except asyncio.CancelledError:
            bucket.tokens = min(capacity, bucket.tokens + 1)
            raise

    def _refill(self, key: str, capacity: float, rate: float) -> _Bucket:
        """按经过的时间补充令牌，必要时创建新桶。

        Args:
            key: 计数键。
            capacity: 桶容量。
            rate: 每秒补充的令牌数。

        Returns:
            补充后的桶。
        """
        now = self._clock.monotonic()
        bucket = self._buckets.get(key)
        if bucket is None:
            if len(self._buckets) >= self._max_keys:
                self._evict_idle(now)
            bucket = self._buckets[key] = _Bucket(
                tokens=capacity, updated=now, capacity=capacity, rate=rate
            )
            return bucket
        # 限制在版本之间可能变化：以当前限制为准
        bucket.capacity, bucket.rate = capacity, rate
        bucket.tokens = min(capacity, bucket.tokens + (now - bucket.updated) * rate)
        bucket.updated = now
        return bucket

    def _evict_idle(self, now: float) -> None:
        """清理已经回满（空闲）的桶。清理它们不会放宽任何限制，因为新建的桶本来就是满的。

        Args:
            now: 当前时刻。
        """
        idle = [
            k
            for k, b in self._buckets.items()
            if b.tokens + (now - b.updated) * b.rate >= b.capacity
        ]
        for k in idle:
            del self._buckets[k]
