"""分布式速率限制的**参考实现**：基于 Redis 的共享令牌桶（可选，核心包不依赖 Redis）。

状态：这是给宿主参考和改造的实现，不是默认组件。执行器默认仍使用进程内的
:class:`~agent_tools.governance.ratelimit.TokenBucketRateLimiter`；只有宿主显式构造本类并通过
``ToolExecutor(rate_limiter=...)`` 传入时才会生效。本包**不声明**对 ``redis`` 的依赖：客户端按
:class:`RedisScriptClient` 协议鸭子类型传入（``redis.asyncio.Redis`` 满足该协议，需要宿主自行
安装）。

**验证范围（重要）**：Python 一侧的逻辑（参数与返回值约定、等待、取消退还、故障模式、多个
限流器实例共享状态）用内存中的假客户端测试，假客户端按与 Lua 脚本相同的算法实现。
**Lua 脚本本身没有在真实 Redis 上运行过**（开发环境没有 Redis，也不允许网络连接）；
真实 Redis 上的验证步骤见 ``tests/test_redis_ratelimit.py`` 中带 ``redis`` 标记的测试。
投入生产前必须先在目标 Redis 版本上跑通该测试。

语义（与进程内实现保持一致，差异单独列出）：

* 按 ``key``（执行器传入 ``工具名|租户``）计数；所有使用同一个 Redis、同一个 ``prefix`` 的实例
  共享同一个桶，因此限制的是**所有实例合计**的速率；
* 没有令牌时：下一个令牌在 ``min(limit.max_wait_s, max_wait_s)`` 之内就预留并等待，否则抛出
  ``RateLimitedError``；
* 等待期间被取消：在后台尽力退还预留的令牌（不阻塞取消的传播），``CancelledError`` 原样传播。

它**不是**什么：

* 不是事务，也不提供严格一次的计数。以下情况会多扣或少扣令牌：脚本已经执行、但回复在途中
  丢失或调用方在此刻被取消（多扣，偏向更严格）；退还失败（多扣）；Redis 主从切换丢失最近的
  写入（少扣，偏向更宽松）；键因过期或内存淘汰被删除（等同于桶回满，少扣）；
* 时间取自 Redis 服务端的 ``TIME``，避免各实例时钟不一致；主从切换后新主节点时钟回退时，
  本次不补充令牌（偏向更严格）；
* 只协调速率限制。熔断、结果缓存、调用去重、单飞协调和并发额度仍然只在各自进程内有效
  （见架构文档 §14.5 的作用范围表）。

Redis 不可用时的行为由 ``on_error`` 决定：``fail_closed``（默认）拒绝调用
（``rate_limiter_unavailable``）；``fail_open`` 放行调用并累加 :attr:`failures`。

要求：脚本在写入前调用 ``TIME``，需要按“效果复制”执行脚本的 Redis（5.0 及以上的默认行为）。
每个脚本只访问一个键，可以用于 Redis Cluster。
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import Awaitable, Callable
from typing import Any, Literal, Protocol

from ..protocol.errors import RateLimitedError
from ..protocol.spec import RateLimit

__all__ = [
    "ACQUIRE_SCRIPT",
    "REFUND_SCRIPT",
    "RedisScriptClient",
    "RedisTokenBucketRateLimiter",
]

#: 取令牌脚本。KEYS[1] = 桶；ARGV = 容量、每秒补充数、允许的最长等待（秒）、键的过期时间（毫秒）。
#: 返回 ``{granted(0/1), wait_seconds(字符串)}``：granted=1 且 wait>0 表示已预留、需要等待。
ACQUIRE_SCRIPT = """
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local max_wait = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * rate)
local wait = 0
local granted = 1
if tokens < 1 then
  wait = (1 - tokens) / rate
  if wait > max_wait then granted = 0 end
end
if granted == 1 then tokens = tokens - 1 end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
return {granted, tostring(wait)}
"""

#: 退还脚本。KEYS[1] = 桶；ARGV[1] = 容量。桶已不存在（过期）时不做任何事。
REFUND_SCRIPT = """
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
if tokens ~= nil then
  redis.call('HSET', KEYS[1], 'tokens', tostring(math.min(tonumber(ARGV[1]), tokens + 1)))
end
return 1
"""


class RedisScriptClient(Protocol):
    """本实现需要的最小 Redis 客户端接口（``redis.asyncio.Redis`` 满足）。"""

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """原子地执行一段 Lua 脚本。

        Args:
            script: 脚本源码。
            numkeys: 键的数量。
            *keys_and_args: 先是键，后是参数。

        Returns:
            脚本的返回值（数组以列表返回；字符串可能是 ``bytes`` 或 ``str``）。

        Raises:
            Exception: 连接或执行失败（由限流器按 ``on_error`` 处理）。
        """
        ...


class RedisTokenBucketRateLimiter:
    """基于 Redis 的共享令牌桶限流器（参考实现，见模块说明的验证范围与限制）。

    满足 :class:`~agent_tools.governance.ratelimit.RateLimiter` 协议。只在事件循环线程中使用；
    不拥有 Redis 客户端，客户端的连接、认证、TLS 与关闭都由宿主负责。
    """

    def __init__(
        self,
        client: RedisScriptClient,
        *,
        prefix: str = "agent-tools:ratelimit:",
        on_error: Literal["fail_closed", "fail_open"] = "fail_closed",
        op_timeout_s: float = 1.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """创建限流器。

        Args:
            client: Redis 客户端（鸭子类型）。
            prefix: 键前缀。同一个 Redis 上互不相干的部署应当使用不同的前缀；共用前缀的实例
                共享限额。
            on_error: Redis 不可用（异常或超时）时的行为：``fail_closed`` 拒绝调用，
                ``fail_open`` 放行调用。
            op_timeout_s: 单次 Redis 操作的超时（秒）；超时按不可用处理。
            sleep: 等待函数（测试可以注入）。

        Raises:
            ValueError: 超时不是正数，或 ``on_error`` 取值非法。
        """
        if op_timeout_s <= 0:
            raise ValueError("op_timeout_s must be positive")
        if on_error not in ("fail_closed", "fail_open"):
            raise ValueError("on_error must be 'fail_closed' or 'fail_open'")
        self._client = client
        self._prefix = prefix
        self._on_error = on_error
        self._op_timeout_s = op_timeout_s
        self._sleep = sleep
        self._refunds: set[asyncio.Task[None]] = set()
        #: Redis 操作失败（含超时、回复格式错误、退还失败）的累计次数，供宿主导出为指标。
        self.failures = 0

    def redis_key(self, key: str) -> str:
        """把计数键映射成 Redis 键。

        使用 SHA-256：键里包含工具名和租户 ID，哈希后租户标识不会以明文出现在 Redis 的键空间
        里，也不受长度和特殊字符的影响。

        Args:
            key: 执行器传入的计数键（``工具名|租户``）。

        Returns:
            ``prefix`` + 十六进制摘要。
        """
        return self._prefix + hashlib.sha256(key.encode()).hexdigest()

    async def acquire(self, key: str, limit: RateLimit, *, max_wait_s: float) -> None:
        """取得一个令牌，必要时在允许的范围内等待。

        Args:
            key: 计数键。
            limit: 速率限制。
            max_wait_s: 调用方允许的最长等待（秒，已经考虑了截止时间）。

        Raises:
            RateLimitedError: 在允许的等待时间内拿不到令牌（``rate_limited``，带
                ``retry_after``）；或 ``fail_closed`` 模式下 Redis 不可用
                （``rate_limiter_unavailable``）。
            asyncio.CancelledError: 被取消。等待阶段被取消时会在后台尽力退还预留的令牌；
                在 Redis 往返途中被取消时无法确定脚本是否已执行，不退还（偏向更严格）。
        """
        capacity = float(limit.burst or limit.requests)
        rate = limit.requests / limit.per_s
        budget = min(limit.max_wait_s, max(max_wait_s, 0.0))
        # 过期时间覆盖“从最深的预留恢复到回满”的时间并留出余量：键过期等同于桶回满，
        # 所以过期不会让限制变得比配置更宽松
        ttl_ms = math.ceil((capacity / rate + limit.max_wait_s) * 2000) + 1000
        redis_key = self.redis_key(key)
        try:
            async with asyncio.timeout(self._op_timeout_s):
                reply = await self._client.eval(
                    ACQUIRE_SCRIPT, 1, redis_key, capacity, rate, budget, ttl_ms
                )
            granted, wait = _parse_reply(reply)
        except Exception:
            # CancelledError 不是 Exception，会原样传播
            self.failures += 1
            if self._on_error == "fail_open":
                return
            raise RateLimitedError(
                "the rate limiter is unavailable", code="rate_limiter_unavailable"
            ) from None
        if not granted:
            raise RateLimitedError("tool rate limit exceeded", retry_after=round(wait, 3))
        if wait <= 0:
            return
        try:
            await self._sleep(wait)
        except asyncio.CancelledError:
            # 不在这里等待退还：取消必须立即传播。退还放到后台任务，失败只计数
            task = asyncio.ensure_future(self._refund(redis_key, capacity))
            self._refunds.add(task)
            task.add_done_callback(self._refunds.discard)
            raise

    async def aclose(self) -> None:
        """等待还在进行的后台退还结束。**不会**关闭 Redis 客户端（由宿主负责）。可以重复调用。"""
        while self._refunds:
            await asyncio.wait(set(self._refunds))

    async def _refund(self, redis_key: str, capacity: float) -> None:
        """退还一个预留的令牌（尽力而为）；失败只累加 :attr:`failures`，不抛出异常。

        Args:
            redis_key: Redis 键。
            capacity: 桶容量（退还后不超过它）。
        """
        try:
            async with asyncio.timeout(self._op_timeout_s):
                await self._client.eval(REFUND_SCRIPT, 1, redis_key, capacity)
        except Exception:
            self.failures += 1


def _parse_reply(reply: Any) -> tuple[bool, float]:
    """解析取令牌脚本的返回值。

    Args:
        reply: ``[granted, wait]``；``wait`` 可能是 ``bytes``、``str`` 或数字。

    Returns:
        ``(是否获得许可, 需要等待或建议重试的秒数)``。

    Raises:
        ValueError: 返回值格式不符合约定，或等待时间不是有限的非负数（按 Redis 不可用处理）。
    """
    if not isinstance(reply, list | tuple) or len(reply) != 2:
        raise ValueError("unexpected rate limiter reply")
    raw = reply[1]
    wait = float(raw.decode() if isinstance(raw, bytes | bytearray) else raw)
    if not math.isfinite(wait) or wait < 0:
        raise ValueError("unexpected rate limiter wait")
    return int(reply[0]) == 1, wait
