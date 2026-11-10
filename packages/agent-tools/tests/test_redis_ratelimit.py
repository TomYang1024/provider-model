"""分布式限流参考实现（``RedisTokenBucketRateLimiter``）的测试。

两层：

* 默认运行：用内存中的 :class:`FakeRedis` 代替 Redis。它按与 Lua 脚本**相同的算法**用 Python
  实现两个脚本，时间可控。这一层验证的是 Python 一侧：脚本参数与返回值的约定、等待与取消退还、
  故障模式、多个限流器实例共享同一份状态时的合计限额。它**不能**证明 Lua 脚本本身正确；
* ``redis`` 标记（默认不运行）：对真实 Redis 执行真正的 Lua 脚本。需要设置环境变量
  ``AGENT_TOOLS_REDIS_URL`` 并安装 ``redis`` 包::

      AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 \\
          uv run --with redis pytest -m redis tests/test_redis_ratelimit.py
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from agent_tools import (
    Principal,
    RateLimit,
    RateLimitedError,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.governance.ratelimit import RateLimiter
from agent_tools.governance.redis_ratelimit import (
    ACQUIRE_SCRIPT,
    REFUND_SCRIPT,
    RedisTokenBucketRateLimiter,
)


class FakeRedis:
    """内存中的假 Redis：只支持本实现用到的两个脚本，时间由测试控制。

    Attributes:
        now: 假的服务端时间（秒）。
        data: 键 → ``{"tokens": 浮点数, "ts": 浮点数}``。
        ttl_ms: 键 → 最近一次设置的过期时间。
        calls: 每次 ``eval`` 的 ``(脚本名, 键)``。
        fail: 下一次调用要抛出的异常（一次性）。
        hang: 为 True 时 ``eval`` 永不返回（模拟网络挂起）。
    """

    def __init__(self) -> None:
        """创建空的假 Redis，时间从 0 开始。"""
        self.now = 0.0
        self.data: dict[str, dict[str, float]] = {}
        self.ttl_ms: dict[str, int] = {}
        self.calls: list[tuple[str, str]] = []
        self.fail: Exception | None = None
        self.hang = False
        self.reply_override: Any = None

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """按脚本内容分派到对应的 Python 实现。

        Args:
            script: 脚本源码（必须是模块导出的两个脚本之一）。
            numkeys: 键的数量（必须为 1）。
            *keys_and_args: 键与参数。

        Returns:
            与 Lua 脚本相同形状的返回值（字符串以 ``bytes`` 返回，模拟默认的 Redis 客户端）。
        """
        assert numkeys == 1
        key, *args = keys_and_args
        await asyncio.sleep(0)
        if self.hang:
            await asyncio.Event().wait()
        if self.fail is not None:
            error, self.fail = self.fail, None
            raise error
        if script == ACQUIRE_SCRIPT:
            self.calls.append(("acquire", key))
            if self.reply_override is not None:
                return self.reply_override
            return self._acquire(key, *args)
        assert script == REFUND_SCRIPT
        self.calls.append(("refund", key))
        return self._refund(key, *args)

    def _acquire(
        self, key: str, capacity: float, rate: float, max_wait: float, ttl_ms: int
    ) -> list[Any]:
        """取令牌脚本的 Python 对照实现（逐行对应 ``ACQUIRE_SCRIPT``）。"""
        bucket = self.data.get(key)
        tokens, ts = (bucket["tokens"], bucket["ts"]) if bucket else (capacity, self.now)
        elapsed = max(self.now - ts, 0.0)
        tokens = min(capacity, tokens + elapsed * rate)
        wait, granted = 0.0, 1
        if tokens < 1:
            wait = (1 - tokens) / rate
            if wait > max_wait:
                granted = 0
        if granted:
            tokens -= 1
        self.data[key] = {"tokens": tokens, "ts": self.now}
        self.ttl_ms[key] = ttl_ms
        return [granted, repr(wait).encode()]

    def _refund(self, key: str, capacity: float) -> int:
        """退还脚本的 Python 对照实现（逐行对应 ``REFUND_SCRIPT``）。"""
        bucket = self.data.get(key)
        if bucket is not None:
            bucket["tokens"] = min(capacity, bucket["tokens"] + 1)
        return 1


LIMIT = RateLimit(requests=2, per_s=1.0)


def test_limiter_satisfies_the_rate_limiter_protocol() -> None:
    """参考实现满足执行器使用的 ``RateLimiter`` 协议（类型检查与构造参数校验）。"""
    limiter: RateLimiter = RedisTokenBucketRateLimiter(FakeRedis())
    assert callable(limiter.acquire)
    with pytest.raises(ValueError):
        RedisTokenBucketRateLimiter(FakeRedis(), op_timeout_s=0)
    with pytest.raises(ValueError):
        RedisTokenBucketRateLimiter(FakeRedis(), on_error="ignore")  # type: ignore[arg-type]


async def test_instances_sharing_one_store_enforce_a_combined_limit() -> None:
    """两个限流器实例（代表两个进程）共用一个存储：限额按合计计算，时间推进后补充令牌。"""
    redis = FakeRedis()
    first, second = RedisTokenBucketRateLimiter(redis), RedisTokenBucketRateLimiter(redis)
    await first.acquire("tool|t1", LIMIT, max_wait_s=10)
    await second.acquire("tool|t1", LIMIT, max_wait_s=10)
    for limiter in (first, second):
        with pytest.raises(RateLimitedError) as info:
            await limiter.acquire("tool|t1", LIMIT, max_wait_s=10)
        assert info.value.code == "rate_limited"
        assert info.value.retry_after == pytest.approx(0.5)
    redis.now = 0.5
    await second.acquire("tool|t1", LIMIT, max_wait_s=10)
    with pytest.raises(RateLimitedError):
        await first.acquire("tool|t1", LIMIT, max_wait_s=10)


async def test_keys_are_isolated_hashed_and_expire() -> None:
    """不同租户、不同前缀互不影响；Redis 键是摘要，不含租户明文；键带过期时间。"""
    redis = FakeRedis()
    limiter = RedisTokenBucketRateLimiter(redis, prefix="p1:")
    other = RedisTokenBucketRateLimiter(redis, prefix="p2:")
    one = RateLimit(requests=1, per_s=10.0)
    await limiter.acquire("tool|tenant-alpha", one, max_wait_s=0)
    await limiter.acquire("tool|tenant-beta", one, max_wait_s=0)
    await other.acquire("tool|tenant-alpha", one, max_wait_s=0)
    with pytest.raises(RateLimitedError):
        await limiter.acquire("tool|tenant-alpha", one, max_wait_s=0)
    assert len(redis.data) == 3
    for key in redis.data:
        assert key.startswith(("p1:", "p2:")) and "tenant" not in key and len(key) == 3 + 64
    # 过期时间不短于“回满所需时间”，否则过期会放宽限制
    assert all(ttl >= 0.1 * 1000 for ttl in redis.ttl_ms.values())


async def test_waits_within_budget_and_refunds_on_cancel() -> None:
    """允许等待时预留令牌并等待；等待被取消时取消立即传播，令牌在后台退还。"""
    redis = FakeRedis()
    sleeps: list[float] = []
    gate = asyncio.Event()

    async def sleep(seconds: float) -> None:
        """记录等待时长并阻塞到测试放行。"""
        sleeps.append(seconds)
        await gate.wait()

    limiter = RedisTokenBucketRateLimiter(redis, sleep=sleep)
    limit = RateLimit(requests=1, per_s=1.0, max_wait_s=2.0)
    await limiter.acquire("k", limit, max_wait_s=10)
    waiter = asyncio.create_task(limiter.acquire("k", limit, max_wait_s=10))
    while not sleeps:
        await asyncio.sleep(0)
    assert sleeps == [pytest.approx(1.0)]
    assert redis.data[limiter.redis_key("k")]["tokens"] == pytest.approx(-1.0)  # 已预留
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await limiter.aclose()
    assert redis.calls[-1][0] == "refund"
    assert redis.data[limiter.redis_key("k")]["tokens"] == pytest.approx(0.0)
    # 调用方的预算（截止时间）比工具允许的等待更短时，以更短的为准
    with pytest.raises(RateLimitedError) as info:
        await limiter.acquire("k", limit, max_wait_s=0.5)
    assert info.value.retry_after == pytest.approx(1.0)


async def test_completed_wait_grants_the_call() -> None:
    """等待正常结束后调用获得许可，不会退还令牌。"""
    redis = FakeRedis()
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        """记录等待时长后立即返回。"""
        slept.append(seconds)

    limiter = RedisTokenBucketRateLimiter(redis, sleep=sleep)
    limit = RateLimit(requests=2, per_s=1.0, burst=1, max_wait_s=1.0)
    await limiter.acquire("k", limit, max_wait_s=10)
    await limiter.acquire("k", limit, max_wait_s=10)
    assert slept == [pytest.approx(0.5)]
    assert [name for name, _ in redis.calls] == ["acquire", "acquire"]


@pytest.mark.parametrize(
    "break_it",
    [
        lambda r: setattr(r, "fail", ConnectionError("redis down")),
        lambda r: setattr(r, "hang", True),
        lambda r: setattr(r, "reply_override", [1]),
        lambda r: setattr(r, "reply_override", [1, b"nan"]),
        lambda r: setattr(r, "reply_override", [1, b"-3"]),
    ],
    ids=["error", "timeout", "short-reply", "nan-wait", "negative-wait"],
)
async def test_unavailable_store_fails_closed_by_default(
    break_it: Callable[[FakeRedis], None],
) -> None:
    """Redis 出错、超时或回复不符合约定：默认拒绝调用（不放行），错误不含连接细节。"""
    redis = FakeRedis()
    limiter = RedisTokenBucketRateLimiter(redis, op_timeout_s=0.05)
    break_it(redis)
    with pytest.raises(RateLimitedError) as info:
        await limiter.acquire("k", LIMIT, max_wait_s=10)
    assert info.value.code == "rate_limiter_unavailable" and info.value.retry_after is None
    assert "redis" not in info.value.safe_message.lower() and limiter.failures == 1


async def test_fail_open_is_explicit_and_counted() -> None:
    """显式选择 fail_open 时，Redis 不可用会放行调用并累加失败计数。"""
    redis = FakeRedis()
    limiter = RedisTokenBucketRateLimiter(redis, on_error="fail_open")
    redis.fail = TimeoutError()
    await limiter.acquire("k", LIMIT, max_wait_s=10)
    assert limiter.failures == 1


async def test_cancellation_during_the_store_call_propagates() -> None:
    """Redis 往返途中被取消：CancelledError 原样传播，不被当作“不可用”吞掉。"""
    redis = FakeRedis()
    redis.hang = True
    limiter = RedisTokenBucketRateLimiter(redis, op_timeout_s=30)
    task = asyncio.create_task(limiter.acquire("k", LIMIT, max_wait_s=10))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert limiter.failures == 0


async def test_refund_failure_is_only_counted() -> None:
    """后台退还失败只计数，不影响取消的传播，也不会抛出未处理的异常。"""
    redis = FakeRedis()
    gate = asyncio.Event()

    async def sleep(seconds: float) -> None:
        """阻塞到被取消。"""
        await gate.wait()

    limiter = RedisTokenBucketRateLimiter(redis, sleep=sleep)
    limit = RateLimit(requests=1, per_s=1.0, max_wait_s=2.0)
    await limiter.acquire("k", limit, max_wait_s=10)
    waiter = asyncio.create_task(limiter.acquire("k", limit, max_wait_s=10))
    await asyncio.sleep(0.01)
    redis.fail = ConnectionError("down")
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await limiter.aclose()
    assert limiter.failures == 1


async def test_two_executors_share_the_limit_per_tool_and_tenant() -> None:
    """接入执行器：两个执行器（代表两个实例）共用存储时，同一工具同一租户的限额合计生效，
    其他租户不受影响；被限流的调用不会执行工具。"""
    redis = FakeRedis()
    calls: list[str] = []

    async def ping() -> str:
        """记录一次执行。"""
        calls.append("x")
        return "pong"

    policy = ToolPolicy(access="public", rate_limit=RateLimit(requests=1, per_s=60.0))
    executors = []
    for _ in range(2):
        registry = ToolRegistry()
        registry.register_function(ping, name="ping", policy=policy)
        executors.append(ToolExecutor(registry, rate_limiter=RedisTokenBucketRateLimiter(redis)))

    def ctx(tenant: str) -> ToolContext:
        """构造租户上下文。"""
        return ToolContext(principal=Principal(tenant_id=tenant))

    invocation = ToolInvocation(call_id="c", tool="ping", arguments={})
    first = await executors[0].execute(invocation, ctx("t1"))
    second = await executors[1].execute(invocation, ctx("t1"))
    other = await executors[1].execute(invocation, ctx("t2"))
    assert first.ok and other.ok
    assert second.error is not None and second.error.code == "rate_limited"
    assert len(calls) == 2
    for executor in executors:
        await executor.aclose()


# ---------------------------------------------------------------------------------
# 真实 Redis（默认不运行）
# ---------------------------------------------------------------------------------
@pytest.mark.redis
async def test_lua_scripts_on_real_redis() -> None:
    """在真实 Redis 上执行 Lua 脚本：突发、拒绝与 retry_after、随时间补充、取消后的退还、
    两个限流器实例合计限额。需要 ``AGENT_TOOLS_REDIS_URL`` 与 ``redis`` 包。"""
    url = os.environ.get("AGENT_TOOLS_REDIS_URL")
    if not url:
        pytest.skip("AGENT_TOOLS_REDIS_URL is not set")
    redis_asyncio = pytest.importorskip("redis.asyncio")
    client = redis_asyncio.from_url(url)
    prefix = f"agent-tools-test:{uuid.uuid4().hex}:"
    first = RedisTokenBucketRateLimiter(client, prefix=prefix)
    second = RedisTokenBucketRateLimiter(client, prefix=prefix)
    limit = RateLimit(requests=2, per_s=1.0)
    try:
        await first.acquire("k", limit, max_wait_s=0)
        await second.acquire("k", limit, max_wait_s=0)
        with pytest.raises(RateLimitedError) as info:
            await first.acquire("k", limit, max_wait_s=0)
        assert info.value.retry_after is not None and 0 < info.value.retry_after <= 0.5
        await asyncio.sleep(0.6)
        await second.acquire("k", limit, max_wait_s=0)

        waiting = RateLimit(requests=1, per_s=1.0, max_wait_s=5.0)
        await first.acquire("w", waiting, max_wait_s=10)
        task = asyncio.create_task(first.acquire("w", waiting, max_wait_s=10))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await first.aclose()
        tokens = float(await client.hget(first.redis_key("w"), "tokens"))
        assert -0.1 <= tokens <= 1.0  # 预留已退还（未退还时小于 -0.5）
        assert await client.pttl(first.redis_key("w")) > 0
        assert first.failures == 0 and second.failures == 0
    finally:
        for key in ("k", "w"):
            await client.delete(first.redis_key(key))
        await client.aclose()
