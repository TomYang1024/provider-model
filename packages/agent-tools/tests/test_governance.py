"""治理的测试：重试条件、退避、截止时间、Retry-After、速率限制、多次孤儿线程的额度归还。"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

import pytest

from agent_tools import (
    HttpTarget,
    NetworkPolicy,
    Principal,
    RateLimit,
    RateLimitedError,
    RetryPolicy,
    TokenBucketRateLimiter,
    ToolAnnotations,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    UpstreamError,
)
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig
from agent_tools.governance.retry import backoff_delay
from agent_tools.runtime.lease import ExecutionLease

IDEMPOTENT = ToolAnnotations(idempotent=True)
RETRY3 = RetryPolicy(max_attempts=3, base_delay_s=0.01, max_delay_s=0.05)
ExecutorFactory = Callable[..., ToolExecutor]


class Flaky:
    """前 N 次调用抛出指定错误，之后返回 ``ok``。"""

    def __init__(self, failures: int, error: Exception) -> None:
        """创建测试工具。

        Args:
            failures: 失败次数。
            error: 每次失败抛出的错误。
        """
        self.failures = failures
        self.error = error
        self.calls = 0

    async def __call__(self) -> str:
        """模拟一次调用。

        Returns:
            ``ok``。

        Raises:
            Exception: 前 ``failures`` 次调用时抛出。
        """
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return "ok"


def flaky_tool(failures: int, error: Exception) -> Callable[[], Any]:
    """把 :class:`Flaky` 包装成带文档字符串的普通异步函数（注册需要可解析的签名）。

    Args:
        failures: 失败次数。
        error: 失败时抛出的错误。

    Returns:
        工具函数，带有 ``state`` 属性指向 :class:`Flaky`。
    """
    state = Flaky(failures, error)

    async def tool() -> str:
        """测试用：按预设次数失败的工具。"""
        return await state()

    tool.state = state  # type: ignore[attr-defined]
    return tool


class SleepRecorder:
    """记录退避等待时长、但不真正等待的 sleep 函数。"""

    def __init__(self) -> None:
        """创建记录器，初始时没有任何等待记录。"""
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        """记录等待时长，并让出一次事件循环。

        Args:
            seconds: 等待秒数。
        """
        self.delays.append(seconds)
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------------
# 退避计算
# ---------------------------------------------------------------------------------
def test_backoff_is_exponential_jittered_capped_and_honors_retry_after() -> None:
    """指数退避 + [0.5, 1.0] 抖动 + 上限；Retry-After 取较大值，超过上限时放弃重试。"""
    policy = RetryPolicy(max_attempts=5, base_delay_s=1.0, max_delay_s=3.0, max_retry_after_s=10)
    assert backoff_delay(policy, 1, None, rng=lambda: 1.0) == 1.0
    assert backoff_delay(policy, 2, None, rng=lambda: 0.0) == 1.0  # 2 * 0.5
    assert backoff_delay(policy, 5, None, rng=lambda: 1.0) == 3.0  # 封顶
    assert backoff_delay(policy, 1, 7.0, rng=lambda: 1.0) == 7.0
    assert backoff_delay(policy, 1, 11.0) is None


# ---------------------------------------------------------------------------------
# 重试条件
# ---------------------------------------------------------------------------------
async def test_default_policy_never_retries(make_executor: ExecutorFactory) -> None:
    """默认 max_attempts=1：即使错误可重试、工具幂等也不重试。"""
    tool = flaky_tool(1, UpstreamError("down", retryable=True))
    registry = ToolRegistry()
    registry.register_function(
        tool, name="t", policy=ToolPolicy(access="public"), annotations=IDEMPOTENT
    )
    result = await make_executor(registry).execute(
        ToolInvocation(call_id="c", tool="t"), ToolContext()
    )
    assert result.error and result.attempts == 1 and tool.state.calls == 1  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("annotations", "error", "expected_calls"),
    [
        (IDEMPOTENT, UpstreamError("down", retryable=True), 3),
        (ToolAnnotations(), UpstreamError("down", retryable=True), 1),  # 未声明幂等
        (IDEMPOTENT, UpstreamError("bad", retryable=False), 1),  # 错误不可重试
    ],
)
async def test_retry_requires_retryable_error_and_idempotent_tool(
    make_executor: ExecutorFactory,
    telemetry: Any,
    annotations: ToolAnnotations,
    error: Exception,
    expected_calls: int,
) -> None:
    """只有“错误可重试 且 工具声明幂等”才重试；重试会记录指标与日志。"""
    tool = flaky_tool(2, error)
    registry = ToolRegistry()
    registry.register_function(
        tool, name="t", policy=ToolPolicy(access="public", retry=RETRY3), annotations=annotations
    )
    sleeper = SleepRecorder()
    result = await make_executor(registry, sleep=sleeper).execute(
        ToolInvocation(call_id="c", tool="t"), ToolContext()
    )
    assert tool.state.calls == expected_calls  # type: ignore[attr-defined]
    if expected_calls == 3:
        assert result.ok and result.attempts == 3
        assert len(sleeper.delays) == 2 and all(0.005 <= d <= 0.05 for d in sleeper.delays)
        assert len(telemetry.logs.events("tool.call.retry")) == 2
        assert sum(p.value for p in telemetry.metric_points("app.tool.retries")) == 2
    else:
        assert result.error and result.attempts == 1 and sleeper.delays == []


async def test_retry_stops_when_deadline_would_be_exceeded(make_executor: ExecutorFactory) -> None:
    """退避之后剩余时间不足时放弃重试，返回最后一次的错误。"""
    tool = flaky_tool(5, UpstreamError("down", retryable=True, retry_after=1.0))
    registry = ToolRegistry()
    registry.register_function(
        tool,
        name="t",
        policy=ToolPolicy(access="public", retry=RetryPolicy(max_attempts=5)),
        annotations=IDEMPOTENT,
    )
    sleeper = SleepRecorder()
    result = await make_executor(registry, sleep=sleeper).execute(
        ToolInvocation(call_id="c", tool="t"), ToolContext.with_timeout(0.5)
    )
    assert result.error and result.error.type == "upstream"
    assert tool.state.calls == 1 and sleeper.delays == []  # type: ignore[attr-defined]


async def test_cancellation_during_backoff_propagates_and_releases_quota(
    make_executor: ExecutorFactory,
) -> None:
    """退避等待期间调用方取消：CancelledError 原样传播，并发额度全部归还。"""
    tool = flaky_tool(5, UpstreamError("down", retryable=True))
    registry = ToolRegistry()
    registry.register_function(
        tool,
        name="t",
        policy=ToolPolicy(
            access="public", retry=RetryPolicy(max_attempts=5, base_delay_s=10, max_delay_s=10)
        ),
        annotations=IDEMPOTENT,
    )
    executor = make_executor(registry)
    task = asyncio.create_task(
        executor.execute(ToolInvocation(call_id="c", tool="t"), ToolContext())
    )
    await asyncio.sleep(0.05)
    assert executor.limiter.inflight == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert executor.limiter.inflight == 0


async def test_sync_retries_release_quota_only_after_all_orphan_threads(
    make_executor: ExecutorFactory,
) -> None:
    """同步工具每次超时都留下一个仍在运行的线程；额度要等调用结束且所有线程都结束才归还。"""
    release = threading.Event()

    def blocking() -> str:
        """测试用：阻塞到测试放行。"""
        release.wait(5)
        return "late"

    registry = ToolRegistry()
    registry.register_function(
        blocking,
        name="b",
        policy=ToolPolicy(
            access="public", timeout_s=0.03, retry=RetryPolicy(max_attempts=2, base_delay_s=0)
        ),
        annotations=IDEMPOTENT,
    )
    executor = make_executor(registry, sync_workers=4)
    result = await executor.execute(ToolInvocation(call_id="c", tool="b"), ToolContext())
    assert result.error and result.error.code == "attempt_timeout" and result.attempts == 2
    assert executor.thread_runner.orphaned == 2
    assert executor.limiter.inflight == 1
    release.set()
    for _ in range(200):
        if executor.limiter.inflight == 0 and executor.thread_runner.orphaned == 0:
            break
        await asyncio.sleep(0.01)
    assert (executor.limiter.inflight, executor.thread_runner.orphaned) == (0, 0)


def test_lease_waits_for_every_held_future() -> None:
    """租约：调用结束后，所有登记的后台任务都结束才归还（顺序无关）。"""
    import concurrent.futures

    async def scenario() -> list[str]:
        """在事件循环中模拟两个后台任务。"""
        released: list[str] = []
        lease = ExecutionLease(asyncio.get_running_loop())
        lease.add(lambda: released.append("quota"))
        first: concurrent.futures.Future[Any] = concurrent.futures.Future()
        second: concurrent.futures.Future[Any] = concurrent.futures.Future()
        lease.hold_until(first)
        lease.hold_until(second)
        first.set_result(None)
        await asyncio.sleep(0)
        lease.finish()
        await asyncio.sleep(0)
        assert released == []
        second.set_result(None)
        await asyncio.sleep(0)
        return released

    assert asyncio.run(scenario()) == ["quota"]


async def test_http_post_needs_idempotency_header_to_retry(
    fake_network: Any, make_response: Any
) -> None:
    """HTTP POST 即使声明幂等，也只有在下游支持 Idempotency-Key 时才重试，且所有重试使用同一个键。"""
    schema = {"type": "object", "properties": {}, "additionalProperties": False}

    async def resolve(host: str, port: int) -> list[str]:
        """解析到公网地址。"""
        return ["93.184.216.34"]

    def executor_for(target: HttpTarget) -> ToolExecutor:
        """为目标创建执行器。"""
        registry = ToolRegistry()
        registry.register_http(
            target,
            name="api.call",
            input_schema=schema,
            policy=ToolPolicy(access="public", retry=RETRY3),
            annotations=IDEMPOTENT,
        )
        adapter = HttpAdapter(
            HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset({"api.example.com"}))),
            resolver=resolve,
            network_backend=fake_network,
        )
        return ToolExecutor(registry, adapters={"http": adapter}, sleep=SleepRecorder())

    plain = HttpTarget(method="POST", base_url="https://api.example.com")
    keyed = HttpTarget(method="POST", base_url="https://api.example.com", idempotency_header=True)
    fake_network.queue(make_response(503))
    async with executor_for(plain) as executor:
        r1 = await executor.execute(ToolInvocation(call_id="c", tool="api_call"), ToolContext())
    assert r1.error and r1.attempts == 1

    fake_network.queue(make_response(503), make_response(503), make_response(200, b"{}"))
    async with executor_for(keyed) as executor:
        r2 = await executor.execute(ToolInvocation(call_id="c", tool="api_call"), ToolContext())
    assert r2.ok and r2.attempts == 3
    keys = {fake_network.request(i)[2]["idempotency-key"] for i in (1, 2, 3)}
    assert len(keys) == 1


# ---------------------------------------------------------------------------------
# 速率限制
# ---------------------------------------------------------------------------------
class FakeClock:
    """可以手动推进的时钟。"""

    def __init__(self) -> None:
        """创建时钟，初始时刻为 0，由测试手动推进 ``now``。"""
        self.now = 0.0

    def monotonic(self) -> float:
        """返回测试设置的当前时刻（不会自动前进）。"""
        return self.now


async def test_token_bucket_burst_refill_and_fail_fast() -> None:
    """令牌桶：突发容量用完后立即失败并给出 retry_after；时间推进后补充令牌。"""
    clock = FakeClock()
    limiter = TokenBucketRateLimiter(clock=clock)
    limit = RateLimit(requests=2, per_s=1.0)
    await limiter.acquire("k", limit, max_wait_s=10)
    await limiter.acquire("k", limit, max_wait_s=10)
    with pytest.raises(RateLimitedError) as info:
        await limiter.acquire("k", limit, max_wait_s=10)
    assert info.value.retry_after == pytest.approx(0.5)
    clock.now = 0.5
    await limiter.acquire("k", limit, max_wait_s=10)


async def test_token_bucket_waits_within_budget_and_refunds_on_cancel() -> None:
    """允许等待时预留令牌并等待；等待被取消时退还令牌，取消信号原样传播。"""
    clock = FakeClock()
    sleeps: list[float] = []
    gate = asyncio.Event()

    async def sleep(seconds: float) -> None:
        """记录等待时长并阻塞到测试放行。"""
        sleeps.append(seconds)
        await gate.wait()

    limiter = TokenBucketRateLimiter(clock=clock, sleep=sleep)
    limit = RateLimit(requests=1, per_s=1.0, max_wait_s=2.0)
    await limiter.acquire("k", limit, max_wait_s=10)
    waiter = asyncio.create_task(limiter.acquire("k", limit, max_wait_s=10))
    await asyncio.sleep(0)
    assert sleeps == [1.0]
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    # 退还后桶里是 0 个令牌（而不是 -1）：等待时间仍然是 1 秒
    with pytest.raises(RateLimitedError) as info:
        await limiter.acquire("k", limit, max_wait_s=0.5)
    assert info.value.retry_after == pytest.approx(1.0)


async def test_rate_limit_is_per_tool_and_tenant(make_executor: ExecutorFactory) -> None:
    """执行器按（工具，租户）限流：一个租户超限不影响另一个租户；本地限流错误不会触发重试。"""

    async def ping() -> str:
        """测试用：返回 pong。"""
        return "pong"

    registry = ToolRegistry()
    registry.register_function(
        ping,
        name="ping",
        policy=ToolPolicy(
            access="public", rate_limit=RateLimit(requests=1, per_s=60), retry=RETRY3
        ),
        annotations=IDEMPOTENT,
    )
    sleeper = SleepRecorder()
    executor = make_executor(registry, sleep=sleeper)

    def ctx(tenant: str) -> ToolContext:
        """构造指定租户的上下文。"""
        return ToolContext(principal=Principal(tenant_id=tenant))

    inv = ToolInvocation(call_id="c", tool="ping")
    assert (await executor.execute(inv, ctx("a"))).ok
    limited = await executor.execute(inv, ctx("a"))
    assert limited.error and (limited.error.type, limited.error.code) == (
        "throttled",
        "rate_limited",
    )
    assert limited.attempts == 1 and sleeper.delays == []
    assert (await executor.execute(inv, ctx("b"))).ok


async def test_rate_limiter_evicts_only_idle_buckets() -> None:
    """桶数量超过上限时只清理已经回满的空闲桶，不会放宽仍在限流中的键。"""
    clock = FakeClock()
    limiter = TokenBucketRateLimiter(clock=clock, max_keys=2)
    limit = RateLimit(requests=1, per_s=100.0)
    await limiter.acquire("busy", limit, max_wait_s=0)
    await limiter.acquire("idle", RateLimit(requests=1, per_s=1000.0), max_wait_s=0)
    clock.now = 1.0  # busy 已回满，idle 仍为空
    await limiter.acquire("new", limit, max_wait_s=0)
    with pytest.raises(RateLimitedError):
        await limiter.acquire("idle", RateLimit(requests=1, per_s=1000.0), max_wait_s=0)
