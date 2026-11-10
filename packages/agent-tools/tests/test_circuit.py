"""熔断（P3）的测试：状态切换（注入时钟，不真正等待）、错误分类、隔离键、并发竞争，
以及与执行器重试、取消、默认关闭行为的配合。"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

import pytest

from agent_tools import (
    CircuitBreakerConfig,
    CircuitOpenError,
    InProcessCircuitBreaker,
    RateLimitedError,
    RetryPolicy,
    ToolAnnotations,
    ToolContext,
    ToolExecutionError,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    ToolTimeoutError,
    UpstreamError,
    default_failure_classifier,
)
from agent_tools.binding import ToolBinding
from agent_tools.governance.circuit import circuit_key
from agent_tools.protocol.spec import HttpTarget, McpTarget, ToolSpec

PUBLIC = ToolPolicy(access="public")


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self) -> None:
        """从 1000 秒开始计时。"""
        self.now = 1000.0

    def monotonic(self) -> float:
        """返回当前时刻。"""
        return self.now


def breaker(clock: FakeClock, **kw: Any) -> InProcessCircuitBreaker:
    """创建使用注入时钟的熔断器。

    Args:
        clock: 时钟。
        **kw: ``CircuitBreakerConfig`` 字段。

    Returns:
        熔断器。
    """
    return InProcessCircuitBreaker(CircuitBreakerConfig(**kw), clock=clock)


async def fail(cb: InProcessCircuitBreaker, key: str = "k", times: int = 1) -> None:
    """记录若干次失败。

    Args:
        cb: 熔断器。
        key: 隔离键。
        times: 次数。
    """
    for _ in range(times):
        (await cb.acquire(key)).record("failure")


# ---------------------------------------------------------------------------------
# 状态机
# ---------------------------------------------------------------------------------
async def test_closed_opens_after_consecutive_failures_and_rejects() -> None:
    """连续失败达到阈值后开启；开启期间直接拒绝，并给出距离半开的剩余时间。"""
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=3, open_s=10)
    await fail(cb, times=2)
    assert cb.state("k") == "closed"
    await fail(cb)
    assert cb.state("k") == "open"
    clock.now += 4
    with pytest.raises(CircuitOpenError) as info:
        await cb.acquire("k")
    assert info.value.code == "circuit_open"
    assert info.value.retryable is True
    assert info.value.retry_after == pytest.approx(6.0)


async def test_success_resets_consecutive_failures() -> None:
    """关闭状态下一次成功会清零连续失败计数，“不连续”的失败不会开启熔断。"""
    cb = breaker(FakeClock(), failure_threshold=2)
    await fail(cb)
    (await cb.acquire("k")).record("success")
    await fail(cb)
    assert cb.state("k") == "closed"


async def test_ignored_outcomes_do_not_count() -> None:
    """``ignored``（例如参数错误、调用方取消）既不计失败也不计成功。"""
    cb = breaker(FakeClock(), failure_threshold=2)
    await fail(cb)
    for _ in range(5):
        (await cb.acquire("k")).record("ignored")
    await fail(cb)
    assert cb.state("k") == "open"


async def test_half_open_success_closes_and_failure_reopens() -> None:
    """开启时长到达后进入半开；探测成功达到阈值后关闭，探测失败则重新开启并重新计时。"""
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=1, open_s=5, success_threshold=2)
    await fail(cb)
    clock.now += 5
    assert cb.state("k") == "half_open"
    (await cb.acquire("k")).record("success")
    assert cb.state("k") == "half_open"
    (await cb.acquire("k")).record("success")
    assert cb.state("k") == "closed"

    await fail(cb)
    clock.now += 5
    (await cb.acquire("k")).record("failure")
    assert cb.state("k") == "open"
    clock.now += 4.9
    assert cb.state("k") == "open"
    clock.now += 0.1
    assert cb.state("k") == "half_open"


async def test_half_open_limits_concurrent_probes() -> None:
    """半开状态只允许 ``half_open_max_calls`` 个探测同时进行；探测结束后释放名额。"""
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=1, open_s=1, half_open_max_calls=1)
    await fail(cb)
    clock.now += 1
    probe = await cb.acquire("k")
    with pytest.raises(CircuitOpenError) as info:
        await cb.acquire("k")
    assert info.value.code == "circuit_half_open"
    probe.record("ignored")  # 例如调用方取消：释放名额，不改变状态
    assert cb.state("k") == "half_open"
    (await cb.acquire("k")).record("success")
    assert cb.state("k") == "closed"


async def test_stale_permit_cannot_close_a_newly_opened_circuit() -> None:
    """熔断开启前发出的慢请求事后成功，结果被忽略，不会关闭刚开启的熔断。"""
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=2, open_s=30)
    slow = await cb.acquire("k")
    await fail(cb, times=2)
    assert cb.state("k") == "open"
    slow.record("success")
    assert cb.state("k") == "open"
    slow.record("success")  # 重复记录只生效一次
    assert cb.state("k") == "open"


async def test_keys_are_isolated_and_capacity_keeps_open_circuits() -> None:
    """不同隔离键互不影响；超过跟踪上限时只淘汰关闭且无失败的键，开启的键不会被淘汰。"""
    cb = breaker(FakeClock(), failure_threshold=1, max_keys=2)
    await fail(cb, "a")
    assert cb.state("b") == "closed"
    for key in ("b", "c", "d"):
        (await cb.acquire(key)).record("success")
    assert cb.state("a") == "open"


async def test_state_change_callback_errors_are_ignored() -> None:
    """状态切换回调抛出异常时不影响熔断判断。"""
    seen: list[tuple[str, str, str]] = []

    def callback(key: str, old: str, new: str) -> None:
        """记录切换后抛出异常。"""
        seen.append((key, old, new))
        raise RuntimeError("telemetry down")

    cb = InProcessCircuitBreaker(
        CircuitBreakerConfig(failure_threshold=1), clock=FakeClock(), on_state_change=callback
    )
    await fail(cb)
    assert seen == [("k", "closed", "open")]
    assert cb.state("k") == "open"


def test_config_validation() -> None:
    """非法的阈值、时长和隔离粒度在构造时被拒绝。"""
    with pytest.raises(ValueError):
        CircuitBreakerConfig(failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreakerConfig(open_s=0)
    with pytest.raises(ValueError):
        CircuitBreakerConfig(scope="tenant")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------
# 错误分类与隔离键
# ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("error", "counted"),
    [
        (UpstreamError("5xx", status_code=503, retryable=True), True),
        (UpstreamError("4xx", status_code=400, retryable=False), False),
        (ToolTimeoutError("slow"), True),
        (ToolTimeoutError("deadline", code="deadline_exceeded", retryable=False), False),
        (ToolExecutionError("bug"), False),
        (RateLimitedError("local"), False),
        (CircuitOpenError("open"), False),
    ],
)
def test_default_failure_classifier(error: Any, counted: bool) -> None:
    """只有可重试的下游错误与单次尝试超时计入熔断失败。"""
    assert default_failure_classifier(error) is counted


def _binding(target: Any, name: str = "t") -> ToolBinding:
    """构造一个绑定（只用于计算隔离键）。

    Args:
        target: 执行目标。
        name: 工具名。

    Returns:
        绑定。
    """
    kind: Literal["http", "mcp"] = "http" if isinstance(target, HttpTarget) else "mcp"
    spec = ToolSpec(name=name, input_schema={"type": "object"}, kind=kind, policy=PUBLIC)
    return ToolBinding(spec=spec, target=target, input_validator=object())  # type: ignore[arg-type]


def test_circuit_keys_by_scope() -> None:
    """``target`` 粒度下同一主机的 HTTP 工具、同一服务端的 MCP 工具共享隔离键；``tool`` 粒度按工具名。"""
    a = _binding(HttpTarget(method="GET", base_url="https://API.example.com/v1"), "a")
    b = _binding(HttpTarget(method="POST", base_url="https://api.example.com:443/v2"), "b")
    m = _binding(McpTarget(server_id="files", remote_name="read"), "m")
    assert (
        circuit_key(a, "target") == circuit_key(b, "target") == "http:https://api.example.com:443"
    )
    assert circuit_key(m, "target") == "mcp:files"
    assert circuit_key(a, "tool") == "tool:a"
    assert circuit_key(a, "tool") != circuit_key(b, "tool")


# ---------------------------------------------------------------------------------
# 与执行器的配合
# ---------------------------------------------------------------------------------
def _flaky_registry(calls: list[str], **policy: Any) -> ToolRegistry:
    """注册一个总是返回可重试下游错误的工具和一个业务失败的工具。

    Args:
        calls: 记录真实调用。
        **policy: ``ToolPolicy`` 的额外字段。

    Returns:
        注册表。
    """
    registry = ToolRegistry()

    async def down() -> str:
        """模拟下游不可用。"""
        calls.append("down")
        raise UpstreamError("service unavailable", status_code=503, retryable=True)

    async def bad_input() -> str:
        """模拟工具业务失败（不计入熔断）。"""
        calls.append("bad")
        raise ToolExecutionError("business rule violated")

    registry.register_function(
        down,
        name="down",
        policy=ToolPolicy(access="public", **policy),
        annotations=ToolAnnotations(idempotent=True),
    )
    registry.register_function(bad_input, name="bad", policy=PUBLIC)
    return registry


async def test_executor_fails_fast_when_open(make_executor: Any, telemetry: Any) -> None:
    """达到阈值后执行器直接返回 ``circuit_open``，不再调用工具；业务错误不会开启熔断。"""
    calls: list[str] = []
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=2, open_s=10)
    executor = make_executor(_flaky_registry(calls), circuit_breaker=cb)
    ctx = ToolContext()
    for i in range(4):
        await executor.execute(ToolInvocation(call_id=f"b{i}", tool="bad"), ctx)
    assert cb.state("tool:bad") == "closed"

    results = [
        await executor.execute(ToolInvocation(call_id=f"d{i}", tool="down"), ctx) for i in range(4)
    ]
    assert [r.error.code for r in results if r.error] == [
        "upstream_error",
        "upstream_error",
        "circuit_open",
        "circuit_open",
    ]
    assert calls.count("down") == 2
    assert results[2].error is not None and results[2].error.type == "circuit_open"
    assert telemetry.logs.events("tool.call.circuit_rejected")

    clock.now += 10  # 半开：允许一次探测
    probe = await executor.execute(ToolInvocation(call_id="p", tool="down"), ctx)
    assert probe.error is not None and probe.error.code == "upstream_error"
    assert cb.state("tool:down") == "open"


async def test_circuit_is_checked_before_each_retry(make_executor: Any) -> None:
    """重试循环中每次尝试都先过熔断准入：熔断在重试过程中开启后，剩余的重试不再调用下游，
    而且 ``CircuitOpenError`` 本身不触发重试。"""
    calls: list[str] = []
    cb = breaker(FakeClock(), failure_threshold=2)
    registry = _flaky_registry(calls, retry=RetryPolicy(max_attempts=5, base_delay_s=0))
    executor = make_executor(registry, circuit_breaker=cb)
    result = await executor.execute(ToolInvocation(call_id="c", tool="down"), ToolContext())
    assert calls == ["down", "down"]
    assert result.error is not None and result.error.code == "circuit_open"
    assert result.attempts == 3


async def test_caller_cancellation_releases_half_open_probe(make_executor: Any) -> None:
    """半开探测进行中调用方取消：探测名额被释放（不计成功也不计失败），之后可以再次探测。"""
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=1, open_s=1)
    started = asyncio.Event()
    registry = ToolRegistry()

    async def slow() -> str:
        """一直等待直到被取消。"""
        started.set()
        await asyncio.sleep(30)
        return "late"

    registry.register_function(slow, name="slow", policy=PUBLIC)
    executor = make_executor(registry, circuit_breaker=cb)
    await fail(cb, "tool:slow")
    clock.now += 1
    task = asyncio.create_task(
        executor.execute(ToolInvocation(call_id="c", tool="slow"), ToolContext())
    )
    await started.wait()
    with pytest.raises(CircuitOpenError):
        await cb.acquire("tool:slow")  # 探测名额被占用
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cb.state("tool:slow") == "half_open"
    (await cb.acquire("tool:slow")).record("success")
    assert cb.state("tool:slow") == "closed"


async def test_concurrent_half_open_calls_admit_one_probe(make_executor: Any) -> None:
    """半开状态下并发到达的调用只有一个成为探测，其余立即收到 ``circuit_half_open``。"""
    clock = FakeClock()
    cb = breaker(clock, failure_threshold=1, open_s=1)
    gate = asyncio.Event()
    registry = ToolRegistry()

    async def ok() -> str:
        """等待放行后成功。"""
        await gate.wait()
        return "ok"

    registry.register_function(
        ok, name="ok", policy=PUBLIC, annotations=ToolAnnotations(concurrency_safe=True)
    )
    executor = make_executor(registry, circuit_breaker=cb)
    await fail(cb, "tool:ok")
    clock.now += 1
    batch = asyncio.create_task(
        executor.execute_many(
            [ToolInvocation(call_id=str(i), tool="ok") for i in range(3)], ToolContext()
        )
    )
    await asyncio.sleep(0.05)
    gate.set()
    results = await batch
    assert sorted(r.status for r in results) == ["error", "error", "success"]
    assert {r.error.code for r in results if r.error} == {"circuit_half_open"}
    assert cb.state("tool:ok") == "closed"


async def test_breaker_is_off_by_default(make_executor: Any) -> None:
    """没有配置熔断器时行为与 P2 相同：多次失败也不会出现 ``circuit_open``。"""
    calls: list[str] = []
    executor = make_executor(_flaky_registry(calls))
    for i in range(10):
        result = await executor.execute(ToolInvocation(call_id=str(i), tool="down"), ToolContext())
        assert result.error is not None and result.error.code == "upstream_error"
    assert len(calls) == 10


async def test_broken_host_classifier_is_ignored(make_executor: Any) -> None:
    """宿主提供的分类函数抛出异常时按“不计入”处理，调用结果不受影响。"""

    def classifier(error: Any) -> bool:
        """总是抛出异常的分类函数。"""
        raise RuntimeError("bug")

    calls: list[str] = []
    cb = InProcessCircuitBreaker(
        CircuitBreakerConfig(failure_threshold=1), clock=FakeClock(), classifier=classifier
    )
    executor = make_executor(_flaky_registry(calls), circuit_breaker=cb)
    for i in range(3):
        result = await executor.execute(ToolInvocation(call_id=str(i), tool="down"), ToolContext())
        assert result.error is not None and result.error.code == "upstream_error"
    assert cb.state("tool:down") == "closed"
