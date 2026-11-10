"""共享状态参考实现（``RedisResultStore``、``RedisCircuitBreaker``）的测试。

两层：

* 默认运行：用内存中的 :class:`FakeRedis` 代替 Redis，时间可控。``GET`` / ``SET PX`` 直接模拟；
  两个熔断脚本按与 Lua **相同的算法**用 Python 实现。这一层验证的是 Python 一侧：键与参数约定、
  许可的代、探测名额、后台记录、故障模式、多个实例共享状态，以及接入执行器后的行为。它
  **不能**证明 Lua 脚本本身正确，也不能证明真实 Redis 的过期、类型与回复格式与假设一致；
* ``redis`` 标记（默认不运行）：对真实 Redis 执行真正的命令与 Lua 脚本。需要设置环境变量
  ``AGENT_TOOLS_REDIS_URL`` 并安装 ``redis`` 包::

      AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 \\
          uv run --with redis pytest -m redis tests/test_redis_state.py

  真实测试只使用带随机前缀的键，结束时逐个删除自己创建的键，不清空数据库。
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest

from agent_tools import (
    CachePolicy,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    DedupePolicy,
    Principal,
    ResultStore,
    ToolAnnotations,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    UpstreamError,
)
from agent_tools.governance.redis_circuit import (
    ACQUIRE_SCRIPT,
    RECORD_SCRIPT,
    RedisCircuitBreaker,
)
from agent_tools.governance.redis_store import RedisResultStore


class FakeRedis:
    """内存中的假 Redis：``GET``、``SET PX`` 和两个熔断脚本，时间由测试控制。

    Attributes:
        now_ms: 假的服务端时间（毫秒）。
        strings: 字符串键 → ``(值, 过期时刻毫秒)``。
        hashes: 哈希键 → 字段（熔断状态；数值字段用 ``int`` 保存）。
        ttl_ms: 哈希键 → 最近一次设置的过期时间。
        calls: 每次操作的 ``(操作名, 键)``。
        fail: 不为 ``None`` 时每次操作都抛出它（模拟 Redis 不可用）。
        hang: 为 True 时操作永不返回（模拟网络挂起）。
        reply_override: 不为 ``None`` 时 ``eval`` 直接返回它（模拟格式错误的回复）。
        decode: 为 True 时 ``get`` 返回 ``str``（模拟开启了 ``decode_responses``）。
    """

    def __init__(self) -> None:
        """创建空的假 Redis，时间从 1_000_000 毫秒开始。"""
        self.now_ms = 1_000_000
        self.strings: dict[str, tuple[bytes, int]] = {}
        self.hashes: dict[str, dict[str, Any]] = {}
        self.ttl_ms: dict[str, int] = {}
        self.calls: list[tuple[str, str]] = []
        self.fail: Exception | None = None
        self.hang = False
        self.reply_override: Any = None
        self.decode = False

    async def _enter(self, op: str, key: str) -> None:
        """每次操作的公共前置：让出一次事件循环、记录调用、模拟挂起或失败。

        Args:
            op: 操作名。
            key: 键。
        """
        await asyncio.sleep(0)
        self.calls.append((op, key))
        if self.hang:
            await asyncio.Event().wait()
        if self.fail is not None:
            raise self.fail

    async def get(self, name: str) -> Any:
        """模拟 ``GET``：过期的键视为不存在。

        Args:
            name: 键。

        Returns:
            值；不存在或已过期时为 ``None``。
        """
        await self._enter("get", name)
        item = self.strings.get(name)
        if item is None or item[1] <= self.now_ms:
            return None
        return item[0].decode("latin-1") if self.decode else item[0]

    async def set(self, name: str, value: bytes, *, px: int) -> Any:
        """模拟 ``SET name value PX px``。

        Args:
            name: 键。
            value: 值。
            px: 过期时间（毫秒，必须是正整数，与真实 Redis 一致）。

        Returns:
            ``True``。
        """
        await self._enter("set", name)
        assert isinstance(px, int) and px > 0
        self.strings[name] = (bytes(value), self.now_ms + px)
        return True

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
        name = "acquire" if script is ACQUIRE_SCRIPT else "record"
        assert script in (ACQUIRE_SCRIPT, RECORD_SCRIPT)
        await self._enter(name, key)
        if self.reply_override is not None:
            return self.reply_override
        ttl = self.ttl_ms.get(key)
        # 模拟键过期：超过过期时间没有写入的哈希消失
        if ttl is not None and self.hashes.get(key, {}).get("_touched", 0) + ttl <= self.now_ms:
            self.hashes.pop(key, None)
            self.ttl_ms.pop(key, None)
        if name == "acquire":
            return self._acquire(key, *(int(a) for a in args))
        generation, probe, outcome, fail_threshold, success_threshold, ttl_ms = args
        reply = self._record(
            key,
            int(generation),
            probe,
            outcome,
            int(fail_threshold),
            int(success_threshold),
            int(ttl_ms),
        )
        if not self.hashes.get(key, True):
            del self.hashes[key]  # 脚本没有写入任何字段：真实 Redis 不会创建空哈希
        return reply

    def _touch(self, key: str, ttl_ms: int) -> None:
        """模拟 ``PEXPIRE``：记录过期时间与最近一次续期的时刻。

        Args:
            key: 键。
            ttl_ms: 过期时间（毫秒）。
        """
        self.ttl_ms[key] = ttl_ms
        self.hashes[key]["_touched"] = self.now_ms

    def _acquire(self, key: str, open_ms: int, max_probes: int, probe_ttl: int, ttl: int) -> Any:
        """准入脚本的 Python 版本（与 ``ACQUIRE_SCRIPT`` 逐步对应）。

        Args:
            key: 键。
            open_ms: 开启时长。
            max_probes: 半开探测名额。
            probe_ttl: 探测租约有效期。
            ttl: 键的过期时间。

        Returns:
            ``[allowed, 代, 距离半开的毫秒数, 状态, 失败计数, 是否切换]``。
        """
        d = self.hashes.get(key, {})
        state = d.get("state", "closed")
        gen = d.get("gen", 0)
        changed = 0
        if state == "open":
            elapsed = self.now_ms - d.get("opened_ms", 0)
            if elapsed < 0:
                elapsed = 0
                d["opened_ms"] = self.now_ms
                self._touch(key, ttl)
            if elapsed < open_ms:
                return [0, gen, open_ms - elapsed, b"open", 0, 0]
            state, gen, changed = "half_open", gen + 1, 1
            d.update(state=state, gen=gen, fail=0, succ=0, probes=0)
            self._touch(key, ttl)
        if state == "half_open":
            probes = d.get("probes", 0)
            if probes > 0 and self.now_ms - d.get("probe_ms", 0) >= probe_ttl:
                probes = 0
            if probes >= max_probes:
                return [0, gen, 0, b"half_open", 0, changed]
            d.update(probes=probes + 1, probe_ms=self.now_ms)
            self._touch(key, ttl)
            return [1, gen, 0, b"half_open", 0, changed]
        return [1, gen, 0, b"closed", d.get("fail", 0), 0]

    def _record(
        self,
        key: str,
        generation: int,
        probe: str,
        outcome: str,
        fail_threshold: int,
        success_threshold: int,
        ttl: int,
    ) -> Any:
        """记录脚本的 Python 版本（与 ``RECORD_SCRIPT`` 逐步对应）。

        Args:
            key: 键。
            generation: 许可的代。
            probe: ``'1'`` 表示许可占用了探测名额。
            outcome: ``success`` / ``failure`` / ``ignored``。
            fail_threshold: 失败阈值。
            success_threshold: 成功阈值。
            ttl: 键的过期时间。

        Returns:
            ``[是否切换, 状态]``。
        """
        d = self.hashes.setdefault(key, {})
        state = d.get("state", "closed")
        gen = d.get("gen", 0)
        if gen != generation:
            return [0, state.encode()]
        if probe == "1" and d.get("probes", 0) > 0:
            d["probes"] -= 1
        if outcome == "ignored":
            return [0, state.encode()]

        def trip() -> None:
            """切换到开启状态。"""
            d.update(state="open", gen=gen + 1, fail=0, succ=0, probes=0, opened_ms=self.now_ms)
            self._touch(key, ttl)

        if state == "half_open":
            if outcome == "failure":
                trip()
                return [1, b"open"]
            succ = d.get("succ", 0) + 1
            if succ >= success_threshold:
                d.update(state="closed", gen=gen + 1, fail=0, succ=0, probes=0)
                self._touch(key, ttl)
                return [1, b"closed"]
            d["succ"] = succ
            return [0, state.encode()]
        if state == "closed":
            if outcome == "success":
                if d.get("fail", 0) > 0:
                    d["fail"] = 0
                return [0, state.encode()]
            fail = d.get("fail", 0) + 1
            if fail >= fail_threshold:
                trip()
                return [1, b"open"]
            d["fail"] = fail
            self._touch(key, ttl)
        return [0, state.encode()]


def ctx_of(tenant: str, user: str = "u1", request_id: str = "req-1") -> ToolContext:
    """构造带租户、用户和固定请求 ID 的上下文。

    Args:
        tenant: 租户 ID。
        user: 用户 ID。
        request_id: 请求 ID（去重键的一部分）。

    Returns:
        调用上下文。
    """
    return ToolContext(request_id=request_id, principal=Principal(tenant_id=tenant, user_id=user))


# ---------------------------------------------------------------------------------
# RedisResultStore
# ---------------------------------------------------------------------------------
def test_store_satisfies_the_result_store_protocol() -> None:
    """参考实现满足 ``ResultStore`` 协议（由 mypy 检查赋值），并拒绝空前缀与非正数上限。"""
    store: ResultStore = RedisResultStore(FakeRedis(), prefix="p:")
    assert store is not None
    with pytest.raises(ValueError):
        RedisResultStore(FakeRedis(), prefix="")
    with pytest.raises(ValueError):
        RedisResultStore(FakeRedis(), prefix="p:", max_value_bytes=0)


async def test_store_round_trip_prefix_and_expiry() -> None:
    """读写往返：键带前缀；过期时间向上取整到毫秒；到期后读不到；不同前缀互不可见。"""
    redis = FakeRedis()
    store = RedisResultStore(redis, prefix="app:cache:")
    other = RedisResultStore(redis, prefix="app:dedupe:")

    assert await store.get("cache:abc") is None
    await store.set("cache:abc", b"\x00value\xff", 1.2001)
    assert redis.strings["app:cache:cache:abc"] == (b"\x00value\xff", redis.now_ms + 1201)
    assert await store.get("cache:abc") == b"\x00value\xff"
    assert await other.get("cache:abc") is None

    redis.now_ms += 1200
    assert await store.get("cache:abc") == b"\x00value\xff"
    redis.now_ms += 1
    assert await store.get("cache:abc") is None


@pytest.mark.parametrize("ttl_s", [0, -1, float("nan"), float("inf")])
async def test_store_ignores_non_positive_or_non_finite_ttl(ttl_s: float) -> None:
    """有效期不是正的有限数时不写入，也不向 Redis 发送命令。"""
    redis = FakeRedis()
    await RedisResultStore(redis, prefix="p:").set("k", b"v", ttl_s)
    assert redis.strings == {} and redis.calls == []


async def test_store_rejects_oversized_values_and_decoded_replies() -> None:
    """超过上限的值抛出异常而不是静默丢弃；客户端返回字符串时报错而不是猜测编码；
    ``bytearray`` 回复被转换成 ``bytes``。"""
    redis = FakeRedis()
    store = RedisResultStore(redis, prefix="p:", max_value_bytes=4)
    with pytest.raises(ValueError):
        await store.set("big", b"12345", 10)
    assert redis.strings == {}
    await store.set("ok", b"1234", 10)
    redis.decode = True
    with pytest.raises(TypeError):
        await store.get("ok")
    redis.decode = False
    redis.strings["p:arr"] = (bytearray(b"ab"), redis.now_ms + 1000)  # type: ignore[assignment]
    value = await store.get("arr")
    assert value == b"ab" and type(value) is bytes


def cached_executors(redis: FakeRedis, calls: list[str]) -> list[ToolExecutor]:
    """创建两个共用同一个 Redis 结果缓存的执行器（代表两个实例）。

    Args:
        redis: 假 Redis。
        calls: 记录真实执行（元素为执行器编号）。

    Returns:
        两个执行器；工具 ``lookup`` 只读、按租户范围缓存 60 秒。
    """
    executors = []
    policy = ToolPolicy(access="public", cache=CachePolicy(ttl_s=60, scope="tenant"))
    for index in range(2):

        async def lookup(q: str, _index: int = index) -> str:
            """查询（测试替身）：返回值带上执行它的实例编号。"""
            calls.append(f"i{_index}")
            return f"{q}@i{_index}"

        registry = ToolRegistry()
        registry.register_function(
            lookup,
            name="lookup",
            policy=policy,
            annotations=ToolAnnotations(read_only=True),
            args_model=None,
        )
        executors.append(
            ToolExecutor(registry, result_cache=RedisResultStore(redis, prefix="t:cache:"))
        )
    return executors


async def test_cache_is_shared_across_instances_but_not_across_tenants() -> None:
    """结果缓存：实例 0 写入后实例 1 命中（不执行工具，结果标明复用）；另一个租户不命中；
    过期后重新执行。"""
    redis, calls = FakeRedis(), []
    first, second = cached_executors(redis, calls)
    invocation = ToolInvocation(call_id="c", tool="lookup", arguments={"q": "x"})

    a = await first.execute(invocation, ctx_of("t1"))
    b = await second.execute(invocation, ctx_of("t1", user="u2"))
    assert (a.to_model_text(), a.reused) == ("x@i0", None)
    assert (b.to_model_text(), b.reused) == ("x@i0", "cache")
    other = await second.execute(invocation, ctx_of("t2"))
    assert (other.to_model_text(), other.reused) == ("x@i1", None)
    assert calls == ["i0", "i1"]
    assert all(key.startswith("t:cache:cache:") for key in redis.strings)
    assert not any("t1" in key or "t2" in key for key in redis.strings)  # 键里没有明文租户

    redis.now_ms += 60_001
    again = await second.execute(invocation, ctx_of("t1"))
    assert (again.to_model_text(), again.reused) == ("x@i1", None)
    for executor in (first, second):
        await executor.aclose()


async def test_cache_store_failure_is_a_miss() -> None:
    """缓存存储不可用（读写都失败）时按未命中处理：工具照常执行，结果正常返回。"""
    redis, calls = FakeRedis(), []
    first, second = cached_executors(redis, calls)
    redis.fail = ConnectionError("redis down")
    invocation = ToolInvocation(call_id="c", tool="lookup", arguments={"q": "x"})
    for executor in (first, second):
        result = await executor.execute(invocation, ctx_of("t1"))
        assert result.ok and not result.reused
    assert calls == ["i0", "i1"]
    for executor in (first, second):
        await executor.aclose()


def dedupe_executors(
    redis: FakeRedis, calls: list[str], gate: asyncio.Event | None = None, **store_kw: Any
) -> list[ToolExecutor]:
    """创建两个共用同一个 Redis 去重存储的执行器（代表两个实例）。

    Args:
        redis: 假 Redis。
        calls: 记录真实执行（元素为执行器编号）。
        gate: 不为 ``None`` 时工具等它被设置后才返回（制造并发）。
        **store_kw: 传给 ``RedisResultStore`` 的其他参数。

    Returns:
        两个执行器；工具 ``charge`` 声明了 60 秒的去重窗口。
    """
    executors = []
    policy = ToolPolicy(access="public", dedupe=DedupePolicy(window_s=60))
    for index in range(2):

        async def charge(amount: int, _index: int = index) -> str:
            """扣款（测试替身）：有副作用的工具。"""
            calls.append(f"i{_index}")
            if gate is not None:
                await gate.wait()
            return f"charged {amount} by i{_index}"

        registry = ToolRegistry()
        registry.register_function(charge, name="charge", policy=policy)
        store = RedisResultStore(redis, prefix="t:dedupe:", **store_kw)
        executors.append(ToolExecutor(registry, dedupe_store=store))
    return executors


async def test_completed_call_is_replayed_on_another_instance() -> None:
    """调用去重：实例 0 完成并记录的调用，被重复提交到实例 1 时重放记录的结果、不再执行；
    请求 ID、参数或租户不同的调用是不同的调用。"""
    redis, calls = FakeRedis(), []
    first, second = dedupe_executors(redis, calls)
    invocation = ToolInvocation(call_id="c1", tool="charge", arguments={"amount": 5})

    done = await first.execute(invocation, ctx_of("t1"))
    replay = await second.execute(invocation, ctx_of("t1"))
    assert (done.to_model_text(), done.reused) == ("charged 5 by i0", None)
    assert (replay.to_model_text(), replay.reused) == ("charged 5 by i0", "dedupe")
    assert calls == ["i0"]

    for ctx, args in (
        (ctx_of("t1", request_id="req-2"), {"amount": 5}),
        (ctx_of("t1"), {"amount": 6}),
        (ctx_of("t2"), {"amount": 5}),
    ):
        fresh = await second.execute(
            ToolInvocation(call_id="c1", tool="charge", arguments=args), ctx
        )
        assert fresh.ok and not fresh.reused
    assert calls == ["i0", "i1", "i1", "i1"]
    for executor in (first, second):
        await executor.aclose()


async def test_concurrent_duplicates_on_two_instances_both_execute() -> None:
    """**不提供的保证**：同一个调用同时到达两个实例时，两边都读不到记录，都会执行。
    共享存储只能重放已经完成的调用，不是跨实例的并发协调或严格一次执行。"""
    redis, calls, gate = FakeRedis(), [], asyncio.Event()
    first, second = dedupe_executors(redis, calls, gate)
    invocation = ToolInvocation(call_id="c1", tool="charge", arguments={"amount": 5})

    tasks = [
        asyncio.create_task(executor.execute(invocation, ctx_of("t1")))
        for executor in (first, second)
    ]
    while len(calls) < 2:
        await asyncio.sleep(0)
    gate.set()
    results = await asyncio.gather(*tasks)
    assert sorted(calls) == ["i0", "i1"]
    assert all(r.ok and not r.reused for r in results)
    for executor in (first, second):
        await executor.aclose()


async def test_dedupe_store_failure_fails_closed() -> None:
    """去重存储读不到时不执行工具（``dedupe_unavailable``）：不知道是否执行过，失败即关闭。"""
    redis, calls = FakeRedis(), []
    first, _ = dedupe_executors(redis, calls)
    redis.fail = ConnectionError("redis down")
    result = await first.execute(
        ToolInvocation(call_id="c1", tool="charge", arguments={"amount": 5}), ctx_of("t1")
    )
    assert result.error is not None and result.error.code == "dedupe_unavailable"
    assert calls == []
    await first.aclose()


async def test_oversized_dedupe_record_is_not_written() -> None:
    """记录超过存储上限时不写入：第一次调用的结果照常返回，Redis 中没有留下记录
    （之后的重复提交会再次执行——上限需要按结果大小规划）。"""
    redis, calls = FakeRedis(), []
    first, second = dedupe_executors(redis, calls, max_value_bytes=8)
    invocation = ToolInvocation(call_id="c1", tool="charge", arguments={"amount": 5})
    done = await first.execute(invocation, ctx_of("t1"))
    assert done.ok
    assert redis.strings == {}
    again = await second.execute(invocation, ctx_of("t1"))
    assert again.ok and not again.reused
    assert calls == ["i0", "i1"]
    for executor in (first, second):
        await executor.aclose()


# ---------------------------------------------------------------------------------
# RedisCircuitBreaker
# ---------------------------------------------------------------------------------
CONFIG = CircuitBreakerConfig(failure_threshold=3, open_s=30.0, half_open_max_calls=1)


def breakers(redis: FakeRedis, count: int = 2, **kw: Any) -> list[RedisCircuitBreaker]:
    """创建若干共用同一个 Redis 与前缀的熔断器（代表若干实例）。

    Args:
        redis: 假 Redis。
        count: 实例数。
        **kw: 传给 ``RedisCircuitBreaker`` 的其他参数（默认使用 :data:`CONFIG`）。

    Returns:
        熔断器列表。
    """
    kw.setdefault("config", CONFIG)
    return [RedisCircuitBreaker(redis, prefix="t:circuit:", **kw) for _ in range(count)]


async def attempt(breaker: RedisCircuitBreaker, outcome: str, key: str = "tool:x") -> None:
    """完成一次“准入 → 记录结果 → 等待后台写入”。

    Args:
        breaker: 熔断器。
        outcome: 结果分类。
        key: 隔离键。
    """
    permit = await breaker.acquire(key)
    permit.record(outcome)  # type: ignore[arg-type]
    await breaker.aclose()


async def trip(redis: FakeRedis, pair: list[RedisCircuitBreaker], key: str = "tool:x") -> None:
    """让两个实例交替记录失败，直到熔断开启。

    Args:
        redis: 假 Redis（未使用，只为调用处可读）。
        pair: 熔断器列表。
        key: 隔离键。
    """
    for index in range(CONFIG.failure_threshold):
        await attempt(pair[index % len(pair)], "failure", key)


def test_breaker_satisfies_the_protocol_and_validates_options() -> None:
    """参考实现满足 ``CircuitBreaker`` 协议（由 mypy 检查赋值）；非法的超时与故障模式被拒绝。"""
    breaker: CircuitBreaker = RedisCircuitBreaker(FakeRedis())
    assert breaker.scope == "tool"
    assert breaker.is_failure(UpstreamError("down", retryable=True))
    assert not breaker.is_failure(UpstreamError("bad request", retryable=False))
    for kw in ({"op_timeout_s": 0}, {"probe_ttl_s": -1}, {"idle_ttl_s": 0}, {"on_error": "open"}):
        with pytest.raises(ValueError):
            RedisCircuitBreaker(FakeRedis(), **kw)  # type: ignore[arg-type]


async def test_failures_from_all_instances_open_one_shared_circuit() -> None:
    """两个实例的失败合计达到阈值后，熔断对两个实例同时开启；``retry_after`` 随时间减少；
    其他隔离键不受影响；键经过哈希并带过期时间（不短于开启时长的两倍）。"""
    redis = FakeRedis()
    first, second = breakers(redis)
    await attempt(first, "failure")
    await attempt(second, "failure")
    await second.acquire("tool:x")  # 还没到阈值：仍然放行
    await attempt(first, "failure")

    for breaker in (first, second):
        with pytest.raises(CircuitOpenError) as info:
            await breaker.acquire("tool:x")
        assert info.value.code == "circuit_open"
        assert info.value.retry_after == 30.0
    redis.now_ms += 12_500
    with pytest.raises(CircuitOpenError) as info:
        await second.acquire("tool:x")
    assert info.value.retry_after == 17.5

    await second.acquire("tool:y")
    key = first.redis_key("tool:x")
    assert key.startswith("t:circuit:") and "tool:x" not in key
    assert redis.hashes[key]["state"] == "open"
    assert redis.ttl_ms[key] == 3_600_000
    long_open = RedisCircuitBreaker(redis, CircuitBreakerConfig(open_s=7200.0))
    assert long_open._ttl_ms == 14_400_000


async def test_success_resets_the_shared_failure_count() -> None:
    """关闭状态下的成功把共享的失败计数清零：之后需要重新累计到阈值才会开启。"""
    redis = FakeRedis()
    first, second = breakers(redis)
    await attempt(first, "failure")
    await attempt(second, "failure")
    await attempt(first, "success")
    await attempt(second, "failure")
    await attempt(first, "failure")
    await first.acquire("tool:x")  # 2 次失败，未到阈值
    await attempt(second, "failure")
    with pytest.raises(CircuitOpenError):
        await first.acquire("tool:x")


async def test_healthy_calls_cost_one_round_trip() -> None:
    """关闭且没有失败计数时：成功与“不计入”的结果不产生第二次 Redis 往返，也不创建键。"""
    redis = FakeRedis()
    (breaker,) = breakers(redis, 1)
    await attempt(breaker, "success")
    await attempt(breaker, "ignored")
    assert [op for op, _ in redis.calls] == ["acquire", "acquire"]
    assert redis.hashes == {}


async def test_half_open_probes_are_limited_across_instances() -> None:
    """开启时长过后进入半开：所有实例合计只放行一个探测；探测成功后对所有实例关闭。"""
    redis = FakeRedis()
    first, second = breakers(redis)
    await trip(redis, [first, second])
    redis.now_ms += 30_000

    probe = await first.acquire("tool:x")
    for breaker in (first, second):
        with pytest.raises(CircuitOpenError) as info:
            await breaker.acquire("tool:x")
        assert info.value.code == "circuit_half_open"
    probe.record("success")
    probe.record("failure")  # 重复记录不生效
    await first.aclose()

    for breaker in (first, second):
        await breaker.acquire("tool:x")
    assert redis.hashes[first.redis_key("tool:x")]["state"] == "closed"


async def test_failed_probe_reopens_and_restarts_the_timer() -> None:
    """探测失败：重新开启并重新计时；``success_threshold`` 大于 1 时需要累计多次探测成功。"""
    redis = FakeRedis()
    config = CircuitBreakerConfig(
        failure_threshold=3, open_s=30.0, half_open_max_calls=2, success_threshold=2
    )
    first, second = breakers(redis, config=config)
    await trip(redis, [first, second])
    redis.now_ms += 30_000
    await attempt(first, "failure")
    with pytest.raises(CircuitOpenError) as info:
        await second.acquire("tool:x")
    assert info.value.code == "circuit_open" and info.value.retry_after == 30.0

    redis.now_ms += 30_000
    one = await first.acquire("tool:x")
    two = await second.acquire("tool:x")
    with pytest.raises(CircuitOpenError):
        await first.acquire("tool:x")  # 两个名额都被占用
    one.record("success")
    await first.aclose()
    assert redis.hashes[first.redis_key("tool:x")]["state"] == "half_open"
    two.record("success")
    await second.aclose()
    assert redis.hashes[first.redis_key("tool:x")]["state"] == "closed"


async def test_stale_permits_cannot_change_a_newer_state() -> None:
    """状态切换之前发放的许可事后才完成：结果被忽略（慢请求的成功不会关闭刚开启的熔断，
    旧探测的失败也不会重新开启已经关闭的熔断）。"""
    redis = FakeRedis()
    first, second = breakers(redis)
    await attempt(first, "failure")  # 让失败计数不为 0，这样慢请求的成功会真正写入
    slow = await second.acquire("tool:x")
    await attempt(first, "failure")
    await attempt(first, "failure")
    slow.record("success")
    await second.aclose()
    with pytest.raises(CircuitOpenError):
        await first.acquire("tool:x")

    redis.now_ms += 30_000
    await attempt(first, "success")  # 探测成功 → 关闭
    late = RedisCircuitBreaker(redis, CONFIG, prefix="t:circuit:")
    late._record_later("tool:x", 1, "half_open", 0, "failure")  # 代 1 的旧探测迟到的失败
    await late.aclose()
    await second.acquire("tool:x")
    assert redis.hashes[first.redis_key("tool:x")]["state"] == "closed"


async def test_cancelled_probe_releases_its_slot() -> None:
    """探测被取消（记录“不计入”）：名额立即释放，状态不变，下一个调用可以继续探测。"""
    redis = FakeRedis()
    first, second = breakers(redis)
    await trip(redis, [first, second])
    redis.now_ms += 30_000
    await attempt(first, "ignored")
    probe = await second.acquire("tool:x")
    assert redis.hashes[first.redis_key("tool:x")]["state"] == "half_open"
    probe.record("success")
    await second.aclose()


async def test_lost_probe_lease_expires() -> None:
    """占用探测名额的实例没有归还（崩溃）：租约过期前其他实例一直被拒绝，过期后恢复探测。"""
    redis = FakeRedis()
    first, second = breakers(redis, probe_ttl_s=10.0)
    await trip(redis, [first, second])
    redis.now_ms += 30_000
    await first.acquire("tool:x")  # 永不归还
    redis.now_ms += 9_999
    with pytest.raises(CircuitOpenError):
        await second.acquire("tool:x")
    redis.now_ms += 1
    await attempt(second, "success")
    assert redis.hashes[first.redis_key("tool:x")]["state"] == "closed"


async def test_clock_going_backwards_never_extends_the_open_period() -> None:
    """Redis 时钟回退（主从切换）：开启时长从回退后的时刻重新计，不会超过 ``open_s``。"""
    redis = FakeRedis()
    first, second = breakers(redis)
    await trip(redis, [first, second])
    redis.now_ms -= 500_000
    with pytest.raises(CircuitOpenError) as info:
        await first.acquire("tool:x")
    assert info.value.retry_after == 30.0
    redis.now_ms += 30_000
    await second.acquire("tool:x")


async def test_expired_key_means_closed() -> None:
    """状态哈希过期后等同于关闭：失败计数被遗忘，持有旧代许可的记录被忽略。"""
    redis = FakeRedis()
    first, second = breakers(redis, idle_ttl_s=100.0)
    await trip(redis, [first, second])
    redis.now_ms += 100_000
    permit = await first.acquire("tool:x")
    assert redis.hashes == {}
    permit.record("failure")
    await first.aclose()
    assert redis.hashes[first.redis_key("tool:x")]["fail"] == 1


async def test_state_changes_are_reported_to_the_triggering_instance() -> None:
    """状态切换回调只通知触发切换的实例；回调抛出的异常不影响熔断。"""
    redis = FakeRedis()
    seen: list[tuple[str, str, str, str]] = []

    def listener(name: str) -> Any:
        """生成记录切换的回调（第二个实例的回调总是抛出异常）。"""

        def on_change(key: str, old: str, new: str) -> None:
            """记录一次切换。"""
            seen.append((name, key, old, new))
            if name == "b":
                raise RuntimeError("listener failed")

        return on_change

    first = RedisCircuitBreaker(redis, CONFIG, prefix="t:", on_state_change=listener("a"))
    second = RedisCircuitBreaker(redis, CONFIG, prefix="t:", on_state_change=listener("b"))
    await attempt(first, "failure")
    await attempt(first, "failure")
    await attempt(second, "failure")
    redis.now_ms += 30_000
    await attempt(first, "success")
    assert seen == [
        ("b", "tool:x", "closed", "open"),
        ("a", "tool:x", "open", "half_open"),
        ("a", "tool:x", "half_open", "closed"),
    ]


@pytest.mark.parametrize(
    "broken",
    [
        {"fail": ConnectionError("redis down")},
        {"hang": True},
        {"reply_override": [1, 0]},
        {"reply_override": [1, 0, 0, b"melted", 0, 0]},
        {"reply_override": [1, -1, 0, b"closed", 0, 0]},
        {"reply_override": "OK"},
    ],
)
async def test_unavailable_redis_falls_back_to_the_local_breaker(broken: dict[str, Any]) -> None:
    """Redis 不可用（异常、超时、回复格式错误）且 ``on_error="local"``：改由本实例的进程内熔断器
    判断——调用继续放行，本实例累计到阈值后只对本实例开启；另一个实例不受影响；失败被计数。"""
    redis = FakeRedis()
    first, second = breakers(redis, op_timeout_s=0.02)
    for name, value in broken.items():
        setattr(redis, name, value)

    for _ in range(CONFIG.failure_threshold):
        permit = await first.acquire("tool:x")
        permit.record("failure")
    with pytest.raises(CircuitOpenError) as info:
        await first.acquire("tool:x")
    assert info.value.code == "circuit_open"
    assert first.failures == CONFIG.failure_threshold + 1
    await second.acquire("tool:x")  # 本地状态不共享
    assert redis.hashes == {}


async def test_fail_closed_rejects_when_redis_is_unavailable() -> None:
    """``on_error="fail_closed"``：Redis 不可用时拒绝尝试（``circuit_unavailable``）；
    Redis 恢复后恢复正常。"""
    redis = FakeRedis()
    (breaker,) = breakers(redis, 1, on_error="fail_closed")
    redis.fail = TimeoutError("redis down")
    with pytest.raises(CircuitOpenError) as info:
        await breaker.acquire("tool:x")
    assert info.value.code == "circuit_unavailable" and breaker.failures == 1
    redis.fail = None
    await attempt(breaker, "success")


async def test_record_failure_is_only_counted() -> None:
    """后台记录失败（Redis 在尝试期间变得不可用）：不抛出异常，只计数；这次结果丢失。"""
    redis = FakeRedis()
    (breaker,) = breakers(redis, 1)
    permit = await breaker.acquire("tool:x")
    redis.fail = ConnectionError("redis down")
    permit.record("failure")
    await breaker.aclose()
    assert breaker.failures == 1
    redis.fail = None
    redis.reply_override = ["x"]
    (await breaker.acquire("tool:y")).record("failure")  # 准入回复格式错误 → 本地回退的许可
    await breaker.aclose()
    assert breaker.failures == 2
    assert redis.hashes == {}


async def test_cancellation_during_acquire_propagates() -> None:
    """在 Redis 往返途中被取消：``CancelledError`` 原样传播，不会被当成 Redis 故障而回退放行。"""
    redis = FakeRedis()
    redis.hang = True
    (breaker,) = breakers(redis, 1, op_timeout_s=30.0)
    task = asyncio.create_task(breaker.acquire("tool:x"))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert breaker.failures == 0


async def test_two_executors_share_one_circuit() -> None:
    """接入执行器：实例 0 上的下游失败使熔断开启后，实例 1 的调用被直接拒绝、工具不执行；
    开启时长过后由一个探测调用恢复。"""
    redis = FakeRedis()
    calls: list[str] = []
    healthy = False

    async def flaky() -> str:
        """下游不健康时抛出可重试的上游错误。"""
        calls.append("x")
        if not healthy:
            raise UpstreamError("down", retryable=True)
        return "ok"

    pair = breakers(redis)
    executors = []
    for breaker in pair:
        registry = ToolRegistry()
        registry.register_function(flaky, name="flaky", policy=ToolPolicy(access="public"))
        executors.append(ToolExecutor(registry, circuit_breaker=breaker))
    invocation = ToolInvocation(call_id="c", tool="flaky", arguments={})

    for _ in range(CONFIG.failure_threshold):
        failed = await executors[0].execute(invocation, ctx_of("t1"))
        assert failed.error is not None and failed.error.type == "upstream"
        await pair[0].aclose()
    rejected = await executors[1].execute(invocation, ctx_of("t2"))
    assert rejected.error is not None and rejected.error.code == "circuit_open"
    assert len(calls) == CONFIG.failure_threshold

    healthy = True
    redis.now_ms += 30_000
    recovered = await executors[1].execute(invocation, ctx_of("t2"))
    assert recovered.ok
    await pair[1].aclose()
    assert (await executors[0].execute(invocation, ctx_of("t1"))).ok
    for executor in executors:
        await executor.aclose()


# ---------------------------------------------------------------------------------
# 真实 Redis（默认不运行）
# ---------------------------------------------------------------------------------
def _real_client() -> Any:
    """按环境变量创建真实 Redis 客户端；没有配置或没有安装 ``redis`` 包时跳过测试。

    Returns:
        ``redis.asyncio.Redis`` 客户端（不解码回复）。
    """
    url = os.environ.get("AGENT_TOOLS_REDIS_URL")
    if not url:
        pytest.skip("AGENT_TOOLS_REDIS_URL is not set")
    redis_asyncio = pytest.importorskip("redis.asyncio")
    return redis_asyncio.from_url(url)


@pytest.mark.redis
async def test_result_store_on_real_redis() -> None:
    """在真实 Redis 上验证结果存储：二进制值往返、``PX`` 过期、前缀隔离、字节回复。
    只删除自己创建的键。"""
    client = _real_client()
    prefix = f"agent-tools-test:{uuid.uuid4().hex}:"
    store = RedisResultStore(client, prefix=prefix + "a:")
    other = RedisResultStore(client, prefix=prefix + "b:")
    try:
        payload = bytes(range(256))
        await store.set("k", payload, 0.3)
        assert await store.get("k") == payload
        assert await other.get("k") is None
        assert 0 < await client.pttl(store.redis_key("k")) <= 300
        await asyncio.sleep(0.4)
        assert await store.get("k") is None
        assert await store.get("missing") is None
    finally:
        await client.delete(store.redis_key("k"), other.redis_key("k"))
        await client.aclose()


@pytest.mark.redis
async def test_circuit_scripts_on_real_redis() -> None:
    """在真实 Redis 上执行熔断的 Lua 脚本：两个实例合计开启、``retry_after``、半开探测名额、
    探测成功后关闭、旧许可被忽略、探测失败重新开启、键的过期时间。只删除自己创建的键。"""
    client = _real_client()
    prefix = f"agent-tools-test:{uuid.uuid4().hex}:"
    config = CircuitBreakerConfig(failure_threshold=2, open_s=0.5)
    first = RedisCircuitBreaker(client, config, prefix=prefix)
    second = RedisCircuitBreaker(client, config, prefix=prefix)
    key = first.redis_key("k")
    try:
        slow = await second.acquire("k")
        await attempt(first, "failure", "k")
        assert int(await client.hget(key, "fail")) == 1
        await attempt(second, "failure", "k")
        assert await client.hget(key, "state") == b"open"
        assert 0 < await client.pttl(key) <= 3_600_000
        with pytest.raises(CircuitOpenError) as info:
            await first.acquire("k")
        assert info.value.retry_after is not None and 0 < info.value.retry_after <= 0.5
        slow.record("success")  # 开启之前发放的许可
        await second.aclose()
        assert await client.hget(key, "state") == b"open"

        await asyncio.sleep(0.6)
        probe = await first.acquire("k")
        with pytest.raises(CircuitOpenError) as info:
            await second.acquire("k")
        assert info.value.code == "circuit_half_open"
        probe.record("failure")
        await first.aclose()
        assert await client.hget(key, "state") == b"open"

        await asyncio.sleep(0.6)
        await attempt(second, "success", "k")
        assert await client.hget(key, "state") == b"closed"
        await first.acquire("k")
        assert first.failures == 0 and second.failures == 0
    finally:
        await client.delete(key)
        await client.aclose()
