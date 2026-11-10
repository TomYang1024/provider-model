"""结果缓存与调用去重（P3）的测试：租户 / 用户 / 权限隔离、鉴权不可绕过、过期、容量、
并发单飞、失败与取消、存储故障，以及默认关闭与配置校验。"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import ValidationError

from agent_tools import (
    CachePolicy,
    CircuitBreakerConfig,
    DedupePolicy,
    InMemoryResultStore,
    InProcessCircuitBreaker,
    Principal,
    ToolAnnotations,
    ToolContext,
    ToolExecutionError,
    ToolInvocation,
    ToolOutput,
    ToolPolicy,
    ToolRegistry,
    ToolSpec,
    UpstreamError,
)
from agent_tools.protocol.invocation import TextBlock

READ_ONLY = ToolAnnotations(read_only=True, concurrency_safe=True)


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self) -> None:
        """从 0 开始计时。"""
        self.now = 0.0

    def monotonic(self) -> float:
        """返回当前时刻。"""
        return self.now


class BrokenStore:
    """读或写总是失败的存储。"""

    def __init__(self, fail_get: bool = True, fail_set: bool = True) -> None:
        """创建存储。

        Args:
            fail_get: 读取是否失败。
            fail_set: 写入是否失败。
        """
        self.fail_get, self.fail_set = fail_get, fail_set
        self.inner = InMemoryResultStore()

    async def get(self, key: str) -> bytes | None:
        """读取，或模拟失败。"""
        if self.fail_get:
            raise ConnectionError("store down")
        return await self.inner.get(key)

    async def set(self, key: str, value: bytes, ttl_s: float) -> None:
        """写入，或模拟失败。"""
        if self.fail_set:
            raise ConnectionError("store down")
        await self.inner.set(key, value, ttl_s)


def user(uid: str | None = "u1", tenant: str | None = "t1", *scopes: str) -> ToolContext:
    """构造带身份的上下文。"""
    return ToolContext(
        principal=Principal(user_id=uid, tenant_id=tenant, scopes=frozenset(scopes or {"read"}))
    )


def lookup_registry(calls: list[str], scope: str = "principal", ttl_s: float = 60) -> ToolRegistry:
    """注册可缓存的只读工具 ``lookup`` 和总是失败的 ``broken``。

    Args:
        calls: 记录真实执行。
        scope: 缓存范围。
        ttl_s: 缓存有效期。

    Returns:
        注册表。
    """
    registry = ToolRegistry()
    policy = ToolPolicy(
        access="scoped",
        required_scopes=frozenset({"read"}),
        cache=CachePolicy(ttl_s=ttl_s, scope=scope),  # type: ignore[arg-type]
    )

    async def lookup(q: str) -> dict[str, Any]:
        """查询（测试替身）。"""
        calls.append(q)
        return {"q": q, "n": len(calls)}

    async def broken(q: str) -> str:
        """总是失败。"""
        calls.append("broken")
        raise UpstreamError("down", retryable=True)

    registry.register_function(lookup, name="lookup", policy=policy, annotations=READ_ONLY)
    registry.register_function(broken, name="broken", policy=policy, annotations=READ_ONLY)
    return registry


def inv(tool: str = "lookup", q: str = "a", call_id: str = "c") -> ToolInvocation:
    """构造调用。"""
    return ToolInvocation(call_id=call_id, tool=tool, arguments={"q": q})


# ---------------------------------------------------------------------------------
# 结果缓存
# ---------------------------------------------------------------------------------
async def test_cache_hit_skips_execution(make_executor: Any, telemetry: Any) -> None:
    """同一身份、相同参数在 TTL 内的第二次调用命中缓存：工具不执行，结果标记为 ``cache``。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls), result_cache=InMemoryResultStore())
    first = await executor.execute(inv(), user())
    second = await executor.execute(inv(call_id="c2"), user())
    other = await executor.execute(inv(q="b"), user())
    assert calls == ["a", "b"]
    assert first.reused is None and second.reused == "cache"
    assert second.structured == first.structured and second.attempts == 0
    assert other.reused is None
    assert telemetry.logs.events("tool.call.reused")


async def test_principal_scope_isolates_users_tenants_and_scopes(make_executor: Any) -> None:
    """``principal`` 范围：不同用户、不同租户、不同权限集合之间都不共享结果。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls), result_cache=InMemoryResultStore())
    for ctx in (user("u1"), user("u2"), user("u1", "t2"), user("u1", "t1", "read", "admin")):
        await executor.execute(inv(), ctx)
    assert len(calls) == 4


async def test_tenant_scope_shares_within_tenant_and_scope_set(make_executor: Any) -> None:
    """``tenant`` 范围：同租户、同权限集合的用户共享；权限集合或租户不同则不共享。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls, "tenant"), result_cache=InMemoryResultStore())
    await executor.execute(inv(), user("u1"))
    hit = await executor.execute(inv(), user("u2"))
    await executor.execute(inv(), user("u3", "t1", "read", "admin"))
    await executor.execute(inv(), user("u1", "t2"))
    assert hit.reused == "cache"
    assert len(calls) == 3


async def test_missing_identity_disables_cache(make_executor: Any) -> None:
    """缺少所需身份字段（匿名用户、没有租户）时不使用缓存，避免匿名调用之间共享结果。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls), result_cache=InMemoryResultStore())
    for _ in range(2):
        result = await executor.execute(inv(), user(None))
        assert result.reused is None
    executor2 = make_executor(lookup_registry(calls, "tenant"), result_cache=InMemoryResultStore())
    for _ in range(2):
        await executor2.execute(inv(), user("u1", None))
    assert len(calls) == 4


async def test_cache_cannot_bypass_authorization(make_executor: Any) -> None:
    """缓存查找发生在鉴权之后：没有权限的调用方拿不到已缓存的结果。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls), result_cache=InMemoryResultStore())
    await executor.execute(inv(), user())
    stranger = ToolContext(principal=Principal(user_id="u1", tenant_id="t1"))
    denied = await executor.execute(inv(), stranger)
    assert denied.error is not None and denied.error.code == "permission_denied"
    assert denied.content == [] and calls == ["a"]


async def test_cache_expiry_and_capacity(make_executor: Any) -> None:
    """过期后重新执行；容量满时淘汰最久未使用的条目。"""
    calls: list[str] = []
    clock = FakeClock()
    store = InMemoryResultStore(max_entries=1, clock=clock)
    executor = make_executor(lookup_registry(calls, ttl_s=10), result_cache=store)
    await executor.execute(inv(), user())
    clock.now += 9.9
    assert (await executor.execute(inv(), user())).reused == "cache"
    clock.now += 0.2
    assert (await executor.execute(inv(), user())).reused is None
    await executor.execute(inv(q="b"), user())  # 挤掉 "a"
    assert (await executor.execute(inv(), user())).reused is None
    assert calls == ["a", "a", "b", "a"]


async def test_failures_are_not_cached(make_executor: Any) -> None:
    """失败结果不写入缓存：每次都会重新执行。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls), result_cache=InMemoryResultStore())
    for _ in range(3):
        result = await executor.execute(inv("broken"), user())
        assert result.error is not None and result.reused is None
    assert calls == ["broken"] * 3


async def test_concurrent_misses_execute_once(make_executor: Any) -> None:
    """同键的并发未命中只有一个执行者，其余等待后复用它的结果。"""
    calls: list[str] = []
    gate = asyncio.Event()
    registry = ToolRegistry()

    async def lookup(q: str) -> str:
        """等待放行后返回。"""
        calls.append(q)
        await gate.wait()
        return q.upper()

    registry.register_function(
        lookup,
        name="lookup",
        policy=ToolPolicy(access="public", cache=CachePolicy(ttl_s=60)),
        annotations=READ_ONLY,
    )
    executor = make_executor(registry, result_cache=InMemoryResultStore())
    batch = asyncio.create_task(
        executor.execute_many([inv(call_id=str(i)) for i in range(5)], user())
    )
    await asyncio.sleep(0.05)
    gate.set()
    results = await batch
    assert calls == ["a"]
    assert all(r.ok and r.to_model_text() == "A" for r in results)
    assert sorted(str(r.reused) for r in results) == ["None"] + ["cache"] * 4


async def test_owner_failure_or_cancellation_lets_waiters_run(make_executor: Any) -> None:
    """执行者失败或被取消时不会把错误传给等待者：等待者各自执行。"""
    calls: list[str] = []
    started = asyncio.Event()
    registry = ToolRegistry()

    async def lookup(q: str) -> str:
        """第一次调用一直等待（直到被取消），之后立即返回。"""
        calls.append(q)
        if len(calls) == 1:
            started.set()
            await asyncio.sleep(30)
        return "ok"

    registry.register_function(
        lookup,
        name="lookup",
        policy=ToolPolicy(access="public", cache=CachePolicy(ttl_s=60)),
        annotations=READ_ONLY,
    )
    executor = make_executor(registry, result_cache=InMemoryResultStore())
    owner = asyncio.create_task(executor.execute(inv(), user()))
    await started.wait()
    waiter = asyncio.create_task(executor.execute(inv(call_id="w"), user()))
    await asyncio.sleep(0.02)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    result = await waiter
    assert result.ok and result.reused is None
    assert len(calls) == 2


async def test_cache_store_failures_degrade_to_execution(make_executor: Any) -> None:
    """缓存存储读写失败时按未命中处理，调用照常成功。"""
    calls: list[str] = []
    executor = make_executor(lookup_registry(calls), result_cache=BrokenStore())
    for _ in range(2):
        assert (await executor.execute(inv(), user())).ok
    assert calls == ["a", "a"]


async def test_result_filter_runs_on_cache_hits(make_executor: Any) -> None:
    """缓存的是过滤之前的输出：命中时仍按本次调用的上下文执行 ``ResultFilter``。"""
    calls: list[str] = []

    class Tag:
        """在输出末尾追加调用方的会话 ID。"""

        def filter(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
            """追加会话标记。"""
            return ToolOutput(content=[*output.content, TextBlock(text=f"for {ctx.session_id}")])

    executor = make_executor(
        lookup_registry(calls), result_cache=InMemoryResultStore(), result_filter=Tag()
    )
    base = user().principal
    first = await executor.execute(inv(), ToolContext(principal=base, session_id="s1"))
    second = await executor.execute(inv(), ToolContext(principal=base, session_id="s2"))
    assert second.reused == "cache"
    assert first.to_model_text().endswith("for s1")
    assert second.to_model_text().endswith("for s2")
    assert "for s1" not in second.to_model_text()


async def test_cache_requires_store_and_new_version_misses(make_executor: Any) -> None:
    """没有配置缓存存储时照常执行；新版本的工具不会复用旧版本的结果。"""
    calls: list[str] = []
    registry = lookup_registry(calls)
    executor = make_executor(registry)
    for _ in range(2):
        assert (await executor.execute(inv(), user())).reused is None
    assert len(calls) == 2

    executor = make_executor(registry, result_cache=InMemoryResultStore())
    await executor.execute(inv(), user())

    async def lookup(q: str) -> str:
        """新版本。"""
        calls.append("v2")
        return "v2"

    registry.register_function(
        lookup,
        name="lookup",
        version="2",
        policy=ToolPolicy(
            access="scoped", required_scopes=frozenset({"read"}), cache=CachePolicy(ttl_s=60)
        ),
        annotations=READ_ONLY,
    )
    registry.set_active("lookup", "2")
    result = await executor.execute(inv(), user())
    assert result.reused is None and calls[-1] == "v2"


def test_cache_requires_read_only_tools() -> None:
    """只有只读、非不可逆的工具可以声明缓存；缓存与去重不能同时声明。"""
    with pytest.raises(ValidationError, match="read_only"):
        ToolSpec(
            name="w",
            input_schema={"type": "object"},
            kind="python",
            policy=ToolPolicy(access="public", cache=CachePolicy(ttl_s=1)),
        )
    with pytest.raises(ValidationError, match="read_only"):
        ToolSpec(
            name="w",
            input_schema={"type": "object"},
            kind="python",
            annotations=ToolAnnotations(read_only=True, destructive=True),
            policy=ToolPolicy(access="public", cache=CachePolicy(ttl_s=1)),
        )
    with pytest.raises(ValidationError, match="mutually exclusive"):
        ToolPolicy(access="public", cache=CachePolicy(ttl_s=1), dedupe=DedupePolicy())


# ---------------------------------------------------------------------------------
# 调用去重
# ---------------------------------------------------------------------------------
def charge_registry(calls: list[dict[str, Any]], window_s: float = 3600) -> ToolRegistry:
    """注册有副作用、声明了去重的工具 ``charge``，以及业务失败的 ``reject``。

    Args:
        calls: 记录真实执行。
        window_s: 去重窗口。

    Returns:
        注册表。
    """
    registry = ToolRegistry()
    policy = ToolPolicy(access="public", dedupe=DedupePolicy(window_s=window_s))

    async def charge(amount: int) -> dict[str, Any]:
        """扣款（测试替身）。"""
        calls.append({"amount": amount})
        return {"charged": amount, "seq": len(calls)}

    async def reject(amount: int) -> str:
        """业务失败。"""
        calls.append({"reject": amount})
        raise ToolExecutionError("card declined", code="card_declined")

    registry.register_function(charge, name="charge", policy=policy)
    registry.register_function(reject, name="reject", policy=policy)
    return registry


def charge(call_id: str = "c1", amount: int = 5, tool: str = "charge") -> ToolInvocation:
    """构造扣款调用。"""
    return ToolInvocation(call_id=call_id, tool=tool, arguments={"amount": amount})


def ctx_for(request_id: str = "req-1", uid: str = "u1", tenant: str = "t1") -> ToolContext:
    """构造带固定请求 ID 的上下文。"""
    return ToolContext(request_id=request_id, principal=Principal(user_id=uid, tenant_id=tenant))


async def test_duplicate_submission_is_replayed(make_executor: Any) -> None:
    """同一 request_id + call_id + 参数的重复提交返回第一次的结果，工具只执行一次。"""
    calls: list[dict[str, Any]] = []
    executor = make_executor(charge_registry(calls), dedupe_store=InMemoryResultStore())
    first = await executor.execute(charge(), ctx_for())
    again = await executor.execute(charge(), ctx_for())
    assert len(calls) == 1
    assert again.reused == "dedupe" and again.structured == first.structured
    assert again.attempts == first.attempts


async def test_dedupe_key_scope(make_executor: Any) -> None:
    """不同 call_id、不同参数、不同请求、不同用户或租户都视为不同的逻辑调用。"""
    calls: list[dict[str, Any]] = []
    executor = make_executor(charge_registry(calls), dedupe_store=InMemoryResultStore())
    await executor.execute(charge(), ctx_for())
    await executor.execute(charge("c2"), ctx_for())
    await executor.execute(charge(amount=6), ctx_for())
    await executor.execute(charge(), ctx_for("req-2"))
    await executor.execute(charge(), ctx_for(uid="u2"))
    await executor.execute(charge(), ctx_for(tenant="t2"))
    assert len(calls) == 6


async def test_started_failures_are_recorded_and_replayed(make_executor: Any) -> None:
    """工具已经开始执行后的失败也会被记录：重复提交返回同一个失败，而不是再执行一次。"""
    calls: list[dict[str, Any]] = []
    executor = make_executor(charge_registry(calls), dedupe_store=InMemoryResultStore())
    first = await executor.execute(charge(tool="reject"), ctx_for())
    again = await executor.execute(charge(tool="reject"), ctx_for())
    assert len(calls) == 1
    assert first.error is not None and again.error == first.error
    assert again.reused == "dedupe"


async def test_failures_before_execution_are_not_recorded(make_executor: Any) -> None:
    """工具没有开始执行的失败（例如熔断拒绝）不记录：之后的重复提交可以真正执行。"""
    calls: list[dict[str, Any]] = []
    cb = InProcessCircuitBreaker(CircuitBreakerConfig(failure_threshold=1, open_s=3600))
    (await cb.acquire("tool:charge")).record("failure")
    executor = make_executor(
        charge_registry(calls), dedupe_store=InMemoryResultStore(), circuit_breaker=cb
    )
    rejected = await executor.execute(charge(), ctx_for())
    assert rejected.error is not None and rejected.error.code == "circuit_open"
    executor2 = make_executor(charge_registry(calls), dedupe_store=InMemoryResultStore())
    assert (await executor2.execute(charge(), ctx_for())).ok
    assert len(calls) == 1


async def test_dedupe_without_store_or_with_broken_store_fails_closed(make_executor: Any) -> None:
    """声明了去重却没有存储，或读取存储失败：安全失败，不在没有去重的情况下执行。"""
    calls: list[dict[str, Any]] = []
    for kwargs in ({}, {"dedupe_store": BrokenStore()}):
        executor = make_executor(charge_registry(calls), **kwargs)
        result = await executor.execute(charge(), ctx_for())
        assert result.error is not None and result.error.code == "dedupe_unavailable"
    assert calls == []


async def test_concurrent_duplicates_wait_for_first(make_executor: Any) -> None:
    """并发的重复提交等待第一次执行结束，然后重放它的结果。"""
    calls: list[dict[str, Any]] = []
    gate = asyncio.Event()
    registry = ToolRegistry()

    async def charge_slow(amount: int) -> int:
        """等待放行后扣款。"""
        calls.append({"amount": amount})
        await gate.wait()
        return amount

    registry.register_function(
        charge_slow,
        name="charge",
        policy=ToolPolicy(access="public", dedupe=DedupePolicy()),
        annotations=ToolAnnotations(concurrency_safe=True),
    )
    executor = make_executor(registry, dedupe_store=InMemoryResultStore())
    batch = asyncio.create_task(executor.execute_many([charge(), charge()], ctx_for()))
    await asyncio.sleep(0.05)
    gate.set()
    results = await batch
    assert len(calls) == 1
    assert [r.reused for r in results] == [None, "dedupe"]


async def test_owner_cancelled_after_start_reports_unknown_effect(make_executor: Any) -> None:
    """第一次执行开始后被取消：等待中的重复提交得到 ``duplicate_call_aborted``，不会再执行一次。"""
    calls: list[dict[str, Any]] = []
    started = asyncio.Event()
    registry = ToolRegistry()

    async def charge_hang(amount: int) -> int:
        """一直等待直到被取消。"""
        calls.append({"amount": amount})
        started.set()
        await asyncio.sleep(30)
        return amount

    registry.register_function(
        charge_hang, name="charge", policy=ToolPolicy(access="public", dedupe=DedupePolicy())
    )
    executor = make_executor(registry, dedupe_store=InMemoryResultStore())
    owner = asyncio.create_task(executor.execute(charge(), ctx_for()))
    await started.wait()
    waiter = asyncio.create_task(executor.execute(charge(), ctx_for()))
    await asyncio.sleep(0.02)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    result = await waiter
    assert result.error is not None and result.error.code == "duplicate_call_aborted"
    assert len(calls) == 1


async def test_record_write_failure_is_reported_to_waiters(make_executor: Any) -> None:
    """记录写入失败时结果照常返回，但并发等待的重复提交不会再执行（效果未知）。"""
    calls: list[dict[str, Any]] = []
    gate = asyncio.Event()
    registry = ToolRegistry()

    async def charge_slow(amount: int) -> int:
        """等待放行后扣款。"""
        calls.append({"amount": amount})
        await gate.wait()
        return amount

    registry.register_function(
        charge_slow,
        name="charge",
        policy=ToolPolicy(access="public", dedupe=DedupePolicy()),
        annotations=ToolAnnotations(concurrency_safe=True),
    )
    store = BrokenStore(fail_get=False, fail_set=True)
    executor = make_executor(registry, dedupe_store=store)
    batch = asyncio.create_task(executor.execute_many([charge(), charge()], ctx_for()))
    await asyncio.sleep(0.05)
    gate.set()
    first, second = await batch
    assert first.ok
    assert second.error is not None and second.error.code == "duplicate_call_aborted"
    assert len(calls) == 1


async def test_dedupe_window_expiry(make_executor: Any) -> None:
    """超过去重窗口后，重复提交会再次执行（去重不是永久的幂等保证）。"""
    calls: list[dict[str, Any]] = []
    clock = FakeClock()
    executor = make_executor(
        charge_registry(calls, window_s=60), dedupe_store=InMemoryResultStore(clock=clock)
    )
    await executor.execute(charge(), ctx_for())
    clock.now += 61
    again = await executor.execute(charge(), ctx_for())
    assert again.reused is None and len(calls) == 2


def test_cache_and_dedupe_stores_must_differ(registry: ToolRegistry) -> None:
    """结果缓存与去重必须使用不同的存储实例，避免缓存条目挤掉去重记录。"""
    from agent_tools import ToolExecutor

    store = InMemoryResultStore()
    with pytest.raises(ValueError, match="different"):
        ToolExecutor(registry, result_cache=store, dedupe_store=store)


# ---------------------------------------------------------------------------------
# 内存存储
# ---------------------------------------------------------------------------------
async def test_in_memory_store_ttl_lru_and_byte_limit() -> None:
    """内存存储：TTL 过期删除；按最近使用淘汰；总字节数受限；超过上限的单个值不保存。"""
    clock = FakeClock()
    store = InMemoryResultStore(max_entries=2, max_bytes=10, clock=clock)
    await store.set("a", b"1234", 5)
    await store.set("b", b"5678", 5)
    assert await store.get("a") == b"1234"  # a 成为最近使用
    await store.set("c", b"90", 5)  # 条目数超限：淘汰 b
    assert await store.get("b") is None
    await store.set("d", b"xxxxxxxx", 5)  # 字节数超限：继续淘汰
    assert store.size_bytes <= 10
    await store.set("big", b"x" * 11, 5)
    assert await store.get("big") is None
    clock.now += 5
    assert await store.get("d") is None
    await store.set("e", b"1", 0)
    assert await store.get("e") is None


async def test_start_timeout_is_not_recorded_for_dedupe(make_executor: Any) -> None:
    """同步工具等不到线程而超时（``start_timeout``，工具没有运行）时不写去重记录，重复提交可以执行。"""
    import threading

    release = threading.Event()
    calls: list[str] = []
    registry = ToolRegistry()

    def blocker() -> str:
        """占住唯一的线程直到被放行。"""
        release.wait(5)
        return "done"

    def charge_sync(amount: int) -> int:
        """同步扣款（测试替身）。"""
        calls.append("charge")
        return amount

    registry.register_function(blocker, name="blocker", policy=ToolPolicy(access="public"))
    registry.register_function(
        charge_sync,
        name="charge",
        policy=ToolPolicy(access="public", timeout_s=0.2, dedupe=DedupePolicy()),
    )
    executor = make_executor(registry, sync_workers=1, dedupe_store=InMemoryResultStore())
    hold = asyncio.create_task(
        executor.execute(ToolInvocation(call_id="b", tool="blocker"), ToolContext())
    )
    await asyncio.sleep(0.05)
    starved = await executor.execute(charge(), ctx_for())
    assert starved.error is not None and starved.error.code == "start_timeout"
    release.set()
    await hold
    retried = await executor.execute(charge(), ctx_for())
    assert retried.ok and retried.reused is None and calls == ["charge"]
