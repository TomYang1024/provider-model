"""执行器的测试：执行链、安全检查顺序、输出限制、超时、分段并发、取消与同步工具额度。"""

from __future__ import annotations

import asyncio
import base64
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest
from pydantic import BaseModel

from agent_tools import (
    AuthzDecision,
    ExecutorClosedError,
    ImageBlock,
    InvalidArgumentsError,
    OutputLimits,
    Principal,
    TextBlock,
    ToolAnnotations,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolOutput,
    ToolPolicy,
    ToolRegistry,
    ToolSpec,
)

PUBLIC = ToolPolicy(access="public")


class Point(BaseModel):
    """二维点（定义在模块级，函数注解才能被解析）。"""

    x: int
    y: int


SAFE = ToolAnnotations(concurrency_safe=True)

ExecutorFactory = Callable[..., ToolExecutor]


def inv(
    tool: str, arguments: dict[str, Any] | str | None = None, call_id: str = "c1"
) -> ToolInvocation:
    """构造调用请求的简写。

    Args:
        tool: 工具名或别名。
        arguments: 参数。
        call_id: 调用 ID。

    Returns:
        调用请求。
    """
    return ToolInvocation(
        call_id=call_id, tool=tool, arguments={} if arguments is None else arguments
    )


class Timeline:
    """记录工具开始、结束时刻，用于断言并发分段。"""

    def __init__(self) -> None:
        """创建空时间线。"""
        self.events: list[tuple[str, str]] = []
        self.active = 0
        self.peak = 0

    def start(self, name: str) -> None:
        """记录开始，并更新同时运行数的峰值。

        Args:
            name: 调用标识。
        """
        self.events.append((name, "start"))
        self.active += 1
        self.peak = max(self.peak, self.active)

    def end(self, name: str) -> None:
        """记录结束。

        Args:
            name: 调用标识。
        """
        self.events.append((name, "end"))
        self.active -= 1

    def index(self, name: str, phase: str) -> int:
        """返回事件在时间线中的位置。

        Args:
            name: 调用标识。
            phase: ``start`` 或 ``end``。

        Returns:
            位置下标。
        """
        return self.events.index((name, phase))


def register_timed(registry: ToolRegistry, timeline: Timeline) -> None:
    """注册一个并发安全和一个非并发安全的计时工具（都会 sleep 20ms）。

    Args:
        registry: 注册表。
        timeline: 时间线。
    """

    async def safe_op(tag: str) -> str:
        """并发安全的计时工具。"""
        timeline.start(tag)
        await asyncio.sleep(0.02)
        timeline.end(tag)
        return tag

    async def unsafe_op(tag: str) -> str:
        """非并发安全的计时工具。"""
        timeline.start(tag)
        await asyncio.sleep(0.02)
        timeline.end(tag)
        return tag

    registry.register_function(safe_op, name="safe", policy=PUBLIC, annotations=SAFE)
    registry.register_function(unsafe_op, name="unsafe", policy=PUBLIC)


# ---------------------------------------------------------------------------------
# 基本执行
# ---------------------------------------------------------------------------------
async def test_async_and_sync_tools_and_context_injection(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """异步与同步工具都能执行；ToolContext 被注入；dict 返回值同时填充 structured。"""

    async def add(a: int, b: int) -> dict[str, int]:
        """加法。

        测试用：返回 dict，用来验证 structured 字段会被同时填充。
        """
        return {"sum": a + b}

    def whoami(ctx: ToolContext) -> str:
        """返回调用方。"""
        return f"{ctx.principal.user_id}@{ctx.request_id}"

    registry.register_function(add, name="math.add", policy=PUBLIC)
    registry.register_function(whoami, name="whoami", policy=PUBLIC)
    executor = make_executor(registry)
    ctx = ToolContext(request_id="req-1", principal=Principal(user_id="u1"))

    r1 = await executor.execute(inv("math_add", '{"a": 1, "b": 2}'), ctx)
    assert (r1.status, r1.structured, r1.alias, r1.tool, r1.version) == (
        "success",
        {"sum": 3},
        "math_add",
        "math.add",
        "1",
    )
    r2 = await executor.execute(inv("whoami"), ctx)
    assert r2.to_model_text() == "u1@req-1"
    assert r2.attempts == 1


async def test_empty_string_arguments_mean_empty_object(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """空字符串参数视为 ``{}``，与 Provider SDK 的约定一致。"""

    async def ping() -> str:
        """返回 pong。"""
        return "pong"

    registry.register_function(ping, name="ping", policy=PUBLIC)
    result = await make_executor(registry).execute(inv("ping", "  "), ToolContext())
    assert result.to_model_text() == "pong"


async def test_unknown_and_disabled_tools(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """未知工具与停用工具都返回 not_found 类错误结果，而不是抛出异常。"""

    async def t() -> str:
        """测试工具。

        测试用：返回固定值，本身不是被测对象，只用来驱动执行器。
        """
        return "x"

    registry.register_function(t, name="t", policy=PUBLIC)
    registry.disable("t")
    executor = make_executor(registry)
    unknown = await executor.execute(inv("nope"), ToolContext())
    disabled = await executor.execute(inv("t"), ToolContext())
    assert unknown.error and unknown.error.code == "tool_not_found"
    assert disabled.error and disabled.error.code == "tool_disabled"
    assert unknown.alias == "nope"


# ---------------------------------------------------------------------------------
# 安全：鉴权、输入
# ---------------------------------------------------------------------------------
async def test_execution_is_authorized_even_if_tool_is_hidden(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """工具不在调用方的列表中时，直接调用也会在执行时被拒绝（隐藏不等于权限控制）。"""

    async def secret_op() -> str:
        """需要权限的工具。"""
        return "ok"

    registry.register_function(
        secret_op,
        name="admin.op",
        policy=ToolPolicy(access="scoped", required_scopes=frozenset({"admin"})),
    )
    executor = make_executor(registry)
    guest = Principal(user_id="guest")
    assert executor.list_tools(guest).tools == []
    denied = await executor.execute(inv("admin_op"), ToolContext(principal=guest))
    assert denied.error and denied.error.type == "permission"

    admin = Principal(scopes=frozenset({"admin"}))
    assert [t.alias for t in executor.list_tools(admin).tools] == ["admin_op"]
    assert (await executor.execute(inv("admin_op"), ToolContext(principal=admin))).ok


async def test_authorization_happens_before_argument_validation(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """无权限的调用方即使传了非法参数，也只会看到权限错误，看不到校验细节。"""

    async def op(n: int) -> int:
        """需要权限的工具。"""
        return n

    registry.register_function(
        op, name="op", policy=ToolPolicy(access="scoped", required_scopes=frozenset({"s"}))
    )
    result = await make_executor(registry).execute(inv("op", "not json"), ToolContext())
    assert result.error and result.error.type == "permission"


async def test_authorizer_exception_hides_tool_and_fails_closed(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """授权实现抛出异常时：列表中隐藏该工具，执行返回内部错误结果（失败即拒绝）。"""

    class Broken:
        """总是抛异常的授权实现。"""

        def authorize(self, principal: Principal, spec: ToolSpec) -> AuthzDecision:
            """模拟授权系统故障。"""
            raise RuntimeError("authz backend down")

    async def t() -> str:
        """测试工具。

        测试用：返回固定值，本身不是被测对象，只用来驱动执行器。
        """
        return "x"

    registry.register_function(t, name="t", policy=PUBLIC)
    executor = make_executor(registry, authorizer=Broken())
    assert executor.list_tools(Principal()).tools == []
    result = await executor.execute(inv("t"), ToolContext())
    assert result.error and result.error.type == "internal"
    assert "authz" not in result.error.message


async def test_argument_size_parsing_and_validation_errors(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """参数过大、非法 JSON、非对象、未声明字段都返回 invalid_arguments，且不回显参数值。"""

    async def echo(text: str) -> str:
        """回显。

        测试用：参数大小上限为 64 字节，用来触发各类参数错误。
        """
        return text

    registry.register_function(
        echo, name="echo", policy=ToolPolicy(access="public", max_input_bytes=64)
    )
    executor = make_executor(registry, max_raw_input_bytes=10_000)
    ctx = ToolContext()
    cases = {
        "arguments_too_large": '{"text": "' + "x" * 100 + '"}',
        "arguments_not_json": "{not json",
        "arguments_not_object": "[1, 2]",
        "arguments_invalid": '{"text": "SECRET-123", "extra": 1}',
    }
    for code, raw in cases.items():
        result = await executor.execute(inv("echo", raw), ctx)
        assert result.error and (result.error.type, result.error.code) == (
            "invalid_arguments",
            code,
        )
        assert "SECRET-123" not in result.error.message
    global_limit = make_executor(registry, max_raw_input_bytes=8)
    too_big = await global_limit.execute(inv("echo", '{"text": "hello"}'), ctx)
    assert too_big.error and too_big.error.code == "arguments_too_large"


async def test_pydantic_args_model_with_nested_types(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """嵌套的 Pydantic 参数会被解析成模型对象后传给函数。"""

    async def norm(p: Point) -> int:
        """曼哈顿距离。"""
        assert isinstance(p, Point)
        return abs(p.x) + abs(p.y)

    registry.register_function(norm, name="norm", policy=PUBLIC)
    result = await make_executor(registry).execute(
        inv("norm", {"p": {"x": -2, "y": 3}}), ToolContext()
    )
    assert result.to_model_text() == "5"


# ---------------------------------------------------------------------------------
# 工具失败与输出
# ---------------------------------------------------------------------------------
async def test_tool_exceptions_do_not_leak_messages(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """工具抛出的普通异常只暴露类型名；ToolError 子类可以给出自己的安全摘要。"""

    async def boom() -> None:
        """抛出带敏感信息的异常。"""
        raise ValueError("password=hunter2")

    async def refuse() -> None:
        """主动给出安全的错误摘要。"""
        raise InvalidArgumentsError("city is not supported", code="unsupported_city")

    def bad_return() -> object:
        """返回不支持的类型。"""
        return object()

    registry.register_function(boom, name="boom", policy=PUBLIC)
    registry.register_function(refuse, name="refuse", policy=PUBLIC)
    registry.register_function(bad_return, name="bad", policy=PUBLIC)
    executor = make_executor(registry)

    r = await executor.execute(inv("boom"), ToolContext())
    assert r.error and (r.error.type, r.error.message) == ("execution", "tool raised ValueError")
    assert "hunter2" not in str(telemetry.logs.records)
    assert telemetry.logs.events("tool.call.tool_raised")[0]["exc_type"] == "ValueError"

    r = await executor.execute(inv("refuse"), ToolContext())
    assert r.error and (r.error.code, r.error.message) == (
        "unsupported_city",
        "city is not supported",
    )

    r = await executor.execute(inv("bad"), ToolContext())
    assert r.error and r.error.code == "invalid_tool_output"


async def test_output_limits_by_type(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """文本在 UTF-8 边界截断并标记；JSON、图片、图片数量、合计超限都报错。"""
    limits = OutputLimits(
        max_text_bytes=100, max_json_bytes=50, max_image_bytes=10, max_images=1, max_total_bytes=150
    )
    policy = ToolPolicy(access="public", output_limits=limits)
    png = base64.b64encode(b"x" * 30).decode()

    async def long_text() -> str:
        """多字节字符的长文本。"""
        return "汉" * 200

    async def big_json() -> dict[str, str]:
        """超大的 JSON。"""
        return {"k": "v" * 100}

    async def big_image() -> ToolOutput:
        """超大的图片。"""
        return ToolOutput(content=[ImageBlock(mime_type="image/png", data_b64=png)])

    async def two_images() -> ToolOutput:
        """超过数量上限的图片。"""
        url = ImageBlock(mime_type="image/png", url="https://example.com/a.png")
        return ToolOutput(content=[url, url])

    async def total() -> ToolOutput:
        """单块不超限、合计超限。"""
        return ToolOutput(content=[TextBlock(text="a" * 90), TextBlock(text="b" * 90)])

    for fn in (long_text, big_json, big_image, two_images, total):
        registry.register_function(fn, name=fn.__name__, policy=policy)
    executor = make_executor(registry)
    ctx = ToolContext()

    text = await executor.execute(inv("long_text"), ctx)
    block = text.content[0]
    assert isinstance(block, TextBlock) and block.truncated
    assert len(block.text.encode()) <= 100
    assert "[truncated" in block.text
    block.text.encode("utf-8")  # 截断后仍是合法 UTF-8

    for name, code in (
        ("big_json", "output_too_large"),
        ("big_image", "output_too_large"),
        ("two_images", "too_many_images"),
        ("total", "output_too_large"),
    ):
        r = await executor.execute(inv(name), ctx)
        assert r.error and (r.error.type, r.error.code) == ("output", code), name


# ---------------------------------------------------------------------------------
# 超时与截止时间
# ---------------------------------------------------------------------------------
async def test_attempt_timeout_and_deadline(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """区分工具自身超时（可重试）与整体截止时间（不可重试）；截止时间已过时不调用工具。"""
    calls = 0

    async def slow() -> str:
        """很慢的工具。"""
        nonlocal calls
        calls += 1
        await asyncio.sleep(10)
        return "late"

    registry.register_function(
        slow, name="slow", policy=ToolPolicy(access="public", timeout_s=0.05)
    )
    executor = make_executor(registry)

    r = await executor.execute(inv("slow"), ToolContext())
    assert r.error and (r.error.code, r.error.retryable) == ("attempt_timeout", True)

    r = await executor.execute(inv("slow"), ToolContext.with_timeout(0.02))
    assert r.error and (r.error.code, r.error.retryable) == ("deadline_exceeded", False)

    before = calls
    r = await executor.execute(inv("slow"), ToolContext(deadline=time.monotonic() - 1))
    assert r.error and r.error.code == "deadline_exceeded"
    assert calls == before


async def test_tool_raising_builtin_timeout_is_not_confused_with_attempt_timeout(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """工具自己抛出的 TimeoutError 属于工具失败，而不是执行器的超时。"""

    async def raises_timeout() -> None:
        """抛出内置 TimeoutError。"""
        raise TimeoutError("upstream")

    registry.register_function(raises_timeout, name="rt", policy=PUBLIC)
    r = await make_executor(registry).execute(inv("rt"), ToolContext())
    assert r.error and r.error.type == "execution"


# ---------------------------------------------------------------------------------
# 批量执行与并发分段
# ---------------------------------------------------------------------------------
async def test_execute_many_segments_by_concurrency_safe(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """连续的并发安全调用并发执行；非并发安全的调用是屏障；结果顺序与输入一致。"""
    timeline = Timeline()
    register_timed(registry, timeline)
    executor = make_executor(registry)
    calls = [
        inv("safe", {"tag": "s1"}, "1"),
        inv("safe", {"tag": "s2"}, "2"),
        inv("unsafe", {"tag": "u1"}, "3"),
        inv("safe", {"tag": "s3"}, "4"),
        inv("safe", {"tag": "s4"}, "5"),
    ]
    results = await executor.execute_many(calls, ToolContext())
    assert [r.call_id for r in results] == ["1", "2", "3", "4", "5"]
    assert all(r.ok for r in results)
    t = timeline.index
    # s1、s2 重叠
    assert t("s2", "start") < t("s1", "end")
    # u1 在 s1、s2 都结束后才开始，且在 s3、s4 开始前结束
    assert t("u1", "start") > max(t("s1", "end"), t("s2", "end"))
    assert t("u1", "end") < min(t("s3", "start"), t("s4", "start"))
    assert t("s4", "start") < t("s3", "end")


async def test_execute_many_respects_max_parallel_and_sequential_mode(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """并发段内同时执行的数量不超过 max_parallel；sequential 模式完全不并发。"""
    timeline = Timeline()
    register_timed(registry, timeline)
    executor = make_executor(registry, max_parallel=2)
    calls = [inv("safe", {"tag": f"s{i}"}, str(i)) for i in range(6)]
    await executor.execute_many(calls, ToolContext())
    assert timeline.peak == 2

    timeline2 = Timeline()
    registry2 = ToolRegistry()
    register_timed(registry2, timeline2)
    await make_executor(registry2).execute_many(calls, ToolContext(), mode="sequential")
    assert timeline2.peak == 1


async def test_failures_do_not_stop_the_batch_and_unknown_tools_are_barriers(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """一个调用失败不影响后续调用；无法解析的调用按非并发安全处理。"""
    timeline = Timeline()
    register_timed(registry, timeline)
    calls = [
        inv("safe", {"tag": "a"}, "1"),
        inv("missing", {}, "2"),
        inv("safe", {"tag": "b"}, "3"),
    ]
    results = await make_executor(registry).execute_many(calls, ToolContext())
    assert [r.status for r in results] == ["success", "error", "success"]
    assert timeline.index("b", "start") > timeline.index("a", "end")


async def test_batch_uses_one_snapshot_and_snapshot_id(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """整批使用同一个快照；指定 snapshot_id 时使用列表对应的版本；快照失效时返回 snapshot_expired。"""

    async def v1() -> str:
        """版本 1。

        测试用：返回 ``v1``，用来确认执行时使用的是哪个版本。
        """
        return "v1"

    async def v2() -> str:
        """版本 2。

        测试用：返回 ``v2``，用来确认执行时使用的是哪个版本。
        """
        return "v2"

    registry.register_function(v1, name="t", policy=PUBLIC)
    executor = make_executor(registry)
    listing = executor.list_tools(Principal())
    registry.register_function(v2, name="t", policy=PUBLIC, version="2")
    registry.set_active("t", "2")

    pinned = await executor.execute_many([inv("t")], ToolContext(), snapshot_id=listing.snapshot_id)
    current = await executor.execute_many([inv("t")], ToolContext())
    assert (pinned[0].to_model_text(), current[0].to_model_text()) == ("v1", "v2")

    expired = await executor.execute_many([inv("t")], ToolContext(), snapshot_id="does-not-exist")
    assert expired[0].error and expired[0].error.code == "snapshot_expired"


async def test_batch_overflow_returns_errors_for_excess_calls(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """超过 max_batch_size 的调用直接返回错误结果，前面的调用照常执行。"""

    async def t() -> str:
        """测试工具。

        测试用：返回固定值，本身不是被测对象，只用来驱动执行器。
        """
        return "ok"

    registry.register_function(t, name="t", policy=PUBLIC)
    results = await make_executor(registry, max_batch_size=2).execute_many(
        [inv("t", call_id=str(i)) for i in range(3)], ToolContext()
    )
    assert [r.status for r in results] == ["success", "success", "error"]
    assert results[2].error and results[2].error.code == "batch_too_large"
    assert await make_executor(registry).execute_many([], ToolContext()) == []


async def test_per_tool_concurrency_limit_waits_within_deadline(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """单工具并发上限：等不到额度时返回 concurrency_limited；等待时间计入截止时间。"""
    gate = asyncio.Event()

    async def one_at_a_time() -> str:
        """一次只允许一个调用。"""
        await gate.wait()
        return "done"

    registry.register_function(
        one_at_a_time,
        name="single",
        policy=ToolPolicy(access="public", max_concurrency=1),
        annotations=SAFE,
    )
    executor = make_executor(registry)
    first = asyncio.create_task(executor.execute(inv("single"), ToolContext()))
    await asyncio.sleep(0.01)
    second = await executor.execute(inv("single"), ToolContext.with_timeout(0.05))
    assert second.error and second.error.code == "concurrency_limited"
    gate.set()
    assert (await first).ok
    assert executor.limiter.inflight == 0


# ---------------------------------------------------------------------------------
# 取消与关闭
# ---------------------------------------------------------------------------------
async def test_caller_cancellation_propagates_and_releases_everything(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """调用方取消：抛出 CancelledError，不返回部分结果；工具被取消，额度全部归还，Span 标记为 cancelled。"""
    started = asyncio.Event()
    cancelled_tools = 0

    async def wait_forever() -> str:
        """一直等待，直到被取消。"""
        nonlocal cancelled_tools
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled_tools += 1
            raise
        return "never"

    registry.register_function(wait_forever, name="w", policy=PUBLIC, annotations=SAFE)
    executor = make_executor(registry)
    batch = asyncio.create_task(
        executor.execute_many([inv("w", call_id="a"), inv("w", call_id="b")], ToolContext())
    )
    await started.wait()
    await asyncio.sleep(0.01)
    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch
    assert cancelled_tools == 2
    assert executor.limiter.inflight == 0
    statuses = [s.attributes.get("app.tool.status") for s in telemetry.finished_spans()]
    assert statuses == ["cancelled", "cancelled"]


async def test_cancellation_while_waiting_for_concurrency_slot(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """在排队等待并发额度时被取消，CancelledError 同样原样传播，且不会多占额度。"""
    gate = asyncio.Event()

    async def hold() -> str:
        """占住唯一的额度。"""
        await gate.wait()
        return "ok"

    registry.register_function(
        hold, name="h", policy=ToolPolicy(access="public", max_concurrency=1)
    )
    executor = make_executor(registry)
    first = asyncio.create_task(executor.execute(inv("h"), ToolContext()))
    await asyncio.sleep(0.01)
    waiter = asyncio.create_task(executor.execute(inv("h"), ToolContext()))
    await asyncio.sleep(0.01)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    gate.set()
    assert (await first).ok
    assert executor.limiter.inflight == 0


async def test_aclose_cancels_after_grace_and_rejects_new_calls(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """关闭执行器：宽限期后取消在途调用，调用方收到 executor_closed 结果；之后的调用抛出 ExecutorClosedError。"""
    started = asyncio.Event()

    async def wait_forever() -> str:
        """一直等待。

        测试用：模拟长时间运行的工具，让执行器关闭时它仍在途。
        """
        started.set()
        await asyncio.sleep(3600)
        return "never"

    registry.register_function(wait_forever, name="w", policy=PUBLIC)
    executor = make_executor(registry)
    call = asyncio.create_task(executor.execute(inv("w"), ToolContext()))
    await started.wait()
    await executor.aclose(grace_s=0.02)
    result = await call
    assert result.error and (result.error.type, result.error.code) == (
        "cancelled",
        "executor_closed",
    )
    with pytest.raises(ExecutorClosedError):
        await executor.execute(inv("w"), ToolContext())


# ---------------------------------------------------------------------------------
# 同步工具：线程无法终止时的额度管理
# ---------------------------------------------------------------------------------
async def test_sync_tool_timeout_keeps_quota_until_thread_finishes(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """同步工具超时后立即返回结果，但线程继续运行；额度和线程槽位要等线程结束才归还。"""
    release = threading.Event()
    observed_cancel = threading.Event()

    def blocking(cancel: threading.Event) -> str:
        """阻塞到测试放行；超时后可以看到协作式取消事件被设置。"""
        release.wait(5)
        if cancel.is_set():
            observed_cancel.set()
        return "done"

    registry.register_function(
        blocking, name="b", policy=ToolPolicy(access="public", timeout_s=0.05)
    )
    executor = make_executor(registry, sync_workers=2)
    result = await executor.execute(inv("b"), ToolContext())
    assert result.error and result.error.code == "attempt_timeout"
    runner = executor.thread_runner
    assert runner.orphaned == 1
    assert runner.available_slots == 1
    assert executor.limiter.inflight == 1  # 额度仍被孤儿线程占用

    release.set()
    for _ in range(100):
        if runner.orphaned == 0 and executor.limiter.inflight == 0:
            break
        await asyncio.sleep(0.01)
    assert (runner.orphaned, runner.available_slots, executor.limiter.inflight) == (0, 2, 0)
    assert observed_cancel.is_set()
    orphan_points = telemetry.metric_points("app.tool.sync.orphaned")
    assert orphan_points and orphan_points[0].value == 0


async def test_thread_pool_exhaustion_returns_concurrency_limited(
    registry: ToolRegistry, make_executor: ExecutorFactory
) -> None:
    """线程全部被孤儿线程占用时，新的同步调用在超时前拿不到线程，返回 start_timeout（并发受限）。"""
    release = threading.Event()

    def blocking() -> str:
        """阻塞到测试放行。"""
        release.wait(5)
        return "done"

    registry.register_function(
        blocking, name="b", policy=ToolPolicy(access="public", timeout_s=0.05)
    )
    executor = make_executor(registry, sync_workers=1)
    try:
        first = await executor.execute(inv("b"), ToolContext())
        assert first.error and first.error.code == "attempt_timeout"
        second = await executor.execute(inv("b"), ToolContext())
        assert second.error and (second.error.type, second.error.code) == (
            "throttled",
            "start_timeout",
        )
        assert second.error.retryable
    finally:
        release.set()
