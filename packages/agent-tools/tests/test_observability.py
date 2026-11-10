"""可观测性的测试：Span、指标标签、元数据日志、遥测故障隔离与上下文隔离。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.semconv._incubating.attributes import gen_ai_attributes as genai
from opentelemetry.semconv.attributes import error_attributes

from agent_tools import (
    Principal,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.observability import semconv as sc

PUBLIC = ToolPolicy(access="public")
ExecutorFactory = Callable[..., ToolExecutor]
SECRET = "SECRET-VALUE-9f3a"


async def echo(text: str) -> str:
    """回显参数（用于检查日志中不出现内容）。"""
    return text


async def fail() -> None:
    """总是失败的工具。"""
    raise RuntimeError("boom")


def test_semconv_names_match_installed_package() -> None:
    """复制的属性名与已安装的语义规范包一致（规范升级时这里会提醒）。"""
    assert sc.GEN_AI_OPERATION_NAME == genai.GEN_AI_OPERATION_NAME
    assert sc.GEN_AI_TOOL_NAME == genai.GEN_AI_TOOL_NAME
    assert sc.GEN_AI_TOOL_CALL_ID == genai.GEN_AI_TOOL_CALL_ID
    assert sc.ERROR_TYPE == error_attributes.ERROR_TYPE
    assert genai.GenAiOperationNameValues.EXECUTE_TOOL.value == sc.OPERATION_EXECUTE_TOOL


async def test_span_is_child_of_caller_and_carries_tool_attributes(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """execute_tool Span 挂在调用方当前 Span 下，名称带别名，属性完整；失败时状态为 ERROR。"""
    registry.register_function(echo, name="text.echo", policy=PUBLIC)
    registry.register_function(fail, name="fail", policy=PUBLIC)
    executor = make_executor(registry)
    tracer = telemetry.tracer_provider.get_tracer("test")
    with tracer.start_as_current_span("agent.turn") as parent:
        ok = await executor.execute(
            ToolInvocation(call_id="c1", tool="text_echo", arguments={"text": "hi"}),
            ToolContext(request_id="req-9"),
        )
        bad = await executor.execute(ToolInvocation(call_id="c2", tool="fail"), ToolContext())

    spans = {s.name: s for s in telemetry.finished_spans()}
    span = spans["execute_tool text_echo"]
    assert span.parent is not None
    assert span.parent.span_id == parent.get_span_context().span_id
    assert span.kind == trace.SpanKind.INTERNAL
    attrs = dict(span.attributes or {})
    assert attrs[sc.GEN_AI_OPERATION_NAME] == "execute_tool"
    assert attrs[sc.GEN_AI_TOOL_NAME] == "text_echo"
    assert attrs[sc.GEN_AI_TOOL_CALL_ID] == "c1"
    assert attrs[sc.APP_TOOL_NAME] == "text.echo"
    assert attrs[sc.APP_REQUEST_ID] == "req-9"
    assert attrs[sc.APP_TOOL_STATUS] == "success"
    assert ok.trace_id == format(span.context.trace_id, "032x")

    failed = spans["execute_tool fail"]
    assert failed.status.status_code == trace.StatusCode.ERROR
    assert dict(failed.attributes or {})[sc.ERROR_TYPE] == "execution"
    assert bad.span_id == format(failed.context.span_id, "016x")


async def test_metrics_use_low_cardinality_labels(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """指标标签只有别名、类型、状态和错误类型；未注册的工具名统一记为 _other。"""
    registry.register_function(echo, name="echo", policy=PUBLIC)
    executor = make_executor(registry)
    await executor.execute(
        ToolInvocation(call_id="a", tool="echo", arguments={"text": "x"}), ToolContext()
    )
    await executor.execute(ToolInvocation(call_id="b", tool="made-up-by-model-123"), ToolContext())

    calls = telemetry.metric_points(sc.METRIC_CALLS)
    labels = [dict(p.attributes) for p in calls]
    assert {lbl[sc.GEN_AI_TOOL_NAME] for lbl in labels} == {"echo", sc.UNRESOLVED_TOOL}
    allowed_keys = {sc.GEN_AI_TOOL_NAME, sc.APP_TOOL_KIND, sc.APP_TOOL_STATUS, sc.ERROR_TYPE}
    assert all(set(lbl) <= allowed_keys for lbl in labels)
    assert telemetry.metric_points(sc.METRIC_DURATION)
    inflight = telemetry.metric_points(sc.METRIC_INFLIGHT)
    assert inflight and inflight[0].value == 0


async def test_metadata_logs_never_contain_arguments_or_outputs(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """默认的 metadata 日志只有字节数和哈希，日志与 Span 中都不出现参数和输出内容。"""
    registry.register_function(echo, name="echo", policy=PUBLIC)
    executor = make_executor(registry)
    result = await executor.execute(
        ToolInvocation(call_id="c", tool="echo", arguments={"text": SECRET}),
        ToolContext(principal=Principal(user_id="u1")),
    )
    assert SECRET in result.to_model_text()  # 结果本身不脱敏
    finish = telemetry.logs.events("tool.call.finish")[0]
    assert finish["args_bytes"] > 0 and finish["args_sha256"]
    assert finish["output_bytes"] > 0
    assert finish["user_id"] == "u1"
    assert SECRET not in repr(telemetry.logs.records)
    for span in telemetry.finished_spans():
        assert SECRET not in repr(dict(span.attributes or {}))


async def test_log_mode_off_disables_logs(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """log_mode="off" 时不输出任何日志，Span 与指标不受影响。"""
    registry.register_function(echo, name="echo", policy=PUBLIC)
    executor = make_executor(registry, log_mode="off")
    await executor.execute(
        ToolInvocation(call_id="c", tool="echo", arguments={"text": "x"}), ToolContext()
    )
    assert telemetry.logs.records == []
    assert telemetry.finished_spans()


async def test_telemetry_failures_do_not_change_results(registry: ToolRegistry) -> None:
    """日志器、tracer 抛出异常时：调用结果不变，失败被计数；取消信号不受影响。"""

    class BrokenLogger:
        """所有方法都抛异常的日志器。"""

        def __getattr__(self, name: str) -> Callable[..., None]:
            """返回一个总是抛异常的方法。"""

            def raiser(*args: Any, **kwargs: Any) -> None:
                """模拟日志系统故障。"""
                raise RuntimeError("log sink down")

            return raiser

    class BrokenTracer:
        """创建 Span 时抛异常的 tracer。"""

        def start_span(self, *args: Any, **kwargs: Any) -> Any:
            """模拟 tracer 故障。"""
            raise RuntimeError("tracer down")

    registry.register_function(echo, name="echo", policy=PUBLIC)
    async with ToolExecutor(
        registry,
        logger=BrokenLogger(),
        tracer=BrokenTracer(),  # type: ignore[arg-type]
    ) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="echo", arguments={"text": "ok"}), ToolContext()
        )
        assert result.ok and result.to_model_text() == "ok"
        assert result.trace_id is None
        assert executor.instrumentation.failures > 0


async def test_concurrent_calls_keep_their_own_parent_spans(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """100 个并发调用各自挂在自己的父 Span 下，上下文互不串扰。"""

    async def nap(i: int) -> int:
        """短暂让出事件循环，制造交错执行。"""
        await asyncio.sleep(0.001 * (i % 5))
        return i

    registry.register_function(nap, name="nap", policy=PUBLIC)
    executor = make_executor(registry, max_inflight=200)
    tracer = telemetry.tracer_provider.get_tracer("test")

    async def one(i: int) -> tuple[int, int]:
        """在独立的父 Span 下执行一次调用。

        Args:
            i: 序号。

        Returns:
            ``(父 Span ID, 序号)``。
        """
        with tracer.start_as_current_span(f"parent-{i}") as parent:
            await executor.execute(
                ToolInvocation(call_id=f"c{i}", tool="nap", arguments={"i": i}), ToolContext()
            )
            return parent.get_span_context().span_id, i

    pairs = await asyncio.gather(*(one(i) for i in range(100)))
    by_call = {
        dict(s.attributes or {})[sc.GEN_AI_TOOL_CALL_ID]: s
        for s in telemetry.finished_spans()
        if s.name == "execute_tool nap"
    }
    assert len(by_call) == 100
    for parent_id, i in pairs:
        span = by_call[f"c{i}"]
        assert span.parent is not None and span.parent.span_id == parent_id


async def test_sync_tool_thread_sees_execute_tool_span(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """同步工具在线程中运行时，当前 Span 是 execute_tool Span（Trace 上下文被带入线程）。"""

    def current_span_id() -> str:
        """返回线程中看到的当前 Span ID。"""
        return format(trace.get_current_span().get_span_context().span_id, "016x")

    registry.register_function(current_span_id, name="sid", policy=PUBLIC)
    result = await make_executor(registry).execute(
        ToolInvocation(call_id="c", tool="sid"), ToolContext()
    )
    assert result.to_model_text() == result.span_id


@pytest.mark.parametrize("mode", ["metadata"])
async def test_error_log_contains_error_type_but_not_exception_message(
    registry: ToolRegistry, make_executor: ExecutorFactory, telemetry: Any, mode: str
) -> None:
    """失败日志包含错误类型和错误码，不包含工具异常的消息。"""
    registry.register_function(fail, name="fail", policy=PUBLIC)
    await make_executor(registry, log_mode=mode).execute(
        ToolInvocation(call_id="c", tool="fail"), ToolContext()
    )
    error = telemetry.logs.events("tool.call.error")[0]
    assert (error["error_type"], error["error_code"]) == ("execution", "tool_raised")
    assert "boom" not in repr(telemetry.logs.records)
