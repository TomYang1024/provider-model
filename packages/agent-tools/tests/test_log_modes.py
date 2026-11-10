"""日志模式（redacted / full）与 ResultFilter 的测试。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from agent_tools import (
    OutputLimits,
    TextBlock,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolOutput,
    ToolPolicy,
    ToolRegistry,
    ToolSpec,
)
from agent_tools.observability import semconv as sc

ExecutorFactory = Callable[..., ToolExecutor]
ARGS = {"note": "mail me at ann@example.com, api_key=ABCDEF123456 " + "x" * 10000}


async def echo(note: str) -> str:
    """测试用：原样返回备注（用于检查日志预览中的脱敏与截断）。"""
    return note


def registry_with_echo(**policy: Any) -> ToolRegistry:
    """注册回显工具。

    Args:
        **policy: ``ToolPolicy`` 的额外字段。

    Returns:
        注册表。
    """
    registry = ToolRegistry()
    registry.register_function(echo, name="echo", policy=ToolPolicy(access="public", **policy))
    return registry


@pytest.mark.parametrize(
    ("mode", "limit", "email_visible"),
    [("redacted", 1024, False), ("full", 8192, True)],
)
async def test_content_modes_log_scrubbed_truncated_previews(
    make_executor: ExecutorFactory, telemetry: Any, mode: str, limit: int, email_visible: bool
) -> None:
    """redacted / full 记录参数与输出预览：凭证格式总会被清除，redacted 还清除个人信息；预览按模式截断。"""
    executor = make_executor(registry_with_echo(), log_mode=mode)
    result = await executor.execute(
        ToolInvocation(call_id="c", tool="echo", arguments=ARGS), ToolContext()
    )
    assert "ABCDEF123456" in result.to_model_text()  # 结果本身不被日志脱敏改变

    finish = telemetry.logs.events("tool.call.finish")[0]
    for field in ("arguments", "output"):
        text = finish[field]
        assert "ABCDEF123456" not in text and "[REDACTED]" in text
        assert ("ann@example.com" in text) is email_visible
        assert "[truncated" in text and len(text) <= limit + 40
    span = next(s for s in telemetry.finished_spans() if s.name.startswith("execute_tool"))
    attrs = dict(span.attributes or {})
    assert attrs[sc.GEN_AI_TOOL_CALL_ARGUMENTS] == finish["arguments"]
    assert attrs[sc.GEN_AI_TOOL_CALL_RESULT] == finish["output"]


async def test_metadata_mode_has_no_content_fields(
    make_executor: ExecutorFactory, telemetry: Any
) -> None:
    """默认的 metadata 模式不记录参数与输出字段，也不写入对应的 Span 属性。"""
    executor = make_executor(registry_with_echo())
    await executor.execute(ToolInvocation(call_id="c", tool="echo", arguments=ARGS), ToolContext())
    finish = telemetry.logs.events("tool.call.finish")[0]
    assert "arguments" not in finish and "output" not in finish
    span = next(s for s in telemetry.finished_spans() if s.name.startswith("execute_tool"))
    assert sc.GEN_AI_TOOL_CALL_ARGUMENTS not in dict(span.attributes or {})


class Redactor:
    """测试用的 ResultFilter：把文本中的 ``internal`` 替换为 ``[hidden]``，并记录调用。"""

    def __init__(self) -> None:
        """创建过滤器，并把调用次数初始化为 0（用于断言失败的调用不经过过滤器）。"""
        self.calls = 0

    def filter(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
        """替换文本块中的敏感词。

        Args:
            spec: 工具规格。
            output: 输出。
            ctx: 调用上下文。

        Returns:
            过滤后的输出。
        """
        self.calls += 1
        blocks = [
            TextBlock(text=b.text.replace("internal", "[hidden]"))
            if isinstance(b, TextBlock)
            else b
            for b in output.content
        ]
        return ToolOutput(content=blocks, structured=output.structured)


async def test_result_filter_changes_result_but_not_failures(
    make_executor: ExecutorFactory,
) -> None:
    """ResultFilter 改写返回给模型的成功结果；失败的调用不经过过滤器。"""
    redactor = Redactor()
    executor = make_executor(registry_with_echo(), result_filter=redactor)
    ok = await executor.execute(
        ToolInvocation(call_id="a", tool="echo", arguments={"note": "see internal wiki"}),
        ToolContext(),
    )
    bad = await executor.execute(
        ToolInvocation(call_id="b", tool="echo", arguments={}), ToolContext()
    )
    assert ok.to_model_text() == "see [hidden] wiki"
    assert bad.error and redactor.calls == 1


async def test_result_filter_failures_and_growth_fail_closed(
    make_executor: ExecutorFactory,
) -> None:
    """过滤器抛出异常时不返回未过滤的内容；过滤后的输出仍要满足大小限制。"""

    class Broken:
        """测试用：总是失败的过滤器。"""

        def filter(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
            """模拟过滤器故障。"""
            raise RuntimeError("filter crashed")

    class Inflate:
        """测试用：把输出放大到超过限制的过滤器。"""

        def filter(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
            """返回超大的 JSON 输出。"""
            return ToolOutput(structured={"blob": "y" * 500})

    inv = ToolInvocation(call_id="c", tool="echo", arguments={"note": "SENSITIVE"})
    broken = await make_executor(registry_with_echo(), result_filter=Broken()).execute(
        inv, ToolContext()
    )
    assert broken.error and broken.error.type == "internal"
    assert broken.content == [] and "SENSITIVE" not in broken.to_model_text()

    limits = OutputLimits(max_json_bytes=100)
    inflated = await make_executor(
        registry_with_echo(output_limits=limits), result_filter=Inflate()
    ).execute(inv, ToolContext())
    assert inflated.error and inflated.error.type == "output"
