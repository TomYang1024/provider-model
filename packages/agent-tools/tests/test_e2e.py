"""端到端测试：Provider SDK → Agent 循环示例 → 工具调用基础设施 → Python 工具 → 结果回填模型。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from agent_tools import Principal, ToolExecutor

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "agent_loop.py"


def load_example() -> ModuleType:
    """按文件路径加载示例模块（examples 不是包）。

    Returns:
        示例模块。
    """
    spec = importlib.util.spec_from_file_location("agent_loop_example", EXAMPLE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def test_agent_loop_end_to_end(telemetry: Any) -> None:
    """两轮对话：并发段 + 屏障调用，结果回填模型，全链路在同一个 Trace 中。"""
    example = load_example()
    timeline = example.ToolTimeline()
    provider = example.ScriptedProvider()
    tracer = telemetry.tracer_provider.get_tracer("test")
    principal = Principal(user_id="u1", scopes=frozenset({"notes:write"}))

    async with (
        example.build_sdk(provider, tracer=tracer) as sdk,
        ToolExecutor(
            example.build_registry(timeline), tracer=tracer, logger=telemetry.logs
        ) as executor,
    ):
        with tracer.start_as_current_span("agent.run") as root:
            run = await example.run_agent(
                sdk, executor, principal=principal, prompt="天气？", session_id="test-session"
            )

    # 结果与回填
    assert run.rounds == 2
    assert [r.status for r in run.tool_results] == ["success"] * 3
    assert "晴" in run.final_text and "多云" in run.final_text and "saved" in run.final_text
    first_request, second_request = provider.requests
    assert sorted(t["function"]["name"] for t in first_request["tools"]) == [
        "notes_save",
        "weather_get",
    ]
    tool_messages = [m for m in second_request["messages"] if m["role"] == "tool"]
    assert [m["tool_call_id"] for m in tool_messages] == ["call_1", "call_2", "call_3"]

    # 并发分段：两个天气查询重叠；记事工具在两者都结束后才开始
    at = timeline.at
    assert at("weather:上海", "start") < at("weather:北京", "end")
    assert at("notes.save", "start") >= max(at("weather:北京", "end"), at("weather:上海", "end"))

    # 同一个 Trace：两次模型调用和三次工具调用都挂在 agent.run 下
    trace_id = root.get_span_context().trace_id
    spans = telemetry.finished_spans()
    tool_spans = [s for s in spans if s.name.startswith("execute_tool")]
    chat_spans = [s for s in spans if s.name.startswith("chat ")]
    assert len(tool_spans) == 3 and len(chat_spans) == 2
    assert {s.context.trace_id for s in tool_spans + chat_spans} == {trace_id}
    # 工具调用与模型调用使用同一个 request_id，日志可以直接关联
    tool_request_ids = {r["request_id"] for r in telemetry.logs.events("tool.call.finish")}
    assert len(tool_request_ids) == 1
    tool_logs = telemetry.logs.events("tool.call.finish")
    assert all(entry["session_id"] == "test-session" for entry in tool_logs)
    assert all(entry["trace_id"] == f"{trace_id:032x}" for entry in tool_logs)
    assert all(entry["span_id"] for entry in tool_logs)


async def test_hidden_tool_called_by_model_is_denied_at_execution(telemetry: Any) -> None:
    """调用方没有 notes:write 权限：工具不出现在列表中，模型仍然调用时在执行阶段被拒绝。"""
    example = load_example()
    provider = example.ScriptedProvider()
    async with (
        example.build_sdk(provider) as sdk,
        ToolExecutor(
            example.build_registry(example.ToolTimeline()), logger=telemetry.logs
        ) as executor,
    ):
        run = await example.run_agent(
            sdk, executor, principal=Principal(user_id="guest"), prompt="?"
        )

    assert [t["function"]["name"] for t in provider.requests[0]["tools"]] == ["weather_get"]
    statuses = [(r.alias, r.status, r.error.type if r.error else None) for r in run.tool_results]
    assert statuses[2] == ("notes_save", "error", "permission")
    assert "permission_denied" in run.final_text


async def test_real_mode_uses_environment_sdk(monkeypatch: Any, capsys: Any) -> None:
    """真实模式使用环境配置入口，并通过模拟传输验证完整执行流程。"""
    example = load_example()
    provider = example.ScriptedProvider()
    calls: list[bool] = []

    def from_env(*, tracer: Any) -> Any:
        """记录真实模式选择，并返回使用测试传输的 SDK。"""
        calls.append(True)
        return example.build_sdk(provider, tracer=tracer)

    monkeypatch.setattr(example.ProviderSDK, "from_env", from_env)
    await example.main(["--real", "--verbose", "--session-id", "session-test"])
    output = capsys.readouterr().out
    assert calls == [True]
    assert len(provider.requests) == 2
    assert "会话 ID：session-test" in output
    assert "Trace ID：" in output
    assert "模型模式：真实服务" in output
    assert "天气数据是模拟的" in output
    assert "最终回答" in output
    assert "[第 1 轮] 请求模型" in output
    assert "[工具请求]" in output and "参数预览=" in output
    assert "[工具结果]" in output and "尝试次数=" in output
    assert "[第 2 轮] 模型返回" in output


async def test_agent_loop_calls_local_mcp_server(capfd: Any) -> None:
    """模拟模型请求 MCP 工具，验证真实 stdio 子进程执行并将结果回填。"""
    example = load_example()
    await example.main(["--mcp-demo", "--verbose"])
    output = capfd.readouterr().out
    assert "MCP 工具已注册：mcp.demo.echo" in output
    assert "名称=mcp_demo_echo" in output
    assert "[success] mcp_demo_echo: echo 你好 MCP 123" in output
    assert "最终回答（第 2 轮）：已完成：echo 你好 MCP 123" in output
