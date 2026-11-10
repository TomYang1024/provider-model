"""端到端示例：Provider SDK → Agent 循环 → 工具调用基础设施 → Python 工具 → 结果回填模型。

使用一个脚本化的假 Provider（``httpx2.MockTransport``），不需要 API Key 和网络：

* 第 1 轮：模型同时请求两个并发安全的天气查询，以及一个需要权限、非并发安全的记事工具；
* 第 2 轮：模型读到三个工具结果后给出最终回答。

运行模拟模型：``uv run python packages/agent-tools/examples/agent_loop.py``。
运行真实模型：先配置对应 Provider 的 API Key，再加 ``--real``；工具数据仍为模拟数据。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from collections.abc import Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx2
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from provider_sdk import Message, ProviderSDK, RequestContext, SDKConfig, ToolResult

from agent_tools import (
    Principal,
    ToolAnnotations,
    ToolContext,
    ToolExecutionResult,
    ToolExecutor,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.integrations.provider_sdk import run_tool_calls, to_tool_definitions
from agent_tools.observability.redaction import preview

PROVIDER = "deepseek"
MODEL = "deepseek-flash"
DEMO_ENV = {"DEEPSEEK_API_KEY": "sk-demo000000000000000000000000"}


@dataclass
class ToolTimeline:
    """记录工具开始与结束的时刻，用来观察并发分段是否生效。"""

    events: list[tuple[str, str, float]] = field(default_factory=list)

    def mark(self, tool: str, phase: str) -> None:
        """记录一个事件。

        Args:
            tool: 工具与参数的简短标识。
            phase: ``start`` 或 ``end``。
        """
        self.events.append((tool, phase, time.perf_counter()))

    def at(self, tool: str, phase: str) -> float:
        """查询某个事件的时刻。

        Args:
            tool: 工具与参数的简短标识。
            phase: ``start`` 或 ``end``。

        Returns:
            事件的 ``perf_counter`` 时刻。

        Raises:
            KeyError: 事件不存在。
        """
        for name, ph, ts in self.events:
            if name == tool and ph == phase:
                return ts
        raise KeyError((tool, phase))


def build_registry(timeline: ToolTimeline) -> ToolRegistry:
    """注册示例工具。

    * ``weather.get``：异步、公开、并发安全；
    * ``notes.save``：同步（在专用线程池中运行）、需要 ``notes:write`` 权限、非并发安全，
      并通过注入的 :class:`ToolContext` 读取请求 ID。

    Args:
        timeline: 记录工具执行时刻的时间线。

    Returns:
        注册好工具的注册表。
    """
    registry = ToolRegistry()

    async def get_weather(city: str) -> dict[str, Any]:
        """查询城市的当前天气。"""
        timeline.mark(f"weather:{city}", "start")
        await asyncio.sleep(0.05)  # 模拟 I/O
        timeline.mark(f"weather:{city}", "end")
        condition = {"北京": "晴", "上海": "多云"}.get(city, "未知")
        return {"city": city, "condition": condition, "temp_c": 25}

    def save_note(text: str, ctx: ToolContext) -> str:
        """保存一条备忘。"""
        timeline.mark("notes.save", "start")
        timeline.mark("notes.save", "end")
        return f"saved {len(text)} chars for request {ctx.request_id}"

    registry.register_function(
        get_weather,
        name="weather.get",
        policy=ToolPolicy(access="public", timeout_s=5),
        annotations=ToolAnnotations(read_only=True, concurrency_safe=True, open_world=True),
    )
    registry.register_function(
        save_note,
        name="notes.save",
        policy=ToolPolicy(access="scoped", required_scopes=frozenset({"notes:write"})),
    )
    return registry


class ScriptedProvider:
    """脚本化的 OpenAI 兼容假 Provider：第 1 轮返回工具调用，第 2 轮返回最终回答。"""

    def __init__(self, *, mcp_demo: bool = False) -> None:
        """创建假 Provider；可选择生成本地 MCP 工具调用。"""
        self.mcp_demo = mcp_demo
        self.requests: list[dict[str, Any]] = []

    def transport(self) -> httpx2.MockTransport:
        """返回供 Provider SDK 使用的 Mock 传输层。"""
        return httpx2.MockTransport(self._handle)

    def _handle(self, request: httpx2.Request) -> httpx2.Response:
        """按轮次返回预设的响应。

        Args:
            request: SDK 发出的请求。

        Returns:
            OpenAI 兼容的 Chat Completions 响应。
        """
        body = json.loads(request.content)
        self.requests.append(body)
        if len(self.requests) == 1:
            return httpx2.Response(200, json=self._tool_call_response())
        tool_messages = [m for m in body["messages"] if m["role"] == "tool"]
        summary = " | ".join(str(m["content"]) for m in tool_messages)
        return httpx2.Response(
            200, json=_completion({"role": "assistant", "content": f"已完成：{summary}"}, "stop")
        )

    def _tool_call_response(self) -> dict[str, Any]:
        """构造第 1 轮的工具调用响应（使用模型侧别名）。

        即使 ``notes_save`` 没有出现在发给模型的工具列表里（调用方没有权限），
        这里也照样请求它，用来演示执行时鉴权：隐藏工具不等于权限控制。

        Returns:
            默认模式包含三个工具调用，MCP 演示模式包含一个回显调用。
        """
        if self.mcp_demo:
            calls = [_tool_call("call_mcp", "mcp_demo_echo", {"text": "你好 MCP"})]
            message = {"role": "assistant", "content": None, "tool_calls": calls}
            return _completion(message, "tool_calls")
        calls = [
            _tool_call("call_1", "weather_get", {"city": "北京"}),
            _tool_call("call_2", "weather_get", {"city": "上海"}),
            _tool_call("call_3", "notes_save", {"text": "提醒：带伞"}),
        ]
        message = {"role": "assistant", "content": None, "tool_calls": calls}
        return _completion(message, "tool_calls")


def _tool_call(call_id: str, name: str, args: dict[str, Any]) -> dict[str, Any]:
    """构造一个 OpenAI 格式的工具调用。

    Args:
        call_id: 调用 ID。
        name: 工具名（模型侧别名）。
        args: 参数。

    Returns:
        工具调用字典。
    """
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)},
    }


def _completion(message: dict[str, Any], finish_reason: str) -> dict[str, Any]:
    """构造一个 OpenAI 格式的 Chat Completions 响应。

    Args:
        message: assistant 消息。
        finish_reason: 结束原因。

    Returns:
        响应字典。
    """
    return {
        "id": "cmpl-demo",
        "object": "chat.completion",
        "created": 1,
        "model": MODEL,
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


@dataclass
class AgentRun:
    """一次 Agent 运行的结果。"""

    final_text: str
    rounds: int
    tool_results: list[ToolExecutionResult] = field(default_factory=list)


async def run_agent(
    sdk: ProviderSDK,
    executor: ToolExecutor,
    *,
    principal: Principal,
    prompt: str,
    max_rounds: int = 5,
    provider: str = PROVIDER,
    model: str = MODEL,
    verbose: bool = False,
    session_id: str | None = None,
) -> AgentRun:
    """最小的 Agent 循环（相当于宿主的 Agent Runtime）。

    每一轮：列出调用方可见的工具 → 调用模型 → 如果模型请求工具，就交给执行器批量执行，
    把结果作为 ``tool`` 消息回填，进入下一轮。轮次有上限，防止模型无限调用工具。

    Args:
        sdk: Provider SDK 客户端。
        executor: 工具执行器。
        principal: 已经认证过的调用方身份与权限。
        prompt: 用户输入。
        max_rounds: 最多轮次。
        provider: 模型服务商名称。
        model: 模型名称，应与服务商配置一致。
        verbose: 显示调用轮次与脱敏后的工具参数预览。
        session_id: 同一次会话的标识；不传时生成新的标识。

    Returns:
        最终回答、实际轮次和全部工具执行结果。

    Raises:
        RuntimeError: 超过最多轮次仍未得到最终回答。
    """
    session_id = session_id or f"session_{uuid.uuid4().hex}"
    history: list[Message | ToolResult] = [Message.system("你是助手"), Message.user(prompt)]
    all_results: list[ToolExecutionResult] = []
    for round_no in range(1, max_rounds + 1):
        listing = executor.list_tools(principal)
        if verbose:
            names = ", ".join(tool.alias for tool in listing.tools) or "无"
            print(
                f"\n[第 {round_no} 轮] 请求模型 {provider}/{model}；"
                f"消息数={len(history)}；可用工具={names}"
            )
        response = await sdk.messages(
            provider=provider,
            model=model,
            messages=history,
            tools=to_tool_definitions(listing),
            context=RequestContext(
                user_id=principal.user_id, tenant_id=principal.tenant_id, session_id=session_id
            ),
        )
        if verbose:
            usage = response.usage
            print(
                f"[第 {round_no} 轮] 模型返回：request_id={response.request_id}，"
                f"结束原因={response.finish_reason}，工具调用数={len(response.tool_calls)}，"
                f"输入 token={usage.input_tokens}，输出 token={usage.output_tokens}，"
                f"来源={usage.source}"
            )
            for call in response.tool_calls:
                args_preview = preview(call.arguments, pii=True, max_chars=512)
                print(f"[工具请求] id={call.id}，名称={call.name}，参数预览={args_preview}")
        history.append(response.message)
        if response.finish_reason != "tool_calls":
            return AgentRun(final_text=response.text, rounds=round_no, tool_results=all_results)
        # 沿用模型调用的 request_id，使模型调用与工具调用的日志可以直接关联
        ctx = ToolContext.with_timeout(
            30, principal=principal, request_id=response.request_id, session_id=session_id
        )
        results, tool_messages = await run_tool_calls(
            executor, response, ctx, snapshot_id=listing.snapshot_id
        )
        if verbose:
            for result in results:
                print(
                    f"[工具结果] id={result.call_id}，名称={result.alias}，状态={result.status}，"
                    f"尝试次数={result.attempts}，耗时={result.duration_ms}ms，"
                    f"错误码={result.error.code if result.error else '-'}"
                )
        all_results.extend(results)
        history.extend(tool_messages)
    raise RuntimeError("the agent did not finish within max_rounds")


def build_sdk(provider: ScriptedProvider, *, tracer: trace.Tracer | None = None) -> ProviderSDK:
    """创建连接到假 Provider 的 SDK 客户端。

    Args:
        provider: 假 Provider。
        tracer: 可选的 tracer（测试中用于收集 Span）。

    Returns:
        SDK 客户端。
    """
    config = SDKConfig.from_env(DEMO_ENV)
    return ProviderSDK(config, tracer=tracer, transports={PROVIDER: provider.transport()})


async def main(argv: Sequence[str] | None = None) -> None:
    """运行示例并打印结果。

    Args:
        argv: 命令行参数；None 表示读取进程参数。
    """
    parser = argparse.ArgumentParser(description="演示模型与工具调用；天气和记事为模拟数据")
    parser.add_argument("--real", action="store_true", help="使用环境变量配置的真实模型服务")
    parser.add_argument("--provider", default=PROVIDER, help="真实模型服务商，默认 deepseek")
    parser.add_argument("--model", default=MODEL, help="模型名称，默认 deepseek-flash")
    parser.add_argument("--verbose", action="store_true", help="显示每轮调用和脱敏参数预览")
    parser.add_argument("--mcp-demo", action="store_true", help="启动本地只读 MCP 回显服务")
    parser.add_argument("--session-id", help="沿用已有会话 ID；默认每次运行生成一个")
    parser.add_argument("--prompt", help="自定义发送给模型的请求")
    args = parser.parse_args(argv)
    if not args.real and (args.provider != PROVIDER or args.model != MODEL):
        parser.error("--provider 和 --model 只能与 --real 一起使用")

    timeline = ToolTimeline()
    session_id = args.session_id or f"session_{uuid.uuid4().hex}"
    # 显式注入同一个 tracer；不修改宿主进程的全局 OpenTelemetry 配置。
    tracer_provider = TracerProvider()
    tracer = tracer_provider.get_tracer("agent-tools-example")
    try:
        async with AsyncExitStack() as stack:
            registry = build_registry(timeline)
            adapters: dict[str, Any] = {}
            if args.mcp_demo:
                # 延迟导入：未启用 MCP 时不要求安装 MCP 可选依赖。
                from agent_tools.adapters.mcp import (
                    McpAdapter,
                    McpServerConfig,
                    McpServerManager,
                    StdioTransportConfig,
                )
                from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

                server_path = Path(__file__).with_name("mcp_echo_server.py")
                manager = McpServerManager(
                    [
                        McpServerConfig(
                            server_id="demo",
                            transport=StdioTransportConfig(
                                command=sys.executable, args=("-I", str(server_path))
                            ),
                        )
                    ]
                )
                stack.push_async_callback(manager.aclose)
                report = await McpToolSync(registry, manager).sync(
                    "demo",
                    [
                        McpToolAllow(
                            remote_name="echo",
                            policy=ToolPolicy(access="public"),
                            annotations=ToolAnnotations(read_only=True, concurrency_safe=True),
                            description="使用 MCP 回显工具，为文本添加 echo 前缀和 123 后缀。",
                        )
                    ],
                )
                if not report.registered:
                    raise RuntimeError("MCP 回显工具注册失败")
                adapters["mcp"] = McpAdapter(manager)
                print(f"MCP 工具已注册：{', '.join(report.registered)}")
            sdk = (
                ProviderSDK.from_env(tracer=tracer)
                if args.real
                else build_sdk(ScriptedProvider(mcp_demo=args.mcp_demo), tracer=tracer)
            )
            print("模型模式：真实服务" if args.real else "模型模式：模拟服务")
            print("注意：天气数据是模拟的，记事工具不会实际保存。")
            print(f"会话 ID：{session_id}")
            executor = ToolExecutor(registry, tracer=tracer, adapters=adapters)
            async with sdk, executor:
                with tracer.start_as_current_span("agent.run") as span:
                    print(f"Trace ID：{span.get_span_context().trace_id:032x}")
                    prompt = args.prompt or (
                        "请调用 MCP 回显工具，处理文本：你好 MCP"
                        if args.mcp_demo
                        else "北京和上海今天天气怎么样？顺便记一下带伞。"
                    )
                    run = await run_agent(
                        sdk,
                        executor,
                        principal=Principal(user_id="demo-user", scopes=frozenset({"notes:write"})),
                        prompt=prompt,
                        provider=args.provider,
                        model=args.model,
                        verbose=args.verbose,
                        session_id=session_id,
                    )
    finally:
        tracer_provider.shutdown()
    for result in run.tool_results:
        print(f"[{result.status}] {result.alias}: {result.to_model_text()}")
    print(f"最终回答（第 {run.rounds} 轮）：{run.final_text}")


if __name__ == "__main__":
    asyncio.run(main())
