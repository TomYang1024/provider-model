"""P3 能力与 HTTP、MCP 工具统一执行链的兼容性测试。

* HTTP：内存网络（``FakeNetwork``，包在真实的 ``GuardedNetworkBackend`` 里）上验证
  ``target`` 粒度的熔断（同一主机的两个工具共享隔离键）、只读 HTTP 工具的结果缓存、
  超限 JSON 响应交给 ``on_oversize``；
* MCP：stdio 测试服务端上验证需要审批的 MCP 工具（拒绝时远端不会被调用）与熔断。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from agent_tools import (
    ApprovalDecision,
    ApprovalPolicy,
    ApprovalRequest,
    CachePolicy,
    CircuitBreakerConfig,
    HttpTarget,
    InMemoryResultStore,
    InProcessCircuitBreaker,
    NetworkPolicy,
    OutputLimitError,
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
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig
from agent_tools.adapters.mcp import (
    McpAdapter,
    McpServerConfig,
    McpServerManager,
    StdioTransportConfig,
)
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

HOST = "api.example.com"
SCHEMA = {
    "type": "object",
    "properties": {"q": {"type": "string"}},
    "required": ["q"],
    "additionalProperties": False,
}
SERVER_SCRIPT = Path(__file__).resolve().parent / "mcp_stdio_server.py"


async def public_dns(host: str, port: int) -> list[str]:
    """测试 DNS：解析到一个公网地址。"""
    return ["93.184.216.34"]


def http_executor(fake_network: Any, **executor_kwargs: Any) -> ToolExecutor:
    """注册同一主机上的两个 HTTP 工具，返回使用内存网络的执行器。

    Args:
        fake_network: 内存网络夹具。
        **executor_kwargs: 传给执行器的参数。

    Returns:
        执行器。
    """
    registry = ToolRegistry()
    policy = ToolPolicy(
        access="public",
        cache=CachePolicy(ttl_s=60, scope="tenant"),
        output_limits=OutputLimits(max_json_bytes=1024),
    )
    for name in ("search", "lookup"):
        registry.register_http(
            HttpTarget(method="GET", base_url=f"https://{HOST}", path_template=f"/{name}"),
            name=name,
            input_schema=SCHEMA,
            policy=policy,
            annotations=ToolAnnotations(read_only=True),
        )
    adapter = HttpAdapter(
        HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset({HOST}))),
        resolver=public_dns,
        network_backend=fake_network,
    )
    return ToolExecutor(registry, adapters={"http": adapter}, **executor_kwargs)


CTX = ToolContext(principal=Principal(user_id="u", tenant_id="t"))


def call(tool: str, q: str = "a") -> ToolInvocation:
    """构造 HTTP 工具调用。"""
    return ToolInvocation(call_id="c", tool=tool, arguments={"q": q})


async def test_http_circuit_by_target_and_cache(fake_network: Any, make_response: Any) -> None:
    """HTTP：同一主机的工具共享 ``target`` 隔离键——一个工具的 5xx 使另一个工具也被快速拒绝；
    缓存命中不访问网络。"""
    cb = InProcessCircuitBreaker(CircuitBreakerConfig(failure_threshold=2, scope="target"))
    async with http_executor(
        fake_network, circuit_breaker=cb, result_cache=InMemoryResultStore()
    ) as executor:
        fake_network.queue(make_response(200, json.dumps({"hits": 1}).encode()))
        first = await executor.execute(call("lookup"), CTX)
        cached = await executor.execute(call("lookup"), CTX)
        assert first.ok and cached.reused == "cache"
        assert len(fake_network.connections) == 1

        fake_network.queue(make_response(503), make_response(503))
        for q in ("x", "y"):
            failed = await executor.execute(call("search", q), CTX)
            assert failed.error is not None and failed.error.code == "upstream_server_error"
        rejected = await executor.execute(call("lookup", "new"), CTX)
        assert rejected.error is not None and rejected.error.code == "circuit_open"
        assert len(fake_network.connections) == 3
        assert cb.state(f"http:https://{HOST}:443") == "open"


async def test_http_oversize_hook(fake_network: Any, make_response: Any) -> None:
    """HTTP：超过输出限制的 JSON 响应交给 ``on_oversize`` 改写。"""

    class Summarize:
        """把超限输出改写成一行摘要。"""

        async def on_oversize(
            self, spec: ToolSpec, output: ToolOutput, error: OutputLimitError, ctx: ToolContext
        ) -> ToolOutput:
            """返回摘要。"""
            return ToolOutput(content=[TextBlock(text=f"{spec.name}: too many rows")])

    async with http_executor(fake_network, on_oversize=Summarize()) as executor:
        fake_network.queue(make_response(200, json.dumps({"rows": ["r" * 50] * 100}).encode()))
        result = await executor.execute(call("search"), CTX)
    assert result.ok and result.to_model_text() == "search: too many rows"


class Reject:
    """总是拒绝的审批处理器。"""

    def __init__(self) -> None:
        """创建处理器。"""
        self.requests: list[ApprovalRequest] = []

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        """记录请求并拒绝。"""
        self.requests.append(request)
        return ApprovalDecision(False, approver="ops")


async def test_mcp_tool_with_approval_and_circuit() -> None:
    """MCP：白名单中声明需要审批的远端工具，被拒绝时不会调用远端；熔断按服务端隔离。"""
    manager = McpServerManager(
        [
            McpServerConfig(
                server_id="local",
                transport=StdioTransportConfig(
                    command=sys.executable, args=("-I", str(SERVER_SCRIPT))
                ),
            )
        ]
    )
    registry = ToolRegistry()
    await McpToolSync(registry, manager).sync(
        "local",
        [
            McpToolAllow(
                remote_name="secret_echo",
                policy=ToolPolicy(access="public", approval=ApprovalPolicy(arguments="none")),
            ),
            McpToolAllow(remote_name="pid", policy=ToolPolicy(access="public")),
        ],
    )
    handler = Reject()
    cb = InProcessCircuitBreaker(CircuitBreakerConfig(scope="target"))
    async with ToolExecutor(
        registry,
        adapters={"mcp": McpAdapter(manager)},
        approval_handler=handler,
        circuit_breaker=cb,
    ) as executor:
        rejected = await executor.execute(
            ToolInvocation(call_id="c", tool="mcp_local_secret_echo", arguments={"text": "hi"}),
            ToolContext(),
        )
        assert rejected.error is not None and rejected.error.code == "approval_rejected"
        assert handler.requests[0].arguments is None
        ok = await executor.execute(
            ToolInvocation(call_id="p", tool="mcp_local_pid"), ToolContext()
        )
        assert ok.ok
        assert cb.state("mcp:local") == "closed"
