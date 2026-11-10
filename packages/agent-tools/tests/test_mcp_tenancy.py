"""MCP 租户会话隔离的测试（``McpServerConfig.session_scope="tenant"``）。

隔离方式与 ``test_mcp.py`` 相同：

* Streamable HTTP：内存中的 ASGI 应用 + 测试专用的 ``http_client_factory``。除最终的 HTTP 传输
  外都是生产代码（会话选择、凭证获取、SDK 会话、结果映射、密钥清除）；
* stdio：受控的测试子进程，验证“每个租户一个进程”、密钥环境变量、重连与关闭后的回收。

覆盖：凭证与会话按租户分开、并发下不串用、没有租户时安全失败、服务端范围保持原行为、
重连与凭证变更（失效）、数量上限与空闲回收、冷却期按租户统计、取消后的在途计数、
发现阶段的租户上下文。
"""

from __future__ import annotations

import asyncio
import contextvars
import os
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx2
import mcp.types as mt
import pytest
from mcp.server.lowlevel import Server

from agent_tools import (
    NetworkPolicy,
    PermissionDeniedError,
    Principal,
    SecretRef,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.adapters.mcp import (
    HttpTransportConfig,
    McpAdapter,
    McpServerConfig,
    McpServerManager,
    StdioTransportConfig,
)
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

PUBLIC = ToolPolicy(access="public")
SERVER_SCRIPT = Path(__file__).resolve().parent / "mcp_stdio_server.py"
#: 当前 HTTP 请求带来的 Authorization 请求头（由 ASGI 包装层设置，工具处理函数读取）。
REQUEST_AUTH: contextvars.ContextVar[str] = contextvars.ContextVar("auth", default="")


def token(tenant: str | None, generation: int = 1) -> str:
    """生成某个租户的测试凭证（足够长，会被精确清除）。

    Args:
        tenant: 租户 ID；``None`` 表示服务端级凭证。
        generation: 凭证代次（模拟轮换）。

    Returns:
        凭证值。
    """
    return f"token-{tenant or 'server'}-gen{generation}-0123456789"


class TenantSecrets:
    """按租户返回凭证的密钥提供者，并记录每次查询使用的上下文。"""

    def __init__(self) -> None:
        """创建提供者：所有租户的凭证从第 1 代开始。"""
        self.generation: dict[str | None, int] = {}
        self.contexts: list[ToolContext] = []
        self.unavailable: set[str | None] = set()

    async def get_secret(self, name: str, ctx: ToolContext) -> str:
        """返回调用上下文所属租户的凭证。

        Args:
            name: 密钥名（``tag`` 返回租户 ID 本身，其余返回凭证）。
            ctx: 调用上下文；只应带有租户。

        Returns:
            凭证值。

        Raises:
            KeyError: 该租户被标记为不可用。
        """
        self.contexts.append(ctx)
        tenant = ctx.principal.tenant_id
        if tenant in self.unavailable:
            raise KeyError(name)
        if name == "tag":
            return tenant or "-"
        return token(tenant, self.generation.get(tenant, 1))


@dataclass
class Remote:
    """内存 MCP 服务端的状态。

    Attributes:
        seen: 每次 ``whoami`` 调用的 ``(参数 text, 请求携带的 Authorization)``。
        opened: 客户端工厂每次建立会话时收到的 Authorization。
        gate: ``hold`` 工具等待的事件（用于制造在途请求）。
        holding: 正在等待的 ``hold`` 调用数。
        reject: 这些 Authorization 值的请求直接返回 401（模拟凭证错误导致握手失败）。
    """

    seen: list[tuple[str, str]] = field(default_factory=list)
    opened: list[str] = field(default_factory=list)
    gate: asyncio.Event = field(default_factory=asyncio.Event)
    holding: int = 0
    reject: set[str] = field(default_factory=set)


@pytest.fixture
async def remote() -> AsyncIterator[tuple[Remote, Any]]:
    """启动内存中的 Streamable HTTP 服务端。

    Yields:
        ``(远端状态, http_client_factory)``。
    """
    state = Remote()

    async def list_tools(ctx: Any, params: Any) -> mt.ListToolsResult:
        """列出测试工具。"""
        schema = {"type": "object", "properties": {"text": {"type": "string"}}}
        return mt.ListToolsResult(
            tools=[mt.Tool(name=n, description=n, input_schema=schema) for n in ("whoami", "hold")]
        )

    async def call_tool(ctx: Any, params: mt.CallToolRequestParams) -> mt.CallToolResult:
        """``whoami`` 记录并回显请求的凭证；``hold`` 等到测试放行。"""
        text = str((params.arguments or {}).get("text", ""))
        auth = REQUEST_AUTH.get()
        if params.name == "hold":
            state.holding += 1
            try:
                await state.gate.wait()
            finally:
                state.holding -= 1
        else:
            state.seen.append((text, auth))
        return mt.CallToolResult(content=[mt.TextContent(type="text", text=f"{text} via {auth}")])

    server = Server("tenants", on_list_tools=list_tools, on_call_tool=call_tool)
    app = server.streamable_http_app(stateless_http=True, json_response=True)

    async def capture(scope: Any, receive: Any, send: Any) -> None:
        """把请求的 Authorization 放进上下文变量后转交给 MCP 应用；被拒绝的凭证返回 401。"""
        if scope["type"] == "http":
            headers = {k.decode(): v.decode() for k, v in scope["headers"]}
            auth = headers.get("authorization", "")
            if auth in state.reject:
                await send({"type": "http.response.start", "status": 401, "headers": []})
                await send({"type": "http.response.body", "body": b""})
                return
            REQUEST_AUTH.set(auth)
        await app(scope, receive, send)

    def factory(cfg: HttpTransportConfig, headers: Any) -> httpx2.AsyncClient:
        """创建连接内存应用的 HTTP 客户端，并记录这条会话使用的凭证。"""
        state.opened.append(dict(headers).get("Authorization", ""))
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=capture),
            base_url="http://localhost:8000",
            headers=dict(headers),
        )

    ready, stop = asyncio.Event(), asyncio.Event()

    async def serve() -> None:
        """在后台任务中运行 ASGI 应用的 lifespan，直到测试结束。"""
        async with app.router.lifespan_context(app):
            ready.set()
            await stop.wait()

    task = asyncio.create_task(serve())
    await ready.wait()
    yield state, factory
    state.gate.set()
    stop.set()
    await task


class FakeClock:
    """可以手动推进的单调时钟（用于空闲回收、LRU 与冷却期）。"""

    def __init__(self) -> None:
        """创建时钟，初始时刻为 1000。"""
        self.now = 1000.0

    def monotonic(self) -> float:
        """返回测试设置的当前时刻。"""
        return self.now


async def loopback(host: str, port: int) -> list[str]:
    """测试 DNS：解析到回环地址（网络策略显式放行 127.0.0.0/8）。"""
    return ["127.0.0.1"]


def http_server(scope: str = "tenant", **kw: Any) -> McpServerConfig:
    """构造指向内存服务端、带凭证的 HTTP 服务端配置。

    Args:
        scope: 会话范围。
        **kw: ``McpServerConfig`` 的其他字段。

    Returns:
        服务端配置。
    """
    network = NetworkPolicy(
        allowed_hosts=frozenset({"localhost"}),
        allowed_ports=frozenset({8000}),
        allow_http=True,
        allowed_networks=("127.0.0.0/8",),
    )
    return McpServerConfig(
        server_id="remote",
        transport=HttpTransportConfig(
            url="http://localhost:8000/mcp", network=network, auth=SecretRef(name="api")
        ),
        session_scope=scope,  # type: ignore[arg-type]
        **kw,
    )


def tenant_ctx(tenant: str | None, **principal: Any) -> ToolContext:
    """构造某个租户的调用上下文。

    Args:
        tenant: 租户 ID。
        **principal: ``Principal`` 的其他字段。

    Returns:
        调用上下文。
    """
    return ToolContext(principal=Principal(tenant_id=tenant, **principal))


async def http_setup(
    remote: tuple[Remote, Any],
    server: McpServerConfig,
    *,
    discovery_tenant: str | None = "ops",
    clock: FakeClock | None = None,
) -> tuple[ToolExecutor, McpServerManager, TenantSecrets]:
    """创建管理器、用指定租户做发现，并返回执行器。

    Args:
        remote: ``remote`` 夹具的返回值。
        server: 服务端配置。
        discovery_tenant: 发现使用的租户；``None`` 表示不带上下文。
        clock: 可注入的时钟。

    Returns:
        ``(执行器, 管理器, 密钥提供者)``。
    """
    _, factory = remote
    secrets = TenantSecrets()
    manager = McpServerManager(
        [server], secrets=secrets, resolver=loopback, http_client_factory=factory, clock=clock
    )
    registry = ToolRegistry()
    allow = [McpToolAllow(remote_name=n, policy=PUBLIC) for n in ("whoami", "hold")]
    ctx = tenant_ctx(discovery_tenant) if discovery_tenant else None
    await McpToolSync(registry, manager).sync("remote", allow, ctx=ctx)
    return ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)}), manager, secrets


async def call(executor: ToolExecutor, tool: str, tenant: str | None, text: str = "") -> Any:
    """以某个租户的身份执行一次调用。

    Args:
        executor: 执行器。
        tool: 远端工具名（``whoami`` / ``hold`` / stdio 工具）。
        tenant: 租户 ID。
        text: ``text`` 参数。

    Returns:
        调用结果。
    """
    alias = f"mcp_remote_{tool}" if tool in ("whoami", "hold") else f"mcp_local_{tool}"
    args = {"text": text} if text else {}
    return await executor.execute(
        ToolInvocation(call_id="c", tool=alias, arguments=args), tenant_ctx(tenant)
    )


# ---------------------------------------------------------------------------------
# 凭证与会话按租户分开
# ---------------------------------------------------------------------------------
async def test_concurrent_tenants_never_share_sessions_or_credentials(remote: Any) -> None:
    """多个租户并发调用：每个请求只带自己租户的凭证；每个租户只建立一条会话（单飞）。"""
    state, _ = remote
    executor, manager, _ = await http_setup(remote, http_server())
    state.opened.clear()
    tenants = ["a", "b", "c"]
    results = await asyncio.gather(
        *(call(executor, "whoami", t, text=t) for _ in range(8) for t in tenants)
    )
    assert all(r.ok for r in results)
    assert len(state.seen) == 24
    for tenant, auth in state.seen:
        assert auth == f"Bearer {token(tenant)}"
    # 发现用的 ops 会话之外，每个租户恰好一条会话
    assert sorted(state.opened) == sorted(f"Bearer {token(t)}" for t in tenants)
    assert sorted(manager.session_tenants("remote"), key=str) == ["a", "b", "c", "ops"]
    await executor.aclose()


async def test_result_only_scrubs_the_calling_tenants_credential(remote: Any) -> None:
    """服务端回显凭证时，结果中本租户的凭证被清除；登记的密钥集合里没有其他租户的凭证。"""
    executor, manager, _ = await http_setup(remote, http_server())
    result = await call(executor, "whoami", "a", text="a")
    assert result.ok and token("a") not in result.to_model_text()
    assert "[REDACTED_SECRET]" in result.to_model_text()
    await call(executor, "whoami", "b", text="b")
    assert set(manager.known_secrets("remote", tenant_id="a")) == {
        token("a"),
        f"Bearer {token('a')}",
    }
    assert token("a") not in manager.known_secrets("remote", tenant_id="b")
    await executor.aclose()


async def test_secret_provider_sees_only_the_tenant(remote: Any) -> None:
    """获取会话凭证的上下文只带租户：不带触发连接的那次调用的用户、权限或请求 ID。"""
    executor, _, secrets = await http_setup(remote, http_server())
    secrets.contexts.clear()
    ctx = ToolContext(
        request_id="req-user-1",
        principal=Principal(tenant_id="a", user_id="alice", scopes=frozenset({"admin"})),
    )
    result = await executor.execute(
        ToolInvocation(call_id="c", tool="mcp_remote_whoami", arguments={}), ctx
    )
    assert result.ok
    (seen,) = secrets.contexts
    assert seen.principal == Principal(tenant_id="a")
    assert seen.request_id == "mcp-connect-remote"
    await executor.aclose()


async def test_missing_tenant_fails_closed_without_connecting(remote: Any) -> None:
    """租户范围的服务端收到没有租户的调用：拒绝（permission），不建立会话、不到达远端。"""
    state, _ = remote
    executor, manager, _ = await http_setup(remote, http_server())
    state.opened.clear()
    result = await call(executor, "whoami", None, text="anonymous")
    assert result.error is not None
    assert (result.error.type, result.error.code) == ("permission", "mcp_tenant_required")
    assert state.opened == [] and state.seen == []
    assert None not in manager.session_tenants("remote")
    await executor.aclose()


async def test_server_scope_keeps_shared_session_and_server_credential(remote: Any) -> None:
    """默认的服务端范围保持原行为：所有租户共享一条会话，凭证用不带租户的上下文获取。"""
    state, _ = remote
    executor, manager, secrets = await http_setup(
        remote, http_server(scope="server"), discovery_tenant=None
    )
    for tenant in ("a", "b", None):
        assert (await call(executor, "whoami", tenant, text=str(tenant))).ok
    assert state.opened == [f"Bearer {token(None)}"]
    assert {auth for _, auth in state.seen} == {f"Bearer {token(None)}"}
    assert all(c.principal.tenant_id is None for c in secrets.contexts)
    assert manager.session_tenants("remote") == [None]
    assert manager.connected("remote")
    await executor.aclose()


async def test_discovery_requires_tenant_context_for_tenant_scope(remote: Any) -> None:
    """租户范围的服务端做发现时必须指定租户；缺少时失败且注册表不变。"""
    _, factory = remote
    manager = McpServerManager(
        [http_server()], secrets=TenantSecrets(), resolver=loopback, http_client_factory=factory
    )
    registry = ToolRegistry()
    sync = McpToolSync(registry, manager)
    allow = [McpToolAllow(remote_name="whoami", policy=PUBLIC)]
    before = registry.snapshot().id
    with pytest.raises(PermissionDeniedError) as info:
        await sync.sync("remote", allow)
    assert info.value.code == "mcp_tenant_required" and registry.snapshot().id == before
    report = await sync.sync("remote", allow, ctx=tenant_ctx("ops"))
    assert report.registered == ("mcp.remote.whoami",)
    await manager.aclose()


# ---------------------------------------------------------------------------------
# 凭证变更、失效与冷却期
# ---------------------------------------------------------------------------------
async def test_invalidate_refetches_credentials_for_one_tenant_only(remote: Any) -> None:
    """凭证轮换：失效前仍用旧会话；让一个租户失效后它用新凭证重建会话，其他租户的会话不受影响。"""
    state, _ = remote
    executor, manager, secrets = await http_setup(remote, http_server())
    await call(executor, "whoami", "a", text="a")
    await call(executor, "whoami", "b", text="b")
    secrets.generation["a"] = 2
    await call(executor, "whoami", "a", text="a")
    assert state.seen[-1] == ("a", f"Bearer {token('a', 1)}")  # 会话未失效，仍是旧凭证

    opened_before = len(state.opened)
    assert await manager.invalidate("remote", tenant_id="a") == 1
    assert not manager.connected("remote", tenant_id="a")
    assert manager.connected("remote", tenant_id="b")
    await call(executor, "whoami", "a", text="a")
    await call(executor, "whoami", "b", text="b")
    assert state.seen[-2] == ("a", f"Bearer {token('a', 2)}")
    assert state.seen[-1] == ("b", f"Bearer {token('b', 1)}")
    assert len(state.opened) == opened_before + 1  # 只有租户 a 重新建立了会话

    # 不指定租户：该服务端的全部会话失效
    assert await manager.invalidate("remote") == 3
    assert manager.session_tenants("remote") == []
    await executor.aclose()


async def test_graceful_invalidate_lets_inflight_calls_finish(remote: Any) -> None:
    """默认失效方式：在途请求在旧会话上正常完成；新调用使用新会话；旧会话随后被关闭。"""
    state, _ = remote
    executor, manager, _ = await http_setup(remote, http_server())
    inflight = asyncio.create_task(call(executor, "hold", "a"))
    while state.holding == 0:
        await asyncio.sleep(0.01)
    old = manager._sessions[("remote", "a")]
    assert await manager.invalidate("remote", tenant_id="a") == 1
    assert old.retired and old.connection is not None  # 还有在途请求，连接暂不关闭
    fresh = await call(executor, "whoami", "a", text="a")
    assert fresh.ok and manager._sessions[("remote", "a")] is not old
    state.gate.set()
    assert (await inflight).ok
    await manager.aclose()
    assert old.connection is None and old.inflight == 0
    await executor.aclose()


async def test_connect_failures_cool_down_per_tenant(remote: Any) -> None:
    """冷却期按租户统计：一个租户的凭证被拒绝并进入冷却，不影响其他租户。"""
    state, _ = remote
    clock = FakeClock()
    server = http_server(max_consecutive_failures=2, failure_cooldown_s=60, connect_timeout_s=5)
    executor, manager, _ = await http_setup(remote, server, clock=clock)
    state.reject.add(f"Bearer {token('bad')}")
    codes = [(await call(executor, "whoami", "bad")).error.code for _ in range(3)]
    assert codes == ["mcp_connect_failed", "mcp_connect_failed", "mcp_server_unavailable"]
    assert (await call(executor, "whoami", "good", text="good")).ok
    # 凭证修复后让该租户失效：冷却期清零，立即可以重连
    state.reject.clear()
    assert (await call(executor, "whoami", "bad")).error.code == "mcp_server_unavailable"
    await manager.invalidate("remote", tenant_id="bad")
    assert (await call(executor, "whoami", "bad", text="bad")).ok
    await executor.aclose()


async def test_unavailable_tenant_secret_does_not_affect_others(remote: Any) -> None:
    """某个租户取不到凭证：该租户的调用失败且不建立会话，错误不含密钥系统细节；其他租户正常。"""
    state, _ = remote
    executor, _, secrets = await http_setup(remote, http_server())
    secrets.unavailable.add("x")
    state.opened.clear()
    failed = await call(executor, "whoami", "x")
    assert failed.error is not None and failed.error.code == "secret_unavailable"
    assert state.opened == []
    assert (await call(executor, "whoami", "a", text="a")).ok
    await executor.aclose()


# ---------------------------------------------------------------------------------
# 会话数量上限、空闲回收与取消
# ---------------------------------------------------------------------------------
async def test_session_limit_evicts_least_recently_used_idle_session(remote: Any) -> None:
    """达到会话上限时回收最久未使用的空闲会话；被回收的租户下次调用会重新建立会话。"""
    state, _ = remote
    clock = FakeClock()
    server = http_server(max_tenant_sessions=2, tenant_idle_ttl_s=None)
    executor, manager, _ = await http_setup(remote, server, clock=clock)  # ops 会话占 1 个
    clock.now += 1
    await call(executor, "whoami", "a", text="a")  # ops、a
    clock.now += 1
    await call(executor, "whoami", "b", text="b")  # 回收最久未使用的 ops
    assert sorted(manager.session_tenants("remote"), key=str) == ["a", "b"]
    clock.now += 1
    await call(executor, "whoami", "a", text="a")  # a 变成最近使用
    clock.now += 1
    await call(executor, "whoami", "c", text="c")  # 回收 b
    assert sorted(manager.session_tenants("remote"), key=str) == ["a", "c"]
    opened = len(state.opened)
    assert (await call(executor, "whoami", "b", text="b")).ok
    assert len(state.opened) == opened + 1
    await executor.aclose()


async def test_session_limit_with_all_sessions_busy_is_throttled(remote: Any) -> None:
    """所有会话都有在途请求时，新租户的调用被限流（可重试），在途请求不受影响。"""
    state, _ = remote
    server = http_server(max_tenant_sessions=2, tenant_idle_ttl_s=None)
    executor, manager, _ = await http_setup(remote, server)
    await manager.invalidate("remote")  # 清掉发现用的会话
    busy = [asyncio.create_task(call(executor, "hold", t)) for t in ("a", "b")]
    while state.holding < 2:
        await asyncio.sleep(0.01)
    rejected = await call(executor, "whoami", "c", text="c")
    assert rejected.error is not None
    assert (rejected.error.type, rejected.error.code) == ("throttled", "mcp_session_limit")
    assert rejected.error.retryable and state.seen == []
    state.gate.set()
    assert all(r.ok for r in await asyncio.gather(*busy))
    assert (await call(executor, "whoami", "c", text="c")).ok  # 现在有空闲会话可以回收
    await executor.aclose()


async def test_idle_sessions_are_reclaimed(remote: Any) -> None:
    """空闲超过 ``tenant_idle_ttl_s`` 的会话：在别的租户取会话时被回收，或由宿主主动清理。"""
    clock = FakeClock()
    executor, manager, _ = await http_setup(remote, http_server(tenant_idle_ttl_s=100), clock=clock)
    await call(executor, "whoami", "a", text="a")
    clock.now += 50
    await call(executor, "whoami", "b", text="b")
    clock.now += 60  # ops、a 已空闲超过 100 秒，b 只有 60 秒
    await call(executor, "whoami", "c", text="c")
    assert sorted(manager.session_tenants("remote"), key=str) == ["b", "c"]
    clock.now += 200
    assert await manager.close_idle_sessions() == 2
    assert manager.session_tenants("remote") == []
    await executor.aclose()


async def test_cancellation_releases_the_session_for_reclaim(remote: Any) -> None:
    """调用方取消：CancelledError 原样传播，在途计数归还，会话之后可以被正常回收和关闭。"""
    state, _ = remote
    executor, manager, _ = await http_setup(remote, http_server())
    task = asyncio.create_task(call(executor, "hold", "a"))
    while state.holding == 0:
        await asyncio.sleep(0.01)
    session = manager._sessions[("remote", "a")]
    assert session.inflight == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.inflight == 0
    assert await manager.invalidate("remote", tenant_id="a") == 1
    assert session.connection is None
    await executor.aclose()


async def test_closed_manager_rejects_all_tenants(remote: Any) -> None:
    """管理器关闭后：所有租户的会话都被关闭，之后的调用返回 mcp_manager_closed。"""
    executor, manager, _ = await http_setup(remote, http_server())
    await call(executor, "whoami", "a", text="a")
    sessions = list(manager._sessions.values())
    await manager.aclose()
    assert all(s.connection is None for s in sessions) and manager.session_tenants("remote") == []
    closed = await call(executor, "whoami", "a", text="a")
    assert closed.error is not None and closed.error.code == "mcp_manager_closed"
    await manager.aclose()  # 可以重复调用
    await executor.aclose()


def test_tenant_limits_are_validated() -> None:
    """会话范围与租户会话上限的配置校验。"""
    transport = StdioTransportConfig(command="x")
    with pytest.raises(ValueError):
        McpServerConfig(server_id="s", transport=transport, session_scope="user")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        McpServerConfig(server_id="s", transport=transport, max_tenant_sessions=0)
    with pytest.raises(ValueError):
        McpServerConfig(server_id="s", transport=transport, tenant_idle_ttl_s=0)
    with pytest.raises(ValueError):
        StdioTransportConfig(command="x", secret_env={"BAD NAME": "api"})


# ---------------------------------------------------------------------------------
# stdio：每个租户一个子进程
# ---------------------------------------------------------------------------------
def alive(pid: int) -> bool:
    """判断进程是否仍然存在。

    Args:
        pid: 进程号。

    Returns:
        进程存在时为 True。
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def stdio_setup(**kw: Any) -> tuple[ToolExecutor, McpServerManager, TenantSecrets]:
    """启动租户范围的 stdio 服务端配置并完成发现（发现使用租户 ``ops``）。

    Args:
        **kw: ``McpServerConfig`` 的其他字段。

    Returns:
        ``(执行器, 管理器, 密钥提供者)``。
    """
    server = McpServerConfig(
        server_id="local",
        transport=StdioTransportConfig(
            command=sys.executable,
            args=("-I", str(SERVER_SCRIPT)),
            secret_env={"TENANT_TOKEN": "api", "TENANT_TAG": "tag"},
        ),
        session_scope="tenant",
        **kw,
    )
    secrets = TenantSecrets()
    manager = McpServerManager([server], secrets=secrets)
    registry = ToolRegistry()
    allow = [McpToolAllow(remote_name=n, policy=PUBLIC) for n in ("pid", "env", "slow")]
    await McpToolSync(registry, manager).sync("local", allow, ctx=tenant_ctx("ops"))
    return ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)}), manager, secrets


async def stdio_call(executor: ToolExecutor, tool: str, tenant: str, **args: Any) -> Any:
    """以某个租户的身份调用 stdio 工具。

    Args:
        executor: 执行器。
        tool: 远端工具名。
        tenant: 租户 ID。
        **args: 工具参数。

    Returns:
        调用结果。
    """
    return await executor.execute(
        ToolInvocation(call_id="c", tool=f"mcp_local_{tool}", arguments=args), tenant_ctx(tenant)
    )


async def test_stdio_tenants_get_separate_processes_and_secret_env() -> None:
    """stdio：每个租户一个子进程，各自通过环境变量拿到自己的凭证；关闭后全部子进程被终止。"""
    executor, manager, _ = await stdio_setup()
    pids = {t: int((await stdio_call(executor, "pid", t)).to_model_text()) for t in ("a", "b")}
    assert pids["a"] != pids["b"]
    assert int((await stdio_call(executor, "pid", "a")).to_model_text()) == pids["a"]  # 复用
    for tenant in ("a", "b"):
        tag = await stdio_call(executor, "env", tenant, name="TENANT_TAG")
        assert tag.to_model_text() == tenant  # 短值不会被清除，可以直接核对
        secret = await stdio_call(executor, "env", tenant, name="TENANT_TOKEN")
        assert secret.to_model_text() == "[REDACTED_SECRET]"  # 子进程拿到了凭证，回显被清除
        assert token(tenant) in manager.known_secrets("local", tenant_id=tenant)
    assert token("b") not in manager.known_secrets("local", tenant_id="a")
    await executor.aclose()
    for pid in pids.values():
        for _ in range(50):
            if not alive(pid):
                break
            await asyncio.sleep(0.1)
        assert not alive(pid)


async def test_stdio_reconnect_stays_within_the_tenant() -> None:
    """一个租户的子进程退出：只有该租户的调用报告连接丢失并在下次调用时重连（重新取凭证），
    其他租户的进程不受影响。"""
    executor, _, secrets = await stdio_setup()
    pid_a = int((await stdio_call(executor, "pid", "a")).to_model_text())
    pid_b = int((await stdio_call(executor, "pid", "b")).to_model_text())
    secrets.contexts.clear()
    os.kill(pid_a, 9)
    await asyncio.sleep(0.3)
    lost = await stdio_call(executor, "pid", "a")
    assert lost.error is not None and lost.error.code == "mcp_connection_lost"
    assert int((await stdio_call(executor, "pid", "b")).to_model_text()) == pid_b
    again = await stdio_call(executor, "pid", "a")
    assert again.ok and int(again.to_model_text()) not in (pid_a, pid_b)
    assert {c.principal.tenant_id for c in secrets.contexts} == {"a"}
    await executor.aclose()


async def test_forced_invalidate_interrupts_inflight_calls() -> None:
    """强制失效（凭证吊销）：在途请求得到可重试的 mcp_connection_lost，旧子进程被终止。"""
    executor, manager, _ = await stdio_setup()
    pid = int((await stdio_call(executor, "pid", "a")).to_model_text())
    inflight = asyncio.create_task(stdio_call(executor, "slow", "a", seconds=30))
    await asyncio.sleep(0.5)
    assert await manager.invalidate("local", tenant_id="a", force=True) == 1
    result = await asyncio.wait_for(inflight, 10)
    assert result.error is not None
    assert (result.error.code, result.error.retryable) == ("mcp_connection_lost", True)
    for _ in range(50):
        if not alive(pid):
            break
        await asyncio.sleep(0.1)
    assert not alive(pid)
    fresh = await stdio_call(executor, "pid", "a")
    assert fresh.ok and int(fresh.to_model_text()) != pid
    await executor.aclose()
