"""MCP Streamable HTTP 代理支持与 HTTP 传输层代理固定后端的测试。

没有真实网络：代理由内存网络后端扮演，记录“连接到了哪里”和“线上发了什么字节”。这足以验证
客户端一侧的保证（只连接代理、凭证只发给代理、发送前预检、默认不使用代理）；代理转发后的
完整 MCP 会话需要真实 TCP，见 ``test_network_e2e.py`` 中带 ``network`` 标记的代理用例。
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from typing import Any

import httpcore2
import httpx2
import mcp.types as mt
import pytest
from mcp.server.lowlevel import Server

from agent_tools import (
    HttpTarget,
    MappingSecretProvider,
    NetworkPolicy,
    PermissionDeniedError,
    SecretRef,
    ToolContext,
    ToolExecutor,
    ToolInfraError,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    UpstreamError,
)
from agent_tools.adapters.http import (
    HttpAdapter,
    HttpAdapterConfig,
    ProxyOnlyNetworkBackend,
    create_http_client,
)
from agent_tools.adapters.mcp import (
    HttpTransportConfig,
    McpAdapter,
    McpProxyConfig,
    McpServerConfig,
    McpServerManager,
)
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

HOST = "mcp.example.com"
PROXY = "proxy.internal"
PROXY_SECRET = "svc-user:proxy-password-0123456789"
TARGET_SECRET = "target-token-0123456789"
PUBLIC_IP = "93.184.216.34"


class _Stream(httpcore2.AsyncNetworkStream):
    """内存网络流：每次 ``read`` 返回一段预设数据，记录写入的字节与 TLS 握手的主机名。"""

    def __init__(self, net: ProxyNet, chunks: list[bytes]) -> None:
        """创建网络流。

        Args:
            net: 所属的内存网络。
            chunks: 依次返回的数据段（一段对应一个 HTTP 响应，避免 CONNECT 响应与隧道内的
                响应被一次读走）。
        """
        self._net = net
        self._chunks = list(chunks)
        self.written = b""

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        """返回下一段数据；没有数据时返回空字节（连接关闭）。"""
        return self._chunks.pop(0) if self._chunks else b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        """记录写入的字节。"""
        self.written += buffer

    async def aclose(self) -> None:
        """关闭网络流（无操作）。"""

    async def start_tls(
        self, ssl_context: Any, server_hostname: str | None = None, timeout: float | None = None
    ) -> httpcore2.AsyncNetworkStream:
        """记录 TLS 握手的主机名与此前已写入的字节数，不做真实握手。"""
        self._net.tls.append((server_hostname, len(self.written)))
        return self

    def get_extra_info(self, info: str) -> Any:
        """不提供额外信息。"""
        return None


class ProxyNet(httpcore2.AsyncNetworkBackend):
    """扮演代理的内存网络后端：记录每条 TCP 连接的目标，按预设数据段应答。"""

    def __init__(self, *chunks: bytes) -> None:
        """创建内存网络。

        Args:
            *chunks: 第一条连接依次返回的数据段。
        """
        self.chunks = list(chunks)
        self.connections: list[tuple[str, int]] = []
        self.streams: list[_Stream] = []
        self.tls: list[tuple[str | None, int]] = []

    @property
    def written(self) -> bytes:
        """第一条连接上写入的全部字节（SDK 之后可能再开连接，那些连接没有预设应答）。"""
        return self.streams[0].written if self.streams else b""

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore2.AsyncNetworkStream:
        """记录连接目标并返回内存网络流。"""
        self.connections.append((host, port))
        chunks, self.chunks = self.chunks, []
        stream = _Stream(self, chunks)
        self.streams.append(stream)
        return stream

    async def connect_unix_socket(
        self, path: str, timeout: float | None = None, socket_options: Any = None
    ) -> httpcore2.AsyncNetworkStream:
        """不支持 Unix 套接字。"""
        raise AssertionError("unix sockets are not used")

    async def sleep(self, seconds: float) -> None:
        """等待（委托给 asyncio）。"""
        await asyncio.sleep(seconds)


def response(status: int) -> bytes:
    """构造一个没有响应体的原始 HTTP 响应。

    Args:
        status: 状态码。

    Returns:
        原始响应字节。
    """
    return f"HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()


async def public(host: str, port: int) -> list[str]:
    """测试 DNS：目标解析到公网地址。"""
    return [PUBLIC_IP]


async def private(host: str, port: int) -> list[str]:
    """测试 DNS：目标解析到内网地址。"""
    return ["10.0.0.7"]


def server(url: str, **transport: Any) -> McpServerConfig:
    """构造经由代理访问的 MCP 服务端配置。

    Args:
        url: MCP 端点。
        **transport: ``HttpTransportConfig`` 的其他字段。

    Returns:
        服务端配置。
    """
    transport.setdefault("proxy", McpProxyConfig(url=f"http://{PROXY}:3128", auth_secret="proxy"))
    transport.setdefault(
        "network",
        NetworkPolicy(allowed_hosts=frozenset({HOST}), allow_http=url.startswith("http://")),
    )
    return McpServerConfig(
        server_id="ext",
        transport=HttpTransportConfig(url=url, auth=SecretRef(name="api"), **transport),
        connect_timeout_s=5,
    )


def manager_for(config: McpServerConfig, net: ProxyNet, resolver: Any = public) -> McpServerManager:
    """创建使用内存网络的管理器（生产路径：不注入测试用的客户端工厂）。

    Args:
        config: 服务端配置。
        net: 内存网络。
        resolver: DNS 解析函数。

    Returns:
        管理器。
    """
    return McpServerManager(
        [config],
        secrets=MappingSecretProvider({"proxy": PROXY_SECRET, "api": TARGET_SECRET}),
        resolver=resolver,
        network_backend=net,
    )


# ---------------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "url",
    [
        "socks5://proxy.internal:1080",
        "http://user:pass@proxy.internal:3128",
        "http://proxy.internal:3128/path",
        "http://proxy.internal:3128?x=1",
        "http://",
    ],
)
def test_proxy_config_rejects_unsafe_urls(url: str) -> None:
    """代理地址只接受 http/https，不能内嵌凭证、路径或查询（凭证必须走 SecretProvider）。"""
    with pytest.raises(ValueError):
        McpProxyConfig(url=url)


def test_proxy_is_explicit_and_reported() -> None:
    """默认不使用代理（连接时检查）；配置代理后 guard_mode 如实报告为发送前预检；
    代理凭证不能作为普通请求头配置。"""
    policy = NetworkPolicy(allowed_hosts=frozenset({HOST}))
    direct = HttpTransportConfig(url=f"https://{HOST}/mcp", network=policy)
    assert direct.proxy is None and direct.guard_mode == "connect"
    proxied = HttpTransportConfig(
        url=f"https://{HOST}/mcp", network=policy, proxy=McpProxyConfig(url="http://p:3128")
    )
    assert proxied.guard_mode == "preflight"
    with pytest.raises(ValueError):
        HttpTransportConfig(
            url=f"https://{HOST}/mcp", network=policy, headers={"Proxy-Authorization": "Basic x"}
        )
    with pytest.raises(ValueError):
        McpProxyConfig(url="http://p:3128", auth_secret="bad name")


# ---------------------------------------------------------------------------------
# 客户端只连接代理；凭证的去向
# ---------------------------------------------------------------------------------
async def test_https_target_uses_connect_tunnel_and_keeps_credentials_apart() -> None:
    """https 目标：只与代理建立 TCP 连接并发 CONNECT；代理凭证只在 CONNECT 中，目标凭证只在
    TLS 隧道内；TLS 的主机名是目标主机（端到端校验）。"""
    net = ProxyNet(b"HTTP/1.1 200 Connection established\r\n\r\n", response(503))
    manager = manager_for(server(f"https://{HOST}/mcp"), net)
    with pytest.raises(UpstreamError) as info:
        await manager.list_tools("ext")
    assert info.value.code == "mcp_connect_failed"
    assert net.connections and set(net.connections) == {(PROXY, 3128)}  # 没有直连目标的连接
    (sni, tunnel_start), *_ = net.tls
    assert sni == HOST
    connect, inner = net.written[:tunnel_start], net.written[tunnel_start:]
    assert connect.startswith(f"CONNECT {HOST}:443 HTTP/1.1".encode())
    basic = base64.b64encode(PROXY_SECRET.encode())
    assert b"Proxy-Authorization: Basic " + basic in connect
    assert TARGET_SECRET.encode() not in connect
    assert inner.startswith(b"POST /mcp HTTP/1.1")
    assert f"Authorization: Bearer {TARGET_SECRET}".encode() in inner
    assert b"Proxy-Authorization" not in inner and basic not in inner
    await manager.aclose()


async def test_plain_http_target_is_forwarded_through_the_proxy() -> None:
    """显式允许的明文 http 目标：以绝对地址形式发给代理（正向代理），同样只连接代理。"""
    net = ProxyNet(response(503))
    manager = manager_for(server(f"http://{HOST}/mcp"), net)
    with pytest.raises(UpstreamError):
        await manager.list_tools("ext")
    assert net.connections and set(net.connections) == {(PROXY, 3128)} and net.tls == []
    assert net.written.startswith(f"POST http://{HOST}/mcp HTTP/1.1".encode())
    assert b"Proxy-Authorization: Basic " in net.written
    await manager.aclose()


async def test_proxy_credentials_are_registered_for_scrubbing() -> None:
    """代理凭证（原值、密码、Base64 形式）登记为会话密钥，供日志与结果精确清除。"""
    net = ProxyNet(response(503))
    manager = manager_for(server(f"http://{HOST}/mcp"), net)
    with pytest.raises(UpstreamError):
        await manager.list_tools("ext")
    known = set(manager.known_secrets("ext"))
    assert {PROXY_SECRET, "proxy-password-0123456789", TARGET_SECRET} <= known
    assert base64.b64encode(PROXY_SECRET.encode()).decode() in known
    await manager.aclose()


async def test_malformed_or_missing_proxy_credential_fails_before_connecting() -> None:
    """代理凭证格式不是“用户名:密码”或取不到：安全失败，不建立任何连接。"""
    for secrets in ({"proxy": "no-colon-value", "api": TARGET_SECRET}, {"api": TARGET_SECRET}):
        net = ProxyNet(response(503))
        manager = McpServerManager(
            [server(f"https://{HOST}/mcp")],
            secrets=MappingSecretProvider(secrets),
            resolver=public,
            network_backend=net,
        )
        with pytest.raises(ToolInfraError) as info:
            await manager.list_tools("ext")
        assert info.value.code == "secret_unavailable" and net.connections == []
        await manager.aclose()


async def test_environment_proxies_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """没有显式配置代理时，环境变量里的代理不生效：直连已校验的 IP（默认安全策略不变）。"""
    monkeypatch.setenv("HTTPS_PROXY", "http://evil-proxy.example:8080")
    monkeypatch.setenv("ALL_PROXY", "http://evil-proxy.example:8080")
    net = ProxyNet(response(503))
    config = server(f"https://{HOST}/mcp", proxy=None)
    manager = manager_for(config, net)
    with pytest.raises(UpstreamError):
        await manager.list_tools("ext")
    assert net.connections and set(net.connections) == {(PUBLIC_IP, 443)}
    assert all(b"CONNECT" not in stream.written for stream in net.streams)
    await manager.aclose()


# ---------------------------------------------------------------------------------
# 发送前预检与固定后端
# ---------------------------------------------------------------------------------
async def test_preflight_blocks_private_resolution_before_reaching_the_proxy() -> None:
    """代理模式的预检：目标解析到内网，或地址不在白名单时，在连接代理之前就被拒绝。"""
    net = ProxyNet(response(503))
    manager = manager_for(server(f"https://{HOST}/mcp"), net, resolver=private)
    with pytest.raises(PermissionDeniedError) as info:
        await manager.list_tools("ext")
    assert info.value.code == "destination_not_allowed" and net.connections == []
    await manager.aclose()

    other = server(
        "https://other.example.com/mcp", network=NetworkPolicy(allowed_hosts=frozenset({HOST}))
    )
    manager = manager_for(other, net)
    with pytest.raises(PermissionDeniedError):
        await manager.list_tools("ext")
    assert net.connections == []
    await manager.aclose()


async def test_proxy_only_backend_refuses_direct_connections() -> None:
    """固定后端：只允许连接配置的代理主机与端口，其他目标一律拒绝。"""
    net = ProxyNet(b"")
    backend = ProxyOnlyNetworkBackend("Proxy.Internal", 3128, inner=net)
    await backend.connect_tcp("proxy.internal", 3128)
    for host, port in ((HOST, 443), ("proxy.internal", 443), ("169.254.169.254", 80)):
        with pytest.raises(PermissionDeniedError):
            await backend.connect_tcp(host, port)
    with pytest.raises(ToolInfraError):
        await backend.connect_unix_socket("/var/run/docker.sock")
    assert net.connections == [("proxy.internal", 3128)]


def test_proxy_auth_requires_a_proxy() -> None:
    """只给代理凭证而没有代理地址属于配置错误。"""
    config = HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset({HOST})))
    with pytest.raises(ValueError):
        create_http_client(config, proxy_auth=("u", "p"))


@pytest.fixture
async def asgi_factory() -> AsyncIterator[tuple[list[str], Any]]:
    """内存 MCP 服务端与客户端工厂（代替代理之后的真实网络）。

    Yields:
        ``(服务端收到的调用, http_client_factory)``。
    """
    calls: list[str] = []

    async def list_tools(ctx: Any, params: Any) -> mt.ListToolsResult:
        """列出一个工具。"""
        return mt.ListToolsResult(
            tools=[mt.Tool(name="echo", description="echo", input_schema={"type": "object"})]
        )

    async def call_tool(ctx: Any, params: mt.CallToolRequestParams) -> mt.CallToolResult:
        """记录调用并返回固定文本。"""
        calls.append(params.name)
        return mt.CallToolResult(content=[mt.TextContent(type="text", text="ok")])

    app = Server("p", on_list_tools=list_tools, on_call_tool=call_tool).streamable_http_app(
        stateless_http=True, json_response=True
    )

    def factory(cfg: HttpTransportConfig, headers: Any) -> httpx2.AsyncClient:
        """创建连接内存应用的客户端（管理器仍会给它套上逐请求预检）。"""
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app),
            base_url="http://localhost:8000",
            headers=headers,
        )

    ready, stop = asyncio.Event(), asyncio.Event()

    async def serve() -> None:
        """在后台任务中运行 lifespan。"""
        async with app.router.lifespan_context(app):
            ready.set()
            await stop.wait()

    task = asyncio.create_task(serve())
    await ready.wait()
    yield calls, factory
    stop.set()
    await task


async def test_every_request_is_preflighted_after_the_session_is_established(
    asgi_factory: Any,
) -> None:
    """会话建立后 DNS 改指内网（重绑定的一种表现）：之后的请求在发送前被拒绝，调用失败而不是
    挂起，请求不会到达服务端。这只是尽力而为的预检，不能替代代理的访问控制。"""
    calls, factory = asgi_factory
    answers = {"ip": PUBLIC_IP}

    async def resolver(host: str, port: int) -> list[str]:
        """可在测试中切换结果的 DNS。"""
        return [answers["ip"]]

    # SDK 的服务端会校验 Host 头，所以内存服务端使用 localhost；DNS 结果由测试控制
    network = NetworkPolicy(
        allowed_hosts=frozenset({"localhost"}), allowed_ports=frozenset({8000}), allow_http=True
    )
    manager = McpServerManager(
        [server("http://localhost:8000/mcp", network=network)],
        secrets=MappingSecretProvider({"proxy": PROXY_SECRET, "api": TARGET_SECRET}),
        resolver=resolver,
        http_client_factory=factory,
    )
    registry = ToolRegistry()
    policy = ToolPolicy(access="public", timeout_s=5)
    await McpToolSync(registry, manager).sync(
        "ext", [McpToolAllow(remote_name="echo", policy=policy)]
    )
    executor = ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)})
    invocation = ToolInvocation(call_id="c", tool="mcp_ext_echo", arguments={})
    assert (await executor.execute(invocation, ToolContext())).ok
    answers["ip"] = "169.254.169.254"
    blocked = await asyncio.wait_for(executor.execute(invocation, ToolContext()), 10)
    # SDK 把被拒绝的请求当作传输失败，会话被回收；重连时的预检给出明确的拒绝原因
    assert blocked.error is not None and blocked.error.code == "mcp_connection_lost"
    again = await executor.execute(invocation, ToolContext())
    assert again.error is not None and again.error.code == "destination_not_allowed"
    assert calls == ["echo"]
    await executor.aclose()


# ---------------------------------------------------------------------------------
# P1 HTTP 适配器共用同一个代理传输层
# ---------------------------------------------------------------------------------
async def test_http_adapter_proxy_mode_only_connects_to_the_proxy() -> None:
    """HTTP 适配器的代理模式（原有配置方式，凭证内嵌在代理地址中）：只连接代理，
    凭证仍然只出现在 CONNECT 请求里。"""
    net = ProxyNet(b"HTTP/1.1 200 Connection established\r\n\r\n", response(204))
    registry = ToolRegistry()
    registry.register_http(
        HttpTarget(method="GET", base_url=f"https://{HOST}"),
        name="api.call",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        policy=ToolPolicy(access="public"),
    )
    adapter = HttpAdapter(
        HttpAdapterConfig(
            network=NetworkPolicy(allowed_hosts=frozenset({HOST})),
            proxy=f"http://legacy:legacy-pass@{PROXY}:3128",
        ),
        resolver=public,
        network_backend=net,
    )
    async with ToolExecutor(registry, adapters={"http": adapter}) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="api_call", arguments={}), ToolContext()
        )
    assert result.ok and adapter.guard_mode == "preflight"
    assert net.connections == [(PROXY, 3128)]
    (sni, tunnel_start), *_ = net.tls
    assert sni == HOST
    basic = base64.b64encode(b"legacy:legacy-pass")
    assert b"Proxy-Authorization: Basic " + basic in net.written[:tunnel_start]
    assert basic not in net.written[tunnel_start:]
