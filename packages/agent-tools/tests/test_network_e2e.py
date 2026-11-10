"""真实网络的端到端测试（P3 对 P2 已知限制的验证），打 ``network`` 标记，默认不运行。

运行方式::

    uv run pytest -m network tests/test_network_e2e.py

需要允许在 127.0.0.1 上监听与连接（某些沙箱环境禁止本地 TCP，此时这些测试无法运行）。
全部走生产路径：``McpServerManager`` 不注入测试用的 HTTP 客户端，使用
``create_http_client`` 的连接时 IP 检查传输层（``GuardedNetworkBackend`` + ``AnyIOBackend``）
与真实的 TLS 握手。服务端是同一事件循环里运行的 uvicorn。

覆盖：
* Streamable HTTP 在真实 TCP 上的调用，以及调用方超时后服务端确实收到取消通知；
* 私有 CA 签发证书的 TLS 握手（SNI / 主机名校验），以及证书主机名不符、CA 不受信任时握手失败；
* P1 HTTP 适配器在真实 TLS 上的调用；
* 传输层响应大小上限在真实网络上的行为（JSON 响应与 SSE 事件）；
* 经由真实 HTTP 代理（本地的最小 CONNECT / 正向代理）的 Streamable HTTP：明文与 TLS 目标、
  代理认证、取消通知，以及客户端没有绕过代理直连。

测试使用本地服务与临时证书，不连接外部生产 MCP 服务端；环境必须允许回环 TCP。
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import mcp.types as mt
import pytest
import uvicorn
from mcp.server.lowlevel import Server
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from agent_tools import (
    HttpTarget,
    MappingSecretProvider,
    NetworkPolicy,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    UpstreamError,
)
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig
from agent_tools.adapters.mcp import (
    HttpTransportConfig,
    McpAdapter,
    McpProxyConfig,
    McpServerConfig,
    McpServerManager,
)
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pki import Pki, make_pki

pytestmark = pytest.mark.network

PUBLIC = ToolPolicy(access="public", max_input_bytes=512 * 1024)


def _loopback_available() -> bool:
    """检查当前环境是否允许在回环地址上监听并建立连接（有些沙箱允许监听但禁止连接）。"""
    try:
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            with socket.create_connection(server.getsockname(), timeout=2):
                pass
    except OSError:
        return False
    return True


if not _loopback_available():  # pragma: no cover - 取决于运行环境
    pytest.skip("loopback TCP is not permitted in this environment", allow_module_level=True)


# ---------------------------------------------------------------------------------
# 证书
# ---------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Pki:
    """生成私有 CA 和服务端证书。"""
    return make_pki(tmp_path_factory.mktemp("pki"))


# ---------------------------------------------------------------------------------
# 服务端
# ---------------------------------------------------------------------------------
@dataclass
class Remote:
    """真实 MCP 服务端记录的事件。"""

    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    calls: list[str] = field(default_factory=list)


def mcp_app(remote: Remote, *, json_response: bool) -> Any:
    """构造有状态的 Streamable HTTP MCP 应用（取消通知需要会话来关联请求）。

    Args:
        remote: 记录事件的状态。
        json_response: 是否用 JSON 响应（否则用 SSE）。

    Returns:
        ASGI 应用。
    """

    async def list_tools(ctx: Any, params: Any) -> mt.ListToolsResult:
        """列出工具。"""
        schema = {
            "type": "object",
            "properties": {"text": {"type": "string"}, "n": {"type": "integer"}},
        }
        return mt.ListToolsResult(
            tools=[
                mt.Tool(name=n, description=n, input_schema=schema) for n in ("echo", "slow", "big")
            ]
        )

    async def call_tool(ctx: Any, params: mt.CallToolRequestParams) -> mt.CallToolResult:
        """执行工具；``slow`` 被取消时记录下来。"""
        args = params.arguments or {}
        remote.calls.append(params.name)
        if params.name == "slow":
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                remote.cancelled.set()
                raise
        text = "x" * int(args.get("n", 0)) if params.name == "big" else str(args.get("text", ""))
        return mt.CallToolResult(content=[mt.TextContent(type="text", text=text)])

    server = Server("network-e2e", on_list_tools=list_tools, on_call_tool=call_tool)
    return server.streamable_http_app(stateless_http=False, json_response=json_response)


@asynccontextmanager
async def serve(
    app: Any, *, cert: Path | None = None, key: Path | None = None
) -> AsyncIterator[int]:
    """在当前事件循环中运行 uvicorn（含 lifespan），返回监听端口。

    Args:
        app: ASGI 应用。
        cert: TLS 证书（提供时启用 HTTPS）。
        key: TLS 私钥。

    Yields:
        端口号。
    """
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=0,
        log_level="warning",
        lifespan="on",
        ssl_certfile=str(cert) if cert else None,
        ssl_keyfile=str(key) if key else None,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    assert server.started, "uvicorn did not start"
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)


async def localhost(host: str, port: int) -> list[str]:
    """测试 DNS：localhost → 127.0.0.1（网络策略显式放行 127.0.0.0/8）。"""
    return ["127.0.0.1"]


def policy(port: int, *, https: bool) -> NetworkPolicy:
    """只放行本地服务端的网络策略。"""
    return NetworkPolicy(
        allowed_hosts=frozenset({"localhost"}),
        allowed_ports=frozenset({port}),
        allow_http=not https,
        allowed_networks=("127.0.0.0/8",),
    )


async def mcp_executor(
    port: int, *, https: bool, ca: Path | None = None, **server_kw: Any
) -> tuple[ToolExecutor, McpServerManager]:
    """用生产路径（无测试工厂）连接真实服务端，同步工具并返回执行器。"""
    scheme = "https" if https else "http"
    config = McpServerConfig(
        server_id="net",
        transport=HttpTransportConfig(
            url=f"{scheme}://localhost:{port}/mcp",
            network=policy(port, https=https),
            ca_bundle=str(ca) if ca else None,
        ),
        connect_timeout_s=10,
        **server_kw,
    )
    manager = McpServerManager([config], resolver=localhost)
    registry = ToolRegistry()
    allow = [
        McpToolAllow(remote_name=n, policy=PUBLIC.model_copy(update={"timeout_s": 5}))
        for n in ("echo", "slow", "big")
    ]
    await McpToolSync(registry, manager).sync("net", allow)
    return ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)}), manager


async def call(executor: ToolExecutor, tool: str, args: dict[str, Any], **ctx: Any) -> Any:
    """执行一次调用。"""
    return await executor.execute(
        ToolInvocation(call_id="c", tool=f"mcp_net_{tool}", arguments=args), ToolContext(**ctx)
    )


# ---------------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------------
async def test_streamable_http_over_real_tcp_and_cancellation() -> None:
    """真实 TCP 上的 Streamable HTTP：调用成功；调用方超时后服务端收到取消通知并停止处理。"""
    remote = Remote()
    async with serve(mcp_app(remote, json_response=False)) as port:
        executor, manager = await mcp_executor(port, https=False)
        ok = await call(executor, "echo", {"text": "over tcp"})
        assert ok.ok and ok.to_model_text() == "over tcp"
        timed_out = await call(
            executor, "slow", {}, deadline=asyncio.get_running_loop().time() + 0.5
        )
        assert timed_out.error is not None and timed_out.error.type == "timeout"
        await asyncio.wait_for(remote.cancelled.wait(), 5)
        again = await call(executor, "echo", {"text": "still connected"})
        assert again.ok and manager.connected("net")
        await executor.aclose()


async def test_tls_with_private_ca(pki: Pki) -> None:
    """私有 CA 签发的 localhost 证书：指定 ``ca_bundle`` 后握手成功（经连接时检查的传输层）。"""
    remote = Remote()
    app = mcp_app(remote, json_response=True)
    async with serve(app, cert=pki.good_cert, key=pki.good_key) as port:
        executor, _ = await mcp_executor(port, https=True, ca=pki.ca)
        result = await call(executor, "echo", {"text": "over tls"})
        assert result.ok and result.to_model_text() == "over tls"
        await executor.aclose()


async def test_tls_rejects_untrusted_ca_and_wrong_hostname(pki: Pki) -> None:
    """不信任 CA（未指定 ``ca_bundle``）或证书主机名不是 localhost 时，握手失败、无法连接。"""
    remote = Remote()
    app = mcp_app(remote, json_response=True)
    async with serve(app, cert=pki.good_cert, key=pki.good_key) as port:
        with pytest.raises(UpstreamError) as info:
            await mcp_executor(port, https=True, ca=None)
        assert info.value.code == "mcp_connect_failed"
    async with serve(
        mcp_app(remote, json_response=True), cert=pki.wrong_cert, key=pki.wrong_key
    ) as port:
        with pytest.raises(UpstreamError) as info:
            await mcp_executor(port, https=True, ca=pki.ca)
        assert info.value.code == "mcp_connect_failed"
    assert remote.calls == []


async def test_http_adapter_over_real_tls(pki: Pki) -> None:
    """P1 HTTP 适配器在真实 TLS 上调用成功（连接已校验 IP，按原始主机名校验证书）。"""

    async def weather(request: Request) -> JSONResponse:
        """返回查询参数中的城市。"""
        return JSONResponse({"city": request.query_params.get("city")})

    app = Starlette(routes=[Route("/weather", weather)])
    async with serve(app, cert=pki.good_cert, key=pki.good_key) as port:
        registry = ToolRegistry()
        registry.register_http(
            HttpTarget(
                method="GET", base_url=f"https://localhost:{port}", path_template="/weather"
            ),
            name="weather",
            input_schema={
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
                "additionalProperties": False,
            },
            policy=PUBLIC,
        )
        adapter = HttpAdapter(
            HttpAdapterConfig(network=policy(port, https=True), ca_bundle=str(pki.ca)),
            resolver=localhost,
        )
        async with ToolExecutor(registry, adapters={"http": adapter}) as executor:
            result = await executor.execute(
                ToolInvocation(call_id="w", tool="weather", arguments={"city": "Paris"}),
                ToolContext(),
            )
        assert result.ok and result.structured == {"city": "Paris"}


@pytest.mark.parametrize("json_response", [True, False], ids=["json", "sse"])
async def test_response_cap_over_real_network(json_response: bool) -> None:
    """真实网络上的传输层大小上限：JSON 响应和 SSE 事件超限都返回 ``mcp_response_too_large``，
    连接继续可用。"""
    remote = Remote()
    async with serve(mcp_app(remote, json_response=json_response)) as port:
        executor, manager = await mcp_executor(port, https=False, max_response_bytes=32 * 1024)
        too_big = await call(executor, "big", {"n": 500_000})
        assert too_big.error is not None and too_big.error.code == "mcp_response_too_large"
        small = await call(executor, "big", {"n": 100})
        assert small.ok and manager.connected("net")
        await executor.aclose()


# ---------------------------------------------------------------------------------
# 经由真实代理
# ---------------------------------------------------------------------------------
PROXY_CREDENTIAL = "proxy-user:proxy-password-0123456789"


@dataclass
class ProxyLog:
    """本地代理记录的事件。

    Attributes:
        requests: 每个代理请求的请求行（``CONNECT host:port`` 或 ``POST http://...``）。
        unauthorized: 因缺少或错误的 ``Proxy-Authorization`` 被拒绝的请求数。
    """

    requests: list[str] = field(default_factory=list)
    unauthorized: int = 0


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """把一个方向的字节原样转发到另一端，直到任一端关闭。"""
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


@asynccontextmanager
async def serve_proxy(log: ProxyLog) -> AsyncIterator[int]:
    """启动一个最小的本地 HTTP 代理（要求 Basic 认证），返回监听端口。

    支持 ``CONNECT``（建立隧道后盲转发）和明文正向代理（把绝对地址改写成路径后转发）。
    只用于测试：没有访问控制，转发目标固定解析为 127.0.0.1。

    明文转发时**每个到代理的连接只处理一个请求**：代理只解析并改写连接上的第一个请求，之后是
    盲转发；所以转发时强制 ``Connection: close``，让上游在响应后关闭连接，客户端下一个请求
    另开连接。否则客户端复用连接发出的后续请求不会被改写（上游收到绝对地址形式的请求行）、
    不会被记录，也不会检查代理凭证。

    上游连接失败时回复 502 并关闭连接，不让处理函数带着未关闭的连接异常退出。

    （2026-11-10 的夹具修复：以上两点是静态检查发现后修改的；本夹具至今没有在允许回环 TCP
    的环境中运行过，首次运行时仍可能需要调整。）

    Args:
        log: 记录代理请求的对象。

    Yields:
        代理端口。
    """
    expected = "Basic " + base64.b64encode(PROXY_CREDENTIAL.encode()).decode()
    tasks: set[asyncio.Task[None]] = set()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """处理一个到代理的连接。"""
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()
            return
        first, *lines = head.decode("latin-1").split("\r\n")
        headers = {k.strip().lower(): v.strip() for k, _, v in (ln.partition(":") for ln in lines)}
        method, target, _ = first.split(" ", 2)
        if headers.get("proxy-authorization") != expected:
            log.unauthorized += 1
            writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return
        log.requests.append(f"{method} {target}")
        try:
            if method == "CONNECT":
                port = int(target.rsplit(":", 1)[1])
                up_reader, up_writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await writer.drain()
            else:
                rest = target.split("://", 1)[1]
                authority, _, path = rest.partition("/")
                port = int(authority.rsplit(":", 1)[1])
                up_reader, up_writer = await asyncio.open_connection("127.0.0.1", port)
                # 去掉逐跳头（代理凭证不能转发给目标），并强制上游在响应后关闭连接
                hop_by_hop = ("proxy-", "connection:", "keep-alive:")
                kept = [ln for ln in lines if ln and not ln.lower().startswith(hop_by_hop)]
                kept.append("Connection: close")
                request = "\r\n".join([f"{method} /{path} HTTP/1.1", *kept]) + "\r\n\r\n"
                up_writer.write(request.encode("latin-1"))
                await up_writer.drain()
        except (OSError, ValueError, IndexError):
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            with contextlib.suppress(ConnectionError):
                await writer.drain()
            writer.close()
            return
        for coro in (_pipe(reader, up_writer), _pipe(up_reader, writer)):
            task = asyncio.create_task(coro)
            tasks.add(task)
            task.add_done_callback(tasks.discard)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        for task in list(tasks):
            task.cancel()
        await server.wait_closed()


async def proxied_executor(
    port: int, proxy_port: int, *, https: bool, ca: Path | None = None
) -> tuple[ToolExecutor, McpServerManager]:
    """用生产路径、经由本地代理连接真实服务端，同步工具并返回执行器。

    Args:
        port: MCP 服务端端口。
        proxy_port: 代理端口。
        https: 目标是否使用 TLS。
        ca: 目标证书的私有 CA。

    Returns:
        ``(执行器, 管理器)``。
    """
    scheme = "https" if https else "http"
    config = McpServerConfig(
        server_id="net",
        transport=HttpTransportConfig(
            url=f"{scheme}://localhost:{port}/mcp",
            network=policy(port, https=https),
            ca_bundle=str(ca) if ca else None,
            proxy=McpProxyConfig(url=f"http://127.0.0.1:{proxy_port}", auth_secret="proxy"),
        ),
        connect_timeout_s=10,
    )
    manager = McpServerManager(
        [config], resolver=localhost, secrets=MappingSecretProvider({"proxy": PROXY_CREDENTIAL})
    )
    registry = ToolRegistry()
    allow = [
        McpToolAllow(remote_name=n, policy=PUBLIC.model_copy(update={"timeout_s": 5}))
        for n in ("echo", "slow")
    ]
    await McpToolSync(registry, manager).sync("net", allow)
    return ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)}), manager


async def test_streamable_http_through_real_proxy_and_cancellation() -> None:
    """明文目标经真实代理：调用成功，所有请求都以绝对地址经过代理；超时后服务端收到取消通知。"""
    remote, log = Remote(), ProxyLog()
    async with serve(mcp_app(remote, json_response=False)) as port, serve_proxy(log) as proxy_port:
        executor, manager = await proxied_executor(port, proxy_port, https=False)
        ok = await call(executor, "echo", {"text": "via proxy"})
        assert ok.ok and ok.to_model_text() == "via proxy"
        timed_out = await call(
            executor, "slow", {}, deadline=asyncio.get_running_loop().time() + 0.5
        )
        assert timed_out.error is not None and timed_out.error.type == "timeout"
        await asyncio.wait_for(remote.cancelled.wait(), 5)
        assert log.unauthorized == 0 and log.requests
        assert all(
            r.startswith(
                ("POST http://localhost", "GET http://localhost", "DELETE http://localhost")
            )
            for r in log.requests
        )
        assert manager.connected("net")
        await executor.aclose()


async def test_tls_through_real_proxy_tunnel(pki: Pki) -> None:
    """TLS 目标经真实代理的 CONNECT 隧道：端到端握手仍按私有 CA 与主机名校验，调用成功。"""
    remote, log = Remote(), ProxyLog()
    app = mcp_app(remote, json_response=True)
    async with (
        serve(app, cert=pki.good_cert, key=pki.good_key) as port,
        serve_proxy(log) as proxy_port,
    ):
        executor, _ = await proxied_executor(port, proxy_port, https=True, ca=pki.ca)
        result = await call(executor, "echo", {"text": "tls via proxy"})
        assert result.ok and result.to_model_text() == "tls via proxy"
        assert log.requests and set(log.requests) == {f"CONNECT localhost:{port}"}
        await executor.aclose()
    # 证书主机名不符时，隧道建立后握手失败（代理不能替目标通过校验）
    async with (
        serve(mcp_app(remote, json_response=True), cert=pki.wrong_cert, key=pki.wrong_key) as port,
        serve_proxy(log) as proxy_port,
    ):
        with pytest.raises(UpstreamError) as info:
            await proxied_executor(port, proxy_port, https=True, ca=pki.ca)
        assert info.value.code == "mcp_connect_failed"
