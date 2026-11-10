"""MCP Client 工具适配器（P2，需要 ``agent-tools[mcp]``，基于官方 SDK ``mcp>=2.3,<2.4``）。

本模块负责“连接 MCP 服务端并调用远端工具”；工具的发现与注册在
:mod:`agent_tools.integrations.mcp` 中（那里会使用注册表）。

连接与会话（已按 ``mcp 2.3.0`` 的实际实现核实）：

* ``mcp.Client`` 只能 ``async with`` 进入一次，退出时会关闭内部的 anyio task group，
  **必须在进入它的同一个任务中退出**。因此每条连接由一个专属后台任务持有
  （:class:`_Connection`），调用方从任意任务并发发起请求（SDK 的 JSON-RPC 分发器支持并发请求）；
* 每个服务端最多一条活动连接，首次使用时建立；并发的首次调用只会建立一次连接（单飞）；
* 服务端进程退出或连接断开后，SDK 不会让持有任务自行结束，而是让后续请求抛出
  ``MCPError(CONNECTION_CLOSED)``。适配器据此回收这条连接，**本次调用**返回可重试的
  ``mcp_connection_lost``，下一次调用再重新连接；
* 重连是**受控的**：没有后台重连循环；连续建立连接失败 ``max_consecutive_failures`` 次后，
  进入 ``failure_cooldown_s`` 的冷却期，期间调用直接失败（``mcp_server_unavailable``）；
* 取消：调用方取消（包括执行器的超时）时，SDK 会向服务端发送 ``notifications/cancelled``
  （stdio 下已验证服务端确实收到并停止）。但通知只是请求，**不保证**远端已经停止执行，
  也不能撤回已经发生的副作用。

安全默认值：

* 不提供 sampling、elicitation、roots、logging 回调，服务端无法反向调用模型或读取本地信息；
  服务端要求额外输入时，调用失败（``mcp_input_required``）；
* 关闭 SDK 的响应缓存，保证工具列表与调用结果都来自服务端的当前状态；
* stdio：命令、参数、工作目录、额外环境变量都由宿主配置。SDK 总会继承一组固定的“安全”
  环境变量（POSIX 上为 HOME、LOGNAME、PATH、SHELL、TERM、USER），再合并宿主显式给出的变量，
  其余环境变量不会传给子进程；
* Streamable HTTP：地址必须通过 :class:`NetworkPolicy`（协议、主机白名单、端口）检查；
  每次建立连接前做一次 DNS 预检，并使用 P1 的连接时检查传输层（解析、检查全部地址、
  直接连接已校验 IP）。SDK 只会跟随**同源**重定向，且每个新连接仍会被检查。
  凭证通过 ``SecretProvider`` 在建立会话时获取；
* 服务端返回的内容、错误文本都是不可信的：错误文本默认不交给模型
  （``expose_error_text=False``），结果统一标记为不可信。

P3 新增（全部默认关闭，保持 P2 行为）：

* **stdio 隔离**（``StdioTransportConfig.isolation``）：经由
  :mod:`agent_tools.runtime.stdio_launcher` 启动服务端，设置资源限制、可加外部沙箱命令前缀，
  并在传输层限制单条 JSON-RPC 消息的大小（超长响应变为 ``mcp_response_too_large`` 错误，
  连接继续可用）；
* **HTTP 响应大小上限**（``McpServerConfig.max_response_bytes``）：JSON 响应在传输层流式计数，
  超过上限立即中断；SSE 响应使用 SDK 的单事件上限。两者都映射为
  ``OutputLimitError(mcp_response_too_large)``。启用后请求头带 ``Accept-Encoding: identity``，
  避免压缩响应在解压后突破上限；
* **私有 CA**（``HttpTransportConfig.ca_bundle``）：用指定的 CA 文件校验服务端证书。

租户会话隔离（``McpServerConfig.session_scope``，默认 ``"server"`` 保持原行为）：

* ``"server"``：每个服务端一条会话，被所有调用共享；凭证用**不带租户**的合成上下文获取，
  不会因为“谁先调用”而变成某个租户的凭证；
* ``"tenant"``：每个 ``(服务端, 租户)`` 一条独立会话（独立的 ``mcp.Client``、独立的 HTTP
  连接池或独立的 stdio 子进程）。租户取自 ``ToolContext.principal.tenant_id``（由宿主在认证后
  设置，不来自模型）；没有租户的调用**直接拒绝**（``mcp_tenant_required``），不会退回共享会话。
  凭证用只带该租户、不带用户与权限的合成上下文获取：同一租户的会话被该租户的所有用户共享，
  所以凭证只能是租户级的，不能是用户级的；
* 会话数量有上限（``max_tenant_sessions``），达到上限时回收最久未使用且没有在途请求的会话，
  全部忙碌时新租户的调用失败（``mcp_session_limit``，可重试）；空闲超过 ``tenant_idle_ttl_s``
  的会话在下一次取会话时被回收（没有后台清理任务）；
* 重连：连接断开后由**同一租户**的下一次调用按需重连，并重新获取凭证；连续失败计数与冷却期
  按会话（租户）分别统计，一个租户的凭证错误不会让其他租户进入冷却；
* 凭证变更：调用 :meth:`McpServerManager.invalidate` 让会话失效，下一次调用建立新会话并重新
  获取凭证。默认等在途请求结束后再关闭旧会话，``force=True`` 立即关闭；
* 工具目录（注册表）仍是服务端级别的：发现时用宿主指定的租户上下文读取列表，所有租户看到
  同一份工具定义。服务端若按租户返回不同的工具集，本模块**不支持**按租户区分注册表。

代理（``HttpTransportConfig.proxy``，默认不使用，不读取环境变量中的代理）：

* 只支持 ``http`` / ``https`` 代理；凭证（``用户名:密码``）通过 ``SecretProvider`` 获取，只进入
  发给代理的 ``Proxy-Authorization``；
* 代理模式下目标地址由代理解析和连接，**客户端无法做连接时 IP 检查**，退回到：建立会话前的
  URL 与 DNS 预检、**每个请求发送前**的 URL 与 DNS 预检（:class:`_PreflightTransport`，包括
  SDK 跟随的同源重定向）、以及“只能连接代理地址”的固定后端。预检与代理各自解析一次 DNS，
  两次结果可能不同，所以**无法防止 DNS 重绑定**；必须由代理自身的访问控制（目标白名单、
  禁止访问内网与元数据地址）兜底。``https`` 目标经 ``CONNECT`` 隧道，TLS 仍由客户端端到端校验
  证书与主机名；明文 ``http`` 目标（需要显式 ``allow_http``）的全部内容对代理可见。

仍然存在的限制：注册表不按租户区分；代理模式不具备连接时 IP 检查；不支持 SOCKS 代理。
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import re
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar
from urllib.parse import urlsplit

try:
    import anyio
    import httpcore2
    import httpx2
    import mcp_types
    import pydantic
    from mcp import Client, StdioServerParameters
    from mcp import types as mcp_t
    from mcp.client import InputRequiredRoundsExceededError
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared.exceptions import MCPError, NoBackChannelError, UrlElicitationRequiredError
except ImportError as exc:  # pragma: no cover - 由打包测试覆盖
    raise ImportError(
        "agent_tools.adapters.mcp requires the 'mcp' extra: pip install 'agent-tools[mcp]'"
    ) from exc

from ..binding import ToolBinding
from ..protocol.context import Principal, ToolContext
from ..protocol.errors import (
    ConcurrencyLimitedError,
    OutputLimitError,
    PermissionDeniedError,
    ToolExecutionError,
    ToolInfraError,
    UpstreamError,
)
from ..protocol.invocation import ContentBlock, ImageBlock, TextBlock, ToolOutput
from ..protocol.spec import RESERVED_HEADERS, McpTarget, SecretRef
from ..runtime.clock import Clock, MonotonicClock
from ..runtime.isolation import StdioIsolation
from ..security.net import NetworkPolicy, Resolver, resolve_checked, system_resolver
from ..security.secrets import SecretProvider
from .base import AttemptScope
from .http import HttpAdapterConfig, create_http_client

__all__ = [
    "HttpTransportConfig",
    "McpAdapter",
    "McpProxyConfig",
    "McpServerConfig",
    "McpServerManager",
    "StdioTransportConfig",
    "map_call_result",
]

_T = TypeVar("_T")
_IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp"}
#: stdio 启动器合成的“响应过大”错误码（见 ``agent_tools.runtime.stdio_launcher``）。
_LAUNCHER_TOO_LARGE = -32090
#: HTTP 传输层大小上限触发时，错误信息中的标记（SDK 会把它包进 PARSE_ERROR 的消息里）。
_HTTP_TOO_LARGE_MARKER = "agent-tools: MCP response exceeded"
_LAUNCHER_MODULE = "agent_tools.runtime.stdio_launcher"
_MAX_ERROR_TEXT = 300
_SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,128}$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")

#: 会话的共享范围：``server`` 所有调用共享一条会话；``tenant`` 每个租户一条独立会话。
SessionScope = Literal["server", "tenant"]
_MAX_LINK_TEXT = 512

#: 由宿主提供、用于替代默认 Streamable HTTP 客户端的工厂（仅用于测试，例如接入进程内的
#: ASGI 应用）。**生产环境不要使用**：它会绕过 P1 的连接时检查传输层。
HttpClientFactory = Callable[["HttpTransportConfig", Mapping[str, str]], Any]


@dataclass(frozen=True, slots=True)
class StdioTransportConfig:
    """stdio 子进程传输的配置（全部由宿主提供，不来自模型或服务端）。

    Attributes:
        command: 可执行文件。
        args: 命令行参数。
        cwd: 工作目录；``None`` 表示继承当前进程的工作目录。
        env: 额外显式传给子进程的环境变量（合并在 SDK 的固定安全变量之上）。不要在这里放密钥。
        isolation: 隔离配置（P3）；``None`` 表示直接启动服务端（P2 行为，stdout 没有大小上限）。
        secret_env: 环境变量名 → 密钥名。启动子进程时通过 ``SecretProvider`` 获取密钥并作为
            环境变量传入（覆盖 ``env`` 中的同名变量）；取到的值会登记为已知密钥，在日志、审计
            和结果中做精确清除。``session_scope="tenant"`` 时用租户上下文获取，每个租户的
            子进程拿到各自的凭证。注意：环境变量对子进程及其后代可见，这是 stdio 传输的固有边界。
    """

    command: str
    args: tuple[str, ...] = ()
    cwd: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    isolation: StdioIsolation | None = None
    secret_env: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验命令与参数中没有空字节、命令不为空，以及密钥环境变量的名字合法。

        Raises:
            ValueError: 配置非法。
        """
        if not self.command or "\0" in self.command or any("\0" in a for a in self.args):
            raise ValueError("invalid stdio command")
        for var, name in self.secret_env.items():
            if not _ENV_NAME_RE.match(var) or not _SECRET_NAME_RE.match(name):
                raise ValueError("invalid secret_env entry")


@dataclass(frozen=True, slots=True)
class McpProxyConfig:
    """Streamable HTTP 使用的显式正向代理（部署配置，不来自模型或服务端）。

    安全前提：配置代理后客户端**不再具备连接时 IP 检查**，目标地址的最终访问控制必须由代理
    承担（见模块说明）。代理地址本身被视为可信配置，不经过 ``NetworkPolicy`` 的内网检查。

    Attributes:
        url: 代理地址，``http://host[:port]`` 或 ``https://host[:port]``；不能带用户信息、路径、
            查询或片段（凭证用 ``auth_secret``，避免出现在配置与日志里）。
        auth_secret: 代理 Basic 认证的密钥名；密钥值的格式是 ``用户名:密码``，通过
            ``SecretProvider`` 获取（租户会话用租户上下文）。``None`` 表示代理不需要认证。
    """

    url: str
    auth_secret: str | None = None

    def __post_init__(self) -> None:
        """校验代理地址与密钥名。

        Raises:
            ValueError: 协议不是 http/https、缺少主机、带有用户信息 / 路径 / 查询 / 片段，
                或密钥名格式非法。
        """
        parts = urlsplit(self.url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError("proxy url must be an http(s) URL with a host")
        if parts.username or parts.password:
            raise ValueError("proxy url must not contain credentials; use auth_secret")
        if parts.path not in ("", "/") or parts.query or parts.fragment:
            raise ValueError("proxy url must not contain a path, query or fragment")
        _ = parts.port  # 端口非法时由 urlsplit 抛出 ValueError
        if self.auth_secret is not None and not _SECRET_NAME_RE.match(self.auth_secret):
            raise ValueError("invalid proxy auth_secret name")


@dataclass(frozen=True, slots=True)
class HttpTransportConfig:
    """Streamable HTTP 传输的配置。

    Attributes:
        url: MCP 端点地址；必须能通过 ``network`` 的检查。
        network: 出站网络策略（主机白名单、端口、内网放行）。
        headers: 每个请求都带上的静态请求头（不能包含密钥，密钥用 ``auth``）。
        auth: 凭证引用，建立连接时通过 ``SecretProvider`` 获取。
        ca_bundle: 校验服务端证书使用的 CA 文件（PEM，P3）；``None`` 使用默认信任库。
        proxy: 显式正向代理；``None``（默认）表示直连并启用连接时 IP 检查。设置后退回到
            “发送前预检 + 代理访问控制”，**无法防止 DNS 重绑定**（见 :class:`McpProxyConfig`）。
    """

    url: str
    network: NetworkPolicy
    headers: Mapping[str, str] = field(default_factory=dict)
    auth: SecretRef | None = None
    ca_bundle: str | None = None
    proxy: McpProxyConfig | None = None

    @property
    def guard_mode(self) -> Literal["connect", "preflight"]:
        """当前生效的网络检查方式。

        Returns:
            ``connect``：连接时检查；``preflight``：代理模式下只做发送前预检。
        """
        return "preflight" if self.proxy is not None else "connect"

    def __post_init__(self) -> None:
        """校验地址形式与静态请求头。

        Raises:
            ValueError: 地址不是 http/https、带有用户信息或片段；请求头包含换行或使用了保留请求头。
        """
        parts = urlsplit(self.url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError("url must be an http(s) URL with a host")
        if parts.username or parts.password or parts.fragment:
            raise ValueError("url must not contain credentials or a fragment")
        for name, value in self.headers.items():
            if name.lower() in RESERVED_HEADERS or "\r" in value or "\n" in value:
                raise ValueError(f"header {name!r} is not allowed")
            if name.lower() == "proxy-authorization":
                # 代理凭证只能通过 McpProxyConfig.auth_secret 提供，不能作为普通请求头发给目标
                raise ValueError("header 'Proxy-Authorization' is not allowed")


@dataclass(frozen=True, slots=True)
class McpServerConfig:
    """一个 MCP 服务端的部署配置。

    Attributes:
        server_id: 服务端 ID（``^[a-z0-9][a-z0-9_-]{0,31}$``），用于工具名和 ``McpTarget``。
        transport: 传输方式。
        connect_timeout_s: 建立连接（含握手）的超时。
        max_consecutive_failures: 连续建立连接失败多少次后进入冷却期。
        failure_cooldown_s: 冷却期长度；期间调用直接失败，不尝试连接。
        expose_error_text: ``isError`` 时是否把服务端的错误文本（截断、清除已知密钥后）交给模型。
            默认不交出，因为它是不可信内容，可能包含内部细节。
        max_list_pages: 发现工具时最多读取的分页数。
        max_tools: 发现工具时最多接受的工具数。
        max_response_bytes: Streamable HTTP 单个响应（JSON）或单个 SSE 事件的最大字节数（P3）；
            ``None`` 表示不在传输层限制（P2 行为：JSON 响应不限，SSE 事件使用 SDK 默认的
            1 MiB）。stdio 的消息大小上限在 ``StdioIsolation.max_line_bytes`` 中配置。
        session_scope: 会话与凭证的共享范围。``"server"``（默认，原有行为）：一条会话被所有
            调用共享，凭证是服务端级别；``"tenant"``：每个租户一条独立会话与凭证，没有租户的
            调用被拒绝（见模块说明）。
        max_tenant_sessions: ``"tenant"`` 范围下同时保留的会话数上限（stdio 即子进程数上限）。
        tenant_idle_ttl_s: ``"tenant"`` 范围下会话的空闲回收时间（秒）；``None`` 表示不按空闲
            回收（仍受数量上限约束）。
    """

    server_id: str
    transport: StdioTransportConfig | HttpTransportConfig
    connect_timeout_s: float = 15.0
    max_consecutive_failures: int = 3
    failure_cooldown_s: float = 30.0
    expose_error_text: bool = False
    max_list_pages: int = 50
    max_tools: int = 1000
    max_response_bytes: int | None = None
    session_scope: SessionScope = "server"
    max_tenant_sessions: int = 64
    tenant_idle_ttl_s: float | None = 900.0

    def __post_init__(self) -> None:
        """用 ``McpTarget`` 的规则校验服务端 ID，并检查各项上限为正数、会话范围合法。

        Raises:
            ValueError: 配置非法。
        """
        McpTarget(server_id=self.server_id, remote_name="x")
        if (
            min(self.connect_timeout_s, self.failure_cooldown_s) <= 0
            or min(self.max_consecutive_failures, self.max_list_pages, self.max_tools) <= 0
        ):
            raise ValueError("MCP server limits must be positive")
        if self.max_response_bytes is not None and self.max_response_bytes < 1024:
            raise ValueError("max_response_bytes must be at least 1024")
        if self.session_scope not in ("server", "tenant"):
            raise ValueError("session_scope must be 'server' or 'tenant'")
        if self.max_tenant_sessions <= 0 or (
            self.tenant_idle_ttl_s is not None and self.tenant_idle_ttl_s <= 0
        ):
            raise ValueError("tenant session limits must be positive")


class _ResponseTooLargeError(httpx2.StreamError):
    """HTTP 响应超过传输层大小上限。

    继承 ``httpx2.StreamError``：SDK 处理 JSON 响应时会捕获它，并把它转换成该请求的
    ``PARSE_ERROR``（消息里带有 :data:`_HTTP_TOO_LARGE_MARKER`），而不是让请求一直挂起。
    """


class _CappedStream(httpx2.AsyncByteStream):
    """边读边计数的响应体：累计超过上限时抛出 :class:`_ResponseTooLargeError`。"""

    def __init__(self, inner: httpx2.AsyncByteStream, limit: int) -> None:
        """创建包装流。

        Args:
            inner: 原始响应体。
            limit: 最大字节数。
        """
        self._inner = inner
        self._limit = limit

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """逐块转发响应体，超限时中断。

        Yields:
            响应体数据块。

        Raises:
            _ResponseTooLargeError: 累计字节数超过上限。
        """
        total = 0
        async for chunk in self._inner:
            total += len(chunk)
            if total > self._limit:
                raise _ResponseTooLargeError(f"{_HTTP_TOO_LARGE_MARKER} {self._limit} bytes")
            yield chunk

    async def aclose(self) -> None:
        """关闭原始响应体（释放连接）。"""
        await self._inner.aclose()


class _CappedTransport(httpx2.AsyncBaseTransport):
    """限制非 SSE 响应大小的传输层包装（SSE 由 SDK 的单事件上限负责）。"""

    def __init__(self, inner: httpx2.AsyncBaseTransport, limit: int) -> None:
        """创建包装层。

        Args:
            inner: 实际的传输层（通常是带连接时检查的传输层）。
            limit: 最大字节数。
        """
        self._inner = inner
        self._limit = limit

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        """发送请求；非 SSE 响应的响应体换成计数流。

        Args:
            request: 请求。

        Returns:
            响应（响应体按需读取，超限时在读取过程中失败）。
        """
        response = await self._inner.handle_async_request(request)
        if response.headers.get("content-type", "").startswith("text/event-stream"):
            return response
        stream = response.stream
        assert isinstance(stream, httpx2.AsyncByteStream)
        return httpx2.Response(
            status_code=response.status_code,
            headers=response.headers,
            stream=_CappedStream(stream, self._limit),
            request=request,
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        """关闭实际的传输层。"""
        await self._inner.aclose()


class _DestinationBlockedError(httpx2.ConnectError):
    """代理模式的发送前预检拒绝了请求地址。

    继承 ``httpx2.ConnectError``：SDK 把它当作普通的传输失败处理（请求失败或会话断开），
    不会把它当成未知异常吞掉后让请求一直挂起；请求**不会**被发送给代理。
    """


class _PreflightTransport(httpx2.AsyncBaseTransport):
    """代理模式下，在**每个请求发送前**重新检查地址的传输层包装（尽力而为）。

    检查内容：请求地址的协议、主机、端口必须通过 ``NetworkPolicy``；主机的**全部** DNS 解析
    结果都必须被允许。这覆盖了会话建立之后的请求，以及 SDK 自行跟随的同源重定向。

    它**不能**防止 DNS 重绑定：这里解析一次，代理连接目标时还会自己再解析一次，两次结果可能
    不同。它的作用是在 DNS 记录已经指向内网时尽早拒绝，并保证请求地址不会偏离白名单。
    """

    def __init__(
        self, inner: httpx2.AsyncBaseTransport, policy: NetworkPolicy, resolver: Resolver
    ) -> None:
        """创建包装层。

        Args:
            inner: 实际的传输层（经由代理）。
            policy: 出站网络策略。
            resolver: DNS 解析函数（客户端本地解析，不经过代理）。
        """
        self._inner = inner
        self._policy = policy
        self._resolver = resolver

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        """检查请求地址后转发；检查失败时不发送请求。

        Args:
            request: 请求。

        Returns:
            代理返回的响应。

        Raises:
            _DestinationBlockedError: 地址不被策略允许，或解析结果包含不允许的地址。
            httpx2.ConnectError: 预检的 DNS 解析失败（按连接失败处理）。
        """
        try:
            host, port = self._policy.check_url(str(request.url))
            await resolve_checked(host, port, self._policy, self._resolver)
        except PermissionDeniedError:
            raise _DestinationBlockedError(
                "agent-tools: destination is not allowed by the network policy", request=request
            ) from None
        except UpstreamError:
            raise httpx2.ConnectError(
                "agent-tools: preflight DNS resolution failed", request=request
            ) from None
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        """关闭实际的传输层。"""
        await self._inner.aclose()


class _Connection:
    """一条 MCP 会话：由专属后台任务进入并持有 ``mcp.Client``，直到被关闭。"""

    def __init__(self, client: Client, cleanup: Sequence[Callable[[], Awaitable[None]]]) -> None:
        """创建连接对象（尚未连接）。

        Args:
            client: 尚未进入的 ``mcp.Client``。
            cleanup: 会话结束后需要执行的清理（例如关闭自建的 HTTP 客户端）。
        """
        self.client = client
        self._cleanup = list(cleanup)
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()
        self._error: BaseException | None = None
        self._task: asyncio.Task[None] | None = None
        self.broken = False

    async def start(self, wait_s: float) -> None:
        """启动后台任务并等待握手完成。

        Args:
            wait_s: 握手超时（秒）。

        Raises:
            UpstreamError: 握手超时或失败（``mcp_connect_failed``，可重试）。
            asyncio.CancelledError: 调用方取消（后台任务会被一并关闭）。
        """
        self._task = asyncio.create_task(self._run(), name="agent_tools.mcp.connection")
        try:
            async with asyncio.timeout(wait_s):
                await self._ready.wait()
        except BaseException:
            await self.close()
            raise
        if self._error is not None:
            await self.close()
            raise UpstreamError(
                "could not connect to the MCP server", code="mcp_connect_failed", retryable=True
            ) from self._error

    async def _run(self) -> None:
        """后台任务主体：进入客户端（完成握手），然后一直等到被要求关闭再退出。

        进入和退出都发生在这个任务里，满足 anyio 对 cancel scope 的要求。握手异常会被记录
        并通过 ``_ready`` 通知等待方；取消信号不会被吞掉。
        """
        try:
            async with self.client:
                self._ready.set()
                await self._stop.wait()
        except Exception as exc:
            self._error = exc
        finally:
            self._ready.set()
            for fn in self._cleanup:
                with contextlib.suppress(Exception):
                    await fn()

    async def close(self, grace_s: float = 5.0) -> None:
        """关闭连接：通知后台任务退出（会关闭会话、终止 stdio 子进程），超时则取消它。

        可以重复调用。

        Args:
            grace_s: 等待正常退出的时间（秒）。
        """
        self._stop.set()
        task = self._task
        if task is None or task.done():
            return
        done, _ = await asyncio.wait({task}, timeout=grace_s)
        if not done:
            task.cancel()
            await asyncio.wait({task})


@dataclass
class _Session:
    """一条会话在管理器中的运行时状态，键为 ``(服务端, 租户)``。

    ``session_scope="server"`` 时租户恒为 ``None``（整个服务端一条会话）。连续失败计数、
    冷却期和已知密钥都按会话保存，租户之间互不影响。

    Attributes:
        config: 服务端配置。
        tenant_id: 会话所属租户；服务端级会话为 ``None``。
        lock: 建立连接的单飞锁（同一会话的并发首次调用只连接一次）。
        connection: 当前连接；没有或已断开时为 ``None``。
        failures: 连续建立连接失败的次数。
        cooldown_until: 冷却期结束的 monotonic 时刻。
        secrets: 这条会话建立连接时注入过的密钥值（只属于这个租户）。
        inflight: 正在使用这条会话的请求数；大于 0 时不会被数量上限或空闲超时回收。
        last_used: 最近一次使用结束的 monotonic 时刻（用于 LRU 与空闲回收）。
        retired: 已从管理器移除（失效、被回收或管理器关闭）；在途请求结束后关闭连接，
            新的调用不会再拿到它。
    """

    config: McpServerConfig
    tenant_id: str | None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    connection: _Connection | None = None
    failures: int = 0
    cooldown_until: float = 0.0
    secrets: set[str] = field(default_factory=set)
    inflight: int = 0
    last_used: float = 0.0
    retired: bool = False


def _tenant_key(config: McpServerConfig, ctx: ToolContext | None) -> str | None:
    """计算一次调用应当使用的会话租户键。

    Args:
        config: 服务端配置。
        ctx: 调用上下文；``None`` 表示没有身份信息。

    Returns:
        ``session_scope="server"`` 时恒为 ``None``；``"tenant"`` 时为调用方的租户 ID。

    Raises:
        PermissionDeniedError: 租户范围的服务端收到了没有租户的调用（``mcp_tenant_required``）。
            这里安全失败，**不会**退回到共享会话，否则无租户调用会借用到别的身份。
    """
    if config.session_scope == "server":
        return None
    tenant = ctx.principal.tenant_id if ctx is not None else None
    if not tenant:
        raise PermissionDeniedError(
            "this MCP server requires a tenant identity", code="mcp_tenant_required"
        )
    return tenant


class McpServerManager:
    """管理多个 MCP 服务端的会话：建立、复用、回收、受控重连、失效与关闭。

    会话按 ``(服务端, 租户)`` 保存（见模块说明的“租户会话隔离”）。只在事件循环线程中使用。
    """

    def __init__(
        self,
        servers: Sequence[McpServerConfig],
        *,
        secrets: SecretProvider | None = None,
        resolver: Resolver = system_resolver,
        network_backend: httpcore2.AsyncNetworkBackend | None = None,
        clock: Clock | None = None,
        http_client_factory: HttpClientFactory | None = None,
    ) -> None:
        """创建管理器（不会立即连接）。

        Args:
            servers: 服务端配置；``server_id`` 不能重复。
            secrets: 密钥提供者；配置了 ``auth``、代理 ``auth_secret`` 或 ``secret_env`` 时必须
                提供。租户范围的服务端会用只带 ``tenant_id`` 的上下文调用它。
            resolver: DNS 解析函数（测试可以注入）。
            network_backend: 实际建立连接的网络后端（测试可以注入），会被包在检查后端里。
            clock: 单调时钟（用于冷却期与空闲回收，测试可以注入）。
            http_client_factory: **仅测试使用**：替代默认的受检查 HTTP 客户端（会绕过连接时
                检查与代理；URL 检查与 DNS 预检仍然执行）。

        Raises:
            ValueError: ``server_id`` 重复。
        """
        ids = [s.server_id for s in servers]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate MCP server_id")
        self._configs = {s.server_id: s for s in servers}
        self._sessions: dict[tuple[str, str | None], _Session] = {}
        # 已移除但仍有在途请求的会话：关闭管理器时需要一并关闭
        self._draining_sessions: dict[int, _Session] = {}
        self._closing: set[asyncio.Task[None]] = set()
        self._secrets = secrets
        self._resolver = resolver
        self._network_backend = network_backend
        self._clock = clock or MonotonicClock()
        self._http_client_factory = http_client_factory
        self._closed = False

    @property
    def server_ids(self) -> list[str]:
        """已配置的服务端 ID。"""
        return list(self._configs)

    def config(self, server_id: str) -> McpServerConfig:
        """返回服务端配置。

        Args:
            server_id: 服务端 ID。

        Returns:
            配置。

        Raises:
            ToolInfraError: 服务端未配置（``mcp_server_unknown``）。
        """
        config = self._configs.get(server_id)
        if config is None:
            raise ToolInfraError("MCP server is not configured", code="mcp_server_unknown")
        return config

    def known_secrets(self, server_id: str, *, tenant_id: str | None = None) -> tuple[str, ...]:
        """返回某条会话建立连接时注入过的密钥值（诊断与测试用）。

        执行路径不依赖这个方法：适配器通过 :meth:`call_tool` 的 ``on_secrets`` 拿到**本次调用
        实际使用的会话**的密钥，不会拿到其他租户的值。

        Args:
            server_id: 服务端 ID。
            tenant_id: 租户；服务端级会话传 ``None``。

        Returns:
            密钥值（含带前缀的请求头值）；会话不存在时为空。
        """
        self.config(server_id)
        session = self._sessions.get((server_id, tenant_id))
        return tuple(session.secrets) if session is not None else ()

    def connected(self, server_id: str, *, tenant_id: str | None = None) -> bool:
        """某条会话当前是否有可用连接（用于诊断和测试）。

        Args:
            server_id: 服务端 ID。
            tenant_id: 租户；服务端级会话传 ``None``。

        Returns:
            有未损坏的活动连接时为 True。
        """
        self.config(server_id)
        session = self._sessions.get((server_id, tenant_id))
        conn = session.connection if session is not None else None
        return conn is not None and not conn.broken

    def session_tenants(self, server_id: str) -> list[str | None]:
        """返回某个服务端当前保留的会话所属的租户（用于诊断和测试）。

        Args:
            server_id: 服务端 ID。

        Returns:
            租户 ID 列表；服务端级会话以 ``None`` 表示。
        """
        self.config(server_id)
        return [tenant for sid, tenant in self._sessions if sid == server_id]

    async def list_tools(
        self, server_id: str, *, ctx: ToolContext | None = None
    ) -> list[mcp_t.Tool]:
        """分页读取服务端的全部工具（绕过缓存）。

        Args:
            server_id: 服务端 ID。
            ctx: 发现时使用的上下文。租户范围的服务端**必须**提供带 ``tenant_id`` 的上下文
                （由宿主指定用哪个租户的会话与凭证读取工具列表）；服务端范围时忽略。

        Returns:
            远端工具定义（不可信输入，由调用方做白名单和校验）。

        Raises:
            UpstreamError: 连接失败、协议错误、分页超过上限或游标循环、工具数超过上限。
            PermissionDeniedError: 租户范围的服务端没有得到租户上下文。
        """
        cfg = self.config(server_id)
        tools: list[mcp_t.Tool] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(cfg.max_list_pages):

            async def fetch(client: Client, cur: str | None = cursor) -> mcp_t.ListToolsResult:
                """读取一页工具（绕过 SDK 的响应缓存）。

                Args:
                    client: MCP 客户端。
                    cur: 分页游标。

                Returns:
                    一页结果。
                """
                return await client.list_tools(cursor=cur, cache_mode="bypass")

            page = await self._request(server_id, fetch, ctx=ctx)
            tools.extend(page.tools)
            if len(tools) > cfg.max_tools:
                raise UpstreamError(
                    "the MCP server lists too many tools", code="mcp_too_many_tools"
                )
            cursor = page.next_cursor
            if not cursor:
                return tools
            if cursor in seen:
                raise UpstreamError(
                    "the MCP server returned a repeated page cursor", code="mcp_pagination_error"
                )
            seen.add(cursor)
        raise UpstreamError("the MCP server returned too many pages", code="mcp_pagination_error")

    async def call_tool(
        self,
        server_id: str,
        name: str,
        arguments: dict[str, Any],
        *,
        ctx: ToolContext | None = None,
        on_started: Callable[[], None] | None = None,
        on_secrets: Callable[[Iterable[str]], None] | None = None,
    ) -> mcp_t.CallToolResult:
        """调用远端工具。

        会话选择：服务端范围使用共享会话；租户范围使用 ``ctx.principal.tenant_id`` 对应的会话，
        不同租户的请求不会进入同一条会话、不会带上别的租户的凭证。

        取消：调用方取消时 ``CancelledError`` 原样传播，SDK 会向服务端发送取消通知（不保证
        远端停止）。连接断开时回收连接并抛出可重试错误，**不会**在这里自动重试。

        Args:
            server_id: 服务端 ID。
            name: 远端工具名。
            arguments: 已经过校验的参数。
            ctx: 调用上下文（只读取租户；租户范围的服务端必须提供）。
            on_started: 拿到可用会话、即将发送请求时调用（用于区分“没能开始”与“执行超时”）。
            on_secrets: 请求结束（成功、失败或取消）时调用一次，参数是**本次使用的会话**
                注入过的密钥值，供调用方登记后做精确清除。

        Returns:
            服务端返回的结果（``is_error`` 由调用方映射）。

        Raises:
            UpstreamError: 连接失败或断开、协议错误、结果格式非法。
            ToolExecutionError: 服务端要求额外输入（未启用相关回调）。
            PermissionDeniedError: 租户范围的服务端收到没有租户的调用（``mcp_tenant_required``）。
            ConcurrencyLimitedError: 租户会话数达到上限且全部忙碌（``mcp_session_limit``）。
            ToolInfraError: 管理器已关闭或服务端未配置。
        """
        return await self._request(
            server_id,
            lambda c: c.call_tool(name, arguments),
            ctx=ctx,
            on_started=on_started,
            on_secrets=on_secrets,
        )

    async def invalidate(
        self, server_id: str, *, tenant_id: str | None = None, force: bool = False
    ) -> int:
        """让会话失效：凭证变更（轮换、吊销）或租户下线时调用。

        失效的会话立刻从管理器移除，之后的调用会建立**新会话并重新获取凭证**；失败计数与
        冷却期随之清零。旧会话的处理：

        * ``force=False``（默认）：等在途请求结束后再关闭旧连接；这些请求仍在使用旧凭证；
        * ``force=True``：立即关闭旧连接，在途请求得到可重试的 ``mcp_connection_lost``
          （是否重试仍由工具的幂等声明决定）。凭证被吊销时使用。

        Args:
            server_id: 服务端 ID。
            tenant_id: 只让这个租户的会话失效；``None`` 表示该服务端的**全部**会话
                （服务端范围时就是那条共享会话）。
            force: 是否立即关闭有在途请求的会话。

        Returns:
            失效的会话数。

        Raises:
            ToolInfraError: 服务端未配置。
        """
        self.config(server_id)
        keys = [
            key
            for key in self._sessions
            if key[0] == server_id and (tenant_id is None or key[1] == tenant_id)
        ]
        tasks = [
            task
            for key in keys
            if (task := self._retire(self._sessions.pop(key), force=force)) is not None
        ]
        if tasks:
            await asyncio.wait(tasks)
        return len(keys)

    async def close_idle_sessions(self) -> int:
        """关闭所有租户范围服务端中空闲超过 ``tenant_idle_ttl_s`` 的会话。

        空闲回收平时在取会话时顺带进行（没有后台任务）；长时间没有任何调用的宿主可以定期调用
        这个方法，及时释放连接和 stdio 子进程。有在途请求的会话不会被关闭。

        Returns:
            关闭的会话数。
        """
        now = self._clock.monotonic()
        tasks: list[asyncio.Task[None]] = []
        count = 0
        for server_id in self._configs:
            for session in self._pop_idle(server_id, now):
                count += 1
                task = self._retire(session, force=False)
                if task is not None:
                    tasks.append(task)
        if tasks:
            await asyncio.wait(tasks)
        return count

    async def aclose(self) -> None:
        """关闭所有会话（终止 stdio 子进程、关闭 HTTP 会话）。可以重复调用。

        包括所有租户的会话，以及已经失效但还有在途请求的会话；正在建立中的连接会等它建立
        完成（受连接超时约束）后关闭，不会留下子进程。在途请求得到连接断开的错误。
        """
        self._closed = True
        sessions = [*self._sessions.values(), *self._draining_sessions.values()]
        self._sessions.clear()
        self._draining_sessions.clear()
        for session in sessions:
            session.retired = True
            async with session.lock:
                conn, session.connection = session.connection, None
            if conn is not None:
                await conn.close()
        while self._closing:
            await asyncio.wait(set(self._closing))

    # -- 内部方法 ------------------------------------------------------------------
    def _checkout(self, server_id: str, ctx: ToolContext | None) -> _Session:
        """取得（必要时创建）本次调用使用的会话，并登记一个在途请求。

        整个方法是同步的：查找、回收与创建之间没有让出事件循环，同一个 ``(服务端, 租户)``
        不会被并发创建出两条会话。租户范围的服务端在这里顺带回收空闲超时的会话，并执行
        数量上限（回收最久未使用的空闲会话）。

        Args:
            server_id: 服务端 ID。
            ctx: 调用上下文。

        Returns:
            会话（``inflight`` 已加一，调用方必须配对调用 :meth:`_checkin`）。

        Raises:
            ToolInfraError: 管理器已关闭或服务端未配置。
            PermissionDeniedError: 租户范围的服务端收到没有租户的调用。
            ConcurrencyLimitedError: 会话数达到上限且全部有在途请求（``mcp_session_limit``）。
        """
        if self._closed:
            raise ToolInfraError("the MCP server manager is closed", code="mcp_manager_closed")
        cfg = self.config(server_id)
        key = (server_id, _tenant_key(cfg, ctx))
        session = self._sessions.get(key)
        if cfg.session_scope == "tenant":
            now = self._clock.monotonic()
            for idle in self._pop_idle(server_id, now, keep=key):
                self._retire(idle, force=False)
            if session is None:
                owned = [s for (sid, _), s in self._sessions.items() if sid == server_id]
                if len(owned) >= cfg.max_tenant_sessions:
                    candidates = [s for s in owned if s.inflight == 0]
                    if not candidates:
                        raise ConcurrencyLimitedError(
                            "too many active MCP tenant sessions", code="mcp_session_limit"
                        )
                    victim = min(candidates, key=lambda s: s.last_used)
                    del self._sessions[(server_id, victim.tenant_id)]
                    self._retire(victim, force=False)
        if session is None:
            session = self._sessions[key] = _Session(cfg, key[1])
        session.inflight += 1
        return session

    def _checkin(self, session: _Session) -> None:
        """登记一个在途请求结束；已失效的会话在最后一个在途请求结束时关闭连接。

        Args:
            session: :meth:`_checkout` 返回的会话。
        """
        session.inflight -= 1
        session.last_used = self._clock.monotonic()
        if session.retired and session.inflight == 0:
            self._draining_sessions.pop(id(session), None)
            self._close_connection(session)

    def _pop_idle(
        self, server_id: str, now: float, *, keep: tuple[str, str | None] | None = None
    ) -> list[_Session]:
        """从管理器中移除某个服务端空闲超时的租户会话（不关闭，由调用方处理）。

        Args:
            server_id: 服务端 ID。
            now: 当前 monotonic 时刻。
            keep: 即使空闲超时也保留的会话键（本次调用正要使用的那条）。

        Returns:
            被移除的会话；服务端范围或没有配置空闲时间时为空。
        """
        cfg = self._configs[server_id]
        ttl = cfg.tenant_idle_ttl_s
        if cfg.session_scope != "tenant" or ttl is None:
            return []
        expired = [
            key
            for key, s in self._sessions.items()
            if key[0] == server_id and key != keep and s.inflight == 0 and now - s.last_used >= ttl
        ]
        return [self._sessions.pop(key) for key in expired]

    def _retire(self, session: _Session, *, force: bool) -> asyncio.Task[None] | None:
        """把一条已从管理器移除的会话标记为失效，并在合适的时机关闭它的连接。

        Args:
            session: 已经不在 ``_sessions`` 中的会话。
            force: 有在途请求时是否立即关闭连接。

        Returns:
            关闭连接的后台任务；没有连接，或要等在途请求结束（由 :meth:`_checkin` 关闭）时为
            ``None``。
        """
        session.retired = True
        if session.inflight > 0:
            # 在途请求结束前会话仍然存活：登记下来，管理器关闭时一并处理
            self._draining_sessions[id(session)] = session
            if not force:
                return None
        return self._close_connection(session)

    def _close_connection(self, session: _Session) -> asyncio.Task[None] | None:
        """在后台任务中关闭会话当前的连接（终止 stdio 子进程、关闭 HTTP 客户端）。

        用后台任务而不是直接等待：调用点可能处于被取消的请求的清理路径上，关闭动作不应被
        取消打断。任务会被记录，:meth:`aclose` 会等它们全部结束。

        Args:
            session: 会话。

        Returns:
            关闭任务；没有连接时为 ``None``。
        """
        conn, session.connection = session.connection, None
        if conn is None:
            return None
        conn.broken = True
        task = asyncio.create_task(conn.close(), name="agent_tools.mcp.close")
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)
        return task

    async def _request(
        self,
        server_id: str,
        send: Callable[[Client], Awaitable[_T]],
        *,
        ctx: ToolContext | None = None,
        on_started: Callable[[], None] | None = None,
        on_secrets: Callable[[Iterable[str]], None] | None = None,
    ) -> _T:
        """取得会话、发送请求，并把异常映射成统一错误。

        Args:
            server_id: 服务端 ID。
            send: 使用客户端发送请求的函数。
            ctx: 调用上下文（决定使用哪个租户的会话）。
            on_started: 即将发送请求时调用。
            on_secrets: 请求结束时用本次会话的密钥值调用一次（任何退出路径都会调用）。

        Returns:
            请求结果。

        Raises:
            UpstreamError / ToolExecutionError / ToolInfraError / PermissionDeniedError /
            ConcurrencyLimitedError: 见 :meth:`call_tool`。
            asyncio.CancelledError: 调用方取消（在途计数照常归还）。
        """
        session = self._checkout(server_id, ctx)
        try:
            conn = await self._connection(session)
            if on_started is not None:
                on_started()
            try:
                return await send(conn.client)
            except (
                InputRequiredRoundsExceededError,
                NoBackChannelError,
                UrlElicitationRequiredError,
            ):
                raise ToolExecutionError(
                    "the MCP tool requested input that this client does not provide",
                    code="mcp_input_required",
                ) from None
            except MCPError as exc:
                if _response_too_large(exc):
                    # 传输层大小上限：只有这一条响应被丢弃，连接仍然可用
                    raise OutputLimitError(
                        "the MCP server response exceeded the configured size limit",
                        code="mcp_response_too_large",
                    ) from None
                if exc.code == mcp_types.CONNECTION_CLOSED:
                    await self._discard(session, conn)
                    raise UpstreamError(
                        "the MCP connection was lost", code="mcp_connection_lost", retryable=True
                    ) from None
                # 只暴露错误码，不暴露服务端的错误消息（不可信，可能含内部细节）
                raise UpstreamError(
                    f"the MCP server returned protocol error {exc.code}", code="mcp_protocol_error"
                ) from None
            except pydantic.ValidationError:
                raise UpstreamError(
                    "the MCP server returned a malformed result", code="mcp_invalid_response"
                ) from None
            except (
                anyio.ClosedResourceError,
                anyio.BrokenResourceError,
                anyio.EndOfStream,
                OSError,
            ):
                await self._discard(session, conn)
                raise UpstreamError(
                    "the MCP connection was lost", code="mcp_connection_lost", retryable=True
                ) from None
        finally:
            if on_secrets is not None:
                # 只交出本次会话（本租户）的密钥；连接可能在本次调用中才建立，所以放在最后
                on_secrets(tuple(session.secrets))
            self._checkin(session)

    async def _connection(self, session: _Session) -> _Connection:
        """返回会话的可用连接；没有时建立（同一会话的并发调用只建立一次）。

        Args:
            session: 已登记在途请求的会话。

        Returns:
            可用连接。

        Raises:
            ToolInfraError: 管理器已关闭。
            UpstreamError: 处于冷却期（``mcp_server_unavailable``）或建立连接失败。
        """
        if self._closed:
            raise ToolInfraError("the MCP server manager is closed", code="mcp_manager_closed")
        conn = session.connection
        if conn is not None and not conn.broken:
            return conn
        async with session.lock:
            if self._closed:
                raise ToolInfraError("the MCP server manager is closed", code="mcp_manager_closed")
            if session.connection is not None and not session.connection.broken:
                return session.connection
            now = self._clock.monotonic()
            if now < session.cooldown_until:
                raise UpstreamError(
                    "the MCP server is temporarily unavailable",
                    code="mcp_server_unavailable",
                    retryable=True,
                    retry_after=round(session.cooldown_until - now, 3),
                )
            try:
                conn = await self._open(session)
            except UpstreamError:
                session.failures += 1
                if session.failures >= session.config.max_consecutive_failures:
                    session.cooldown_until = (
                        self._clock.monotonic() + session.config.failure_cooldown_s
                    )
                    session.failures = 0
                raise
            if self._closed:
                # 建立连接期间管理器被关闭：不能把新连接留给后续请求
                await conn.close()
                raise ToolInfraError("the MCP server manager is closed", code="mcp_manager_closed")
            session.failures = 0
            session.connection = conn
            return conn

    async def _open(self, session: _Session) -> _Connection:
        """按传输方式为一条会话建立新连接（含握手）；凭证每次都重新获取。

        凭证、代理凭证和 stdio 的密钥环境变量都用这条会话的租户上下文获取，并记录到
        ``session.secrets``；它们只会进入这条会话自己的 HTTP 客户端或子进程。

        Args:
            session: 会话。

        Returns:
            已完成握手的连接。

        Raises:
            UpstreamError: 握手失败或超时。
            PermissionDeniedError: HTTP 地址不被网络策略允许。
            ToolInfraError: 取不到凭证。
        """
        cfg = session.config
        cleanup: list[Callable[[], Awaitable[None]]] = []
        target: Any
        if isinstance(cfg.transport, StdioTransportConfig):
            stdio = cfg.transport
            command, args = stdio.command, list(stdio.args)
            env = dict(stdio.env)
            for var, secret_name in stdio.secret_env.items():
                value = await self._secret(session, secret_name)
                if "\0" in value:
                    raise ToolInfraError("credential is malformed", code="secret_unavailable")
                session.secrets.add(value)
                env[var] = value
            if stdio.isolation is not None:
                # 经由隔离启动器启动：启动器与服务端同一进程组，SDK 关闭时一起终止
                launcher_config = json.dumps(
                    {
                        "limits": stdio.isolation.limits.to_json(),
                        "max_line_bytes": stdio.isolation.max_line_bytes,
                    },
                    separators=(",", ":"),
                )
                command = sys.executable
                args = [
                    "-I",
                    "-m",
                    _LAUNCHER_MODULE,
                    "--config",
                    launcher_config,
                    "--",
                    *stdio.isolation.wrapper,
                    stdio.command,
                    *stdio.args,
                ]
            target = StdioServerParameters(
                command=command,
                args=args,
                env=env or None,
                cwd=stdio.cwd,
            )
        else:
            http = cfg.transport
            host, port = http.network.check_url(http.url)
            # 预检：在交给 SDK 之前确认解析结果都被允许；直连模式下连接时还会再检查一次，
            # 代理模式下每个请求发送前再预检一次（见 _PreflightTransport）
            await resolve_checked(host, port, http.network, self._resolver)
            headers = dict(http.headers)
            if http.auth is not None:
                headers[http.auth.header] = await self._credential(session, http.auth)
            proxy_auth: tuple[str, str] | None = None
            if http.proxy is not None and http.proxy.auth_secret is not None:
                proxy_auth = await self._proxy_credential(session, http.proxy.auth_secret)
            limit = cfg.max_response_bytes
            if limit is not None:
                # 计数的是线上字节：要求不压缩，避免压缩响应在解压后突破上限
                headers["Accept-Encoding"] = "identity"
            use_proxy = http.proxy is not None
            policy, resolver = http.network, self._resolver

            def wrap(inner: httpx2.AsyncBaseTransport) -> httpx2.AsyncBaseTransport:
                """给传输层加上响应大小上限，以及代理模式的逐请求预检（预检在最外层）。"""
                if limit is not None:
                    inner = _CappedTransport(inner, limit)
                if use_proxy:
                    inner = _PreflightTransport(inner, policy, resolver)
                return inner

            if self._http_client_factory is not None:
                http_client = self._http_client_factory(http, headers)
                # 仅测试路径：工厂创建的客户端同样加上大小上限与预检包装
                http_client._transport = wrap(http_client._transport)
            else:
                http_client = create_http_client(
                    HttpAdapterConfig(
                        network=http.network,
                        ca_bundle=http.ca_bundle,
                        proxy=http.proxy.url if http.proxy is not None else None,
                    ),
                    resolver=self._resolver,
                    network_backend=self._network_backend,
                    headers=headers,
                    wrap_transport=wrap,
                    proxy_auth=proxy_auth,
                )
            cleanup.append(http_client.aclose)
            if limit is not None:
                target = streamable_http_client(
                    http.url, http_client=http_client, max_sse_event_size=limit
                )
            else:
                target = streamable_http_client(http.url, http_client=http_client)
        client = Client(
            target,
            cache=None,
            sampling_callback=None,
            elicitation_callback=None,
            list_roots_callback=None,
            logging_callback=None,
            message_handler=None,
        )
        conn = _Connection(client, cleanup)
        await conn.start(cfg.connect_timeout_s)
        return conn

    async def _secret(self, session: _Session, name: str) -> str:
        """用会话的租户上下文从 ``SecretProvider`` 取得一个密钥的原始值。

        上下文是合成的：只带会话的租户（服务端级会话没有租户），**不带**触发建立连接的那次
        调用的用户、权限或请求 ID。会话被同一租户的所有调用共享，凭证不能取决于谁先调用。

        Args:
            session: 会话。
            name: 密钥名。

        Returns:
            密钥值。

        Raises:
            ToolInfraError: 没有密钥提供者、取不到密钥，或值中有换行
                （``secret_unavailable``，不暴露密钥系统的错误细节）。
        """
        if self._secrets is None:
            raise ToolInfraError("no secret provider is configured", code="secret_unavailable")
        ctx = ToolContext(
            request_id=f"mcp-connect-{session.config.server_id}",
            principal=Principal(tenant_id=session.tenant_id),
        )
        try:
            secret = await self._secrets.get_secret(name, ctx)
        except Exception:
            raise ToolInfraError("credential is unavailable", code="secret_unavailable") from None
        if "\r" in secret or "\n" in secret:
            raise ToolInfraError("credential is malformed", code="secret_unavailable")
        return secret

    async def _credential(self, session: _Session, ref: SecretRef) -> str:
        """取得会话的服务端凭证，并记录其值供调用时做精确清除。

        Args:
            session: 会话（决定用哪个租户的凭证）。
            ref: 凭证引用。

        Returns:
            请求头的值。

        Raises:
            ToolInfraError: 没有密钥提供者或取不到密钥（不暴露密钥系统的错误细节）。
        """
        secret = await self._secret(session, ref.name)
        value = f"{ref.scheme} {secret}" if ref.scheme else secret
        session.secrets.update({secret, value})
        return value

    async def _proxy_credential(self, session: _Session, name: str) -> tuple[str, str]:
        """取得代理的 Basic 认证凭证，并记录其各种形式供精确清除。

        Args:
            session: 会话（租户会话可以使用租户专属的代理凭证）。
            name: 密钥名；密钥值的格式是 ``用户名:密码``。

        Returns:
            ``(用户名, 密码)``。

        Raises:
            ToolInfraError: 取不到密钥，或格式不是 ``用户名:密码``（``secret_unavailable``）。
        """
        raw = await self._secret(session, name)
        user, sep, password = raw.partition(":")
        if not sep or not user:
            raise ToolInfraError("proxy credential is malformed", code="secret_unavailable")
        # 登记原值、密码，以及实际出现在 Proxy-Authorization 里的 Base64 形式
        session.secrets.update({raw, password, base64.b64encode(raw.encode()).decode()})
        return user, password

    async def _discard(self, session: _Session, conn: _Connection) -> None:
        """回收一条已断开的连接；同一会话（同一租户）的下一次调用会重新连接并重新取凭证。

        Args:
            session: 连接所属的会话。
            conn: 要回收的连接。
        """
        conn.broken = True
        async with session.lock:
            if session.connection is conn:
                session.connection = None
        await conn.close()


def _response_too_large(exc: MCPError) -> bool:
    """判断一个 MCP 错误是否来自本模块配置的传输层大小上限。

    三种来源：stdio 启动器合成的错误码；HTTP JSON 响应被计数流中断后 SDK 生成的
    ``PARSE_ERROR``（消息带标记）；SSE 单事件超限后 SDK 生成的 ``CONNECTION_CLOSED``
    （消息来自 ``httpx2`` 的 SSE 解析器）。服务端自己伪造这些错误只会让调用失败，不会带来
    额外权限。

    Args:
        exc: MCP 错误。

    Returns:
        是传输层大小上限触发的错误时为 True。
    """
    message = str(getattr(exc, "message", "") or exc)
    if exc.code == _LAUNCHER_TOO_LARGE:
        return True
    if exc.code == mcp_types.PARSE_ERROR and _HTTP_TOO_LARGE_MARKER in message:
        return True
    return exc.code == mcp_types.CONNECTION_CLOSED and "Server-sent event exceeded" in message


def _truncate(text: str, limit: int) -> str:
    """去掉控制字符（保留换行和制表符）并截断不可信文本。

    Args:
        text: 原始文本。
        limit: 最多保留的字符数。

    Returns:
        处理后的文本。
    """
    cleaned = "".join(ch for ch in text if ch in "\n\t" or ord(ch) >= 32)
    return cleaned if len(cleaned) <= limit else cleaned[:limit] + "…"


def _content_block(item: Any) -> ContentBlock:
    """把一个 MCP 内容项映射成输出内容块。

    不支持的类型**不会被静默丢弃**，而是变成说明性的文本占位符，让模型知道有内容被省略：

    * 文本 → 文本块；PNG/JPEG/GIF/WebP 图片 → 图片块；其他图片格式 → 占位符；
    * 音频 → 占位符（标注格式和大小）；资源链接 → 文本（名称和地址，已截断）；
    * 内嵌资源：文本资源 → 文本块；二进制资源 → 占位符；
    * 未知类型 → 占位符。

    Args:
        item: ``CallToolResult.content`` 中的一项。

    Returns:
        内容块。
    """
    if isinstance(item, mcp_t.TextContent):
        return TextBlock(text=item.text)
    if isinstance(item, mcp_t.ImageContent):
        if item.mime_type in _IMAGE_TYPES:
            return ImageBlock(mime_type=item.mime_type, data_b64=item.data)
        return TextBlock(text=f"[MCP image omitted: unsupported type {item.mime_type[:64]}]")
    if isinstance(item, mcp_t.AudioContent):
        size = len(item.data) * 3 // 4
        return TextBlock(text=f"[MCP audio omitted: {item.mime_type[:64]}, ~{size} bytes]")
    if isinstance(item, mcp_t.ResourceLink):
        text = _truncate(f"{item.name}: {item.uri}", _MAX_LINK_TEXT)
        return TextBlock(text=f"[MCP resource link] {text}")
    if isinstance(item, mcp_t.EmbeddedResource):
        resource = item.resource
        if isinstance(resource, mcp_t.TextResourceContents):
            return TextBlock(text=resource.text)
        mime = (resource.mime_type or "unknown")[:64]
        return TextBlock(
            text=f"[MCP binary resource omitted: {mime}, {_truncate(str(resource.uri), 256)}]"
        )
    kind = _truncate(str(getattr(item, "type", type(item).__name__)), 64)
    return TextBlock(text=f"[unsupported MCP content type omitted: {kind}]")


def map_call_result(
    result: mcp_t.CallToolResult, *, expose_error_text: bool, scrub: Callable[[str], str]
) -> ToolOutput:
    """把 ``CallToolResult`` 映射成 :class:`ToolOutput`，``is_error`` 映射成统一错误。

    Args:
        result: 服务端返回的结果。
        expose_error_text: 是否在错误信息中包含服务端的错误文本（截断、清除已知密钥后）。
        scrub: 清除已知密钥值的函数。

    Returns:
        归一化后的输出；``structured_content`` 是对象时填入 ``structured``。

    Raises:
        ToolExecutionError: ``is_error`` 为 True（``mcp_tool_error``）。
        UpstreamError: 结构化结果无法作为 JSON 使用（``mcp_invalid_response``）。
    """
    if result.is_error:
        message = "the MCP tool reported an error"
        if expose_error_text:
            texts = [c.text for c in result.content if isinstance(c, mcp_t.TextContent)]
            if texts:
                message += ": " + _truncate(scrub(" ".join(texts)), _MAX_ERROR_TEXT)
        raise ToolExecutionError(message, code="mcp_tool_error")
    blocks = [_content_block(item) for item in result.content]
    structured = result.structured_content
    if structured is not None and not isinstance(structured, dict):
        structured = None
    try:
        return ToolOutput(content=blocks, structured=structured)
    except pydantic.ValidationError:
        raise UpstreamError(
            "the MCP server returned a malformed result", code="mcp_invalid_response"
        ) from None


class McpAdapter:
    """执行 :class:`McpTarget` 的适配器。"""

    kind = "mcp"

    def __init__(self, manager: McpServerManager, *, owns_manager: bool = True) -> None:
        """创建适配器。

        Args:
            manager: 服务端管理器。
            owns_manager: 关闭适配器（执行器关闭）时是否一并关闭管理器。
        """
        self._manager = manager
        self._owns_manager = owns_manager

    async def invoke(
        self,
        binding: ToolBinding,
        args: Mapping[str, Any],
        ctx: ToolContext,
        attempt: AttemptScope,
    ) -> ToolOutput:
        """调用远端 MCP 工具。

        会话由管理器按服务端的 ``session_scope`` 选择：租户范围时使用
        ``ctx.principal.tenant_id`` 对应的独立会话与凭证。**本次使用的会话**注入过的凭证会登记到
        本次调用的密钥集合中，执行器据此在日志、Span、审计和结果里做精确清除；其他租户的
        凭证不会被登记，也不会出现在这次调用里。

        Args:
            binding: 工具绑定，目标必须是 :class:`McpTarget`。
            args: 已通过 JSON Schema 校验的参数。
            ctx: 调用上下文（只用其中的租户来选择会话，不会发给服务端）。
            attempt: 本次尝试的执行范围。

        Returns:
            归一化后的输出。

        Raises:
            ToolExecutionError: 远端 ``isError`` 或要求额外输入。
            UpstreamError: 连接、协议或结果格式错误。
            PermissionDeniedError: 租户范围的服务端收到没有租户的调用。
            ConcurrencyLimitedError: 租户会话数达到上限且全部忙碌。
            ToolInfraError: 目标类型不匹配、服务端未配置或管理器已关闭。
            asyncio.CancelledError: 超时或调用方取消（SDK 会发送取消通知）。
        """
        target = binding.target
        if not isinstance(target, McpTarget):
            raise ToolInfraError("binding target is not an MCP target", code="adapter_mismatch")

        def register(values: Iterable[str]) -> None:
            """把本次会话的凭证登记到调用的密钥集合（请求结束时由管理器调用）。

            Args:
                values: 会话建立连接时注入过的密钥值。
            """
            for value in values:
                attempt.secrets.add(value)

        result = await self._manager.call_tool(
            target.server_id,
            target.remote_name,
            dict(args),
            ctx=ctx,
            on_started=attempt.mark_started,
            on_secrets=register,
        )
        cfg = self._manager.config(target.server_id)
        return map_call_result(
            result, expose_error_text=cfg.expose_error_text, scrub=attempt.secrets.scrub_text
        )

    async def aclose(self) -> None:
        """关闭适配器；``owns_manager=True`` 时同时关闭所有 MCP 连接。"""
        if self._owns_manager:
            await self._manager.aclose()
