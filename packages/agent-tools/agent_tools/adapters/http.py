"""HTTP 工具适配器（P1，需要 ``agent-tools[http]``）。

职责：把校验后的参数映射成 HTTP 请求（路径、查询字符串、JSON 请求体），注入密钥，
发送请求并把响应转换成 :class:`ToolOutput`，把状态码与传输错误映射成统一错误。
重试、超时、限流、鉴权、日志都由执行器负责，不在这里。

SSRF 分层防护：

1. **URL**：协议、主机、端口在注册时固定，模型只能提供参数；路径参数做百分号编码，
   拒绝 ``.`` / ``..``；最终地址再按 :class:`NetworkPolicy` 检查协议、主机白名单和端口。
2. **DNS + 实际连接地址**（默认模式）：自定义的 :class:`GuardedNetworkBackend` 在**每次建立连接时**
   自己解析域名、检查全部解析结果，然后**直接连接已经校验过的 IP**，从而避免“先检查、
   后由底层再次解析”带来的 DNS 重绑定窗口。TLS 的 SNI 与证书校验仍使用原始主机名
   （httpcore 的 ``start_tls(server_hostname=原始主机)``，已通过阅读源码确认）。
3. **重定向**：从不自动跟随，3xx 返回 ``redirect_not_followed`` 错误。
4. **代理**：不读取环境变量中的代理（``trust_env=False``）。显式配置 ``proxy`` 时，真正的连接由
   代理发起，基础设施无法在连接时检查目标 IP，只能在发送前做一次**尽力而为的预检**（解析并
   检查目标地址）；这种模式下必须依靠代理自身的访问控制，并且无法防止 DNS 重绑定。
   ``http`` / ``https`` 代理使用 :class:`ProxyOnlyNetworkBackend`：客户端**只能**与配置的代理
   地址建立 TCP 连接，任何绕过代理的直连都会被拒绝（代理地址本身是宿主配置，不经过
   ``NetworkPolicy`` 的内网地址检查）。
5. **响应**：流式读取，超过 ``max_response_bytes`` 立即中断。

实现说明与限制：连接时检查依赖替换 ``httpx2.AsyncHTTPTransport`` 内部的连接池（私有属性
``_pool``）。构造时会检查该属性存在，``httpx2`` 版本已锁定在 ``<3``，并有测试覆盖；升级
``httpx2`` 时必须重新验证。每次连接只按解析顺序尝试已校验的地址，不实现 Happy Eyeballs。
"""

from __future__ import annotations

import base64
import email.utils
import json
import ssl
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote, urlsplit

try:
    import httpcore2
    import httpx2
except ImportError as exc:  # pragma: no cover - 由打包测试覆盖
    raise ImportError(
        "agent_tools.adapters.http requires the 'http' extra: pip install 'agent-tools[http]'"
    ) from exc

from ..binding import ToolBinding
from ..protocol.context import ToolContext
from ..protocol.errors import (
    InvalidArgumentsError,
    OutputLimitError,
    PermissionDeniedError,
    ToolError,
    ToolInfraError,
    UpstreamError,
)
from ..protocol.invocation import ContentBlock, ImageBlock, JsonBlock, TextBlock, ToolOutput
from ..protocol.spec import _PLACEHOLDER_RE, HttpTarget, SecretRef
from ..security.net import NetworkPolicy, Resolver, resolve_checked, system_resolver
from ..security.secrets import SecretProvider
from .base import AttemptScope

__all__ = [
    "GuardedNetworkBackend",
    "HttpAdapter",
    "HttpAdapterConfig",
    "ProxyOnlyNetworkBackend",
    "build_request",
    "create_http_client",
]

_RETRYABLE_STATUS = {
    408: "upstream_timeout",
    425: "upstream_too_early",
    429: "upstream_rate_limited",
}
_IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp"}


@dataclass(frozen=True, slots=True)
class HttpAdapterConfig:
    """HTTP 适配器的部署配置。

    Attributes:
        network: 出站网络策略（主机白名单、端口、内网放行）。
        proxy: 显式代理地址；设置后连接时 IP 检查不可用，只做发送前预检（见模块说明）。
        max_connections: 连接池的最大连接数。
        max_keepalive_connections: 保持活动的最大空闲连接数。
        keepalive_expiry_s: 空闲连接的保持时间（秒）。
        user_agent: ``User-Agent`` 请求头。
        ca_bundle: 校验服务端证书使用的 CA 证书文件（PEM，P3）；``None`` 表示使用默认的
            系统 / certifi 信任库。用于私有 CA 签发证书的内部服务；不会关闭证书校验。
    """

    network: NetworkPolicy
    proxy: str | None = None
    max_connections: int = 32
    max_keepalive_connections: int = 8
    keepalive_expiry_s: float = 5.0
    user_agent: str = "agent-tools"
    ca_bundle: str | None = None

    @property
    def guard_mode(self) -> Literal["connect", "preflight"]:
        """当前生效的网络检查方式。

        ``connect`` 表示连接时检查；``preflight`` 表示代理模式下只做发送前预检。
        """
        return "preflight" if self.proxy else "connect"


class GuardedNetworkBackend(httpcore2.AsyncNetworkBackend):
    """在建立 TCP 连接时检查目标地址，并直接连接已校验 IP 的网络后端。"""

    def __init__(
        self,
        policy: NetworkPolicy,
        *,
        resolver: Resolver = system_resolver,
        inner: httpcore2.AsyncNetworkBackend | None = None,
    ) -> None:
        """创建网络后端。

        Args:
            policy: 出站网络策略。
            resolver: DNS 解析函数（测试可以注入）。
            inner: 实际建立连接的后端；默认 ``httpcore2.AnyIOBackend``。
        """
        self._policy = policy
        self._resolver = resolver
        self._inner = inner or httpcore2.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 - 签名由 httpcore2 规定
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore2.AsyncNetworkStream:
        """解析并检查目标地址，然后连接第一个可以连通的已校验 IP。

        Args:
            host: 请求中的主机名（不是 IP 时会被解析）。
            port: 端口。
            timeout: 连接超时。
            local_address: 本地地址。
            socket_options: 套接字选项。

        Returns:
            已建立的网络流。

        Raises:
            PermissionDeniedError: 主机或任一解析结果不被允许。
            UpstreamError: DNS 解析失败。
            httpcore2.ConnectError: 所有已校验地址都连接失败（由上层映射为传输错误）。
        """
        if port not in self._policy.allowed_ports:
            raise PermissionDeniedError(
                "destination port is not allowed", code="destination_not_allowed"
            )
        ips = await resolve_checked(host, port, self._policy, self._resolver)
        last_error: Exception | None = None
        for ip in ips:
            try:
                return await self._inner.connect_tcp(
                    ip,
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except (OSError, httpcore2.ConnectError, httpcore2.ConnectTimeout) as exc:
                last_error = exc
        assert last_error is not None  # ips 非空，循环至少执行一次
        raise last_error

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 - 签名由 httpcore2 规定
        socket_options: Any = None,
    ) -> httpcore2.AsyncNetworkStream:
        """禁止通过 Unix 套接字连接（HTTP 工具只允许访问白名单内的网络地址）。

        Args:
            path: 套接字路径。
            timeout: 超时。
            socket_options: 套接字选项。

        Raises:
            ToolInfraError: 总是抛出。
        """
        raise ToolInfraError(
            "unix socket connections are not allowed", code="destination_not_allowed"
        )

    async def sleep(self, seconds: float) -> None:
        """委托给内部后端等待（连接池内部使用）。

        Args:
            seconds: 秒数。
        """
        await self._inner.sleep(seconds)


class ProxyOnlyNetworkBackend(httpcore2.AsyncNetworkBackend):
    """代理模式的网络后端：只允许与配置的代理地址建立 TCP 连接。

    代理模式下目标地址由代理解析和连接，客户端无法检查目标 IP。这个后端保证的是另一件事：
    连接池发起的**每一条** TCP 连接都指向宿主配置的代理（主机与端口精确匹配），不会因为
    连接池内部行为或配置错误而绕过代理直连目标。代理地址是可信的部署配置，**不做**
    ``NetworkPolicy`` 的内网地址检查（代理通常就在内网）。
    """

    def __init__(
        self,
        proxy_host: str,
        proxy_port: int,
        *,
        inner: httpcore2.AsyncNetworkBackend | None = None,
    ) -> None:
        """创建后端。

        Args:
            proxy_host: 代理主机名或 IP（不区分大小写）。
            proxy_port: 代理端口。
            inner: 实际建立连接的后端；默认 ``httpcore2.AnyIOBackend``（测试可以注入）。
        """
        self._host = proxy_host.lower().rstrip(".")
        self._port = proxy_port
        self._inner = inner or httpcore2.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 - 签名由 httpcore2 规定
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore2.AsyncNetworkStream:
        """连接代理；目标不是配置的代理地址时拒绝。

        Args:
            host: 连接池要求连接的主机（代理模式下应当总是代理主机）。
            port: 端口。
            timeout: 连接超时。
            local_address: 本地地址。
            socket_options: 套接字选项。

        Returns:
            到代理的网络流。

        Raises:
            PermissionDeniedError: 连接目标不是配置的代理（``destination_not_allowed``）。
            OSError / httpcore2.ConnectError: 连接代理失败（由上层映射为传输错误）。
        """
        if (host.lower().rstrip("."), port) != (self._host, self._port):
            raise PermissionDeniedError(
                "direct connections are not allowed in proxy mode", code="destination_not_allowed"
            )
        return await self._inner.connect_tcp(
            host, port, timeout=timeout, local_address=local_address, socket_options=socket_options
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 - 签名由 httpcore2 规定
        socket_options: Any = None,
    ) -> httpcore2.AsyncNetworkStream:
        """禁止通过 Unix 套接字连接。

        Args:
            path: 套接字路径。
            timeout: 超时。
            socket_options: 套接字选项。

        Raises:
            ToolInfraError: 总是抛出。
        """
        raise ToolInfraError(
            "unix socket connections are not allowed", code="destination_not_allowed"
        )

    async def sleep(self, seconds: float) -> None:
        """委托给内部后端等待（连接池内部使用）。

        Args:
            seconds: 秒数。
        """
        await self._inner.sleep(seconds)


class _ProxyTransport(httpx2.AsyncHTTPTransport):
    """经由 ``http`` / ``https`` 代理的传输层：连接池只能连接代理地址。"""

    def __init__(
        self,
        proxy: httpx2.Proxy,
        config: HttpAdapterConfig,
        inner_backend: httpcore2.AsyncNetworkBackend | None,
    ) -> None:
        """创建传输层：沿用父类对代理参数的解析，再把连接池换成带固定后端的代理连接池。

        对目标的 TLS（``https`` 目标经 ``CONNECT`` 隧道）仍由客户端端到端完成并校验证书与
        主机名；到 ``https`` 代理本身的 TLS 使用 ``httpcore2`` 的默认信任库（与替换前相同）。

        Args:
            proxy: 代理（地址与可选的 Basic 认证）；凭证只进入 ``Proxy-Authorization``，
                只发给代理，不会发给目标服务。
            config: 适配器配置（连接池大小、目标证书校验）。
            inner_backend: 实际建立连接的后端（测试可以注入）。

        Raises:
            RuntimeError: 当前 ``httpx2`` 版本的内部结构不符合预期，无法安装固定后端。
        """
        limits = httpx2.Limits(
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
            keepalive_expiry=config.keepalive_expiry_s,
        )
        super().__init__(proxy=proxy, trust_env=False, verify=_verify(config), limits=limits)
        if not isinstance(getattr(self, "_pool", None), httpcore2.AsyncHTTPProxy):
            raise RuntimeError(
                "unsupported httpx2 internals: cannot install the proxy-only network backend"
            )
        scheme = proxy.url.scheme
        port = proxy.url.port or (443 if scheme == "https" else 80)
        self._pool = httpcore2.AsyncHTTPProxy(
            proxy_url=httpcore2.URL(
                scheme=proxy.url.raw_scheme,
                host=proxy.url.raw_host,
                port=proxy.url.port,
                target=proxy.url.raw_path,
            ),
            proxy_auth=proxy.raw_auth,
            proxy_headers=proxy.headers.raw,
            proxy_ssl_context=proxy.ssl_context,
            ssl_context=httpx2.create_ssl_context(verify=_verify(config), trust_env=False),
            max_connections=limits.max_connections,
            max_keepalive_connections=limits.max_keepalive_connections,
            keepalive_expiry=limits.keepalive_expiry,
            network_backend=ProxyOnlyNetworkBackend(proxy.url.host, port, inner=inner_backend),
        )


class _GuardedTransport(httpx2.AsyncHTTPTransport):
    """使用 :class:`GuardedNetworkBackend` 的传输层：替换父类内部的连接池。"""

    def __init__(self, backend: httpcore2.AsyncNetworkBackend, config: HttpAdapterConfig) -> None:
        """创建传输层。

        Args:
            backend: 网络后端。
            config: 适配器配置（连接池大小等）。

        Raises:
            RuntimeError: 当前 ``httpx2`` 版本的内部结构不符合预期，无法安全地启用连接时检查。
        """
        super().__init__(trust_env=False, verify=_verify(config))
        if not isinstance(getattr(self, "_pool", None), httpcore2.AsyncConnectionPool):
            raise RuntimeError(
                "unsupported httpx2 internals: cannot install the guarded network backend"
            )
        self._pool = httpcore2.AsyncConnectionPool(
            ssl_context=httpx2.create_ssl_context(verify=_verify(config), trust_env=False),
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
            keepalive_expiry=config.keepalive_expiry_s,
            network_backend=backend,
        )


def _verify(config: HttpAdapterConfig) -> ssl.SSLContext | bool:
    """返回证书校验参数：配置了 CA 文件时只信任它，否则使用默认信任库（始终校验证书与主机名）。

    Args:
        config: 适配器配置。

    Returns:
        只信任 ``ca_bundle`` 的 SSL 上下文，或 ``True``（默认信任库）。

    Raises:
        OSError / ssl.SSLError: CA 文件不存在或格式错误（在创建客户端时暴露配置错误）。
    """
    if config.ca_bundle is None:
        return True
    return ssl.create_default_context(cafile=config.ca_bundle)


def create_http_client(
    config: HttpAdapterConfig,
    *,
    resolver: Resolver = system_resolver,
    network_backend: httpcore2.AsyncNetworkBackend | None = None,
    headers: Mapping[str, str] | None = None,
    wrap_transport: Callable[[httpx2.AsyncBaseTransport], httpx2.AsyncBaseTransport] | None = None,
    proxy_auth: tuple[str, str] | None = None,
) -> httpx2.AsyncClient:
    """创建遵守出站网络策略的 ``httpx2`` 客户端（HTTP 工具与 MCP Streamable HTTP 共用）。

    * 没有配置代理时，使用 :class:`GuardedNetworkBackend`：每次建立连接都解析并检查全部
      解析结果，然后直接连接已校验的 IP；
    * 配置了代理时，使用不读取环境变量的显式代理传输层，连接时检查不可用（见模块说明）；
      ``http`` / ``https`` 代理下连接池只能连接代理地址（:class:`ProxyOnlyNetworkBackend`），
      其他代理协议（例如已安装 ``socksio`` 时的 SOCKS）沿用 ``httpx2`` 的默认代理传输层；
    * 不读取环境变量中的代理和证书配置，不自动跟随重定向，不设置客户端超时
      （超时由执行器统一控制）。

    调用方负责在不再使用时 ``await client.aclose()``。

    Args:
        config: 部署配置（网络策略、代理、连接池大小）。
        resolver: DNS 解析函数（测试可以注入）。
        network_backend: 实际建立连接的网络后端（测试可以注入），会被包在检查后端里
            （直连模式是 :class:`GuardedNetworkBackend`，代理模式是
            :class:`ProxyOnlyNetworkBackend`）。
        headers: 每个请求都带上的请求头（例如 MCP 服务端的凭证）。
        wrap_transport: 包装最终传输层的函数（P3：MCP 用它在传输层限制响应大小）；
            包装层位于网络检查之外，不能绕过连接时检查。
        proxy_auth: 代理的 Basic 认证 ``(用户名, 密码)``；只在配置了 ``config.proxy`` 时有意义，
            只放进发给代理的 ``Proxy-Authorization``，不会发给目标服务。

    Returns:
        新的客户端。

    Raises:
        RuntimeError: ``httpx2`` 内部结构不符合预期，无法启用连接时检查或固定代理后端。
        ValueError: 给了 ``proxy_auth`` 却没有配置代理。
    """
    transport: httpx2.AsyncBaseTransport
    if proxy_auth is not None and not config.proxy:
        raise ValueError("proxy_auth requires a proxy")
    if config.proxy:
        proxy = httpx2.Proxy(config.proxy, auth=proxy_auth)
        if proxy.url.scheme in ("http", "https"):
            transport = _ProxyTransport(proxy, config, network_backend)
        else:
            # 其他代理协议（SOCKS）：保持原有行为，交给 httpx2 自己的代理传输层
            transport = httpx2.AsyncHTTPTransport(
                proxy=proxy,
                trust_env=False,
                verify=_verify(config),
                limits=httpx2.Limits(
                    max_connections=config.max_connections,
                    max_keepalive_connections=config.max_keepalive_connections,
                    keepalive_expiry=config.keepalive_expiry_s,
                ),
            )
    else:
        backend = GuardedNetworkBackend(config.network, resolver=resolver, inner=network_backend)
        transport = _GuardedTransport(backend, config)
    if wrap_transport is not None:
        transport = wrap_transport(transport)
    return httpx2.AsyncClient(
        transport=transport,
        follow_redirects=False,
        trust_env=False,
        timeout=None,  # 超时由执行器的 asyncio.timeout 统一控制
        headers={"User-Agent": config.user_agent, **(headers or {})},
    )


def _scalar(value: Any) -> str:
    """把查询参数或路径参数的值转换成字符串。

    Args:
        value: 参数值（已经过 JSON Schema 校验）。

    Returns:
        字符串形式：布尔值为 ``true`` / ``false``，对象和数组为紧凑 JSON。
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def build_request(
    target: HttpTarget, args: Mapping[str, Any]
) -> tuple[str, list[tuple[str, str]], dict[str, Any] | None]:
    """把参数映射成请求地址、查询参数和 JSON 请求体。

    Args:
        target: HTTP 目标。
        args: 已通过 JSON Schema 校验的参数。

    Returns:
        ``(地址, 查询参数列表, 请求体)``；没有请求体参数时请求体为 ``None``。

    Raises:
        InvalidArgumentsError: 缺少路径参数，或路径参数为空、``.``、``..``。
        ToolInfraError: 拼出的地址与注册时的基础地址不一致（不应发生，属于防御性检查）。
    """
    default = target.default_location()
    query: list[tuple[str, str]] = []
    body: dict[str, Any] = {}
    path_values: dict[str, str] = {}
    for name, value in args.items():
        location = target.arg_mapping.get(name, default)
        if location == "path":
            text = _scalar(value)
            if text in ("", ".", ".."):
                raise InvalidArgumentsError(
                    f"path argument {name!r} must not be empty, '.' or '..'",
                    code="arguments_invalid",
                )
            # safe="" 让 "/"、"?"、"#" 等都被编码，参数无法改变路径结构
            path_values[name] = quote(text, safe="")
        elif location == "query":
            if value is None:
                continue
            items = value if isinstance(value, list) else [value]
            query.extend((name, _scalar(item)) for item in items)
        else:
            body[name] = value

    def fill(match: Any) -> str:
        """用编码后的参数替换路径占位符。

        Args:
            match: 正则匹配结果。

        Returns:
            编码后的参数值。

        Raises:
            InvalidArgumentsError: 缺少该路径参数。
        """
        key = match.group(1)
        if key not in path_values:
            raise InvalidArgumentsError(f"missing path argument {key!r}", code="arguments_invalid")
        return path_values[key]

    url = target.base_url + _PLACEHOLDER_RE.sub(fill, target.path_template)
    base, final = urlsplit(target.base_url), urlsplit(url)
    if (final.scheme, final.netloc) != (base.scheme, base.netloc) or not final.path.startswith(
        base.path
    ):
        raise ToolInfraError("request URL escaped the registered base URL", code="url_mismatch")
    return url, query, (body or None)


def _retry_after(value: str | None) -> float | None:
    """解析 ``Retry-After`` 响应头（秒数或 HTTP 日期）。

    Args:
        value: 响应头的值。

    Returns:
        需要等待的秒数（不小于 0）；无法解析时为 ``None``。
    """
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(when.timestamp() - time.time(), 0.0)


def _status_error(status: int, retry_after: float | None) -> UpstreamError:
    """把失败的状态码映射成统一错误。信息中不包含响应体。

    Args:
        status: HTTP 状态码。
        retry_after: ``Retry-After`` 解析结果。

    Returns:
        对应的 :class:`UpstreamError`。
    """
    message = f"upstream returned HTTP {status}"
    if 300 <= status < 400:
        return UpstreamError(
            message + " (redirects are not followed)",
            status_code=status,
            code="redirect_not_followed",
            retryable=False,
        )
    if status in _RETRYABLE_STATUS:
        return UpstreamError(
            message,
            status_code=status,
            code=_RETRYABLE_STATUS[status],
            retryable=True,
            retry_after=retry_after,
        )
    if status >= 500:
        return UpstreamError(
            message,
            status_code=status,
            code="upstream_server_error",
            retryable=status in (500, 502, 503, 504),
            retry_after=retry_after,
        )
    if status in (401, 403):
        return UpstreamError(
            message, status_code=status, code="upstream_auth_failed", retryable=False
        )
    if status == 404:
        return UpstreamError(
            message, status_code=status, code="upstream_not_found", retryable=False
        )
    if 400 <= status < 500:
        return UpstreamError(
            message, status_code=status, code="upstream_client_error", retryable=False
        )
    return UpstreamError(
        message, status_code=status, code="upstream_unexpected_status", retryable=False
    )


def _parse_body(content_type: str, data: bytes) -> ToolOutput:
    """把响应体转换成 :class:`ToolOutput`。

    * JSON（``application/json`` 或 ``+json``）：JSON 块；对象同时填充 ``structured``；
    * 文本（``text/*``）：文本块（按声明的字符集解码，非法字节替换）；
    * PNG / JPEG / GIF / WebP 图片：Base64 图片块；
    * 空响应：空输出；其他类型：报错（避免把任意二进制交给模型）。

    Args:
        content_type: ``Content-Type`` 响应头。
        data: 响应体。

    Returns:
        归一化后的输出。

    Raises:
        UpstreamError: 声明为 JSON 却无法解析，或内容类型不受支持（不可重试）。
    """
    if not data:
        return ToolOutput()
    mime = content_type.split(";", 1)[0].strip().lower()
    if mime == "application/json" or mime.endswith("+json"):
        try:
            value = json.loads(data)
        except (ValueError, UnicodeDecodeError):
            raise UpstreamError(
                "upstream returned invalid JSON", code="upstream_invalid_json", retryable=False
            ) from None
        return ToolOutput(
            content=[JsonBlock(data=value)], structured=value if isinstance(value, dict) else None
        )
    if mime.startswith("text/") or mime == "":
        charset = "utf-8"
        for param in content_type.split(";")[1:]:
            key, _, val = param.strip().partition("=")
            if key.lower() == "charset" and val:
                charset = val.strip('"')
        try:
            text = data.decode(charset, errors="replace")
        except LookupError:
            text = data.decode("utf-8", errors="replace")
        return ToolOutput(content=[TextBlock(text=text)])
    if mime in _IMAGE_TYPES:
        block: ContentBlock = ImageBlock(mime_type=mime, data_b64=base64.b64encode(data).decode())
        return ToolOutput(content=[block])
    raise UpstreamError(
        "upstream returned an unsupported content type",
        code="unsupported_content_type",
        retryable=False,
    )


class HttpAdapter:
    """执行 :class:`HttpTarget` 的适配器。一个适配器持有一个共享的连接池。"""

    kind = "http"

    def __init__(
        self,
        config: HttpAdapterConfig,
        *,
        secrets: SecretProvider | None = None,
        resolver: Resolver = system_resolver,
        network_backend: httpcore2.AsyncNetworkBackend | None = None,
    ) -> None:
        """创建适配器。

        Args:
            config: 部署配置。
            secrets: 密钥提供者；目标声明了 ``auth`` 时必须提供。
            resolver: DNS 解析函数（测试可以注入）。
            network_backend: 实际建立连接的网络后端（测试可以注入 ``httpcore2.AsyncMockBackend``）。
                它会被包在 :class:`GuardedNetworkBackend` 里，检查不会被绕过。
        """
        self._config = config
        self._secrets = secrets
        self._resolver = resolver
        self._client = create_http_client(
            config, resolver=resolver, network_backend=network_backend
        )

    @property
    def guard_mode(self) -> Literal["connect", "preflight"]:
        """当前生效的网络检查方式。"""
        return self._config.guard_mode

    async def invoke(
        self,
        binding: ToolBinding,
        args: Mapping[str, Any],
        ctx: ToolContext,
        attempt: AttemptScope,
    ) -> ToolOutput:
        """发送一次 HTTP 请求。

        取消与超时：执行器的 ``asyncio.timeout`` 或调用方取消会中断请求并关闭连接；
        请求可能已经被下游接收并处理，取消**不能**撤回下游的副作用。

        Args:
            binding: 工具绑定，目标必须是 :class:`HttpTarget`。
            args: 已通过 JSON Schema 校验的参数。
            ctx: 调用上下文（用于按租户获取密钥）。
            attempt: 本次尝试的执行范围（登记密钥、幂等键、开始标记）。

        Returns:
            归一化后的输出。

        Raises:
            PermissionDeniedError: 目标地址不被允许。
            InvalidArgumentsError: 路径参数非法。
            UpstreamError: 下游返回失败状态码、重定向，或传输层出错。
            OutputLimitError: 响应体超过 ``max_response_bytes``。
            ToolInfraError: 缺少密钥提供者或取不到密钥。
            asyncio.CancelledError: 超时或调用方取消（原样传播）。
        """
        target = binding.target
        if not isinstance(target, HttpTarget):
            raise ToolInfraError("binding target is not an HTTP target", code="adapter_mismatch")
        url, query, body = build_request(target, args)
        host, port = self._config.network.check_url(url)
        if self._config.proxy:
            # 代理模式：无法在连接时检查，只能尽力预检（见模块说明中的剩余风险）
            await resolve_checked(host, port, self._config.network, self._resolver)

        headers = dict(target.headers)
        if target.auth is not None:
            headers[target.auth.header] = await self._credential(target.auth, ctx, attempt)
        if target.idempotency_header and attempt.idempotency_key:
            headers["Idempotency-Key"] = attempt.idempotency_key

        params: list[tuple[str, str | int | float | bool | None]] = [*query]
        attempt.mark_started()
        try:
            async with self._client.stream(
                target.method, url, params=params, json=body, headers=headers
            ) as response:
                status = response.status_code
                if status not in target.success_status:
                    raise _status_error(status, _retry_after(response.headers.get("retry-after")))
                data = await self._read_limited(response, target.max_response_bytes)
                content_type = response.headers.get("content-type", "")
        except ToolError:
            raise
        except httpx2.TimeoutException:
            raise UpstreamError(
                "upstream request timed out", code="upstream_timeout", retryable=True
            ) from None
        except httpx2.TransportError as exc:
            raise UpstreamError(
                "could not reach the upstream service", code="connection_failed", retryable=True
            ) from exc
        except httpx2.HTTPError as exc:
            raise UpstreamError(
                "invalid upstream response", code="upstream_protocol_error", retryable=False
            ) from exc
        return _parse_body(content_type, data)

    async def aclose(self) -> None:
        """关闭共享的连接池。"""
        await self._client.aclose()

    async def _credential(self, ref: SecretRef, ctx: ToolContext, attempt: AttemptScope) -> str:
        """取得密钥、登记到本次调用的密钥集合，并生成请求头的值。

        Args:
            ref: 密钥引用。
            ctx: 调用上下文。
            attempt: 本次尝试的执行范围。

        Returns:
            请求头的值（例如 ``Bearer <secret>``）。

        Raises:
            ToolInfraError: 没有配置密钥提供者，或取不到密钥（不暴露密钥系统的错误细节）。
        """
        if self._secrets is None:
            raise ToolInfraError("no secret provider is configured", code="secret_unavailable")
        try:
            secret = await self._secrets.get_secret(ref.name, ctx)
        except Exception:
            raise ToolInfraError("credential is unavailable", code="secret_unavailable") from None
        if "\r" in secret or "\n" in secret:
            raise ToolInfraError("credential is malformed", code="secret_unavailable")
        attempt.secrets.add(secret)
        value = f"{ref.scheme} {secret}" if ref.scheme else secret
        attempt.secrets.add(value)
        return value

    @staticmethod
    async def _read_limited(response: httpx2.Response, limit: int) -> bytes:
        """流式读取响应体，超过上限立即中断。

        Args:
            response: 流式响应。
            limit: 上限（字节）。

        Returns:
            响应体。

        Raises:
            OutputLimitError: 声明的长度或实际读取的长度超过上限。
        """
        declared = response.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > limit:
            raise OutputLimitError(f"upstream response is larger than {limit} bytes")
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > limit:
                raise OutputLimitError(f"upstream response is larger than {limit} bytes")
            chunks.append(chunk)
        return b"".join(chunks)
