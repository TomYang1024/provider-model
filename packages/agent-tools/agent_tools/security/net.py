"""HTTP 工具的出站网络边界（SSRF 防护的策略部分）。

本模块只负责“判断”：URL 是否在白名单内、解析出的 IP 是否允许连接。真正在连接时执行检查、
并直接连接已校验 IP 的网络后端在 :mod:`agent_tools.adapters.http` 中（依赖 ``[http]`` extra）。

默认拒绝的地址：所有非全局单播地址，即回环、私有网段、链路本地（包括云元数据地址
``169.254.169.254``）、运营商级 NAT（``100.64.0.0/10``）、组播、保留与未指定地址；
IPv4 映射的 IPv6 地址按其中的 IPv4 地址判断。宿主可以用 ``allowed_networks`` 显式放行
特定内网网段。
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from ..protocol.errors import PermissionDeniedError, UpstreamError

__all__ = ["NetworkPolicy", "Resolver", "resolve_checked", "system_resolver"]

#: DNS 解析函数：``(主机, 端口) -> IP 字符串列表``。测试可以注入假的解析器。
Resolver = Callable[[str, int], Awaitable[list[str]]]

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


def _blocked(message: str) -> PermissionDeniedError:
    """构造“目标地址不被允许”的错误。信息中不包含解析出的 IP，避免泄露内网拓扑。

    Args:
        message: 不含地址细节的摘要。

    Returns:
        错误对象。
    """
    return PermissionDeniedError(message, code="destination_not_allowed")


@dataclass(frozen=True, slots=True)
class NetworkPolicy:
    """出站网络策略（部署配置，不来自模型）。

    Attributes:
        allowed_hosts: 允许访问的主机：精确主机名，或 ``*.example.com`` 形式的子域通配
            （通配不匹配 ``example.com`` 本身）。为空时拒绝所有 HTTP 工具。
        allowed_ports: 允许的端口；默认只有 443（启用 ``allow_http`` 时再加上 80）。
        allow_http: 是否允许明文 ``http``。
        allowed_networks: 显式放行的网段（CIDR），例如访问内网服务时使用。
    """

    allowed_hosts: frozenset[str]
    allowed_ports: frozenset[int] = frozenset({443})
    allow_http: bool = False
    allowed_networks: tuple[str, ...] = ()
    _networks: tuple[IPNetwork, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """规范化主机名，解析显式放行的网段。

        Raises:
            ValueError: 网段格式非法。
        """
        object.__setattr__(
            self, "allowed_hosts", frozenset(h.lower().rstrip(".") for h in self.allowed_hosts)
        )
        if self.allow_http and 80 not in self.allowed_ports:
            object.__setattr__(self, "allowed_ports", self.allowed_ports | {80})
        nets = tuple(ipaddress.ip_network(n, strict=False) for n in self.allowed_networks)
        object.__setattr__(self, "_networks", nets)

    def host_allowed(self, host: str) -> bool:
        """判断主机名是否在白名单内。

        Args:
            host: 主机名（不区分大小写）。

        Returns:
            在白名单内时为 True。
        """
        host = host.lower().rstrip(".")
        if host in self.allowed_hosts:
            return True
        return any(
            pattern.startswith("*.") and host.endswith(pattern[1:]) and host != pattern[2:]
            for pattern in self.allowed_hosts
        )

    def ip_allowed(self, ip: IPAddress) -> bool:
        """判断一个 IP 是否允许连接。

        Args:
            ip: IP 地址。

        Returns:
            显式放行的网段内，或者是全局单播地址时为 True。
        """
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        if any(ip in net for net in self._networks):
            return True
        return ip.is_global and not ip.is_multicast

    def check_url(self, url: str) -> tuple[str, int]:
        """检查最终请求地址的协议、主机和端口。

        Args:
            url: 完整的请求地址。

        Returns:
            ``(主机, 端口)``。

        Raises:
            PermissionDeniedError: 协议、主机或端口不被允许（``destination_not_allowed``）。
        """
        parts = urlsplit(url)
        if parts.scheme == "http" and not self.allow_http:
            raise _blocked("plain http is not allowed for this deployment")
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise _blocked("unsupported request URL")
        if parts.username or parts.password:
            raise _blocked("credentials in the URL are not allowed")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        if not self.host_allowed(parts.hostname):
            raise _blocked("destination host is not allowed")
        if port not in self.allowed_ports:
            raise _blocked("destination port is not allowed")
        return parts.hostname, port


async def system_resolver(host: str, port: int) -> list[str]:
    """使用系统 DNS 解析主机名（不经过任何代理）。

    Args:
        host: 主机名。
        port: 端口。

    Returns:
        去重后的 IP 字符串列表（保持解析顺序）。

    Raises:
        OSError: 解析失败。
    """
    infos = await asyncio.get_running_loop().getaddrinfo(
        host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
    )
    seen: dict[str, None] = {}
    for info in infos:
        seen.setdefault(str(info[4][0]), None)
    return list(seen)


async def resolve_checked(
    host: str, port: int, policy: NetworkPolicy, resolver: Resolver
) -> list[str]:
    """解析主机并检查**全部**解析结果。

    只要有一个地址不被允许，就拒绝整个请求：混入一个内网地址的 DNS 记录本身就是可疑的，
    只挑“允许的那个”连接会让攻击者有机可乘。主机本身是 IP 字面量时直接检查。

    Args:
        host: 主机名或 IP 字面量。
        port: 端口。
        policy: 网络策略。
        resolver: DNS 解析函数。

    Returns:
        全部通过检查的 IP 列表（按解析顺序）。

    Raises:
        PermissionDeniedError: 主机不在白名单内，或任一解析结果不被允许。
        UpstreamError: DNS 解析失败或没有结果（``dns_failed``，可重试）。
    """
    if not policy.host_allowed(host):
        raise _blocked("destination host is not allowed")
    try:
        literal: IPAddress | None = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        ips = [str(literal)]
    else:
        try:
            ips = await resolver(host, port)
        except OSError:
            raise UpstreamError(
                "DNS resolution failed", code="dns_failed", retryable=True
            ) from None
        if not ips:
            raise UpstreamError(
                "DNS resolution returned no address", code="dns_failed", retryable=True
            )
    for ip in ips:
        try:
            parsed = ipaddress.ip_address(ip.split("%", 1)[0])
        except ValueError:
            raise _blocked("destination address is not allowed") from None
        if not policy.ip_allowed(parsed):
            raise _blocked("destination address is not allowed")
    return ips
