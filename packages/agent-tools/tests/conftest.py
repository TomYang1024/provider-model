"""agent-tools 测试的公共夹具：内存中的 Span / 指标收集器、日志捕获、执行器工厂。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

import httpcore2
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_tools import ExecutorConfig, ToolExecutor, ToolRegistry


class LogCapture:
    """兼容 structlog 接口的日志器，把事件收集到内存中供断言。"""

    def __init__(self) -> None:
        """创建空的日志收集器。"""
        self.records: list[dict[str, Any]] = []

    def _log(self, level: str, event: str, **fields: Any) -> None:
        """记录一条日志。

        Args:
            level: 日志级别。
            event: 事件名。
            **fields: 日志字段。
        """
        self.records.append({"event": event, "level": level, **fields})

    def __getattr__(self, level: str) -> Callable[..., None]:
        """把 ``info`` / ``warning`` / ``error`` 等方法映射到 :meth:`_log`。

        Args:
            level: 日志级别方法名。

        Returns:
            记录该级别日志的函数。

        Raises:
            AttributeError: 不是日志级别方法。
        """
        if level in {"debug", "info", "warning", "error", "critical"}:
            return lambda event, **kw: self._log(level, event, **kw)
        raise AttributeError(level)

    def events(self, name: str | None = None) -> list[dict[str, Any]]:
        """按事件名筛选日志。

        Args:
            name: 事件名；``None`` 表示全部。

        Returns:
            日志记录列表。
        """
        return [r for r in self.records if name is None or r["event"] == name]


@dataclass
class Telemetry:
    """测试用的 OTel 组件：内存 Span 导出器、内存指标读取器和日志捕获。"""

    tracer_provider: TracerProvider
    spans: InMemorySpanExporter
    meter_provider: MeterProvider
    metrics: InMemoryMetricReader
    logs: LogCapture = field(default_factory=LogCapture)

    def finished_spans(self) -> list[ReadableSpan]:
        """返回已经结束的全部 Span。"""
        return list(self.spans.get_finished_spans())

    def metric_points(self, name: str) -> list[Any]:
        """返回指定指标的全部数据点。

        Args:
            name: 指标名。

        Returns:
            数据点列表。
        """
        data = self.metrics.get_metrics_data()
        points: list[Any] = []
        for rm in data.resource_metrics if data else []:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    if metric.name == name:
                        points.extend(metric.data.data_points)
        return points


@pytest.fixture
def telemetry() -> Telemetry:
    """提供一套独立的内存遥测组件。"""
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    return Telemetry(tracer_provider, exporter, meter_provider, reader)


@pytest.fixture
def registry() -> ToolRegistry:
    """提供一个空的工具注册表。"""
    return ToolRegistry()


@pytest.fixture
async def make_executor(
    telemetry: Telemetry,
) -> AsyncIterator[Callable[..., ToolExecutor]]:
    """提供执行器工厂；测试结束时自动关闭创建过的执行器。

    Yields:
        ``factory(registry, **kwargs)``：``kwargs`` 中的 ``config`` 等参数原样传给执行器，
        其余配置项（如 ``max_parallel``）会组装成 :class:`ExecutorConfig`。
    """
    created: list[ToolExecutor] = []
    config_fields = set(ExecutorConfig.__dataclass_fields__)

    def factory(registry: ToolRegistry, **kwargs: Any) -> ToolExecutor:
        """创建一个接入内存遥测组件的执行器。

        Args:
            registry: 工具注册表。
            **kwargs: 执行器参数或 ``ExecutorConfig`` 字段。

        Returns:
            执行器。
        """
        overrides = {k: kwargs.pop(k) for k in list(kwargs) if k in config_fields}
        if overrides:
            kwargs["config"] = ExecutorConfig(**overrides)
        kwargs.setdefault("tracer", telemetry.tracer_provider.get_tracer("test"))
        kwargs.setdefault("meter", telemetry.meter_provider.get_meter("test"))
        kwargs.setdefault("logger", telemetry.logs)
        executor = ToolExecutor(registry, **kwargs)
        created.append(executor)
        return executor

    yield factory
    for executor in created:
        await executor.aclose(grace_s=1.0)


# ---------------------------------------------------------------------------------
# HTTP 测试用的内存网络（P1）
# ---------------------------------------------------------------------------------


def http_response(
    status: int = 200,
    body: bytes = b"",
    *,
    content_type: str = "application/json",
    headers: dict[str, str] | None = None,
    chunked: bool = False,
) -> bytes:
    """构造一个完整的原始 HTTP/1.1 响应（带 ``Connection: close``，每个连接只用一次）。

    Args:
        status: 状态码。
        body: 响应体。
        content_type: ``Content-Type``。
        headers: 额外的响应头。
        chunked: 是否使用分块编码（不声明 Content-Length，用于测试流式大小限制）。

    Returns:
        原始响应字节。
    """
    lines = [f"HTTP/1.1 {status} X", f"Content-Type: {content_type}", "Connection: close"]
    for key, value in (headers or {}).items():
        lines.append(f"{key}: {value}")
    if chunked:
        lines.append("Transfer-Encoding: chunked")
        payload = b"".join(
            f"{len(body[i : i + 1024]):x}\r\n".encode() + body[i : i + 1024] + b"\r\n"
            for i in range(0, len(body), 1024)
        )
        return ("\r\n".join(lines) + "\r\n\r\n").encode() + payload + b"0\r\n\r\n"
    lines.append(f"Content-Length: {len(body)}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode() + body


class _FakeStream(httpcore2.AsyncNetworkStream):
    """内存网络流：按块返回预设的响应，记录写入的请求字节和 TLS 主机名。"""

    def __init__(self, net: FakeNetwork, response: bytes) -> None:
        """创建网络流。

        Args:
            net: 所属的内存网络。
            response: 这个连接要返回的原始响应。
        """
        self._net = net
        self._data = response
        self._index = len(net.requests)
        net.requests.append(b"")

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        """读取下一块响应字节。

        Args:
            max_bytes: 最多读取的字节数。
            timeout: 未使用。

        Returns:
            响应字节；读完后返回空字节。
        """
        chunk, self._data = self._data[:max_bytes], self._data[max_bytes:]
        return chunk

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        """记录写入的请求字节。

        Args:
            buffer: 请求字节。
            timeout: 未使用。
        """
        self._net.requests[self._index] += buffer

    async def aclose(self) -> None:
        """关闭网络流（无操作）。"""

    async def start_tls(
        self, ssl_context: Any, server_hostname: str | None = None, timeout: float | None = None
    ) -> httpcore2.AsyncNetworkStream:
        """记录 TLS 握手使用的主机名（SNI），不做真实握手。

        Args:
            ssl_context: 未使用。
            server_hostname: SNI 主机名。
            timeout: 未使用。

        Returns:
            本网络流。
        """
        self._net.tls_hostnames.append(server_hostname)
        return self

    def get_extra_info(self, info: str) -> Any:
        """不提供额外信息（没有协商 HTTP/2）。

        Args:
            info: 信息名。

        Returns:
            ``None``。
        """
        return None


class FakeNetwork(httpcore2.AsyncNetworkBackend):
    """内存网络后端：每个连接返回队列中的下一个响应，并记录连接地址。"""

    def __init__(self) -> None:
        """创建空的内存网络。"""
        self.responses: list[bytes] = []
        self.connections: list[tuple[str, int]] = []
        self.tls_hostnames: list[str | None] = []
        self.requests: list[bytes] = []
        self.fail_connect = False

    def queue(self, *responses: bytes) -> None:
        """追加预设响应。

        Args:
            *responses: 原始响应字节。
        """
        self.responses.extend(responses)

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore2.AsyncNetworkStream:
        """“建立连接”：记录地址，返回带下一个响应的网络流。

        Args:
            host: 实际连接的地址（应当是已校验的 IP）。
            port: 端口。
            timeout: 未使用。
            local_address: 未使用。
            socket_options: 未使用。

        Returns:
            内存网络流。

        Raises:
            httpcore2.ConnectError: ``fail_connect`` 为 True 时模拟连接失败。
        """
        self.connections.append((host, port))
        if self.fail_connect:
            raise httpcore2.ConnectError("connection refused")
        if not self.responses:
            raise AssertionError("unexpected connection")
        return _FakeStream(self, self.responses.pop(0))

    async def connect_unix_socket(
        self, path: str, timeout: float | None = None, socket_options: Any = None
    ) -> httpcore2.AsyncNetworkStream:
        """不支持 Unix 套接字。

        Raises:
            AssertionError: 总是抛出。
        """
        raise AssertionError("unix sockets are not used")

    async def sleep(self, seconds: float) -> None:
        """等待（委托给 asyncio）。

        Args:
            seconds: 秒数。
        """
        await asyncio.sleep(seconds)

    def request(self, index: int = -1) -> tuple[str, str, dict[str, str], bytes]:
        """解析记录下来的某个请求。

        Args:
            index: 请求序号。

        Returns:
            ``(方法, 请求目标, 小写请求头, 请求体)``。
        """
        head, _, body = self.requests[index].partition(b"\r\n\r\n")
        first, *header_lines = head.decode().split("\r\n")
        method, target, _ = first.split(" ", 2)
        headers = {}
        for line in header_lines:
            key, _, value = line.partition(":")
            headers[key.strip().lower()] = value.strip()
        return method, target, headers, body


@pytest.fixture
def fake_network() -> FakeNetwork:
    """提供一个空的内存网络。"""
    return FakeNetwork()


@pytest.fixture
def make_response() -> Callable[..., bytes]:
    """提供构造原始 HTTP 响应的函数（见 :func:`http_response`）。"""
    return http_response
