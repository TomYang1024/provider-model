"""带 OpenTelemetry CLIENT Span 的 HTTP 传输层，每个 HTTP 请求一个 Span。

这里刻意不使用通用的自动埋点（如 opentelemetry-instrumentation-httpx），因为它会向
所有出站请求注入 ``traceparent``。Provider API 属于第三方服务：默认不向其发送内部
Trace 头（可通过 ``ProviderConfig.propagate_trace_context`` 显式开启）。

Span 覆盖整个 HTTP 交互，包括流式响应体：响应流关闭时 Span 才结束，
因此 SSE 流的完整时长可以在 HTTP Span 上看到。
"""

from __future__ import annotations

import urllib.request
from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import httpx2
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

# 稳定版 HTTP 语义规范的属性名
_HTTP_METHOD = "http.request.method"
_HTTP_STATUS = "http.response.status_code"
_URL_FULL = "url.full"
_SERVER_ADDRESS = "server.address"
_SERVER_PORT = "server.port"
_ERROR_TYPE = "error.type"
_RESEND = "http.request.resend_count"


def resolve_proxy(base_url: str, *, proxy: str | None, trust_env: bool) -> str | None:
    """解析 ``base_url`` 应使用的代理地址。

    每个 Provider 初始化时解析一次，原因有二：
    1. httpx 风格的环境变量代理会以 *mount* 形式挂载独立传输层，绕过 :class:`TracingTransport`；
    2. httpx 在使用操作系统代理设置时会忽略 ``NO_PROXY``，而 urllib 的 ``proxy_bypass``
       能同时正确处理环境变量和系统代理例外规则。
    """
    # 优先级：显式配置 > 环境变量 / 系统设置 > 不使用代理
    if proxy:
        return proxy
    if not trust_env:
        return None
    parts = urlsplit(base_url)
    host = parts.hostname or ""
    if urllib.request.proxy_bypass(host):
        return None
    proxies = urllib.request.getproxies()
    return proxies.get(parts.scheme) or proxies.get("all") or None


class _SpanClosingStream(httpx2.AsyncByteStream):
    """包装响应体流：流被关闭（或读取出错）时结束 HTTP Span。"""

    def __init__(self, inner: httpx2.AsyncByteStream, span: trace.Span) -> None:
        self._inner = inner
        self._span = span
        self._ended = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._inner:
                yield chunk
        except BaseException as exc:
            # 读取响应体过程中断（网络错误、取消等）：标记错误并结束 Span
            self._end(exc)
            raise

    def _end(self, exc: BaseException | None = None) -> None:
        # 幂等：异常路径和 aclose 都可能调用
        if self._ended:
            return
        self._ended = True
        if exc is not None:
            self._span.set_attribute(_ERROR_TYPE, type(exc).__qualname__)
            self._span.set_status(Status(StatusCode.ERROR, type(exc).__qualname__))
        self._span.end()

    async def aclose(self) -> None:
        try:
            await self._inner.aclose()
        finally:
            self._end()


class TracingTransport(httpx2.AsyncBaseTransport):
    """包装另一个异步传输层，并为每个请求记录一个 HTTP CLIENT Span。"""

    def __init__(
        self,
        inner: httpx2.AsyncBaseTransport,
        *,
        tracer: trace.Tracer,
        propagate_context: bool = False,
    ) -> None:
        self._inner = inner
        self._tracer = tracer
        self._propagate = propagate_context

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        url = request.url
        # 父 Span 为当前上下文中的适配器 Span（由 CallObserver.activate 设置）
        span = self._tracer.start_span(
            request.method,
            kind=SpanKind.CLIENT,
            attributes={
                _HTTP_METHOD: request.method,
                # 绝不记录查询字符串：部分 API 允许把密钥放在 query 中
                _URL_FULL: f"{url.scheme}://{url.host}{url.path}",
                _SERVER_ADDRESS: url.host,
                _SERVER_PORT: url.port or (443 if url.scheme == "https" else 80),
            },
        )
        # openai SDK 重试时会带上重试次数请求头，记录为 resend_count
        retry = request.headers.get("x-stainless-retry-count")
        if retry and retry.isdigit() and int(retry) > 0:
            span.set_attribute(_RESEND, int(retry))
        if self._propagate:
            # 仅在显式开启时注入 traceparent / tracestate
            carrier: dict[str, str] = {}
            propagate.inject(carrier, context=trace.set_span_in_context(span))
            for key, value in carrier.items():
                request.headers[key] = value
        try:
            response = await self._inner.handle_async_request(request)
        except BaseException as exc:
            # 连接失败 / 超时 / 取消：没有响应，直接结束 Span
            span.set_attribute(_ERROR_TYPE, type(exc).__qualname__)
            span.set_status(Status(StatusCode.ERROR, type(exc).__qualname__))
            span.end()
            raise
        span.set_attribute(_HTTP_STATUS, response.status_code)
        if response.status_code >= 400:
            span.set_attribute(_ERROR_TYPE, str(response.status_code))
            span.set_status(Status(StatusCode.ERROR))
        stream = response.stream
        assert isinstance(stream, httpx2.AsyncByteStream)
        # 此时只收到响应头；用包装流替换响应体，等响应体关闭后再结束 Span
        return httpx2.Response(
            status_code=response.status_code,
            headers=response.headers,
            stream=_SpanClosingStream(stream, span),
            extensions=response.extensions,
            request=request,
        )

    async def aclose(self) -> None:
        await self._inner.aclose()
