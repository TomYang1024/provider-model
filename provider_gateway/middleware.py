"""纯 ASGI 中间件：提取 W3C Trace 上下文、创建 SERVER Span、处理 request_id、
限制请求体大小、输出访问日志。

使用纯 ASGI（而非 BaseHTTPMiddleware）实现，确保流式响应和客户端断开事件
能够原样透传，不会被缓冲或吞掉。
"""

from __future__ import annotations

import re
import time
import uuid
from typing import Any

import structlog
from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# 客户端传入的 X-Request-ID 只接受安全字符，否则重新生成，防止日志注入
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")
# 健康检查不创建 Span、不输出访问日志，避免探针产生大量噪声
_UNTRACED = ("/health/",)
log = structlog.get_logger("provider_gateway.access")


class BodyTooLarge(Exception):
    """流式读取请求体时超过大小限制。"""

    pass


class GatewayMiddleware:
    """网关的通用 HTTP 处理：Trace、request_id、请求体大小限制、访问日志。"""

    def __init__(
        self,
        app: ASGIApp,
        *,
        tracer: trace.Tracer | None = None,
        max_body_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        self.app = app
        self._tracer = tracer
        self.max_body_bytes = max_body_bytes

    @property
    def tracer(self) -> trace.Tracer:
        # 懒解析：TracerProvider 在 lifespan 启动阶段才安装，中间件创建时还不存在
        return self._tracer or trace.get_tracer("provider_gateway")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {
            k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])
        }
        incoming = headers.get("x-request-id", "")
        request_id = incoming if _REQUEST_ID_RE.match(incoming) else f"req_{uuid.uuid4().hex}"
        scope.setdefault("state", {})["request_id"] = request_id
        path: str = scope.get("path", "")
        method: str = scope.get("method", "GET")
        traced = not path.startswith(_UNTRACED)
        status_holder: dict[str, int] = {"status": 500}
        t0 = time.perf_counter()

        # 拦截响应开始消息：记录状态码，并在响应头中回传 X-Request-ID
        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                message.setdefault("headers", [])
                message["headers"] = [*message["headers"], (b"x-request-id", request_id.encode())]
            await send(message)

        received = 0

        # 边读边统计请求体大小：覆盖未声明 Content-Length 的分块上传
        async def receive_wrapper() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_bytes:
                    raise BodyTooLarge
            return message

        # 已声明 Content-Length 且超限：不读取请求体，直接返回 413
        declared = headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > self.max_body_bytes:
            await _plain(
                send,
                413,
                b'{"error":{"type":"invalid_request","code":'
                b'"body_too_large","message":"request body too large"}}',
            )
            return

        # 绑定到 structlog 上下文变量：本请求内的所有日志自动带上 request_id
        structlog.contextvars.bind_contextvars(request_id=request_id)
        token = None
        span: trace.Span | None = None
        try:
            if traced:
                # 从 traceparent / tracestate 请求头提取上游 Trace 上下文
                parent = propagate.extract(headers)
                route = path  # 路由集合固定，属于低基数
                span = self.tracer.start_span(
                    f"{method} {route}",
                    context=parent,
                    kind=SpanKind.SERVER,
                    attributes={
                        "http.request.method": method,
                        "url.path": path,
                        "http.route": route,
                        "url.scheme": scope.get("scheme", "http"),
                        "app.llm.request_id": request_id,
                        "user_agent.original": headers.get("user-agent", "")[:256],
                    },
                )
                # 在整个请求处理期间（包括 SSE 流式输出）把 SERVER Span 设为当前 Span，
                # SDK 的请求 Span 会自动成为它的子 Span
                token = otel_context.attach(trace.set_span_in_context(span, parent))
            try:
                await self.app(scope, receive_wrapper, send_wrapper)
            except BodyTooLarge:
                status_holder["status"] = 413
                await _plain(
                    send,
                    413,
                    b'{"error":{"type":"invalid_request","code":'
                    b'"body_too_large","message":"request body too large"}}',
                )
        except BaseException as exc:
            if span is not None:
                span.set_attribute("error.type", type(exc).__qualname__)
                span.set_status(Status(StatusCode.ERROR))
            raise
        finally:
            # 无论成功、失败还是取消，都要结束 Span、恢复上下文并输出访问日志
            status = status_holder["status"]
            if span is not None:
                span.set_attribute("http.response.status_code", status)
                if status >= 500:
                    span.set_status(Status(StatusCode.ERROR))
                span.end()
            if token is not None:
                otel_context.detach(token)
            duration_ms = round((time.perf_counter() - t0) * 1000, 2)
            fields: dict[str, Any] = {
                "method": method,
                "path": path,
                "status": status,
                "duration_ms": duration_ms,
            }
            if traced:
                log.info("http.request", **fields)
            structlog.contextvars.unbind_contextvars("request_id")


async def _plain(send: Send, status: int, body: bytes) -> None:
    """在中间件层直接返回一个 JSON 响应（不经过 FastAPI）。"""
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
