"""通过 HTTP/SSE 暴露 SDK 的 FastAPI 应用。

路由::

    POST /v1/chat       -> ChatResponse JSON
    POST /v1/messages   -> ChatResponse JSON
    POST /v1/stream     -> StreamEvent 帧组成的 text/event-stream
    GET  /v1/providers  -> 已启用的 Provider 及配置的模型
    GET  /health/live   -> 进程存活（不检查任何依赖）
    GET  /health/ready  -> 已完成初始化且未在停机中
"""

from __future__ import annotations

import asyncio
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from provider_sdk import ChatResponse, ProviderError, ProviderSDK, RequestContext
from provider_sdk.observability import LoggingHandle, configure_logging
from provider_sdk.observability.setup import TelemetryHandle, configure_telemetry

from .schemas import ChatHTTPRequest, MessagesHTTPRequest
from .settings import GatewaySettings
from .sse import SSEResponse, sse_events

log = structlog.get_logger("provider_gateway")


@dataclass
class Lifecycle:
    """进程生命周期状态，与服务器（SIGTERM 处理）共享。"""

    ready: bool = False  # 已完成初始化
    draining: bool = False  # 正在停机：拒绝新请求
    in_flight: int = 0  # 进行中的模型调用数（含流式）

    def start_draining(self) -> None:
        self.draining = True


# 进程级单例：__main__ 中的 DrainingServer 在收到 SIGTERM 时修改它
LIFECYCLE = Lifecycle()


class GatewayError(Exception):
    """网关自身产生的错误（认证、过载、停机等），统一转换为 JSON 错误响应。"""

    def __init__(self, status: int, type_: str, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.type, self.code, self.message = status, type_, code, message


def _error_body(
    request: Request, type_: str, code: str, message: str, **extra: Any
) -> dict[str, Any]:
    return {
        "error": {
            "type": type_,
            "code": code,
            "message": message,
            "request_id": getattr(request.state, "request_id", None),
            **extra,
        }
    }


def create_app(
    settings: GatewaySettings | None = None,
    *,
    sdk: ProviderSDK | None = None,
    lifecycle: Lifecycle | None = None,
    setup_logging: bool = True,
    setup_telemetry: bool = True,
    tracer: Any | None = None,
) -> FastAPI:
    settings = settings or GatewaySettings()
    lifecycle = lifecycle or LIFECYCLE
    tokens = settings.tokens()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # 启动顺序：日志 → Telemetry → SDK → 标记 ready
        logging_handle: LoggingHandle | None = None
        telemetry: TelemetryHandle | None = None
        if setup_logging:
            logging_handle = configure_logging(
                level=settings.log_level,
                service=settings.service_name,
                queue_size=settings.log_queue_size,
            )
        if setup_telemetry:
            telemetry = configure_telemetry(settings.service_name)
        client = sdk or ProviderSDK.from_env(
            tracer=telemetry.tracer if telemetry else None,
            meter=telemetry.meter if telemetry else None,
            service_name=settings.service_name,
        )
        app.state.sdk = client
        lifecycle.ready, lifecycle.draining = True, False
        log.info(
            "gateway.started", providers=client.registry.names(), auth=not settings.auth_disabled
        )
        try:
            yield
        finally:
            # 执行到这里时，uvicorn 已停止接受新连接，并已最多等待
            # shutdown_grace_seconds 让进行中的请求完成（剩余请求已被取消）。
            lifecycle.ready, lifecycle.draining = False, True
            log.info(
                "gateway.stopping",
                in_flight=lifecycle.in_flight,
                open_streams=client.active_streams,
            )
            await asyncio.sleep(0)  # 让被取消的请求任务先执行清理逻辑（记录流终态）
            await client.aclose()  # 关闭仍未关闭的流（记为 cancelled）及 Provider 连接池
            # Telemetry 关闭可能阻塞（导出器重试），放到线程中执行并设置时间上限
            if telemetry is not None:
                flushed = await asyncio.to_thread(
                    telemetry.shutdown, settings.telemetry_flush_timeout
                )
                log.info("gateway.telemetry_flushed", ok=flushed)
            log.info("gateway.stopped")
            # 最后关闭日志队列，确保以上停机日志都已写出
            if logging_handle is not None:
                logging_handle.shutdown()

    # 只有关闭认证（本地开发）时才暴露 OpenAPI 文档
    app = FastAPI(
        title="Provider Gateway",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None if not settings.auth_disabled else "/docs",
        redoc_url=None,
        openapi_url=None if not settings.auth_disabled else "/openapi.json",
    )

    from .middleware import GatewayMiddleware

    app.add_middleware(GatewayMiddleware, tracer=tracer, max_body_bytes=settings.max_body_bytes)

    # -- 依赖项 -------------------------------------------------------------------------
    async def authenticate(request: Request) -> None:
        """校验网关令牌：支持 Bearer 和 X-API-Key 两种方式。"""
        if settings.auth_disabled:
            return
        header = request.headers.get("authorization", "")
        token = (
            header[7:].strip()
            if header.lower().startswith("bearer ")
            else (request.headers.get("x-api-key", "").strip())
        )
        # 使用常量时间比较，防止通过响应时间推测令牌内容
        if not token or not any(hmac.compare_digest(token, t) for t in tokens):
            raise GatewayError(
                401, "authentication", "unauthorized", "missing or invalid gateway API key"
            )

    def acquire_slot() -> None:
        """申请并发槽位：停机中或超出并发上限时立即返回 503（不排队，避免延迟堆积）。

        单线程事件循环中检查和自增之间没有 await，因此无需加锁。
        """
        if lifecycle.draining or not lifecycle.ready:
            raise GatewayError(503, "unavailable", "draining", "gateway is shutting down")
        if lifecycle.in_flight >= settings.max_concurrency:
            raise GatewayError(503, "unavailable", "overloaded", "too many concurrent requests")
        lifecycle.in_flight += 1

    def release_slot() -> None:
        # 流式请求的槽位在 SSE 结束时（sse_events 的 on_done）释放
        lifecycle.in_flight -= 1

    # request_id 来自中间件（X-Request-ID 或自动生成），其余上下文来自请求体
    def context_for(
        request: Request, body: ChatHTTPRequest | MessagesHTTPRequest
    ) -> RequestContext:
        return RequestContext(request_id=request.state.request_id, **body.context.model_dump())

    def options_for(body: ChatHTTPRequest | MessagesHTTPRequest, *, stream: bool) -> dict[str, Any]:
        if body.provider_options and not settings.allow_provider_options:
            raise GatewayError(
                400,
                "invalid_request",
                "provider_options_disabled",
                "provider_options are disabled on this gateway",
            )
        opts = body.sdk_options()
        # 客户端传入的超时不能超过网关配置的上限
        cap = settings.max_stream_timeout if stream else settings.max_request_timeout
        opts["timeout"] = min(body.timeout or cap, cap)
        return opts

    def sdk_of(request: Request) -> ProviderSDK:
        return request.app.state.sdk  # type: ignore[no-any-return]

    # -- 异常处理 -----------------------------------------------------------------------
    @app.exception_handler(ProviderError)
    async def provider_error(request: Request, exc: ProviderError) -> JSONResponse:
        body = {
            "error": {**exc.to_dict(), "request_id": getattr(request.state, "request_id", None)}
        }
        # 可重试的限流类错误附带 Retry-After，提示客户端退避
        headers = {"retry-after": "1"} if exc.retryable and exc.http_status in (429, 503) else None
        return JSONResponse(body, status_code=exc.http_status, headers=headers)

    @app.exception_handler(GatewayError)
    async def gateway_error(request: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            _error_body(request, exc.type, exc.code, exc.message), status_code=exc.status
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # 错误中绝不回显请求输入（可能包含 Prompt），只返回字段位置和原因
        details = [{"loc": list(e.get("loc", [])), "msg": e.get("msg")} for e in exc.errors()][:20]
        return JSONResponse(
            _error_body(
                request,
                "invalid_request",
                "validation_error",
                "invalid request body",
                details=details,
            ),
            status_code=422,
        )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        # 兜底：只记录异常类型，不向客户端暴露内部细节
        log.error("gateway.unhandled_error", error_type=type(exc).__name__)
        return JSONResponse(
            _error_body(request, "internal", "internal_error", "internal server error"),
            status_code=500,
        )

    # -- 路由 ---------------------------------------------------------------------------
    @app.get("/health/live")
    async def live() -> dict[str, str]:
        # Liveness 不依赖 Provider 或 Collector：依赖故障不应导致容器被重启
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        # Readiness：初始化完成且未在停机时才接收流量
        if lifecycle.ready and not lifecycle.draining:
            return JSONResponse({"status": "ready"})
        return JSONResponse(
            {"status": "draining" if lifecycle.draining else "starting"}, status_code=503
        )

    @app.get("/v1/providers", dependencies=[Depends(authenticate)])
    async def providers(request: Request) -> dict[str, Any]:
        reg = sdk_of(request).registry
        return {
            "providers": [
                {
                    "name": n,
                    "models": sorted(reg.config(n).models),
                    "strict_models": reg.config(n).strict_models,
                }
                for n in reg.names()
            ]
        }

    @app.post("/v1/chat", dependencies=[Depends(authenticate)])
    async def chat(request: Request, body: ChatHTTPRequest) -> ChatResponse:
        opts = options_for(body, stream=False)
        acquire_slot()
        try:
            # 客户端断开时 Starlette 会取消本任务，SDK 会把调用记为 cancelled
            return await sdk_of(request).chat(
                provider=body.provider,
                model=body.model,
                prompt=body.prompt,
                system=body.system,
                context=context_for(request, body),
                **opts,
            )
        finally:
            release_slot()

    @app.post("/v1/messages", dependencies=[Depends(authenticate)])
    async def messages(request: Request, body: MessagesHTTPRequest) -> ChatResponse:
        opts = options_for(body, stream=False)
        acquire_slot()
        try:
            return await sdk_of(request).messages(
                provider=body.provider,
                model=body.model,
                messages=body.messages,
                tools=body.tools,
                tool_choice=body.tool_choice,
                parallel_tool_calls=body.parallel_tool_calls,
                context=context_for(request, body),
                **opts,
            )
        finally:
            release_slot()

    @app.post("/v1/stream", dependencies=[Depends(authenticate)])
    async def stream(request: Request, body: MessagesHTTPRequest) -> SSEResponse:
        opts = options_for(body, stream=True)
        acquire_slot()
        try:
            chat_stream = sdk_of(request).stream(
                provider=body.provider,
                model=body.model,
                messages=body.messages,
                tools=body.tools,
                tool_choice=body.tool_choice,
                parallel_tool_calls=body.parallel_tool_calls,
                context=context_for(request, body),
                **opts,
            )
            # 先连接 Provider 再返回响应：流开始前的失败可以返回正确的 HTTP 状态码，
            # 而不是先返回 200 再发送 error 帧
            await chat_stream.start()
        except BaseException:
            # 建连失败：此时还没有 SSE 生成器接管槽位，必须在这里释放
            release_slot()
            raise
        # 建连成功后，槽位的释放交给 SSE 生成器结束时的 on_done 回调
        return SSEResponse(
            sse_events(
                chat_stream,
                ping_interval=settings.sse_ping_interval,
                on_done=release_slot,
                request_id=request.state.request_id,
            ),
            headers={"x-request-id": request.state.request_id},
        )

    return app


def app_factory() -> FastAPI:
    """``uvicorn --factory provider_gateway.app:app_factory`` 的入口。"""
    return create_app()
