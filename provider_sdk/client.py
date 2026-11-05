"""``ProviderSDK``：对外公开的客户端，提供 ``chat()``、``messages()``、``stream()`` 三个方法。"""

from __future__ import annotations

import asyncio
import os
import weakref
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import httpx2
import pydantic
from opentelemetry import metrics, trace

from . import exceptions as errors
from .adapters.base import BaseProviderAdapter
from .config import SDKConfig
from .observability.logging import IOLogger
from .observability.metrics import SDKMetrics
from .observability.middleware import CallObserver, Instrumentation
from .observability.semconv import SCHEMA_URL
from .registry import ProviderRegistry
from .schemas.context import RequestContext
from .schemas.message import Message, ToolChoice, ToolDefinition, ToolResult
from .schemas.request import ChatRequest, Operation, ReasoningConfig, ResponseFormat
from .schemas.response import ChatResponse
from .stream import ChatStream
from .usage import estimate_usage
from .version import __version__

# messages 参数接受的元素类型：Message、ToolResult（自动转为 tool 消息）或普通 dict
MessageInput = Message | ToolResult | Mapping[str, Any]


class ProviderSDK:
    """多 Provider LLM 客户端。

    整个进程应共享同一个实例：它为每个 Provider 持有带连接池的 HTTP 客户端，
    可以安全地被大量任务并发使用。通过 ``await sdk.aclose()`` 或
    ``async with ProviderSDK(...)`` 关闭。

    Args:
        config: SDK 配置；默认使用 :meth:`SDKConfig.from_env`。
        tracer / meter: 注入的 OpenTelemetry 组件。默认通过 ``opentelemetry-api``
            获取*全局* Provider——宿主应用未配置 OTel SDK 时为空操作。
            SDK 自身绝不安装全局 Provider。
        logger: 输出 ``llm.*`` 事件的 structlog 兼容日志器。
        service_name: 日志中 ``service`` 字段的取值。
        transports: 按 Provider 注入的 ``httpx2`` 传输层（测试 / 自定义 TLS）。
    """

    def __init__(
        self,
        config: SDKConfig | None = None,
        *,
        registry: ProviderRegistry | None = None,
        tracer: trace.Tracer | None = None,
        meter: metrics.Meter | None = None,
        logger: Any | None = None,
        service_name: str | None = None,
        transports: Mapping[str, httpx2.AsyncBaseTransport] | None = None,
    ) -> None:
        self.config = config if config is not None else SDKConfig.from_env()
        # 只从 opentelemetry-api 获取 tracer / meter：未配置 SDK 时是 no-op，
        # 之后宿主应用设置全局 Provider 时，代理对象会自动生效
        self.tracer = tracer or trace.get_tracer("provider_sdk", __version__, schema_url=SCHEMA_URL)
        meter = meter or metrics.get_meter("provider_sdk", __version__, schema_url=SCHEMA_URL)
        self.registry = registry or ProviderRegistry(tracer=self.tracer)
        # 确保注册表创建的适配器（HTTP Span）与 SDK 使用同一个 tracer
        self.registry.bind_tracer(self.tracer)
        transports = transports or {}
        for provider in self.config.providers.values():
            self.registry.register(provider, transport=transports.get(provider.name), replace=True)
        service = service_name or os.environ.get("OTEL_SERVICE_NAME") or "provider-sdk"
        self.instrumentation = Instrumentation(
            tracer=self.tracer,
            metrics=SDKMetrics(meter),
            io_logger=IOLogger(self.config.logging, service=service, logger=logger),
            config=self.config.observability,
        )
        # 弱引用集合跟踪打开的流：调用方丢弃引用后不会阻止 GC，
        # 同时 aclose() 时可以兜底关闭仍未关闭的流
        self._streams: weakref.WeakSet[ChatStream] = weakref.WeakSet()
        # 后台释放连接的任务，持有强引用防止被 GC 提前回收
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False

    @classmethod
    def from_env(cls, **kwargs: Any) -> ProviderSDK:
        return cls(SDKConfig.from_env(), **kwargs)

    # ------------------------------------------------------------------------------
    # 公开 API
    # ------------------------------------------------------------------------------
    async def chat(
        self,
        provider: str,
        model: str,
        prompt: str,
        *,
        system: str | None = None,
        **options: Any,
    ) -> ChatResponse:
        """单轮对话：对 :meth:`messages` 的便捷封装。"""
        messages: list[MessageInput] = []
        if system:
            messages.append(Message(role="system", content=system))
        messages.append(Message(role="user", content=prompt))
        request = self._build(provider, model, messages, "chat", options)
        return await self.complete(request)

    async def messages(
        self,
        provider: str,
        model: str,
        messages: Sequence[MessageInput],
        *,
        tools: Sequence[ToolDefinition | Mapping[str, Any]] | None = None,
        tool_choice: ToolChoice | None = None,
        **options: Any,
    ) -> ChatResponse:
        """多轮对话，支持工具调用和工具结果回传。"""
        request = self._build(
            provider,
            model,
            messages,
            "messages",
            {**options, "tools": tools, "tool_choice": tool_choice},
        )
        return await self.complete(request)

    def stream(
        self,
        provider: str,
        model: str,
        messages: Sequence[MessageInput],
        *,
        tools: Sequence[ToolDefinition | Mapping[str, Any]] | None = None,
        tool_choice: ToolChoice | None = None,
        **options: Any,
    ) -> ChatStream:
        """返回一个 :class:`ChatStream`（首次迭代时才建立连接）。

        推荐使用 ``async with sdk.open_stream(...)``（参数相同），
        或在提前停止迭代时调用 ``aclose()``。
        """
        # 注意：这里是同步方法，参数校验错误会立即抛出，而不是延迟到第一次迭代
        request = self._build(
            provider,
            model,
            messages,
            "stream",
            {**options, "tools": tools, "tool_choice": tool_choice},
        )
        return self.stream_request(request)

    open_stream = stream
    """:meth:`stream` 的别名，用于 ``async with`` 场景。"""

    async def complete(self, request: ChatRequest) -> ChatResponse:
        """执行一个已构建好的非流式 :class:`ChatRequest`。"""
        self._ensure_open()
        if request.stream:
            raise errors.InvalidRequestError("use stream_request() for streaming requests")
        adapter = self.registry.get(request.provider)
        # 创建请求 Span + 适配器 Span，并输出 llm.request 日志
        observer = self._begin(request, adapter)
        timeout = request.timeout or adapter.config.timeouts.total
        try:
            # 在请求 Span 上下文中调用适配器，使 HTTP Span 正确挂在其下
            with observer.activate():
                # 总截止时间：覆盖 openai SDK 内部的所有重试
                async with asyncio.timeout(timeout):
                    raw = await adapter.complete(request)
        except asyncio.CancelledError:
            # 调用方取消：记录终态后原样向上传播
            observer.finish("cancelled")
            raise
        except TimeoutError as exc:
            # asyncio.timeout 抛出的是内置 TimeoutError，转换为 SDK 统一异常
            if isinstance(exc, errors.ProviderError):  # pragma: no cover - 并非内置异常子类
                raise
            deadline_err = errors.TimeoutError(
                f"request exceeded deadline of {timeout:g}s",
                provider=request.provider,
                model=request.model,
                error_code="deadline_exceeded",
            )
            observer.finish("timeout", error=deadline_err)
            raise deadline_err from exc
        except errors.ProviderError as exc:
            observer.finish(
                "timeout" if isinstance(exc, errors.TimeoutError) else "error", error=exc
            )
            raise
        except Exception as exc:
            # 兜底：任何非预期异常都映射为 ProviderError，保留异常链
            err = adapter.map_error(exc, request)
            observer.finish("error", error=err)
            raise err from exc
        response = self._finalize_response(raw, observer, request)
        observer.finish("success", response=response)
        return response

    def stream_request(self, request: ChatRequest) -> ChatStream:
        """为一个已构建好的 :class:`ChatRequest` 创建流。"""
        self._ensure_open()
        if not request.stream:
            request = request.model_copy(update={"operation": "stream"})
        return ChatStream(self, request)

    async def aclose(self) -> None:
        """关闭所有打开的流和 Provider HTTP 客户端（幂等）。"""
        if self._closed:
            return
        self._closed = True
        # 1. 关闭仍在进行中的流，记录为 cancelled（例如进程停机时）
        for stream in list(self._streams):
            await stream._close("cancelled")
        # 2. 等待后台释放连接的任务完成
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        # 3. 关闭所有 Provider 的连接池
        await self.registry.aclose()

    async def __aenter__(self) -> ProviderSDK:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @property
    def active_streams(self) -> int:
        return len(self._streams)

    # ------------------------------------------------------------------------------
    # 内部方法（供 ChatStream 使用）
    # ------------------------------------------------------------------------------
    def _ensure_open(self) -> None:
        if self._closed:
            raise errors.ProviderError("ProviderSDK is closed", error_code="client_closed")

    def _build(
        self,
        provider: str,
        model: str,
        messages: Sequence[MessageInput],
        operation: Operation,
        options: Mapping[str, Any],
    ) -> ChatRequest:
        """把便捷参数规范化为 ChatRequest，校验失败统一转为 InvalidRequestError。"""
        # 值为 None 的参数视为未设置
        opts = {k: v for k, v in options.items() if v is not None}
        context = opts.pop("context", None)
        if isinstance(context, Mapping):
            context = RequestContext.model_validate(context)
        # 便捷写法：reasoning=True / False 等价于 ReasoningConfig(enabled=...)
        reasoning = opts.pop("reasoning", None)
        if isinstance(reasoning, bool):
            reasoning = ReasoningConfig(enabled=reasoning)
        # 便捷写法：response_format="json_object"
        response_format = opts.pop("response_format", None)
        if isinstance(response_format, str):
            response_format = ResponseFormat(type=response_format)  # type: ignore[arg-type]
        try:
            normalized: list[Message] = []
            for m in messages:
                if isinstance(m, ToolResult):
                    normalized.append(m.to_message())
                elif isinstance(m, Message):
                    normalized.append(m)
                else:
                    normalized.append(Message.model_validate(m))
            return ChatRequest(
                provider=provider,
                model=model,
                messages=normalized,
                operation=operation,
                context=context or RequestContext(),
                reasoning=reasoning,
                response_format=response_format,
                **opts,
            )
        except pydantic.ValidationError as exc:
            # from None：不把包含输入内容的 pydantic 异常挂到异常链上
            raise _validation_error(exc, provider, model) from None

    def _begin(self, request: ChatRequest, adapter: BaseProviderAdapter) -> CallObserver:
        url = urlsplit(adapter.config.base_url)
        return self.instrumentation.begin(
            request,
            otel_provider_name=adapter.otel_provider_name,
            # 未登记的模型在指标中统一标记为 _other，防止标签基数爆炸
            known_model=self.registry.known_model(request.provider, request.model),
            server_address=url.hostname,
            server_port=url.port or (443 if url.scheme == "https" else 80),
        )

    def _finalize_response(
        self, response: ChatResponse, observer: CallObserver, request: ChatRequest
    ) -> ChatResponse:
        """补充请求级元数据：耗时、Trace 标识、request_id，以及可选的 Usage 估算。"""
        update: dict[str, Any] = {
            "latency_ms": round(observer.elapsed_ms, 2),
            "trace": observer.trace,
            "request_id": request.context.request_id,
        }
        # 只有显式开启时才估算，并明确标记 source="estimated"
        if response.usage.source == "unknown" and self.config.observability.estimate_usage:
            update["usage"] = estimate_usage(request.messages, response.message)
        return response.model_copy(update=update)

    def _track(self, stream: ChatStream) -> None:
        self._streams.add(stream)

    def _untrack(self, stream: ChatStream) -> None:
        self._streams.discard(stream)

    def _background(self, task: asyncio.Task[Any]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


def _validation_error(
    exc: pydantic.ValidationError, provider: str, model: str
) -> errors.InvalidRequestError:
    # include_input=False：错误信息中绝不回显 Prompt 等输入内容，只保留字段路径和原因
    details = "; ".join(
        f"{'.'.join(str(p) for p in e['loc']) or 'request'}: {e['msg']}"
        for e in exc.errors(include_input=False, include_url=False)[:10]
    )
    return errors.InvalidRequestError(
        f"invalid request: {details}",
        provider=provider,
        model=model,
        error_code="validation_error",
    )
