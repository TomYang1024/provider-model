"""可观测性中间件：每次模型调用对应一个 :class:`CallObserver`。

负责 Span 生命周期（SDK 请求 Span + 适配器 Span）、指标和 ``llm.*`` 输入输出日志，
使适配器中不包含任何可观测性代码。所有钩子都有异常保护：Telemetry 内部的失败
会被吞掉并计数，绝不会改变模型调用的结果。
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any, Literal

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from .. import exceptions as errors
from ..config import ObservabilityConfig
from ..schemas.context import TraceMetadata
from ..schemas.request import ChatRequest
from ..schemas.response import ChatResponse
from ..schemas.stream import CONTENT_EVENT_TYPES, StreamEvent
from . import semconv as sc
from .logging import IOLogger
from .metrics import SDKMetrics
from .redaction import sanitize_text

# 调用的五种终态
Outcome = Literal["success", "error", "timeout", "cancelled", "closed_early"]

# (是否流式, 终态) -> (日志级别, 日志事件名)
_LOG_EVENT: dict[tuple[bool, str], tuple[str, str]] = {
    (False, "success"): ("info", "llm.response"),
    (False, "error"): ("error", "llm.error"),
    (False, "timeout"): ("error", "llm.error"),
    (False, "cancelled"): ("warning", "llm.error"),
    (True, "success"): ("info", "llm.stream.completed"),
    (True, "error"): ("error", "llm.error"),
    (True, "timeout"): ("error", "llm.error"),
    (True, "cancelled"): ("warning", "llm.stream.cancelled"),
    (True, "closed_early"): ("warning", "llm.stream.partial"),
}

# 未登记模型在指标中的统一标签值：防止调用方随意传入模型 ID 造成标签基数爆炸
_UNLISTED_MODEL = "_other"


class Instrumentation:
    """进程级的可观测性组件集合（tracer、指标、日志器），由 ProviderSDK 持有。"""

    def __init__(
        self,
        *,
        tracer: trace.Tracer,
        metrics: SDKMetrics,
        io_logger: IOLogger,
        config: ObservabilityConfig,
    ) -> None:
        self.tracer = tracer
        self.metrics = metrics
        self.io = io_logger
        self.config = config
        #: Telemetry 内部失败次数（被吞掉的异常），便于排查
        self.failures = 0

    def begin(
        self,
        request: ChatRequest,
        *,
        otel_provider_name: str,
        known_model: bool,
        server_address: str | None = None,
        server_port: int | None = None,
    ) -> CallObserver:
        return CallObserver(
            self,
            request,
            otel_provider_name=otel_provider_name,
            model_label=request.model if known_model else _UNLISTED_MODEL,
            server_address=server_address,
            server_port=server_port,
        )


class CallObserver:
    """单次调用的观察者：创建时开始 Span，``finish()`` 时记录终态（只记录一次）。"""

    def __init__(
        self,
        inst: Instrumentation,
        request: ChatRequest,
        *,
        otel_provider_name: str,
        model_label: str,
        server_address: str | None,
        server_port: int | None,
    ) -> None:
        self._inst = inst
        self.request = request
        self._t0 = time.perf_counter()
        self._first_content_at: float | None = None  # 首个内容事件时间，用于 TTFT
        self._events = 0  # 已产生的流事件数
        self._finished = False  # 保证终态只记录一次
        self._stream_registered = False  # 是否已计入活跃流数量
        # 指标标签：只包含低基数字段
        self.metric_attrs = {
            sc.GEN_AI_OPERATION_NAME: sc.OPERATION_CHAT,
            sc.GEN_AI_PROVIDER_NAME: otel_provider_name,
            sc.GEN_AI_REQUEST_MODEL: model_label,
            sc.APP_PROVIDER: request.provider,
            sc.APP_OPERATION: request.operation,
        }
        # 成功日志的采样决策在请求开始时确定一次，保证 llm.request 与 llm.response 成对出现；
        # 错误日志不受采样影响，总是记录
        self._log_sampled = random.random() < inst.io.config.sample_rate
        # Span 属性：可以包含 request_id 等高基数字段（Span 不是指标）
        attrs: dict[str, Any] = {
            sc.GEN_AI_OPERATION_NAME: sc.OPERATION_CHAT,
            sc.GEN_AI_PROVIDER_NAME: otel_provider_name,
            sc.GEN_AI_REQUEST_MODEL: request.model,
            sc.APP_PROVIDER: request.provider,
            sc.APP_OPERATION: request.operation,
            sc.APP_REQUEST_ID: request.context.request_id,
            sc.APP_STREAM: request.stream,
            sc.GEN_AI_REQUEST_STREAM: request.stream,
        }
        for attr, value in (
            (sc.GEN_AI_REQUEST_MAX_TOKENS, request.max_tokens),
            (sc.GEN_AI_REQUEST_TEMPERATURE, request.temperature),
            (sc.GEN_AI_REQUEST_TOP_P, request.top_p),
            (sc.GEN_AI_REQUEST_SEED, request.seed),
            (sc.SERVER_ADDRESS, server_address),
            (sc.SERVER_PORT, server_port),
        ):
            if value is not None:
                attrs[attr] = value
        # 默认为无效 Span：即使创建 Span 失败，后续调用也是安全的空操作
        self.span: trace.Span = trace.INVALID_SPAN
        self.adapter_span: trace.Span = trace.INVALID_SPAN
        try:
            # 父 Span 取调用方当前上下文中的 Span
            # （网关从 W3C traceparent 提取，或宿主应用自己创建的 Span）
            self.span = inst.tracer.start_span(
                f"{sc.OPERATION_CHAT} {request.model}", kind=SpanKind.CLIENT, attributes=attrs
            )
            # 适配器 Span 显式挂在请求 Span 之下
            self.adapter_span = inst.tracer.start_span(
                f"provider_sdk.adapter {request.provider}",
                kind=SpanKind.INTERNAL,
                context=trace.set_span_in_context(self.span),
                attributes={sc.APP_PROVIDER: request.provider},
            )
            # 默认不把 Prompt 写入 Span；只有显式开启时才写入脱敏后的内容
            if inst.config.capture_content_in_spans:
                self.span.set_attribute(sc.GEN_AI_INPUT_MESSAGES, self._content_json())
        except Exception:
            inst.failures += 1
        self._log_request()

    # -- 上下文 -------------------------------------------------------------------------
    @property
    def trace(self) -> TraceMetadata:
        """请求 Span 的 W3C 标识；Trace 未启用时字段为空。"""
        ctx = self.span.get_span_context()
        if not ctx.is_valid:
            return TraceMetadata()
        return TraceMetadata(
            trace_id=format(ctx.trace_id, "032x"),
            span_id=format(ctx.span_id, "016x"),
            sampled=ctx.trace_flags.sampled,
        )

    @contextmanager
    def activate(self, *, adapter: bool = True) -> Iterator[None]:
        """在包裹的 await 期间，把请求 Span（和适配器 Span）设为当前 Span。

        attach / detach 发生在同一次 ``__anext__`` 或调用内部，
        因此上下文 token 总是在创建它的上下文中被重置，不会出现跨上下文 detach 的错误。
        """
        with ExitStack() as stack:
            token = otel_context.attach(trace.set_span_in_context(self.span))
            stack.callback(otel_context.detach, token)
            if adapter:
                token2 = otel_context.attach(trace.set_span_in_context(self.adapter_span))
                stack.callback(otel_context.detach, token2)
            yield

    @property
    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self._t0) * 1000

    @property
    def ttft_ms(self) -> float | None:
        if self._first_content_at is None:
            return None
        return (self._first_content_at - self._t0) * 1000

    # -- 流式钩子 -----------------------------------------------------------------------
    def stream_connected(self) -> None:
        """Provider 已接受流式请求：记录事件，并计入活跃流数量。"""

        def run() -> None:
            self.span.add_event("app.llm.stream.connected")
            self._inst.metrics.stream_opened(self._stream_attrs)
            self._stream_registered = True

        self._guard(run)

    @property
    def _stream_attrs(self) -> dict[str, str]:
        # 活跃流计数器的标签只保留 provider 与 model，开启和关闭时必须完全一致
        return {
            k: v
            for k, v in self.metric_attrs.items()
            if k in (sc.GEN_AI_PROVIDER_NAME, sc.GEN_AI_REQUEST_MODEL, sc.APP_PROVIDER)
        }

    def stream_event(self, event: StreamEvent) -> None:
        self._events += 1
        # 第一个内容事件（文本 / 推理 / 工具调用）的时间即 TTFT
        if self._first_content_at is None and event.type in CONTENT_EVENT_TYPES:
            self._first_content_at = time.perf_counter()

            def run() -> None:
                ttft_s = self._first_content_at - self._t0  # type: ignore[operator]
                self.span.add_event("app.llm.first_token")
                self.span.set_attribute(sc.GEN_AI_RESPONSE_TIME_TO_FIRST_CHUNK, ttft_s)
                self.span.set_attribute(sc.APP_TTFT_MS, round(ttft_s * 1000, 2))
                self._inst.metrics.record_ttft(self.metric_attrs, ttft_s)

            self._guard(run)

    # -- 终态 ---------------------------------------------------------------------------
    def finish(
        self,
        outcome: Outcome,
        *,
        response: ChatResponse | None = None,
        error: BaseException | None = None,
        partial_output: bool = False,
    ) -> None:
        """记录终态，保证只记录一次（重复调用直接忽略）。"""
        if self._finished:
            return
        self._finished = True
        duration_ms = self.elapsed_ms
        error_type, error_code, retryable = _describe_error(outcome, error)
        usage = response.usage if response is not None else None

        def spans() -> None:
            span = self.span
            span.set_attribute(sc.APP_OUTCOME, outcome)
            if self.request.stream:
                span.set_attribute(sc.APP_STREAM_EVENTS, self._events)
                span.set_attribute(sc.APP_PARTIAL_OUTPUT, partial_output)
            if response is not None:
                if response.id:
                    span.set_attribute(sc.GEN_AI_RESPONSE_ID, response.id)
                if response.response_model:
                    span.set_attribute(sc.GEN_AI_RESPONSE_MODEL, response.response_model)
                span.set_attribute(
                    sc.GEN_AI_RESPONSE_FINISH_REASONS,
                    [response.raw_finish_reason or response.finish_reason],
                )
                span.set_attribute(sc.APP_USAGE_SOURCE, response.usage.source)
                for attr, value in (
                    (sc.GEN_AI_USAGE_INPUT_TOKENS, response.usage.input_tokens),
                    (sc.GEN_AI_USAGE_OUTPUT_TOKENS, response.usage.output_tokens),
                    (sc.GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS, response.usage.cached_input_tokens),
                    (sc.GEN_AI_USAGE_REASONING_OUTPUT_TOKENS, response.usage.reasoning_tokens),
                ):
                    if value is not None:
                        span.set_attribute(attr, value)
                if self._inst.config.capture_content_in_spans:
                    span.set_attribute(sc.GEN_AI_OUTPUT_MESSAGES, self._content_json(response))
            if outcome == "success":
                span.set_status(Status(StatusCode.OK))
            elif outcome in ("error", "timeout"):
                span.set_attribute(sc.ERROR_TYPE, error_type or "error")
                if error_code:
                    span.set_attribute(sc.APP_ERROR_CODE, error_code)
                span.set_attribute(sc.APP_RETRYABLE, bool(retryable))
                # Span 状态描述只使用脱敏后的安全摘要
                description = (
                    error.safe_message if isinstance(error, errors.ProviderError) else error_type
                )
                span.set_status(Status(StatusCode.ERROR, description))
                if error is not None and isinstance(error, errors.ProviderError):
                    # 手动记录 exception 事件：不使用 record_exception，避免写入完整堆栈
                    span.add_event(
                        "exception",
                        {
                            "exception.type": type(error).__name__,
                            "exception.message": error.safe_message,
                        },
                    )
            else:
                # cancelled / closed_early：不是 Provider 故障，但明确*不是*成功
                # （状态保持 UNSET，由 outcome 和 error.type 说明原因）
                span.set_attribute(sc.ERROR_TYPE, error_type or outcome)
            self.adapter_span.set_attribute(sc.APP_OUTCOME, outcome)
            if outcome in ("error", "timeout"):
                self.adapter_span.set_status(Status(StatusCode.ERROR))
            # 先结束子 Span，再结束父 Span
            self.adapter_span.end()
            span.end()

        def metrics() -> None:
            # Token 指标只统计 Provider 返回的真实值，不混入估算值
            provider_usage = usage is not None and usage.source == "provider"
            self._inst.metrics.record_call(
                self.metric_attrs,
                status=outcome,
                error_type=error_type,
                duration_s=duration_ms / 1000,
                input_tokens=usage.input_tokens if provider_usage and usage else None,
                output_tokens=usage.output_tokens if provider_usage and usage else None,
            )
            if self._stream_registered:
                self._inst.metrics.stream_closed(self._stream_attrs)

        def logs() -> None:
            level, event = _LOG_EVENT.get((self.request.stream, outcome), ("info", "llm.response"))
            # 只有成功日志受采样影响；失败、取消、提前关闭总是记录
            if outcome == "success" and not self._log_sampled:
                return
            fields = self._common_fields()
            fields.update(
                status=outcome,
                duration_ms=round(duration_ms, 2),
                ttft_ms=round(self.ttft_ms, 2) if self.ttft_ms is not None else None,
                error_type=error_type,
                error_code=error_code,
                retryable=retryable,
                error_message=error.safe_message
                if isinstance(error, errors.ProviderError)
                else None,
            )
            if self.request.stream:
                fields["stream_events"] = self._events
                fields["partial_output"] = partial_output
            if response is not None:
                fields.update(
                    input_tokens=response.usage.input_tokens,
                    output_tokens=response.usage.output_tokens,
                    reasoning_tokens=response.usage.reasoning_tokens,
                    usage_source=response.usage.source,
                    finish_reason=response.finish_reason,
                    response_model=response.response_model,
                    # 输出内容是否记录取决于日志模式（默认 metadata_only 不记录）
                    output=self._inst.io.output_payload(response.message),
                )
            self._inst.io.emit(level, event, fields)

        # 三部分相互隔离：任何一部分失败都不影响其余部分。
        # 日志放在最前：它读取 Span 上下文（Span 结束后标识依然有效）。
        self._guard(logs)
        self._guard(metrics)
        self._guard(spans)

    # -- 辅助方法 -----------------------------------------------------------------------
    def _common_fields(self) -> dict[str, Any]:
        """所有 llm.* 日志事件的公共字段；trace_id / span_id 与 SDK 请求 Span 关联。"""
        t = self.trace
        ctx = self.request.context
        return {
            "trace_id": t.trace_id,
            "span_id": t.span_id,
            "request_id": ctx.request_id,
            "provider": self.request.provider,
            "model": self.request.model,
            "operation": self.request.operation,
            "user_id": ctx.user_id,
            "tenant_id": ctx.tenant_id,
            "session_id": ctx.session_id,
            "tags": ctx.tags or None,
        }

    def _log_request(self) -> None:
        if not self._log_sampled:
            return

        def run() -> None:
            fields = self._common_fields()
            fields.update(
                status="started",
                messages=len(self.request.messages),
                tools=len(self.request.tools or []),
                reasoning=self.request.reasoning.enabled if self.request.reasoning else None,
                input=self._inst.io.input_payload(self.request.messages),
            )
            self._inst.io.emit("info", "llm.request", fields)

        self._guard(run)

    def _content_json(self, response: ChatResponse | None = None) -> str:
        """生成写入 Span 的内容（已脱敏、截断）；仅在 capture_content_in_spans 开启时使用。"""
        limit = self._inst.io.config.max_field_chars
        if response is None:
            data: Any = [
                {"role": m.role, "content": sanitize_text(m.text, max_chars=limit)}
                for m in self.request.messages[-self._inst.io.config.max_messages :]
            ]
        else:
            data = [
                {
                    "role": "assistant",
                    "content": sanitize_text(response.text, max_chars=limit),
                    "tool_calls": [tc.name for tc in response.tool_calls],
                }
            ]
        return json.dumps(data, ensure_ascii=False)

    def _guard(self, fn: Any) -> None:
        """执行 Telemetry 操作并吞掉异常：可观测性故障不能影响业务调用。"""
        try:
            fn()
        except Exception:
            self._inst.failures += 1


def _describe_error(
    outcome: Outcome, error: BaseException | None
) -> tuple[str | None, str | None, bool | None]:
    """返回 (error_type, error_code, retryable)，用于 Span / 指标 / 日志。"""
    if outcome == "cancelled":
        return "cancelled", None, None
    if outcome == "closed_early":
        return "closed_early", None, None
    if error is None:
        return None, None, None
    if isinstance(error, errors.ProviderError):
        return error.error_type, error.error_code, error.retryable
    return type(error).__name__, None, None
