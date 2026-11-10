"""工具调用的可观测性：Span、指标和元数据日志。

* 每次调用一个 ``execute_tool {alias}`` Span（INTERNAL，父级为当前上下文），调用期间它是
  当前 Span，所以工具代码（包括线程中的同步工具）产生的 Span 会挂在它下面；
* 默认日志模式为 ``metadata``：只记录工具名、状态、耗时、参数和输出的字节数与哈希，
  **不记录参数和输出内容**；``off`` 关闭日志；
* ``redacted`` / ``full`` 模式会在结束日志和 Span（``gen_ai.tool.call.arguments`` /
  ``gen_ai.tool.call.result``）中记录参数与输出的**预览**：先精确清除本次调用的已知密钥值，
  再按凭证格式清除（``redacted`` 还会清除个人信息），最后截断（1024 / 8192 字符）。
  规则脱敏只是尽力而为，不能保证识别所有敏感内容；这两种模式需要宿主显式开启；
* 所有钩子都有异常保护：遥测内部的 ``Exception`` 会被吞掉并计数，不改变调用结果；
  ``BaseException``（例如 ``asyncio.CancelledError``）不会被吞掉。
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from contextvars import Token
from typing import Any, Literal

import structlog
from opentelemetry import context as otel_context
from opentelemetry import metrics, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from ..protocol.context import ToolContext
from ..protocol.invocation import ToolExecutionResult, ToolInvocation
from ..version import __version__
from . import semconv as sc
from .redaction import preview

__all__ = ["CallObservation", "LogMode", "ToolInstrumentation"]

#: 日志模式：``off`` 不记录；``metadata`` 只记录元数据（默认）；``redacted`` / ``full``
#: 额外记录经过脱敏和截断的参数与输出预览。
LogMode = Literal["off", "metadata", "redacted", "full"]

#: 内容预览的最大字符数。
_PREVIEW_CHARS: dict[str, int] = {"redacted": 1024, "full": 8192}

_DURATION_BUCKETS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300]


class ToolInstrumentation:
    """进程级的可观测性组件（tracer、指标、日志器），由执行器持有。"""

    def __init__(
        self,
        *,
        tracer: trace.Tracer | None = None,
        meter: metrics.Meter | None = None,
        logger: Any | None = None,
        log_mode: LogMode = "metadata",
    ) -> None:
        """创建可观测性组件。

        只依赖 ``opentelemetry-api``：没有配置 OTel SDK 时 tracer 和 meter 都是 no-op。

        Args:
            tracer: OTel tracer；不传时使用全局 tracer。
            meter: OTel meter；不传时使用全局 meter。
            logger: 兼容 structlog 接口（``info`` / ``warning`` / ``error``）的日志器。
            log_mode: 日志模式。
        """
        self.tracer = tracer or trace.get_tracer(
            "agent_tools", __version__, schema_url=sc.SCHEMA_URL
        )
        meter = meter or metrics.get_meter("agent_tools", __version__, schema_url=sc.SCHEMA_URL)
        self.logger = logger or structlog.get_logger("agent_tools")
        self.log_mode = log_mode
        #: 遥测内部失败（被吞掉的异常）的次数，便于排查。
        self.failures = 0
        self.audit_failures = meter.create_counter(
            sc.METRIC_AUDIT_FAILURES, unit="{error}", description="Audit write failures"
        )
        self.duration = meter.create_histogram(
            sc.METRIC_DURATION,
            unit="s",
            description="Tool call duration",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self.calls = meter.create_counter(
            sc.METRIC_CALLS, unit="{call}", description="Tool calls by outcome"
        )
        self.inflight = meter.create_up_down_counter(
            sc.METRIC_INFLIGHT, unit="{call}", description="Tool calls in progress"
        )
        self.orphaned = meter.create_up_down_counter(
            sc.METRIC_SYNC_ORPHANED,
            unit="{thread}",
            description="Sync tool threads still running after timeout or cancellation",
        )
        self.retries = meter.create_counter(
            sc.METRIC_RETRIES, unit="{retry}", description="Automatic tool call retries"
        )
        self.telemetry_failures = meter.create_counter(
            sc.METRIC_TELEMETRY_FAILURES,
            unit="{error}",
            description="Internal telemetry failures swallowed by agent_tools",
        )

    @property
    def records_content(self) -> bool:
        """当前日志模式是否记录参数与输出的预览（``redacted`` / ``full``）。"""
        return self.log_mode in _PREVIEW_CHARS

    def begin(self, invocation: ToolInvocation, ctx: ToolContext) -> CallObservation:
        """开始观察一次调用：创建 Span 并设为当前 Span，记录开始日志。

        Args:
            invocation: 调用请求。
            ctx: 调用上下文。

        Returns:
            本次调用的观察者；必须在同一个任务中调用 ``finish`` 或 ``cancelled``。
        """
        return CallObservation(self, invocation, ctx)

    def orphan_delta(self, delta: int) -> None:
        """更新同步工具孤儿线程数指标（由线程池回调，运行在事件循环线程）。

        Args:
            delta: 变化量（+1 / -1）。
        """
        self.safe(lambda: self.orphaned.add(delta))

    def orphan_error(self, exc: BaseException) -> None:
        """记录孤儿线程最终抛出的异常（结果已被丢弃）。只记录异常类型，不记录消息。

        Args:
            exc: 线程抛出的异常。
        """
        self.log_safe("warning", "tool.sync.orphan_failed", {"exc_type": type(exc).__name__})

    def log(self, level: str, event: str, fields: Mapping[str, Any]) -> None:
        """按日志模式输出一条日志。

        Args:
            level: 日志级别方法名（``info`` / ``warning`` / ``error``）。
            event: 事件名。
            fields: 字段（调用方保证只包含元数据）。
        """
        if self.log_mode == "off":
            return
        getattr(self.logger, level)(event, **fields)

    def log_safe(self, level: str, event: str, fields: Mapping[str, Any]) -> None:
        """输出一条日志，日志器自身出错时吞掉并计数（不影响调用结果）。

        Args:
            level: 日志级别方法名。
            event: 事件名。
            fields: 字段（调用方保证只包含元数据）。
        """
        self.safe(lambda: self.log(level, event, fields))

    def safe(self, fn: Callable[[], object]) -> None:
        """执行一个遥测操作，吞掉并计数其中的 ``Exception``。

        ``BaseException``（例如取消）不会被捕获，保证取消信号不会丢失。

        Args:
            fn: 遥测操作。
        """
        try:
            fn()
        except Exception:
            self.failures += 1
            # 计数器本身失败时只能放弃
            with contextlib.suppress(Exception):
                self.telemetry_failures.add(1)


class CallObservation:
    """单次调用的观察者。所有方法都有异常保护，终态只记录一次。"""

    def __init__(
        self, inst: ToolInstrumentation, invocation: ToolInvocation, ctx: ToolContext
    ) -> None:
        """开始 Span、增加在途计数并记录开始日志。

        Args:
            inst: 进程级可观测性组件。
            invocation: 调用请求。
            ctx: 调用上下文。
        """
        self._inst = inst
        self._t0 = time.perf_counter()
        self._finished = False
        self._span: trace.Span = trace.INVALID_SPAN
        self._token: Token[otel_context.Context] | None = None
        self._ctx = ctx
        self._call_id = invocation.call_id
        # 名字来自模型输出：解析成功前只在日志里记录截断后的值，指标里统一记为 _other
        self._tool_ref = invocation.tool[:64]
        self._alias: str | None = None
        self._name: str | None = None
        self._version: str | None = None
        self._kind: str | None = None
        self.args_bytes = 0
        self.args_sha256: str | None = None
        # 只在 redacted / full 模式下保存，用于结束时生成预览
        self._args_text: str | None = None
        # 精确清除本次调用已知密钥值的函数；默认不做替换
        self._scrub: Callable[[str], str] = _identity
        inst.safe(self._start)

    # -- 生命周期 ------------------------------------------------------------------
    def _start(self) -> None:
        """创建 Span 并设为当前 Span，增加在途计数，记录开始日志。"""
        span = self._inst.tracer.start_span(
            sc.OPERATION_EXECUTE_TOOL,
            kind=SpanKind.INTERNAL,
            attributes={
                sc.GEN_AI_OPERATION_NAME: sc.OPERATION_EXECUTE_TOOL,
                sc.GEN_AI_TOOL_CALL_ID: self._call_id,
                sc.APP_REQUEST_ID: self._ctx.request_id,
            },
        )
        self._span = span
        self._token = otel_context.attach(trace.set_span_in_context(span))
        self._inst.inflight.add(1)
        self._inst.log("info", "tool.call.start", self._fields())

    def set_arguments(self, size: int, raw: Mapping[str, Any] | str) -> None:
        """记录参数的大小和哈希；只有 ``redacted`` / ``full`` 模式才保存内容用于结束时的预览。

        Args:
            size: 参数字节数。
            raw: 原始参数。
        """

        def apply() -> None:
            """计算哈希，按日志模式保存参数文本。"""
            self.args_bytes = size
            data = raw if isinstance(raw, str) else repr(sorted(raw.items()))
            self.args_sha256 = hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]
            if self._inst.records_content:
                self._args_text = (
                    raw
                    if isinstance(raw, str)
                    else json.dumps(raw, ensure_ascii=False, default=str, sort_keys=True)
                )

        self._inst.safe(apply)

    def set_scrubber(self, scrub: Callable[[str], str]) -> None:
        """设置精确清除本次调用已知密钥值的函数，在生成任何内容预览之前调用。

        Args:
            scrub: 输入文本、返回替换后文本的函数（通常是 ``CallSecrets.scrub_text``）。
        """
        self._scrub = scrub

    def retrying(self, *, attempt: int, error_type: str, delay_s: float) -> None:
        """记录一次自动重试：Span 事件、重试计数指标和日志（只有元数据）。

        Args:
            attempt: 即将开始的尝试序号（从 2 开始）。
            error_type: 上一次尝试的错误类型。
            delay_s: 重试前的退避等待（秒）。
        """

        def apply() -> None:
            """写入重试的 Span 事件、计数指标和元数据日志（不含参数或错误详情）。"""
            self._span.add_event(
                "retry", {"attempt": attempt, "error.type": error_type, "delay_s": delay_s}
            )
            self._inst.retries.add(
                1,
                {
                    sc.GEN_AI_TOOL_NAME: self._alias or sc.UNRESOLVED_TOOL,
                    sc.ERROR_TYPE: error_type,
                },
            )
            fields = self._fields()
            fields.update(attempt=attempt, error_type=error_type, delay_s=round(delay_s, 3))
            self._inst.log("warning", "tool.call.retry", fields)

        self._inst.safe(apply)

    def event(self, name: str, attributes: Mapping[str, str | int | float | bool]) -> None:
        """记录执行链中的一个步骤（P3：审批、熔断、结果复用、超大输出处理）。

        写入一个 Span 事件和一条 ``tool.call.<name>`` 日志。调用方只能传入**低基数的元数据**
        （状态、错误码、耗时等），不得传入参数、输出或审批理由等内容。遥测失败会被吞掉并计数。

        Args:
            name: 事件名，例如 ``approval``、``circuit_rejected``、``reused``。
            attributes: 事件属性。
        """

        def apply() -> None:
            """写入 Span 事件和元数据日志。"""
            self._span.add_event(name, dict(attributes))
            fields = self._fields()
            fields.update(attributes)
            self._inst.log("info", f"tool.call.{name}", fields)

        self._inst.safe(apply)

    def resolved(self, *, alias: str, name: str, version: str, kind: str) -> None:
        """工具解析成功后补充 Span 名称和属性。

        Args:
            alias: 模型侧别名。
            name: 内部名。
            version: 版本。
            kind: 工具类型。
        """

        def apply() -> None:
            """更新 Span 名称与属性。"""
            self._alias, self._name, self._version, self._kind = alias, name, version, kind
            self._span.update_name(f"{sc.OPERATION_EXECUTE_TOOL} {alias}")
            self._span.set_attributes(
                {
                    sc.GEN_AI_TOOL_NAME: alias,
                    sc.APP_TOOL_NAME: name,
                    sc.APP_TOOL_VERSION: version,
                    sc.APP_TOOL_KIND: kind,
                }
            )

        self._inst.safe(apply)

    def finish(self, result: ToolExecutionResult, *, output_bytes: int | None = None) -> None:
        """记录调用的终态（成功或工具层失败）。只会生效一次。

        Args:
            result: 调用结果。
            output_bytes: 输出的字节数（成功时）。
        """
        if self._finished:
            return
        self._finished = True
        error_type = result.error.type if result.error else None
        error_code = result.error.code if result.error else None
        self._inst.safe(
            lambda: self._end(
                result.status,
                error_type,
                error_code,
                attempts=result.attempts,
                output_bytes=output_bytes,
                output_text=result.to_model_text() if self._inst.records_content else None,
            )
        )

    def cancelled(self, code: str = "caller_cancelled") -> None:
        """任务被取消时记录终态 ``cancelled``。调用者随后必须重新抛出 ``CancelledError``。

        Args:
            code: 取消来源：``caller_cancelled``（调用方取消）或 ``executor_closed``（执行器关闭）。
        """
        if self._finished:
            return
        self._finished = True
        self._inst.safe(lambda: self._end("cancelled", "cancelled", code))

    @property
    def trace_ids(self) -> tuple[str | None, str | None]:
        """当前 Span 的 ``(trace_id, span_id)``（十六进制）。

        没有有效 Span（例如未配置 OTel SDK）时为 ``(None, None)``。
        """
        span_ctx = self._span.get_span_context()
        if not span_ctx.is_valid:
            return None, None
        return format(span_ctx.trace_id, "032x"), format(span_ctx.span_id, "016x")

    # -- 内部方法 ------------------------------------------------------------------
    def _end(
        self,
        status: str,
        error_type: str | None,
        error_code: str | None,
        *,
        attempts: int = 0,
        output_bytes: int | None = None,
        output_text: str | None = None,
    ) -> None:
        """结束 Span、记录指标和结束日志，并恢复进入调用前的上下文。

        Args:
            status: ``success`` / ``error`` / ``cancelled``。
            error_type: 低基数错误类型。
            error_code: 错误码。
            attempts: 尝试次数。
            output_bytes: 输出字节数。
            output_text: 输出文本（只在 ``redacted`` / ``full`` 模式下传入，用于生成预览）。
        """
        duration = time.perf_counter() - self._t0
        try:
            span = self._span
            span.set_attribute(sc.APP_TOOL_STATUS, status)
            span.set_attribute(sc.APP_TOOL_ATTEMPTS, attempts)
            if error_type is not None:
                span.set_attribute(sc.ERROR_TYPE, error_type)
                span.set_attribute(sc.APP_TOOL_ERROR_CODE, error_code or "")
                span.set_status(Status(StatusCode.ERROR, error_code))
            content = self._content_previews(output_text)
            if sc.GEN_AI_TOOL_CALL_ARGUMENTS in content:
                span.set_attribute(
                    sc.GEN_AI_TOOL_CALL_ARGUMENTS, content[sc.GEN_AI_TOOL_CALL_ARGUMENTS]
                )
            if sc.GEN_AI_TOOL_CALL_RESULT in content:
                span.set_attribute(sc.GEN_AI_TOOL_CALL_RESULT, content[sc.GEN_AI_TOOL_CALL_RESULT])
            span.end()
            labels: dict[str, str] = {
                sc.GEN_AI_TOOL_NAME: self._alias or sc.UNRESOLVED_TOOL,
                sc.APP_TOOL_KIND: self._kind or "unknown",
                sc.APP_TOOL_STATUS: status,
            }
            if error_type is not None:
                labels[sc.ERROR_TYPE] = error_type
            self._inst.duration.record(duration, labels)
            self._inst.calls.add(1, labels)
            self._inst.inflight.add(-1)
            if status == "success":
                level = "info"
            elif status == "cancelled":
                level = "warning"
            else:
                level = "error"
            event = "tool.call.finish" if status == "success" else "tool.call.error"
            fields = self._fields()
            fields.update(
                status=status,
                error_type=error_type,
                error_code=error_code,
                attempts=attempts,
                duration_ms=round(duration * 1000, 3),
                output_bytes=output_bytes,
            )
            if sc.GEN_AI_TOOL_CALL_ARGUMENTS in content:
                fields["arguments"] = content[sc.GEN_AI_TOOL_CALL_ARGUMENTS]
            if sc.GEN_AI_TOOL_CALL_RESULT in content:
                fields["output"] = content[sc.GEN_AI_TOOL_CALL_RESULT]
            self._inst.log(level, event, fields)
        finally:
            # 无论遥测是否出错都要恢复上下文，否则后续代码会挂在一个已结束的 Span 下
            if self._token is not None:
                otel_context.detach(self._token)
                self._token = None

    def _content_previews(self, output_text: str | None) -> dict[str, str]:
        """生成参数与输出的预览（只在 ``redacted`` / ``full`` 模式下有内容）。

        顺序：精确清除已知密钥值 → 按凭证格式清除 →（``redacted``）清除个人信息 → 截断。

        Args:
            output_text: 输出文本；``None`` 表示没有输出（例如被取消）。

        Returns:
            ``{属性名: 预览}``；``metadata`` / ``off`` 模式下为空字典。
        """
        mode = self._inst.log_mode
        if mode not in _PREVIEW_CHARS:
            return {}
        limit, pii = _PREVIEW_CHARS[mode], mode == "redacted"
        out: dict[str, str] = {}
        if self._args_text is not None:
            out[sc.GEN_AI_TOOL_CALL_ARGUMENTS] = preview(
                self._scrub(self._args_text), pii=pii, max_chars=limit
            )
        if output_text is not None:
            out[sc.GEN_AI_TOOL_CALL_RESULT] = preview(
                self._scrub(output_text), pii=pii, max_chars=limit
            )
        return out

    def _fields(self) -> dict[str, Any]:
        """生成日志的公共元数据字段（不含参数和输出内容）。

        Returns:
            字段字典。
        """
        trace_id, span_id = self.trace_ids
        principal = self._ctx.principal
        return {
            "request_id": self._ctx.request_id,
            "call_id": self._call_id,
            "tool": self._alias or self._tool_ref,
            "tool_name": self._name,
            "version": self._version,
            "kind": self._kind,
            "user_id": principal.user_id,
            "tenant_id": principal.tenant_id,
            "session_id": self._ctx.session_id,
            "attributes": dict(self._ctx.attributes) or None,
            "args_bytes": self.args_bytes,
            "args_sha256": self.args_sha256,
            "trace_id": trace_id,
            "span_id": span_id,
        }


def _identity(text: str) -> str:
    """默认的清除函数：原样返回文本。

    Args:
        text: 文本。

    Returns:
        原文本。
    """
    return text
