"""面向应用和网关的、可选的 OpenTelemetry SDK 初始化工具。

SDK 库本身只依赖 ``opentelemetry-api``；本模块需要安装 ``otel`` 扩展
（``opentelemetry-sdk`` + OTLP 导出器）。每个进程调用一次 :func:`configure_telemetry`
（多进程时在 fork 之后、每个 worker 各调用一次），退出时调用 :meth:`TelemetryHandle.shutdown`。

支持标准 ``OTEL_*`` 环境变量，例如 ``OTEL_SERVICE_NAME``、``OTEL_RESOURCE_ATTRIBUTES``、
``OTEL_TRACES_SAMPLER``（及 ``_ARG``）、``OTEL_EXPORTER_OTLP_ENDPOINT``、
``OTEL_EXPORTER_OTLP_PROTOCOL``、``OTEL_TRACES_EXPORTER=none``、
``OTEL_METRICS_EXPORTER=none``、``OTEL_SDK_DISABLED=true``。

可靠性：Span 由 ``BatchSpanProcessor`` 在后台线程导出，队列有界（溢出即丢弃，
绝不阻塞调用方），因此 Collector 不可用时不会导致模型调用失败或变慢。
Flush 和 Shutdown 都有时间上限。
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any

from opentelemetry import metrics, trace

_log = logging.getLogger("provider_sdk.telemetry")


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class TelemetryHandle:
    """configure_telemetry 的返回值：持有 Provider，并负责有时限的 Flush / Shutdown。"""

    tracer_provider: Any | None = None
    meter_provider: Any | None = None
    _shutdown: bool = field(default=False, repr=False)

    @property
    def tracer(self) -> trace.Tracer:
        if self.tracer_provider is None:
            return trace.get_tracer("provider_sdk")
        return self.tracer_provider.get_tracer("provider_sdk")  # type: ignore[no-any-return]

    @property
    def meter(self) -> metrics.Meter:
        if self.meter_provider is None:
            return metrics.get_meter("provider_sdk")
        return self.meter_provider.get_meter("provider_sdk")  # type: ignore[no-any-return]

    def force_flush(self, timeout_s: float = 5.0) -> bool:
        ok = True
        ms = int(timeout_s * 1000)
        if self.tracer_provider is not None:
            ok = bool(self.tracer_provider.force_flush(timeout_millis=ms)) and ok
        if self.meter_provider is not None:
            try:
                ok = bool(self.meter_provider.force_flush(timeout_millis=ms)) and ok
            except Exception:
                ok = False
        return ok

    def shutdown(self, timeout_s: float = 10.0) -> bool:
        """先 Flush 再关闭导出器；阻塞时间绝不超过 ``timeout_s``。

        OTLP 导出器在 Collector 不可达时可能长时间重试，因此在守护线程中执行，
        主线程最多等待 timeout_s，超时后放弃剩余数据，保证进程能按时退出。
        """
        if self._shutdown:
            return True
        self._shutdown = True
        result: dict[str, bool] = {}

        def run() -> None:
            try:
                flushed = self.force_flush(timeout_s * 0.7)
                if self.tracer_provider is not None:
                    self.tracer_provider.shutdown()
                if self.meter_provider is not None:
                    self.meter_provider.shutdown(timeout_millis=int(timeout_s * 300))
                result["ok"] = flushed
            except Exception:  # pragma: no cover - defensive
                _log.exception("telemetry shutdown failed")
                result["ok"] = False

        worker = threading.Thread(target=run, name="otel-shutdown", daemon=True)
        worker.start()
        worker.join(timeout_s)
        if worker.is_alive():
            _log.warning("telemetry shutdown exceeded %.1fs; abandoning export", timeout_s)
            return False
        return result.get("ok", False)


def configure_telemetry(
    service_name: str | None = None,
    *,
    traces: bool = True,
    metrics_enabled: bool = True,
    span_exporter: Any | None = None,
    metric_readers: list[Any] | None = None,
    max_queue_size: int = 2048,
    max_export_batch_size: int = 512,
    schedule_delay_ms: int = 2000,
    export_timeout_ms: int = 10_000,
    metric_export_interval_ms: int = 30_000,
    set_global: bool = True,
) -> TelemetryHandle:
    """创建连接到 OTLP 的 TracerProvider / MeterProvider。

    ``span_exporter`` / ``metric_readers`` 用于替换 OTLP 导出器（测试、控制台调试）。
    """
    # 标准开关：整体禁用 OTel SDK 时返回空句柄，SDK 使用 no-op tracer 正常工作
    if _env_bool("OTEL_SDK_DISABLED"):
        return TelemetryHandle()

    from opentelemetry.sdk.resources import SERVICE_NAME, Resource

    attrs = {}
    # 环境变量 OTEL_SERVICE_NAME 优先于代码中传入的服务名
    if service_name and not os.environ.get("OTEL_SERVICE_NAME"):
        attrs[SERVICE_NAME] = service_name
    resource = Resource.create(attrs)
    handle = TelemetryHandle()

    traces_exporter_env = os.environ.get("OTEL_TRACES_EXPORTER", "otlp").strip().lower()
    if traces and (span_exporter is not None or traces_exporter_env != "none"):
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        # 采样器由 OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG 决定
        # （默认 parentbased_always_on：继承上游采样决策）
        provider = TracerProvider(resource=resource)
        exporter = span_exporter or _otlp_span_exporter()
        provider.add_span_processor(
            # 有界队列 + 批量后台导出：队列满时丢弃 Span，而不是阻塞业务线程
            BatchSpanProcessor(
                exporter,
                max_queue_size=max_queue_size,
                max_export_batch_size=max_export_batch_size,
                schedule_delay_millis=schedule_delay_ms,
                export_timeout_millis=export_timeout_ms,
            )
        )
        handle.tracer_provider = provider
        if set_global:
            trace.set_tracer_provider(provider)

    metrics_exporter_env = os.environ.get("OTEL_METRICS_EXPORTER", "otlp").strip().lower()
    if metrics_enabled and (metric_readers is not None or metrics_exporter_env != "none"):
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        readers = metric_readers
        if readers is None:
            readers = [
                PeriodicExportingMetricReader(
                    _otlp_metric_exporter(),
                    export_interval_millis=metric_export_interval_ms,
                    export_timeout_millis=export_timeout_ms,
                )
            ]
        meter_provider = MeterProvider(resource=resource, metric_readers=readers)
        handle.meter_provider = meter_provider
        if set_global:
            metrics.set_meter_provider(meter_provider)
    return handle


def _protocol(signal: str) -> str:
    return (
        (
            os.environ.get(f"OTEL_EXPORTER_OTLP_{signal}_PROTOCOL")
            or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
            or "grpc"
        )
        .strip()
        .lower()
    )


def _otlp_span_exporter() -> Any:
    if _protocol("TRACES") == "grpc":
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        return OTLPSpanExporter()
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HTTPSpanExporter,
        )
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "OTLP http/protobuf requires opentelemetry-exporter-otlp-proto-http"
        ) from exc
    return HTTPSpanExporter()


def _otlp_metric_exporter() -> Any:
    if _protocol("METRICS") == "grpc":
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        return OTLPMetricExporter()
    try:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter as HTTPMetricExporter,
        )
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "OTLP http/protobuf requires opentelemetry-exporter-otlp-proto-http"
        ) from exc
    return HTTPMetricExporter()
