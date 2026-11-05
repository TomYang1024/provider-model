"""可观测性：OpenTelemetry Trace / Metrics 与 structlog 输入输出日志。

导入本包没有任何副作用（不会安装全局 Provider、导出器或日志配置）。
需要启用 OpenTelemetry SDK 链路时，显式使用 :mod:`provider_sdk.observability.setup`。
"""

from .logging import IOLogger, LoggingHandle, add_trace_context, configure_logging
from .metrics import SDKMetrics
from .middleware import CallObserver, Instrumentation

__all__ = [
    "CallObserver",
    "IOLogger",
    "Instrumentation",
    "LoggingHandle",
    "SDKMetrics",
    "add_trace_context",
    "configure_logging",
]
