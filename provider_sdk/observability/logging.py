"""基于 structlog 的标准化输入输出日志。

包含两部分：

* :class:`IOLogger`：SDK 用于输出 ``llm.*`` 事件。它绝不抛出异常——日志失败不能导致模型调用失败。
* :func:`configure_logging`：可选的进程级配置，供应用和网关使用：JSON 输出到 stdout、
  关联 Trace，以及*有界*非阻塞队列（队列满时丢弃日志并计数）。
  SDK 在 import 时绝不调用它。
"""

from __future__ import annotations

import copy
import logging
import logging.handlers
import queue
import sys
import threading
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from typing import IO, Any

import structlog
from opentelemetry import trace

from ..config import LoggingConfig
from ..schemas.message import Message
from .redaction import sanitize_text

_internal = logging.getLogger("provider_sdk.internal")


def current_trace_ids() -> tuple[str | None, str | None]:
    """返回当前 Span 的 (trace_id, span_id)；不在 Span 中时返回 (None, None)。"""
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None, None
    return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")


def add_trace_context(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """structlog 处理器：为日志添加当前 Span 的 ``trace_id`` / ``span_id``。

    已显式设置 trace_id 的事件（如 llm.* 事件使用 SDK 请求 Span 的 ID）不会被覆盖。
    """
    if "trace_id" not in event_dict:
        trace_id, span_id = current_trace_ids()
        if trace_id:
            event_dict["trace_id"] = trace_id
            event_dict["span_id"] = span_id
    return event_dict


class IOLogger:
    """按配置的内容策略输出 SDK 的 ``llm.*`` 事件。"""

    def __init__(
        self,
        config: LoggingConfig,
        *,
        service: str,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.service = service
        self._logger = logger or structlog.get_logger("provider_sdk.io")
        self.failures = 0
        # 请求了 full 模式但未显式授权：降级为 redacted，并输出一次警告
        if config.mode == "full" and not config.full_content_acknowledged:
            self._safe(
                "warning",
                "llm.logging.full_mode_not_acknowledged",
                {"effective_mode": config.effective_mode},
            )

    @property
    def mode(self) -> str:
        return self.config.effective_mode

    def emit(self, level: str, event: str, fields: Mapping[str, Any]) -> None:
        # 去掉值为 None 的字段，保持日志紧凑
        payload = {"service": self.service, **{k: v for k, v in fields.items() if v is not None}}
        self._safe(level, event, payload)

    def _safe(self, level: str, event: str, payload: Mapping[str, Any]) -> None:
        try:
            getattr(self._logger, level)(event, **payload)
        except Exception:  # pragma: no cover - 防御性代码：日志失败绝不能影响调用
            self.failures += 1

    # -- 内容处理 -----------------------------------------------------------------------
    def _clean(self, text: str) -> str:
        # 所有模式都清除密钥；只有 redacted 模式额外清除个人信息；最后统一截断
        return sanitize_text(
            text, max_chars=self.config.max_field_chars, pii=self.mode == "redacted"
        )

    def input_payload(self, messages: list[Message]) -> list[dict[str, Any]] | None:
        """日志中的输入内容；metadata_only 模式返回 None（不记录）。"""
        if self.mode == "metadata_only":
            return None
        # 只记录最近的若干条消息，限制日志体积
        selected = messages[-self.config.max_messages :]
        out = []
        for m in selected:
            item: dict[str, Any] = {"role": m.role, "content": self._clean(m.text)}
            if m.has_images():
                item["images"] = sum(
                    1 for p in m.content or [] if getattr(p, "type", "") == "image_url"
                )
            if m.tool_calls:
                item["tool_calls"] = [tc.name for tc in m.tool_calls]
            out.append(item)
        return out

    def output_payload(self, message: Message | None) -> dict[str, Any] | None:
        """日志中的输出内容；metadata_only 模式返回 None。推理内容只记录长度。"""
        if self.mode == "metadata_only" or message is None:
            return None
        out: dict[str, Any] = {"content": self._clean(message.text)}
        if message.tool_calls:
            out["tool_calls"] = [
                {"name": tc.name, "arguments": self._clean(tc.arguments)}
                for tc in message.tool_calls
            ]
        if message.reasoning_content:
            out["reasoning_chars"] = len(message.reasoning_content)
        return out


# --------------------------------------------------------------------------------------
# 进程级配置
# --------------------------------------------------------------------------------------
class BoundedQueueHandler(logging.handlers.QueueHandler):
    """非阻塞的队列 Handler：队列满时丢弃日志记录（并计数），绝不阻塞业务线程。"""

    def __init__(self, maxsize: int) -> None:
        super().__init__(queue.Queue(maxsize=maxsize))
        self.dropped = 0
        self._lock = threading.Lock()

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        # structlog 的记录携带预处理好的 dict，原样保留给 ProcessorFormatter；
        # 标准库（如 uvicorn）的记录在调用线程中先格式化消息，避免参数对象在后台线程中被修改
        if isinstance(record.msg, dict):
            return record
        record = copy.copy(record)
        record.msg = record.getMessage()
        record.args = None
        return record

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            with self._lock:
                self.dropped += 1


@dataclass
class LoggingHandle:
    handler: BoundedQueueHandler
    listener: logging.handlers.QueueListener

    @property
    def dropped(self) -> int:
        return self.handler.dropped

    def shutdown(self) -> None:
        """排空队列并停止写入线程（幂等）。进程退出前必须调用，否则可能丢失末尾日志。"""
        if self.listener._thread is not None:
            self.listener.stop()
        root = logging.getLogger()
        if self.handler in root.handlers:
            root.removeHandler(self.handler)


def configure_logging(
    *,
    level: str = "INFO",
    service: str | None = None,
    queue_size: int = 10_000,
    stream: IO[str] | None = None,
) -> LoggingHandle:
    """配置 structlog + 标准库 logging，以 JSON 格式输出到 stdout。

    在进程启动时调用一次（网关会自动调用）。返回的句柄必须在退出时调用
    ``shutdown()``，以刷新缓冲中的日志。
    """
    # structlog 记录与标准库记录共用的处理器链（保证两类日志字段一致）
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_trace_context,
    ]
    if service:

        def add_service(_l: Any, _m: str, ed: MutableMapping[str, Any]) -> Any:
            ed.setdefault("service", service)
            return ed

        shared.append(add_service)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared, structlog.processors.format_exc_info],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(ensure_ascii=False, default=str),
        ],
    )
    # 链路：日志调用 → 有界队列（调用线程，非阻塞）→ 后台线程 → JSON → stdout
    out = logging.StreamHandler(stream or sys.stdout)
    out.setFormatter(formatter)
    queue_handler = BoundedQueueHandler(queue_size)
    listener = logging.handlers.QueueListener(queue_handler.queue, out, respect_handler_level=False)
    root = logging.getLogger()
    # 重复调用时移除旧的队列 Handler，避免日志重复输出
    for h in list(root.handlers):
        if isinstance(h, BoundedQueueHandler):
            root.removeHandler(h)
    root.addHandler(queue_handler)
    root.setLevel(level.upper())
    listener.start()
    return LoggingHandle(handler=queue_handler, listener=listener)
