from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

import httpx2
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from provider_sdk import ProviderSDK, SDKConfig

API_KEYS = {
    "DEEPSEEK_API_KEY": "sk-deepseektestkey000000000000",
    "GLM_API_KEY": "0123456789abcdef0123456789abcdef.glmsecretABCDEF",
    "KIMI_API_KEY": "sk-kimitestkey0000000000000000",
    "QWEN_API_KEY": "sk-qwentestkey0000000000000000",
}


def completion(
    content: str | None = "Hello!",
    *,
    model: str = "test-model",
    tool_calls: list[dict[str, Any]] | None = None,
    finish_reason: str = "stop",
    usage: dict[str, Any] | None = None,
    reasoning: str | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    if reasoning:
        message["reasoning_content"] = reasoning
    body: dict[str, Any] = {
        "id": "cmpl-1",
        "object": "chat.completion",
        "created": 1,
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
    }
    if usage is not False:
        body["usage"] = usage or {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    return body


def chunk(
    delta: dict[str, Any] | None = None,
    *,
    finish_reason: str | None = None,
    usage: dict[str, Any] | None = None,
    choice_usage: dict[str, Any] | None = None,
    no_choices: bool = False,
    model: str = "test-model",
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": "chunk-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": model,
        "choices": [],
    }
    if not no_choices:
        choice: dict[str, Any] = {"index": 0, "delta": delta or {}, "finish_reason": finish_reason}
        if choice_usage:
            choice["usage"] = choice_usage
        body["choices"] = [choice]
    if usage:
        body["usage"] = usage
    return body


def sse_bytes(chunks: list[dict[str, Any]], *, done: bool = True) -> bytes:
    out = b"".join(f"data: {json.dumps(c)}\n\n".encode() for c in chunks)
    return out + (b"data: [DONE]\n\n" if done else b"")


class _SSEBody(httpx2.AsyncByteStream):
    """流式响应体：统计显式关闭次数（= 连接已释放）。"""

    def __init__(
        self,
        owner: MockProvider,
        chunks: list[dict[str, Any]],
        delay: float,
        done: bool,
        hang_after: int | None,
    ) -> None:
        self.owner, self.chunks, self.delay = owner, chunks, delay
        self.done, self.hang_after, self.closed = done, hang_after, False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for i, c in enumerate(self.chunks):
            if self.hang_after is not None and i >= self.hang_after:
                await asyncio.sleep(3600)
            if self.delay:
                await asyncio.sleep(self.delay)
            yield f"data: {json.dumps(c)}\n\n".encode()
        if self.done:
            yield b"data: [DONE]\n\n"

    async def aclose(self) -> None:
        if not self.closed:
            self.closed = True
            self.owner.closed_streams += 1


@dataclass
class MockProvider:
    """可编程的假 Provider：记录收到的请求，按顺序返回预先排队的响应。"""

    responses: list[Callable[[httpx2.Request], Any]] = field(default_factory=list)
    requests: list[httpx2.Request] = field(default_factory=list)
    closed_streams: int = 0

    def body(self, i: int = -1) -> dict[str, Any]:
        return json.loads(self.requests[i].content)

    def json(self, data: dict[str, Any], status: int = 200) -> None:
        self.responses.append(lambda r: httpx2.Response(status, json=data))

    def sse(
        self,
        chunks: list[dict[str, Any]],
        *,
        delay: float = 0.0,
        done: bool = True,
        hang_after: int | None = None,
    ) -> None:
        provider = self

        def handler(_r: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=_SSEBody(provider, chunks, delay, done, hang_after),
            )

        self.responses.append(handler)

    def error(self, status: int, body: Any) -> None:
        self.responses.append(lambda r: httpx2.Response(status, json=body))

    def raise_(self, exc: Exception) -> None:
        def handler(_r: httpx2.Request) -> Any:
            raise exc

        self.responses.append(handler)

    def slow_json(self, data: dict[str, Any], delay: float) -> None:
        async def handler(_r: httpx2.Request) -> httpx2.Response:
            await asyncio.sleep(delay)
            return httpx2.Response(200, json=data)

        self.responses.append(handler)

    async def handle(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(f"unexpected request {request.url}")
        result = self.responses.pop(0)(request)
        if asyncio.iscoroutine(result):
            result = await result
        return result  # type: ignore[no-any-return]

    def transport(self) -> httpx2.MockTransport:
        return httpx2.MockTransport(self.handle)


class LogCapture:
    """兼容 structlog 接口的日志器，把事件收集到内存中供断言。"""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def _log(self, level: str, event: str, **kw: Any) -> None:
        self.records.append({"event": event, "level": level, **kw})

    def __getattr__(self, level: str) -> Callable[..., None]:
        if level in {"debug", "info", "warning", "error", "critical"}:
            return lambda event, **kw: self._log(level, event, **kw)
        raise AttributeError(level)

    def events(self, name: str | None = None) -> list[dict[str, Any]]:
        return [r for r in self.records if name is None or r["event"] == name]


@dataclass
class Harness:
    sdk: ProviderSDK
    mock: MockProvider
    spans: InMemorySpanExporter
    metrics: InMemoryMetricReader
    logs: LogCapture
    tracer_provider: TracerProvider

    def finished_spans(self) -> list[ReadableSpan]:
        return list(self.spans.get_finished_spans())

    def span(self, prefix: str) -> ReadableSpan:
        matches = [s for s in self.finished_spans() if s.name.startswith(prefix)]
        assert matches, (
            f"no span starting with {prefix!r}: {[s.name for s in self.finished_spans()]}"
        )
        return matches[-1]

    def metric_points(self, name: str) -> list[Any]:
        data = self.metrics.get_metrics_data()
        points = []
        for rm in data.resource_metrics if data else []:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == name:
                        points.extend(m.data.data_points)
        return points


@pytest.fixture
def make_harness() -> Callable[..., Harness]:
    created: list[Harness] = []

    def factory(
        env: dict[str, str] | None = None, *, retries: int = 0, **config_overrides: Any
    ) -> Harness:
        environ = {**API_KEYS, **(env or {})}
        config = SDKConfig.from_env(environ)
        config.providers = {
            k: v.model_copy(update={"max_retries": retries}) for k, v in config.providers.items()
        }
        if config_overrides:
            config = config.model_copy(update=config_overrides)
        mock = MockProvider()
        exporter = InMemorySpanExporter()
        tp = TracerProvider()
        tp.add_span_processor(SimpleSpanProcessor(exporter))
        reader = InMemoryMetricReader()
        mp = MeterProvider(metric_readers=[reader])
        logs = LogCapture()
        transport = mock.transport()
        sdk = ProviderSDK(
            config,
            tracer=tp.get_tracer("test"),
            meter=mp.get_meter("test"),
            logger=logs,
            service_name="test-service",
            transports=dict.fromkeys(config.providers, transport),
        )
        h = Harness(sdk, mock, exporter, reader, logs, tp)
        created.append(h)
        return h

    return factory


@pytest.fixture
def harness(make_harness: Callable[..., Harness]) -> Harness:
    return make_harness()
