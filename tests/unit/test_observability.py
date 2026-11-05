from __future__ import annotations

import asyncio
import io
import json
import logging

import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from provider_sdk import Message, RequestContext
from provider_sdk.observability import configure_logging
from provider_sdk.observability import semconv as sc
from tests.conftest import API_KEYS, Harness, completion

ALLOWED_METRIC_KEYS = {
    sc.GEN_AI_OPERATION_NAME,
    sc.GEN_AI_PROVIDER_NAME,
    sc.GEN_AI_REQUEST_MODEL,
    sc.APP_PROVIDER,
    sc.APP_OPERATION,
    sc.APP_STATUS,
    sc.ERROR_TYPE,
    sc.GEN_AI_TOKEN_TYPE,
}


async def test_span_hierarchy_parent_and_attributes(harness: Harness) -> None:
    parent_tracer = harness.tracer_provider.get_tracer("app")
    harness.mock.json(completion())
    with parent_tracer.start_as_current_span("agent.step") as parent:
        resp = await harness.sdk.chat(
            provider="glm",
            model="glm-4.6",
            prompt="SECRET PROMPT",
            context=RequestContext(request_id="req-abc"),
        )
    sdk_span = harness.span("chat glm-4.6")
    adapter_span = harness.span("provider_sdk.adapter")
    http_span = harness.span("POST")
    # 网关 / 应用 Span -> SDK Span -> 适配器 Span -> HTTP Span
    assert sdk_span.parent.span_id == parent.get_span_context().span_id
    assert adapter_span.parent.span_id == sdk_span.context.span_id
    assert http_span.parent.span_id == adapter_span.context.span_id
    assert sdk_span.kind == SpanKind.CLIENT and http_span.kind == SpanKind.CLIENT
    assert format(sdk_span.context.trace_id, "032x") == resp.trace.trace_id
    a = sdk_span.attributes
    assert a["gen_ai.operation.name"] == "chat"
    assert a["gen_ai.provider.name"] == "zhipu"
    assert a["gen_ai.request.model"] == "glm-4.6"
    assert a["gen_ai.response.model"] == "test-model"
    assert a["gen_ai.usage.input_tokens"] == 10
    assert a["app.llm.request_id"] == "req-abc"
    assert a["app.llm.outcome"] == "success"
    assert a["app.llm.stream"] is False
    assert a["server.address"] == "open.bigmodel.cn"
    assert http_span.attributes["http.response.status_code"] == 200
    assert (
        http_span.attributes["url.full"] == "https://open.bigmodel.cn/api/paas/v4/chat/completions"
    )
    # 默认不在 Span 上记录 Prompt / 输出内容
    for span in harness.finished_spans():
        for value in (span.attributes or {}).values():
            assert "SECRET PROMPT" not in str(value)


async def test_trace_context_propagation_is_opt_in(harness: Harness) -> None:
    cfg = harness.sdk.registry.config("deepseek")
    harness.sdk.registry.register(
        cfg.model_copy(update={"propagate_trace_context": True}),
        transport=harness.mock.transport(),
        replace=True,
    )
    harness.mock.json(completion())
    await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    tp = harness.mock.requests[0].headers.get("traceparent")
    assert tp and tp.split("-")[1] == format(harness.span("POST").context.trace_id, "032x")


async def test_capture_content_in_spans_opt_in(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness(env={"PROVIDER_SDK_CAPTURE_CONTENT": "true"})
    h.mock.json(completion("the answer"))
    await h.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="mail me a@b.com")
    attrs = h.span("chat").attributes
    assert "[EMAIL]" in attrs["gen_ai.input.messages"]
    assert "the answer" in attrs["gen_ai.output.messages"]


async def test_metrics_low_cardinality(harness: Harness) -> None:
    harness.mock.json(completion())
    harness.mock.json(completion())
    harness.mock.error(429, {"error": {"message": "slow down"}})
    ctx = RequestContext(user_id="user-123")
    await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x", context=ctx)
    await harness.sdk.chat(provider="deepseek", model="some-unlisted-model", prompt="x")
    with pytest.raises(Exception):
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    for name in (
        "gen_ai.client.operation.duration",
        "app.llm.requests",
        "gen_ai.client.token.usage",
        "app.llm.errors",
    ):
        points = harness.metric_points(name)
        assert points, name
        for p in points:
            assert set(p.attributes) <= ALLOWED_METRIC_KEYS, (name, p.attributes)
            assert "user-123" not in p.attributes.values()
    models = {
        p.attributes["gen_ai.request.model"] for p in harness.metric_points("app.llm.requests")
    }
    assert models == {"deepseek-flash", "_other"}
    statuses: dict[str, int] = {}
    for p in harness.metric_points("app.llm.requests"):
        status = p.attributes["app.llm.status"]
        statuses[status] = statuses.get(status, 0) + p.value
    assert statuses == {"success": 2, "error": 1}
    err = harness.metric_points("app.llm.errors")[0]
    assert err.attributes["error.type"] == "rate_limit"


async def test_active_streams_metric(harness: Harness) -> None:
    from tests.conftest import chunk

    harness.mock.sse([chunk({"content": "a"}), chunk({}, finish_reason="stop")])
    stream = harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
    )
    await stream.__anext__()
    assert sum(p.value for p in harness.metric_points("app.llm.active_streams")) == 1
    async for _ in stream:
        pass
    assert sum(p.value for p in harness.metric_points("app.llm.active_streams")) == 0
    assert harness.metric_points("gen_ai.client.operation.time_to_first_chunk")


async def test_log_events_metadata_only_by_default(harness: Harness) -> None:
    harness.mock.json(completion("MODEL OUTPUT"))
    await harness.sdk.chat(
        provider="deepseek",
        model="deepseek-flash",
        prompt="USER PROMPT",
        context=RequestContext(request_id="req-1", user_id="u1"),
    )
    req, resp = harness.logs.events("llm.request")[0], harness.logs.events("llm.response")[0]
    assert "input" not in req and "output" not in resp
    assert resp["status"] == "success" and resp["request_id"] == "req-1"
    assert resp["service"] == "test-service"
    assert resp["input_tokens"] == 10 and resp["usage_source"] == "provider"
    assert resp["trace_id"] == format(harness.span("chat").context.trace_id, "032x")
    assert "USER PROMPT" not in json.dumps(harness.logs.records, ensure_ascii=False)
    assert "MODEL OUTPUT" not in json.dumps(harness.logs.records, ensure_ascii=False)


async def test_log_redacted_mode(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness(env={"PROVIDER_SDK_LOG_MODE": "redacted", "PROVIDER_SDK_LOG_MAX_CHARS": "60"})
    h.mock.json(completion("call 13812345678 " + "x" * 100))
    key = API_KEYS["QWEN_API_KEY"]
    await h.sdk.chat(
        provider="deepseek", model="deepseek-flash", prompt=f"my email is a@b.com and key {key}"
    )
    req = h.logs.events("llm.request")[0]
    assert req["input"][0]["content"].startswith("my email is [EMAIL] and key [REDACTED_KEY]")
    out = h.logs.events("llm.response")[0]["output"]["content"]
    assert "[PHONE]" in out and "truncated" in out


async def test_full_mode_requires_acknowledgement(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness(env={"PROVIDER_SDK_LOG_MODE": "full"})
    assert h.sdk.instrumentation.io.mode == "redacted"
    assert h.logs.events("llm.logging.full_mode_not_acknowledged")
    h2 = make_harness(
        env={"PROVIDER_SDK_LOG_MODE": "full", "PROVIDER_SDK_ALLOW_FULL_LOGGING": "true"}
    )
    h2.mock.json(completion())
    key = API_KEYS["DEEPSEEK_API_KEY"]
    await h2.sdk.chat(
        provider="deepseek", model="deepseek-flash", prompt=f"a@b.com Authorization: Bearer {key}"
    )
    content = h2.logs.events("llm.request")[0]["input"][0]["content"]
    assert "a@b.com" in content  # full mode keeps PII...
    assert key not in content  # ...but never credentials


async def test_log_sampling_keeps_errors(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness(env={"PROVIDER_SDK_LOG_SAMPLE_RATE": "0"})
    h.mock.json(completion())
    h.mock.error(500, {"error": {"message": "x"}})
    await h.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    with pytest.raises(Exception):
        await h.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert [r["event"] for r in h.logs.records] == ["llm.error"]


async def test_broken_logger_does_not_break_calls(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness()

    class Exploding:
        def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
            def boom(*a, **k):  # type: ignore[no-untyped-def]
                raise RuntimeError("log sink down")

            return boom

    h.sdk.instrumentation.io._logger = Exploding()
    h.mock.json(completion("still works"))
    resp = await h.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert resp.text == "still works"
    assert h.sdk.instrumentation.io.failures >= 2


async def test_concurrent_calls_have_isolated_context(harness: Harness) -> None:
    async def handler(request):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.01)
        import httpx2

        return httpx2.Response(
            200, json=completion(json.loads(request.content)["messages"][0]["content"])
        )

    harness.mock.responses = [lambda r: handler(r) for _ in range(50)]
    tracer = harness.tracer_provider.get_tracer("app")

    async def one(i: int):  # type: ignore[no-untyped-def]
        with tracer.start_as_current_span(f"parent-{i}") as parent:
            resp = await harness.sdk.chat(
                provider="deepseek",
                model="deepseek-flash",
                prompt=f"p{i}",
                context={"request_id": f"r{i}"},
            )
            return i, parent.get_span_context().trace_id, resp

    results = await asyncio.gather(*(one(i) for i in range(50)))
    for i, trace_id, resp in results:
        assert resp.text == f"p{i}"
        assert resp.request_id == f"r{i}"
        assert resp.trace.trace_id == format(trace_id, "032x")
    assert len({r[2].trace.trace_id for r in results}) == 50
    assert trace.get_current_span() is trace.INVALID_SPAN


def test_configure_logging_json_bounded_and_correlated() -> None:
    import structlog

    buf = io.StringIO()
    handle = configure_logging(service="svc", queue_size=5, stream=buf)
    try:
        log = structlog.get_logger("t")
        log.info("hello", foo=1)
        logging.getLogger("uvicorn.error").warning("foreign %s", "record")
        handle.listener.stop()  # stop draining so the queue fills up
        handle.listener._thread = None
        for _ in range(20):
            log.info("flood")
        assert handle.dropped >= 15
        lines = [json.loads(line) for line in buf.getvalue().splitlines()]
        assert lines[0]["event"] == "hello" and lines[0]["service"] == "svc"
        assert lines[0]["level"] == "info" and "timestamp" in lines[0]
        assert lines[1]["event"] == "foreign record"
    finally:
        handle.shutdown()
        structlog.reset_defaults()


def test_semconv_constants_match_installed_package() -> None:
    from opentelemetry.semconv._incubating.attributes import gen_ai_attributes as ga
    from opentelemetry.semconv._incubating.metrics import gen_ai_metrics as gm
    from opentelemetry.semconv.schemas import Schemas

    for name in dir(sc):
        if name.startswith("GEN_AI_"):
            assert getattr(ga, name) == getattr(sc, name), name
    assert gm.GEN_AI_CLIENT_OPERATION_DURATION == sc.METRIC_OPERATION_DURATION
    assert gm.GEN_AI_CLIENT_TOKEN_USAGE == sc.METRIC_TOKEN_USAGE
    assert gm.GEN_AI_CLIENT_OPERATION_TIME_TO_FIRST_CHUNK == sc.METRIC_TIME_TO_FIRST_CHUNK
    assert sc.SCHEMA_URL in {s.value for s in Schemas}
