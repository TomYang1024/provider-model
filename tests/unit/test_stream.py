from __future__ import annotations

import asyncio

import pytest
from opentelemetry.trace import StatusCode

from provider_sdk import Message, ProviderServerError, RateLimitError
from provider_sdk import errors as sdk_errors
from tests.conftest import Harness, chunk

USAGE = {"prompt_tokens": 8, "completion_tokens": 3, "total_tokens": 11}


def text_chunks() -> list[dict]:
    return [
        chunk({"role": "assistant", "content": ""}),
        chunk({"content": "Py"}),
        chunk({"content": "thon"}),
        chunk({}, finish_reason="stop"),
        chunk(no_choices=True, usage=USAGE),
    ]


async def test_stream_event_order_and_aggregation(harness: Harness) -> None:
    harness.mock.sse(text_chunks())
    async with harness.sdk.open_stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("介绍一下 Python")]
    ) as stream:
        events = [e async for e in stream]
    types = [e.type for e in events]
    assert types == ["message_start", "text_delta", "text_delta", "usage", "message_end"]
    assert [e.seq for e in events] == list(range(len(events)))
    end = events[-1]
    assert end.finish_reason == "stop"
    assert end.response.text == "Python"
    assert end.response.usage.input_tokens == 8 and end.response.usage.source == "provider"
    assert end.response.trace.trace_id == events[0].trace.trace_id
    assert stream.outcome == "success" and stream.response == end.response
    assert harness.mock.body()["stream"] is True
    assert harness.mock.body()["stream_options"] == {"include_usage": True}
    assert harness.mock.closed_streams == 1
    assert harness.sdk.active_streams == 0

    span = harness.span("chat deepseek-flash")
    assert span.status.status_code == StatusCode.OK
    assert span.attributes["app.llm.outcome"] == "success"
    assert span.attributes["gen_ai.usage.output_tokens"] == 3
    assert "gen_ai.response.time_to_first_chunk" in span.attributes
    assert [e.name for e in span.events][:2] == ["app.llm.stream.connected", "app.llm.first_token"]
    completed = harness.logs.events("llm.stream.completed")
    assert completed and completed[0]["ttft_ms"] is not None


async def test_stream_plain_async_for_and_collect(harness: Harness) -> None:
    harness.mock.sse(text_chunks())
    stream = harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("hi")]
    )
    n = 0
    async for _ in stream:
        n += 1
    assert n == 5 and stream.closed

    harness.mock.sse(text_chunks())
    resp = await harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("hi")]
    ).collect()
    assert resp.text == "Python"


async def test_stream_tool_calls_interleaved(harness: Harness) -> None:
    def tc(index: int, *, id: str | None = None, name: str | None = None, args: str = "") -> dict:
        fn: dict = {"arguments": args}
        if name:
            fn["name"] = name
        d: dict = {"index": index, "function": fn}
        if id:
            d["id"] = id
            d["type"] = "function"
        return {"tool_calls": [d]}

    harness.mock.sse(
        [
            chunk(tc(0, id="call_a", name="get_weather", args="")),
            chunk(tc(0, args='{"city":')),
            chunk(tc(1, id="call_b", name="get_time", args='{"tz":"UTC"}')),
            chunk(tc(0, args='"北京"}')),
            chunk({}, finish_reason="tool_calls"),
            chunk(no_choices=True, usage=USAGE),
        ]
    )
    async with harness.sdk.open_stream(
        provider="qwen",
        model="qwen-plus",
        messages=[Message.user("x")],
        tools=[{"name": "get_weather"}, {"name": "get_time"}],
    ) as stream:
        events = [e async for e in stream]
    seq = [(e.type, getattr(e, "index", None)) for e in events]
    assert seq == [
        ("message_start", None),
        ("tool_call_start", 0),
        ("tool_call_delta", 0),
        ("tool_call_start", 1),
        ("tool_call_delta", 1),
        ("tool_call_delta", 0),
        ("tool_call_end", 0),
        ("tool_call_end", 1),
        ("usage", None),
        ("message_end", None),
    ]
    ends = [e for e in events if e.type == "tool_call_end"]
    assert ends[0].tool_call.parsed_arguments() == {"city": "北京"}
    assert ends[1].tool_call.id == "call_b"
    final = events[-1].response
    assert final.finish_reason == "tool_calls"
    assert [t.name for t in final.tool_calls] == ["get_weather", "get_time"]
    assert final.message.content is None


async def test_stream_reasoning_and_kimi_choice_usage(harness: Harness) -> None:
    harness.mock.sse(
        [
            chunk({"role": "assistant", "reasoning_content": "思考"}),
            chunk({"reasoning_content": "中"}),
            chunk({"content": "答案"}),
            chunk({}, finish_reason="stop", choice_usage=USAGE),
        ]
    )
    async with harness.sdk.open_stream(
        provider="kimi", model="kimi-k3", messages=[Message.user("x")]
    ) as stream:
        events = [e async for e in stream]
    assert [e.type for e in events] == [
        "message_start",
        "reasoning_delta",
        "reasoning_delta",
        "text_delta",
        "usage",
        "message_end",
    ]
    assert events[-1].response.reasoning == "思考中"
    assert events[-2].usage.output_tokens == 3
    # 按文档请求流式 Usage；旧版放在 choices[0].usage 中的格式仍能解析
    assert harness.mock.body()["stream_options"] == {"include_usage": True}


async def test_stream_without_usage_reports_unknown(harness: Harness) -> None:
    harness.mock.sse([chunk({"content": "x"}), chunk({}, finish_reason="stop")])
    resp = await harness.sdk.stream(
        provider="glm", model="glm-4.6", messages=[Message.user("x")]
    ).collect()
    assert resp.usage.source == "unknown"


async def test_error_before_first_event_is_raised_without_events(harness: Harness) -> None:
    harness.mock.error(429, {"error": {"message": "rate limited", "type": "rate_limit"}})
    stream = harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
    )
    with pytest.raises(RateLimitError) as info:
        await stream.__anext__()
    assert info.value.retryable is True
    assert stream.outcome == "error"
    span = harness.span("chat deepseek-flash")
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["error.type"] == "rate_limit"


async def test_mid_stream_error_is_not_retryable(harness: Harness) -> None:
    harness.mock.sse(
        [
            chunk({"content": "partial"}),
            {"error": {"message": "upstream exploded", "code": "boom"}},
        ],
        done=False,
    )
    events = []
    with pytest.raises(sdk_errors.ProviderError) as info:
        async with harness.sdk.open_stream(
            provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
        ) as stream:
            async for e in stream:
                events.append(e)
    assert [e.type for e in events] == ["message_start", "text_delta"]
    assert info.value.retryable is False
    err_logs = harness.logs.events("llm.error")
    assert err_logs[-1]["partial_output"] is True
    assert harness.span("chat deepseek-flash").attributes["app.llm.outcome"] == "error"


async def test_truncated_stream_without_done(harness: Harness) -> None:
    harness.mock.sse([chunk({"content": "partial"})], done=False)
    # 缺少 [DONE] / finish_reason 时，响应体结束后流仍能正常结束
    resp = await harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
    ).collect()
    assert resp.finish_reason == "unknown"


async def test_early_close_is_not_success(harness: Harness) -> None:
    harness.mock.sse(text_chunks(), delay=0.01)
    async with harness.sdk.open_stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
    ) as stream:
        async for e in stream:
            if e.type == "text_delta":
                break
    assert stream.outcome == "closed_early"
    await asyncio.sleep(0)
    assert harness.mock.closed_streams == 1, "HTTP stream must be released"
    span = harness.span("chat deepseek-flash")
    assert span.status.status_code != StatusCode.OK
    assert span.attributes["app.llm.outcome"] == "closed_early"
    assert span.attributes["app.llm.partial_output"] is True
    assert harness.logs.events("llm.stream.partial")
    assert not harness.logs.events("llm.stream.completed")
    http = harness.span("POST")
    assert http.end_time is not None


async def test_manual_aclose_without_context_manager(harness: Harness) -> None:
    harness.mock.sse(text_chunks())
    stream = harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
    )
    try:
        async for e in stream:
            if e.type == "text_delta":
                break
    finally:
        await stream.aclose()
    assert stream.outcome == "closed_early"
    assert harness.sdk.active_streams == 0


async def test_cancellation_is_recorded(harness: Harness) -> None:
    harness.mock.sse(text_chunks(), hang_after=2)
    seen = asyncio.Event()

    async def consume() -> None:
        async with harness.sdk.open_stream(
            provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
        ) as stream:
            async for e in stream:
                if e.type == "text_delta":
                    seen.set()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(seen.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.01)
    assert harness.mock.closed_streams == 1
    span = harness.span("chat deepseek-flash")
    assert span.attributes["app.llm.outcome"] == "cancelled"
    assert harness.logs.events("llm.stream.cancelled")
    assert harness.sdk.active_streams == 0


async def test_stream_deadline(harness: Harness) -> None:
    harness.mock.sse(text_chunks(), hang_after=2)
    events = []
    with pytest.raises(sdk_errors.TimeoutError) as info:
        async with harness.sdk.open_stream(
            provider="deepseek",
            model="deepseek-flash",
            messages=[Message.user("x")],
            timeout=0.3,
        ) as stream:
            async for e in stream:
                events.append(e)
    assert info.value.error_code == "deadline_exceeded"
    assert info.value.retryable is False  # output already delivered
    assert len(events) == 2
    await asyncio.sleep(0.01)
    assert harness.mock.closed_streams == 1
    span = harness.span("chat deepseek-flash")
    assert span.attributes["app.llm.outcome"] == "timeout"
    assert span.status.status_code == StatusCode.ERROR


async def test_sdk_aclose_closes_open_streams(harness: Harness) -> None:
    harness.mock.sse(text_chunks(), hang_after=2)
    stream = harness.sdk.stream(
        provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
    )
    await stream.__anext__()
    assert harness.sdk.active_streams == 1
    await harness.sdk.aclose()
    assert stream.outcome == "cancelled"
    assert harness.mock.closed_streams == 1
    with pytest.raises(sdk_errors.ProviderError):
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")


async def test_server_error_mid_stream_maps(harness: Harness) -> None:
    harness.mock.error(503, {"error": {"message": "Server overloaded"}})
    with pytest.raises(ProviderServerError) as info:
        await harness.sdk.stream(
            provider="deepseek", model="deepseek-flash", messages=[Message.user("x")]
        ).collect()
    assert info.value.retryable is True and info.value.status_code == 503
