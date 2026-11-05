from __future__ import annotations

import pytest

from provider_sdk import (
    InvalidRequestError,
    Message,
    ProviderNotFoundError,
    ToolCall,
    ToolDefinition,
    ToolResult,
    UnsupportedCapabilityError,
)
from tests.conftest import API_KEYS, Harness, completion

WEATHER = ToolDefinition(
    name="get_weather",
    description="Get weather",
    parameters={"type": "object", "properties": {"city": {"type": "string"}}},
)


async def test_chat_builds_request_and_normalizes_response(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENAI_ORG_ID", "org-should-not-leak")
    harness.mock.json(completion("你好！", model="deepseek-flash-1017"))
    resp = await harness.sdk.chat(
        provider="deepseek",
        model="deepseek-flash",
        prompt="你好",
        system="你是专业助手",
        temperature=0.3,
        max_tokens=64,
    )
    assert resp.text == "你好！"
    assert resp.provider == "deepseek"
    assert resp.model == "deepseek-flash"
    assert resp.response_model == "deepseek-flash-1017"
    assert resp.finish_reason == "stop"
    assert resp.usage.model_dump() == {
        "input_tokens": 10,
        "output_tokens": 5,
        "total_tokens": 15,
        "reasoning_tokens": None,
        "cached_input_tokens": None,
        "source": "provider",
    }
    assert resp.request_id.startswith("req_")
    assert resp.trace.trace_id and len(resp.trace.trace_id) == 32
    assert resp.latency_ms is not None

    req = harness.mock.requests[0]
    assert str(req.url) == "https://api.deepseek.com/chat/completions"
    assert req.headers["authorization"] == f"Bearer {API_KEYS['DEEPSEEK_API_KEY']}"
    # 内部 Trace 上下文和 OpenAI 组织 ID 不会发送给第三方 Provider
    assert "traceparent" not in req.headers
    assert "openai-organization" not in req.headers
    body = harness.mock.body()
    assert body["model"] == "deepseek-flash"
    assert body["messages"] == [
        {"role": "system", "content": "你是专业助手"},
        {"role": "user", "content": "你好"},
    ]
    assert body["temperature"] == 0.3 and body["max_tokens"] == 64
    assert "stream" not in body
    # 可 JSON 序列化，不包含厂商 SDK 对象
    assert resp.model_dump_json()


async def test_messages_tool_call_roundtrip(harness: Harness) -> None:
    harness.mock.json(
        completion(
            None,
            finish_reason="tool_calls",
            tool_calls=[
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "get_weather", "arguments": '{"city":"北京"}'},
                }
            ],
        )
    )
    first = await harness.sdk.messages(
        provider="kimi",
        model="kimi-k2.6",
        messages=[Message.user("北京天气?")],
        tools=[WEATHER],
        tool_choice="auto",
    )
    assert first.finish_reason == "tool_calls"
    assert first.tool_calls == [
        ToolCall(id="call_1", name="get_weather", arguments='{"city":"北京"}')
    ]
    assert first.tool_calls[0].parsed_arguments() == {"city": "北京"}
    body = harness.mock.body()
    assert body["tools"][0] == {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get weather",
            "parameters": WEATHER.parameters,
        },
    }
    assert body["tool_choice"] == "auto"

    harness.mock.json(completion("晴，25℃"))
    second = await harness.sdk.messages(
        provider="kimi",
        model="kimi-k2.6",
        messages=[
            Message.user("北京天气?"),
            first.message,
            ToolResult(tool_call_id="call_1", content='{"temp":25}'),
        ],
        tools=[WEATHER],
    )
    assert second.text == "晴，25℃"
    msgs = harness.mock.body()["messages"]
    assert msgs[1]["role"] == "assistant"
    assert msgs[1]["tool_calls"][0]["id"] == "call_1"
    assert msgs[2] == {"role": "tool", "tool_call_id": "call_1", "content": '{"temp":25}'}


async def test_multimodal_content_parts(harness: Harness) -> None:
    harness.mock.json(completion("a cat"))
    await harness.sdk.messages(
        provider="glm",
        model="glm-4.6v",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "what is this?"},
                    {"type": "image_url", "url": "https://example.com/cat.png", "detail": "low"},
                ],
            }
        ],
    )
    assert harness.mock.body()["messages"][0]["content"] == [
        {"type": "text", "text": "what is this?"},
        {"type": "image_url", "image_url": {"url": "https://example.com/cat.png", "detail": "low"}},
    ]


async def test_reasoning_non_stream_and_mapping(harness: Harness) -> None:
    harness.mock.json(
        completion(
            "42",
            reasoning="Let me think...",
            usage={
                "prompt_tokens": 3,
                "completion_tokens": 9,
                "total_tokens": 12,
                "completion_tokens_details": {"reasoning_tokens": 7},
                "prompt_cache_hit_tokens": 2,
            },
        )
    )
    resp = await harness.sdk.chat(
        provider="deepseek",
        model="deepseek-v4-pro",
        prompt="q",
        reasoning={"enabled": True, "effort": "max"},
    )
    assert resp.reasoning == "Let me think..."
    assert resp.usage.reasoning_tokens == 7
    assert resp.usage.cached_input_tokens == 2
    body = harness.mock.body()
    assert body["thinking"] == {"type": "enabled"}
    assert body["reasoning_effort"] == "max"

    # 未指定推理配置 -> 使用 Provider 默认值，不发送任何参数
    harness.mock.json(completion("ok"))
    await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="q")
    assert "thinking" not in harness.mock.body()

    # 始终思考的模型（Kimi K3）：不发送 `thinking` 参数，只发送推理深度
    harness.mock.json(completion("ok"))
    await harness.sdk.chat(
        provider="kimi", model="kimi-k3", prompt="q", reasoning={"effort": "low"}
    )
    body = harness.mock.body()
    assert "thinking" not in body
    assert body["reasoning_effort"] == "low"

    harness.mock.json(completion("ok"))
    await harness.sdk.chat(provider="glm", model="glm-4.6", prompt="q", reasoning=False)
    assert harness.mock.body()["thinking"] == {"type": "disabled"}

    harness.mock.json(completion("ok"))
    await harness.sdk.chat(
        provider="qwen",
        model="qwen-plus",
        prompt="q",
        reasoning={"enabled": True, "budget_tokens": 512},
    )
    body = harness.mock.body()
    assert body["enable_thinking"] is True and body["thinking_budget"] == 512


async def test_provider_options_and_reserved_keys(harness: Harness) -> None:
    harness.mock.json(completion())
    await harness.sdk.chat(
        provider="qwen", model="qwen-plus", prompt="hi", provider_options={"enable_search": True}
    )
    assert harness.mock.body()["enable_search"] is True
    with pytest.raises(InvalidRequestError, match="standard parameters"):
        await harness.sdk.chat(
            provider="qwen", model="qwen-plus", prompt="hi", provider_options={"model": "other"}
        )


@pytest.mark.parametrize(
    ("provider", "model", "kwargs", "match"),
    [
        ("glm", "glm-4.6", {"tools": [WEATHER], "tool_choice": "required"}, "forced tool_choice"),
        ("qwen", "qwen-max", {"reasoning": True}, "reasoning"),
        ("glm", "glm-5.3", {"reasoning": False}, "disabling reasoning"),
        ("kimi", "kimi-k3", {"reasoning": False}, "disabling reasoning"),
        ("glm", "glm-4.6", {"reasoning": {"enabled": True, "budget_tokens": 10}}, "budget"),
        ("qwen", "qwen-plus", {"reasoning": {"enabled": True, "effort": "high"}}, "effort"),
        (
            "kimi",
            "kimi-k2.6",
            {"messages": [Message.user([{"type": "image_url", "url": "https://x/y.png"}])]},
            "image input",
        ),
    ],
)
async def test_capability_validation_happens_before_io(
    harness: Harness, provider: str, model: str, kwargs: dict, match: str
) -> None:
    messages = kwargs.pop("messages", [Message.user("hi")])
    with pytest.raises(UnsupportedCapabilityError, match=match):
        await harness.sdk.messages(provider=provider, model=model, messages=messages, **kwargs)
    assert harness.mock.requests == []


async def test_unknown_and_disabled_provider(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness(env={"PROVIDER_SDK_GLM_ENABLED": "false"})
    with pytest.raises(ProviderNotFoundError, match="unknown provider"):
        await h.sdk.chat(provider="nope", model="m", prompt="x")
    with pytest.raises(ProviderNotFoundError) as info:
        await h.sdk.chat(provider="glm", model="glm-4.6", prompt="x")
    assert info.value.error_code == "provider_disabled"


async def test_strict_models(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness()
    h.sdk.registry.register(
        h.sdk.registry.config("deepseek").model_copy(update={"strict_models": True}),
        transport=h.mock.transport(),
        replace=True,
    )
    with pytest.raises(InvalidRequestError) as info:
        await h.sdk.chat(provider="deepseek", model="not-a-model", prompt="x")
    assert info.value.error_code == "model_not_found"


async def test_validation_errors_do_not_echo_input(harness: Harness) -> None:
    with pytest.raises(InvalidRequestError) as info:
        await harness.sdk.messages(
            provider="deepseek",
            model="deepseek-flash",
            messages=[{"role": "tool", "content": "SECRET-PROMPT"}],
        )
    assert "SECRET-PROMPT" not in str(info.value)
    assert info.value.error_code == "validation_error"


async def test_usage_unknown_and_estimated(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness()
    h.mock.json(completion("hello world", usage=False))
    resp = await h.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="hi")
    assert resp.usage.source == "unknown" and resp.usage.input_tokens is None

    h2 = make_harness(env={"PROVIDER_SDK_ESTIMATE_USAGE": "true"})
    h2.mock.json(completion("你好世界", usage=False))
    resp2 = await h2.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="你好")
    assert resp2.usage.source == "estimated"
    assert resp2.usage.output_tokens == 4


async def test_custom_provider_without_client_changes(harness: Harness) -> None:
    from pydantic import SecretStr

    from provider_sdk import ProviderConfig

    harness.sdk.registry.register(
        ProviderConfig(
            name="local-vllm",
            adapter="openai_compatible",
            base_url="http://vllm.internal:8000/v1",
            api_key=SecretStr("x"),
        ),
        transport=harness.mock.transport(),
    )
    harness.mock.json(completion("from vllm"))
    resp = await harness.sdk.chat(provider="local-vllm", model="qwen3-8b", prompt="hi")
    assert resp.text == "from vllm"
    assert str(harness.mock.requests[0].url) == "http://vllm.internal:8000/v1/chat/completions"


async def test_tools_unsupported_model(harness: Harness) -> None:
    from provider_sdk import ModelCapabilities

    cfg = harness.sdk.registry.config("qwen")
    models = {**cfg.models, "qwen-audio": ModelCapabilities(tools=False)}
    harness.sdk.registry.register(
        cfg.model_copy(update={"models": models}), transport=harness.mock.transport(), replace=True
    )
    with pytest.raises(UnsupportedCapabilityError, match="tool calling"):
        await harness.sdk.messages(
            provider="qwen", model="qwen-audio", messages=[Message.user("x")], tools=[WEATHER]
        )
    assert harness.mock.requests == []


async def test_kimi_forced_tool_choice_and_reasoning_echo(harness: Harness) -> None:
    harness.mock.json(completion("ok"))
    await harness.sdk.messages(
        provider="kimi",
        model="kimi-k2.6",
        messages=[
            Message.user("q"),
            Message(
                role="assistant",
                reasoning_content="prior thinking",
                tool_calls=[ToolCall(id="c1", name="get_weather", arguments="{}")],
            ),
            ToolResult(tool_call_id="c1", content="sunny"),
        ],
        tools=[WEATHER],
        tool_choice="required",
    )
    body = harness.mock.body()
    assert body["tool_choice"] == "required"
    assert body["messages"][1]["reasoning_content"] == "prior thinking"
