"""针对真实 Provider API 的在线测试。

需显式运行：``pytest -m integration``。未设置对应密钥的 Provider 会被跳过
（``DEEPSEEK_API_KEY``、``GLM_API_KEY``/``ZHIPUAI_API_KEY``、
``KIMI_API_KEY``/``MOONSHOT_API_KEY``、``QWEN_API_KEY``/``DASHSCOPE_API_KEY``）。
可通过 ``LIVE_<PROVIDER>_MODEL`` 指定测试模型。只有通过这些测试，
docs/providers.md 中的能力才能从 ``documented`` 升级为 ``tested``。
"""

from __future__ import annotations

import os

import pytest

from provider_sdk import Message, ProviderSDK, SDKConfig, ToolDefinition

pytestmark = pytest.mark.integration

LIVE = {
    "deepseek": ("deepseek-flash", ["DEEPSEEK_API_KEY"]),
    "glm": ("glm-4.7-flash", ["GLM_API_KEY", "ZHIPUAI_API_KEY"]),
    "kimi": ("kimi-k2.6", ["KIMI_API_KEY", "MOONSHOT_API_KEY"]),
    "qwen": ("qwen-plus", ["QWEN_API_KEY", "DASHSCOPE_API_KEY"]),
}
TOOL = ToolDefinition(
    name="get_weather",
    description="Get the current weather of a city",
    parameters={"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
)


def _params() -> list:  # type: ignore[type-arg]
    out = []
    for provider, (model, envs) in LIVE.items():
        present = any(os.environ.get(e) or os.environ.get(f"{e}_FILE") for e in envs)
        out.append(
            pytest.param(
                provider,
                os.environ.get(f"LIVE_{provider.upper()}_MODEL", model),
                marks=pytest.mark.skipif(not present, reason=f"no {envs[0]}"),
                id=provider,
            )
        )
    return out


@pytest.fixture
async def sdk():  # type: ignore[no-untyped-def]
    client = ProviderSDK(SDKConfig.from_env())
    yield client
    await client.aclose()


@pytest.mark.parametrize(("provider", "model"), _params())
async def test_live_chat(sdk: ProviderSDK, provider: str, model: str) -> None:
    resp = await sdk.chat(
        provider=provider,
        model=model,
        prompt="Reply with the word: pong",
        max_tokens=16,
        temperature=0,
    )
    assert resp.text.strip()
    assert resp.usage.source == "provider" and resp.usage.input_tokens


@pytest.mark.parametrize(("provider", "model"), _params())
async def test_live_stream_usage(sdk: ProviderSDK, provider: str, model: str) -> None:
    async with sdk.open_stream(
        provider=provider, model=model, max_tokens=32, messages=[Message.user("Count from 1 to 5.")]
    ) as stream:
        types = [e.type async for e in stream]
    assert types[0] == "message_start" and types[-1] == "message_end"
    assert "text_delta" in types
    assert stream.response is not None
    assert stream.response.usage.source == "provider", "stream usage not reported"


@pytest.mark.parametrize(("provider", "model"), _params())
async def test_live_tool_call_stream(sdk: ProviderSDK, provider: str, model: str) -> None:
    async with sdk.open_stream(
        provider=provider,
        model=model,
        tools=[TOOL],
        messages=[Message.user("What's the weather in Beijing? Use the tool.")],
    ) as stream:
        events = [e async for e in stream]
    ends = [e for e in events if e.type == "tool_call_end"]
    starts = [e for e in events if e.type == "tool_call_start"]
    assert len(starts) == len(ends) >= 1
    assert ends[0].tool_call.name == "get_weather"
    assert "city" in ends[0].tool_call.parsed_arguments()


@pytest.mark.parametrize(("provider", "model"), _params())
async def test_live_early_close(sdk: ProviderSDK, provider: str, model: str) -> None:
    async with sdk.open_stream(
        provider=provider, model=model, messages=[Message.user("Write a 300 word story.")]
    ) as stream:
        async for e in stream:
            if e.type == "text_delta":
                break
    assert stream.outcome == "closed_early"
    assert sdk.active_streams == 0
