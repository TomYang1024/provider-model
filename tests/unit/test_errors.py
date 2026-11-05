from __future__ import annotations

import httpx2
import pytest

from provider_sdk import (
    AuthenticationError,
    ContextLengthExceededError,
    InvalidRequestError,
    ProviderConnectionError,
    ProviderServerError,
    ProviderTimeoutError,
    RateLimitError,
)
from tests.conftest import API_KEYS, Harness, completion

MODELS = {
    "deepseek": "deepseek-flash",
    "glm": "glm-4.6",
    "kimi": "kimi-k2.6",
    "qwen": "qwen-plus",
}


@pytest.mark.parametrize(
    ("provider", "status", "body", "cls", "code", "retryable"),
    [
        (
            "deepseek",
            401,
            {"error": {"message": "Authentication Fails"}},
            AuthenticationError,
            "authentication_failed",
            False,
        ),
        (
            "deepseek",
            402,
            {"error": {"message": "Insufficient Balance"}},
            RateLimitError,
            "insufficient_quota",
            False,
        ),
        (
            "deepseek",
            429,
            {"error": {"message": "Rate limit reached"}},
            RateLimitError,
            "rate_limited",
            True,
        ),
        (
            "deepseek",
            400,
            {"error": {"message": "This model's maximum context length is 65536"}},
            ContextLengthExceededError,
            "context_length_exceeded",
            False,
        ),
        (
            "deepseek",
            422,
            {"error": {"message": "Invalid temperature"}},
            InvalidRequestError,
            "invalid_request",
            False,
        ),
        (
            "deepseek",
            500,
            {"error": {"message": "internal"}},
            ProviderServerError,
            "provider_server_error",
            True,
        ),
        (
            "glm",
            429,
            {"error": {"code": "1113", "message": "余额不足或无可用资源包"}},
            RateLimitError,
            "insufficient_quota",
            False,
        ),
        (
            "glm",
            429,
            {"error": {"code": "1302", "message": "并发数过高"}},
            RateLimitError,
            "rate_limited",
            True,
        ),
        (
            "glm",
            400,
            {"error": {"code": "1301", "message": "系统检测到输入或生成内容可能包含不安全"}},
            InvalidRequestError,
            "content_filtered",
            False,
        ),
        (
            "glm",
            400,
            {"error": {"code": "1261", "message": "Prompt 超长"}},
            ContextLengthExceededError,
            "context_length_exceeded",
            False,
        ),
        (
            "kimi",
            429,
            {"error": {"type": "exceeded_current_quota_error", "message": "quota"}},
            RateLimitError,
            "insufficient_quota",
            False,
        ),
        (
            "kimi",
            400,
            {"error": {"type": "content_filter", "message": "high risk"}},
            InvalidRequestError,
            "content_filtered",
            False,
        ),
        (
            "kimi",
            400,
            {
                "error": {
                    "type": "invalid_request_error",
                    "message": "Invalid request: Your request exceeded model token limit: 262144",
                }
            },
            ContextLengthExceededError,
            "context_length_exceeded",
            False,
        ),
        (
            "qwen",
            400,
            {
                "error": {
                    "code": "Arrearage",
                    "message": "Access denied, account is in good standing?",
                }
            },
            RateLimitError,
            "insufficient_quota",
            False,
        ),
        (
            "qwen",
            400,
            {
                "error": {
                    "code": "data_inspection_failed",
                    "message": "Input data may contain inappropriate content.",
                }
            },
            InvalidRequestError,
            "content_filtered",
            False,
        ),
        (
            "qwen",
            401,
            {"error": {"code": "invalid_api_key", "message": "Incorrect API key"}},
            AuthenticationError,
            "authentication_failed",
            False,
        ),
    ],
)
async def test_status_and_business_error_mapping(
    harness: Harness,
    provider: str,
    status: int,
    body: dict,
    cls: type,
    code: str,
    retryable: bool,
) -> None:
    harness.mock.error(status, body)
    with pytest.raises(cls) as info:
        await harness.sdk.chat(provider=provider, model=MODELS[provider], prompt="x")
    err = info.value
    assert type(err) is cls
    assert err.error_code == code
    assert err.retryable is retryable
    assert err.provider == provider and err.model == MODELS[provider]
    assert err.status_code == status
    assert err.__cause__ is not None  # exception chain preserved


async def test_error_message_never_leaks_api_key(harness: Harness) -> None:
    key = API_KEYS["DEEPSEEK_API_KEY"]
    harness.mock.error(401, {"error": {"message": f"Bearer {key} is invalid; api_key={key}"}})
    with pytest.raises(AuthenticationError) as info:
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert key not in str(info.value)
    assert key not in repr(info.value)
    assert all(key not in str(r) for r in harness.logs.records)


async def test_transport_errors(harness: Harness) -> None:
    harness.mock.raise_(httpx2.ConnectError("refused"))
    with pytest.raises(ProviderConnectionError) as info:
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert info.value.retryable

    harness.mock.raise_(httpx2.ReadTimeout("slow"))
    with pytest.raises(ProviderTimeoutError):
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")


async def test_total_deadline_non_stream(harness: Harness) -> None:
    harness.mock.slow_json(completion(), delay=5)
    with pytest.raises(ProviderTimeoutError) as info:
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x", timeout=0.2)
    assert info.value.error_code == "deadline_exceeded"
    span = harness.span("chat deepseek-flash")
    assert span.attributes["app.llm.outcome"] == "timeout"
    assert harness.logs.events("llm.error")[-1]["error_type"] == "timeout"


async def test_empty_choices(harness: Harness) -> None:
    harness.mock.json(
        {"id": "x", "object": "chat.completion", "created": 1, "model": "m", "choices": []}
    )
    with pytest.raises(ProviderServerError) as info:
        await harness.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert info.value.error_code == "empty_response"


async def test_retries_are_bounded(make_harness) -> None:  # type: ignore[no-untyped-def]
    h = make_harness(retries=2)
    for _ in range(3):
        h.mock.error(
            503,
            {"error": {"message": "busy"}},
        )
    # openai 会带退避重试；通过 retry-after-ms 把等待时间压到最短
    h.mock.responses = [
        (
            lambda r: httpx2.Response(
                503, json={"error": {"message": "busy"}}, headers={"retry-after-ms": "1"}
            )
        )
        for _ in range(3)
    ]
    with pytest.raises(ProviderServerError):
        await h.sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert len(h.mock.requests) == 3  # 1 + max_retries, never unbounded
    http_spans = [s for s in h.finished_spans() if s.name == "POST"]
    assert len(http_spans) == 3
