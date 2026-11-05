"""网关测试：FastAPI 应用 + SDK + Mock 的 Provider HTTP（无网络）。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx2
import pytest
from opentelemetry.trace import SpanKind

from provider_gateway.app import Lifecycle, create_app
from provider_gateway.settings import GatewaySettings
from tests.conftest import Harness, chunk, completion

TOKEN = "gw-test-token"
AUTH = {"authorization": f"Bearer {TOKEN}"}


def parse_sse(text: str) -> list[tuple[str, dict[str, Any]]]:
    out = []
    for block in text.strip().split("\n\n"):
        lines = [ln for ln in block.splitlines() if not ln.startswith(":")]
        if not lines:
            continue
        event = next(ln[7:] for ln in lines if ln.startswith("event: "))
        data = json.loads(next(ln[6:] for ln in lines if ln.startswith("data: ")))
        out.append((event, data))
    return out


@asynccontextmanager
async def gateway(h: Harness, **settings: Any) -> AsyncIterator[tuple[httpx2.AsyncClient, Any]]:
    s = GatewaySettings(api_keys=TOKEN, sse_ping_interval=0, **settings)
    lifecycle = Lifecycle()
    app = create_app(
        s,
        sdk=h.sdk,
        lifecycle=lifecycle,
        setup_logging=False,
        setup_telemetry=False,
        tracer=h.tracer_provider.get_tracer("gw"),
    )
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://gw") as client:
            yield client, (app, lifecycle)


@pytest.fixture
def gw_harness(make_harness: Callable[..., Harness]) -> Harness:
    return make_harness()


async def test_health_endpoints_need_no_auth(gw_harness: Harness) -> None:
    async with gateway(gw_harness) as (client, (_app, lifecycle)):
        assert (await client.get("/health/live")).json() == {"status": "ok"}
        assert (await client.get("/health/ready")).status_code == 200
        lifecycle.start_draining()
        assert (await client.get("/health/ready")).status_code == 503
        assert (await client.get("/health/live")).status_code == 200


async def test_auth_required(gw_harness: Harness) -> None:
    async with gateway(gw_harness) as (client, _):
        r = await client.post(
            "/v1/chat", json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"}
        )
        assert r.status_code == 401
        assert r.json()["error"]["code"] == "unauthorized"
        r = await client.post(
            "/v1/chat",
            headers={"x-api-key": "wrong"},
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
        )
        assert r.status_code == 401


def test_gateway_refuses_to_start_without_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GATEWAY_API_KEYS", raising=False)
    with pytest.raises(ValueError, match="no gateway API keys"):
        GatewaySettings()


async def test_chat_and_messages_json(gw_harness: Harness) -> None:
    gw_harness.mock.json(completion("hello"))
    gw_harness.mock.json(completion("again"))
    async with gateway(gw_harness) as (client, _):
        r = await client.post(
            "/v1/chat",
            headers={**AUTH, "x-request-id": "req-xyz"},
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "prompt": "hi",
                "system": "be brief",
                "context": {"user_id": "u1"},
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["message"]["content"] == "hello"
        assert body["request_id"] == "req-xyz"
        assert r.headers["x-request-id"] == "req-xyz"
        assert body["usage"]["source"] == "provider"

        r = await client.post(
            "/v1/messages",
            headers=AUTH,
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert r.status_code == 200 and r.json()["message"]["content"] == "again"
    # Provider API Key 绝不会暴露给客户端
    assert "sk-deepseek" not in r.text


async def test_w3c_trace_context_is_continued(gw_harness: Harness) -> None:
    gw_harness.mock.json(completion())
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    async with gateway(gw_harness) as (client, _):
        r = await client.post(
            "/v1/chat",
            headers={**AUTH, "traceparent": f"00-{trace_id}-00f067aa0ba902b7-01"},
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
        )
    assert r.json()["trace"]["trace_id"] == trace_id
    server = gw_harness.span("POST /v1/chat")
    assert server.kind == SpanKind.SERVER
    assert format(server.context.trace_id, "032x") == trace_id
    sdk_span = gw_harness.span("chat deepseek-flash")
    assert sdk_span.parent.span_id == server.context.span_id


async def test_error_mapping_to_http(gw_harness: Harness) -> None:
    gw_harness.mock.error(429, {"error": {"message": "slow down"}})
    async with gateway(gw_harness) as (client, _):
        r = await client.post(
            "/v1/chat",
            headers=AUTH,
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
        )
        assert r.status_code == 429
        err = r.json()["error"]
        assert err["type"] == "rate_limit" and err["retryable"] is True
        assert err["provider"] == "deepseek" and err["request_id"]

        r = await client.post(
            "/v1/chat", headers=AUTH, json={"provider": "nope", "model": "x", "prompt": "hi"}
        )
        assert r.status_code == 404

        r = await client.post(
            "/v1/messages",
            headers=AUTH,
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "messages": [{"role": "tool", "content": "PROMPT-CONTENT"}],
            },
        )
        assert r.status_code == 422
        assert "PROMPT-CONTENT" not in r.text


async def test_stream_sse(gw_harness: Harness) -> None:
    gw_harness.mock.sse(
        [
            chunk({"content": "Hel"}),
            chunk({"content": "lo"}),
            chunk({}, finish_reason="stop"),
            chunk(no_choices=True, usage={"prompt_tokens": 1, "completion_tokens": 2}),
        ]
    )
    async with gateway(gw_harness) as (client, _):
        r = await client.post(
            "/v1/stream",
            headers=AUTH,
            json={
                "provider": "qwen",
                "model": "qwen-plus",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    events = parse_sse(r.text)
    assert [e for e, _ in events] == [
        "message_start",
        "text_delta",
        "text_delta",
        "usage",
        "message_end",
    ]
    assert events[-1][1]["response"]["message"]["content"] == "Hello"


async def test_stream_error_before_start_is_http_error(gw_harness: Harness) -> None:
    gw_harness.mock.error(401, {"error": {"message": "bad key"}})
    async with gateway(gw_harness) as (client, (_app, lifecycle)):
        r = await client.post(
            "/v1/stream",
            headers=AUTH,
            json={
                "provider": "qwen",
                "model": "qwen-plus",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert r.status_code == 502
        assert r.json()["error"]["type"] == "authentication"
        assert lifecycle.in_flight == 0


async def test_stream_mid_error_frame(gw_harness: Harness) -> None:
    gw_harness.mock.sse([chunk({"content": "x"}), {"error": {"message": "boom"}}], done=False)
    async with gateway(gw_harness) as (client, (_app, lifecycle)):
        r = await client.post(
            "/v1/stream",
            headers=AUTH,
            json={
                "provider": "qwen",
                "model": "qwen-plus",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        events = parse_sse(r.text)
        assert events[-1][0] == "error"
        assert events[-1][1]["error"]["retryable"] is False
        assert lifecycle.in_flight == 0


async def test_draining_rejects_new_requests(gw_harness: Harness) -> None:
    async with gateway(gw_harness) as (client, (_app, lifecycle)):
        lifecycle.start_draining()
        r = await client.post(
            "/v1/chat",
            headers=AUTH,
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
        )
        assert r.status_code == 503 and r.json()["error"]["code"] == "draining"


async def test_body_size_limit(gw_harness: Harness) -> None:
    async with gateway(gw_harness, max_body_bytes=100) as (client, _):
        r = await client.post(
            "/v1/chat",
            headers=AUTH,
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "x" * 500},
        )
        assert r.status_code == 413


async def test_timeout_is_capped(gw_harness: Harness) -> None:
    gw_harness.mock.slow_json(completion(), delay=5)
    async with gateway(gw_harness, max_request_timeout=0.2) as (client, _):
        r = await client.post(
            "/v1/chat",
            headers=AUTH,
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "prompt": "hi",
                "timeout": 999,
            },
        )
    assert r.status_code == 504
    assert r.json()["error"]["code"] == "deadline_exceeded"


async def test_client_disconnect_cancels_upstream_stream(gw_harness: Harness) -> None:
    """Raw ASGI: the client disconnects while the provider is silent."""
    gw_harness.mock.sse([chunk({"content": "first"}), chunk({"content": "never"})], hang_after=1)
    s = GatewaySettings(api_keys=TOKEN, sse_ping_interval=0)
    lifecycle = Lifecycle()
    app = create_app(
        s, sdk=gw_harness.sdk, lifecycle=lifecycle, setup_logging=False, setup_telemetry=False
    )
    body = json.dumps(
        {
            "provider": "deepseek",
            "model": "deepseek-flash",
            "messages": [{"role": "user", "content": "hi"}],
        }
    ).encode()
    got_delta = asyncio.Event()
    sent: list[dict[str, Any]] = []
    request_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        await got_delta.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)
        if b"text_delta" in message.get("body", b""):
            got_delta.set()

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/stream",
        "raw_path": b"/v1/stream",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (b"authorization", f"Bearer {TOKEN}".encode()),
            (b"content-type", b"application/json"),
        ],
        "client": ("127.0.0.1", 1),
        "server": ("gw", 80),
    }
    async with app.router.lifespan_context(app):
        await asyncio.wait_for(app(scope, receive, send), timeout=5)
        await asyncio.sleep(0.05)
        assert lifecycle.in_flight == 0
    assert gw_harness.mock.closed_streams == 1
    span = gw_harness.span("chat deepseek-flash")
    assert span.attributes["app.llm.outcome"] == "cancelled"
    assert gw_harness.logs.events("llm.stream.cancelled")


async def test_shutdown_closes_open_streams(gw_harness: Harness) -> None:
    gw_harness.mock.sse([chunk({"content": "a"})], hang_after=1)
    s = GatewaySettings(api_keys=TOKEN, sse_ping_interval=0)
    lifecycle = Lifecycle()
    app = create_app(
        s, sdk=gw_harness.sdk, lifecycle=lifecycle, setup_logging=False, setup_telemetry=False
    )
    async with app.router.lifespan_context(app):
        stream = gw_harness.sdk.stream(
            provider="deepseek", model="deepseek-flash", messages=[{"role": "user", "content": "x"}]
        )
        await stream.__anext__()
    assert stream.outcome == "cancelled"
    assert lifecycle.draining and not lifecycle.ready
    assert gw_harness.mock.closed_streams == 1


async def test_messages_with_tools_and_parallel_flag(gw_harness: Harness) -> None:
    gw_harness.mock.json(completion("ok"))
    async with gateway(gw_harness) as (client, _):
        r = await client.post(
            "/v1/messages",
            headers=AUTH,
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "messages": [{"role": "user", "content": "hi"}],
                "tools": [{"name": "get_weather", "parameters": {"type": "object"}}],
                "tool_choice": "auto",
                "parallel_tool_calls": True,
                "reasoning": None,
                "provider_options": {"user": "abc"},
            },
        )
    assert r.status_code == 200, r.text
    body = gw_harness.mock.body()
    assert body["parallel_tool_calls"] is True
    assert body["tool_choice"] == "auto"
    assert body["user"] == "abc"


async def test_provider_options_can_be_disabled(gw_harness: Harness) -> None:
    async with gateway(gw_harness, allow_provider_options=False) as (client, _):
        r = await client.post(
            "/v1/chat",
            headers=AUTH,
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "prompt": "hi",
                "provider_options": {"x": 1},
            },
        )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "provider_options_disabled"
