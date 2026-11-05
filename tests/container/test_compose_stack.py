"""容器测试：构建镜像并运行完整的 Compose 编排。

需显式运行：``pytest -m container``（需要运行中的 Docker 守护进程，约 1-2 分钟）。
使用假 Provider（tests/container/compose.test.yaml），不需要真实 API Key。
"""

from __future__ import annotations

import json
import shutil
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx2
import pytest

pytestmark = pytest.mark.container

ROOT = Path(__file__).resolve().parents[2]
FILES = ["-f", "compose.yaml", "-f", "tests/container/compose.test.yaml"]
PROJECT = ["-p", "provider-gateway-test"]
BASE = "http://127.0.0.1:18080"
JAEGER = "http://127.0.0.1:18686"
AUTH = {"authorization": "Bearer container-test-token"}
ENV = {"GATEWAY_PUBLISHED_PORT": "18080", "JAEGER_UI_PORT": "18686"}


def compose(
    *args: str, check: bool = True, timeout: float = 600
) -> subprocess.CompletedProcess[str]:
    import os

    return subprocess.run(
        ["docker", "compose", *PROJECT, *FILES, *args],
        cwd=ROOT,
        check=check,
        text=True,
        capture_output=True,
        timeout=timeout,
        env={**os.environ, **ENV},
    )


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


@pytest.fixture(scope="module")
def stack() -> Iterator[None]:
    if not docker_available():
        pytest.skip("docker daemon not available")
    compose("up", "-d", "--build", "--wait", "--wait-timeout", "120")
    try:
        yield
    finally:
        compose("down", "-v", "--remove-orphans", check=False)


def client() -> httpx2.Client:
    return httpx2.Client(base_url=BASE, timeout=30, trust_env=False)


def test_image_hardening(stack: None) -> None:
    uid = compose("exec", "-T", "provider-gateway", "id", "-u").stdout.strip()
    assert uid == "10001"
    ls = compose(
        "exec",
        "-T",
        "provider-gateway",
        "sh",
        "-c",
        "ls -a / /tmp; test -e /src && echo HAS_SRC; ls /opt/venv/bin | grep -E "
        "'^(pytest|ruff|mypy|uv)$' || true",
        check=False,
    ).stdout
    assert ".env" not in ls and "HAS_SRC" not in ls
    assert not any(t in ls.split() for t in ("pytest", "ruff", "mypy", "uv"))
    history = subprocess.run(
        ["docker", "history", "--no-trunc", "provider-gateway:1.0.0"],
        capture_output=True,
        text=True,
    ).stdout
    assert "sk-" not in history and "API_KEY=" not in history
    inspect = json.loads(
        subprocess.run(
            ["docker", "image", "inspect", "provider-gateway:1.0.0"], capture_output=True, text=True
        ).stdout
    )[0]
    assert inspect["Config"]["User"] == "10001:10001"
    assert inspect["Config"]["Cmd"] == ["python", "-m", "provider_gateway"]


def test_health_chat_and_stream(stack: None) -> None:
    with client() as c:
        assert c.get("/health/live").status_code == 200
        assert c.get("/health/ready").status_code == 200
        assert c.post("/v1/chat", json={}).status_code == 401
        r = c.post(
            "/v1/chat",
            headers=AUTH,
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["message"]["content"] == "pong"
        with c.stream(
            "POST",
            "/v1/stream",
            headers=AUTH,
            json={
                "provider": "deepseek",
                "model": "deepseek-flash",
                "messages": [{"role": "user", "content": "x"}],
            },
        ) as resp:
            assert resp.headers["content-type"].startswith("text/event-stream")
            for line in resp.iter_lines():
                if "text_delta" in line:
                    break  # client disconnects mid-stream


def test_traces_reach_jaeger_through_collector(stack: None) -> None:
    traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    with client() as c:
        r = c.post(
            "/v1/chat",
            headers={**AUTH, "traceparent": traceparent},
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
        )
        assert r.json()["trace"]["trace_id"] == "0af7651916cd43dd8448eb211c80319c"
    deadline = time.monotonic() + 60
    names: set[str] = set()
    while time.monotonic() < deadline:
        try:
            data = httpx2.get(
                f"{JAEGER}/api/traces/0af7651916cd43dd8448eb211c80319c", timeout=5, trust_env=False
            ).json()
            names = {s["operationName"] for t in data.get("data") or [] for s in t["spans"]}
            if {"POST /v1/chat", "chat deepseek-flash", "POST"} <= names:
                break
        except (httpx2.HTTPError, ValueError):
            pass
        time.sleep(2)
    assert {
        "POST /v1/chat",
        "chat deepseek-flash",
        "provider_sdk.adapter deepseek",
        "POST",
    } <= names, names


def test_gateway_survives_collector_outage(stack: None) -> None:
    compose("stop", "otel-collector")
    try:
        with client() as c:
            for _ in range(3):
                t0 = time.monotonic()
                r = c.post(
                    "/v1/chat",
                    headers=AUTH,
                    json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "hi"},
                )
                assert r.status_code == 200
                assert time.monotonic() - t0 < 2
            assert c.get("/health/ready").status_code == 200
    finally:
        compose("start", "otel-collector")


def test_logs_are_json_on_stdout(stack: None) -> None:
    logs = compose("logs", "--no-log-prefix", "provider-gateway").stdout
    records = [json.loads(line) for line in logs.splitlines() if line.startswith("{")]
    events = {r["event"] for r in records}
    assert {"gateway.started", "llm.request", "llm.response", "http.request"} <= events
    resp = next(r for r in records if r["event"] == "llm.response")
    assert resp["service"] == "provider-gateway" and resp["trace_id"]
    assert "sk-container-test" not in logs and "container-test-token" not in logs


def test_sigterm_graceful_shutdown_mid_stream(stack: None) -> None:
    received: list[str] = []

    def consume() -> None:
        try:
            with (
                client() as c,
                c.stream(
                    "POST",
                    "/v1/stream",
                    headers=AUTH,
                    json={
                        "provider": "deepseek",
                        "model": "deepseek-flash",
                        "messages": [{"role": "user", "content": "x"}],
                    },
                ) as resp,
            ):
                for line in resp.iter_lines():
                    received.append(line)
        except httpx2.HTTPError:
            pass

    t = threading.Thread(target=consume, daemon=True)
    t.start()
    deadline = time.monotonic() + 15
    while not any("text_delta" in x for x in received) and time.monotonic() < deadline:
        time.sleep(0.1)
    t0 = time.monotonic()
    compose("stop", "-t", "30", "provider-gateway")
    assert time.monotonic() - t0 < 15
    state = json.loads(
        subprocess.run(
            ["docker", "inspect", compose("ps", "-a", "-q", "provider-gateway").stdout.strip()],
            capture_output=True,
            text=True,
        ).stdout
    )[0]["State"]
    assert state["ExitCode"] == 0, state
    logs = compose("logs", "--no-log-prefix", "provider-gateway").stdout
    events = [json.loads(x)["event"] for x in logs.splitlines() if x.startswith("{")]
    assert "llm.stream.cancelled" in events
    assert events.index("llm.stream.cancelled") < events.index("gateway.stopped")
    assert "gateway.telemetry_flushed" in events
    compose("start", "provider-gateway")
