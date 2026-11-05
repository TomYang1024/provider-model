"""启动真实的网关进程，端到端验证 SIGTERM 处理。

无需 Docker：``python -m provider_gateway`` 连接本地假 Provider，
并故意让 OTLP Collector 不可达。
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx2
import pytest

ROOT = Path(__file__).resolve().parents[2]
TOKEN = "e2e-token"


def _loopback_allowed() -> bool:
    server = socket.socket()
    try:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        client = socket.socket()
        try:
            client.connect(server.getsockname())
        finally:
            client.close()
        return True
    except PermissionError:
        return False
    finally:
        server.close()


pytestmark = pytest.mark.skipif(
    not _loopback_allowed(), reason="loopback TCP is not permitted in this environment"
)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_http(url: str, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx2.get(url, timeout=0.5, trust_env=False).status_code < 500:
                return
        except httpx2.HTTPError:
            pass
        time.sleep(0.1)
    raise TimeoutError(url)


@pytest.fixture
def fake_provider() -> Iterator[int]:
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "tests.e2e.fake_provider", "--port", str(port)],
        cwd=ROOT,
        env={**os.environ, "CHUNK_DELAY": "0.2", "CHUNKS": "100"},
    )
    try:
        wait_http(f"http://127.0.0.1:{port}/", timeout=15)
        yield port
    finally:
        proc.terminate()
        proc.wait(10)


def start_gateway(provider_port: int, grace: float) -> tuple[subprocess.Popen[str], int]:
    port = free_port()
    env = {
        "PATH": os.environ["PATH"],
        # 假 Provider 在本机回环地址上：忽略开发机上的任何代理设置
        "PROVIDER_SDK_TRUST_ENV": "false",
        "GATEWAY_PORT": str(port),
        "GATEWAY_HOST": "127.0.0.1",
        "GATEWAY_API_KEYS": TOKEN,
        "GATEWAY_SHUTDOWN_GRACE_SECONDS": str(grace),
        "GATEWAY_TELEMETRY_FLUSH_TIMEOUT": "2",
        "DEEPSEEK_API_KEY": "sk-e2e00000000000000000000",
        "DEEPSEEK_BASE_URL": f"http://127.0.0.1:{provider_port}",
        "OTEL_SERVICE_NAME": "provider-gateway-e2e",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:9",  # Collector 不可达
        "OTEL_EXPORTER_OTLP_TIMEOUT": "1",
        "OTEL_TRACES_SAMPLER": "always_on",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "provider_gateway"],
        cwd="/",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_http(f"http://127.0.0.1:{port}/health/live")
    return proc, port


def logs_of(proc: subprocess.Popen[str]) -> list[dict]:
    out = proc.stdout.read() if proc.stdout else ""
    records = []
    for line in out.splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            records.append({"event": "raw", "line": line})
    return records


def test_gateway_process_serves_and_shuts_down_gracefully(fake_provider: int) -> None:
    proc, port = start_gateway(fake_provider, grace=1)
    base = f"http://127.0.0.1:{port}"
    auth = {"authorization": f"Bearer {TOKEN}"}
    try:
        assert httpx2.get(f"{base}/health/ready", trust_env=False).status_code == 200
        r = httpx2.post(
            f"{base}/v1/chat",
            headers=auth,
            timeout=10,
            trust_env=False,
            json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "ping"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["message"]["content"] == "pong"  # collector down: still works

        received: list[str] = []
        done = threading.Event()

        def consume() -> None:
            try:
                with httpx2.stream(
                    "POST",
                    f"{base}/v1/stream",
                    headers=auth,
                    timeout=30,
                    trust_env=False,
                    json={
                        "provider": "deepseek",
                        "model": "deepseek-flash",
                        "messages": [{"role": "user", "content": "long"}],
                    },
                ) as resp:
                    for line in resp.iter_lines():
                        received.append(line)
            except httpx2.HTTPError:
                pass
            finally:
                done.set()

        t = threading.Thread(target=consume, daemon=True)
        t.start()
        deadline = time.monotonic() + 10
        while not any("text_delta" in x for x in received) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert any("text_delta" in x for x in received)

        t0 = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=20)
        elapsed = time.monotonic() - t0
    finally:
        if proc.poll() is None:
            proc.kill()
    assert code == 0
    # 停机等待（1 秒）+ 有上限的 Telemetry Flush（2 秒）+ 余量
    assert elapsed < 8, elapsed
    assert done.wait(5)
    assert not any("message_end" in x for x in received), "stream must not report success"

    records = logs_of(proc)
    events = [r.get("event") for r in records]
    assert "gateway.started" in events
    assert "llm.response" in events
    assert "llm.stream.cancelled" in events
    assert "gateway.telemetry_flushed" in events
    assert events.index("llm.stream.cancelled") < events.index("gateway.stopped")
    cancelled = next(r for r in records if r.get("event") == "llm.stream.cancelled")
    assert cancelled["status"] == "cancelled" and cancelled["partial_output"] is True
    assert cancelled["trace_id"] and cancelled["service"] == "provider-gateway-e2e"
    # 日志中绝不出现 API Key
    assert all("sk-e2e" not in json.dumps(r) for r in records)
    assert all(TOKEN not in json.dumps(r) for r in records)
