from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

from provider_sdk import ProviderSDK, SDKConfig
from tests.conftest import Harness, completion


def test_from_env_keys_aliases_and_secret_files(tmp_path: Path) -> None:
    secret = tmp_path / "qwen_key"
    secret.write_text("sk-from-file\n")
    cfg = SDKConfig.from_env(
        {
            "DEEPSEEK_API_KEY": "sk-a",
            "ZHIPUAI_API_KEY": "zp-b",  # alias of GLM
            "MOONSHOT_API_KEY": "sk-c",  # alias of KIMI
            "QWEN_API_KEY_FILE": str(secret),  # Docker secret style
            "QWEN_BASE_URL": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
        }
    )
    assert {n for n, p in cfg.providers.items() if p.enabled} == {"deepseek", "glm", "kimi", "qwen"}
    assert cfg.providers["qwen"].api_key.get_secret_value() == "sk-from-file"
    assert cfg.providers["qwen"].base_url.startswith("https://dashscope-intl")
    assert "sk-a" not in repr(cfg)  # SecretStr


def test_providers_without_keys_are_disabled() -> None:
    cfg = SDKConfig.from_env({"DEEPSEEK_API_KEY": "sk-a"})
    assert cfg.providers["deepseek"].enabled and not cfg.providers["glm"].enabled


def test_toml_overlay(tmp_path: Path) -> None:
    f = tmp_path / "providers.toml"
    f.write_text("""
[logging]
mode = "redacted"

[providers.deepseek]
strict_models = true
max_retries = 1
[providers.deepseek.models."deepseek-flash"]
max_context_tokens = 64000

[providers.internal]
adapter = "openai_compatible"
base_url = "http://llm.internal/v1"
api_key_env = "INTERNAL_KEY"
""")
    cfg = SDKConfig.from_env({"DEEPSEEK_API_KEY": "k", "INTERNAL_KEY": "ik"}, config_file=f)
    ds = cfg.providers["deepseek"]
    assert ds.strict_models and ds.max_retries == 1
    assert ds.models["deepseek-flash"].max_context_tokens == 64000
    assert ds.models["deepseek-flash"].tools is True  # merged, not replaced
    assert "deepseek-v4-pro" in ds.models
    assert cfg.providers["internal"].enabled
    assert cfg.logging.mode == "redacted"


def test_toml_rejects_inline_api_key(tmp_path: Path) -> None:
    f = tmp_path / "bad.toml"
    f.write_text('[providers.deepseek]\napi_key = "sk-inline"\n')
    with pytest.raises(ValueError, match="must not be set in a config file"):
        SDKConfig.from_env({}, config_file=f)


def test_import_has_no_side_effects_and_no_gateway_dependency() -> None:
    code = (
        "import sys, provider_sdk\n"
        "from opentelemetry import trace, metrics\n"
        "assert type(trace.get_tracer_provider()).__name__ == 'ProxyTracerProvider'\n"
        "assert type(metrics.get_meter_provider()).__name__ == '_ProxyMeterProvider'\n"
        "bad = [m for m in sys.modules if m.split('.')[0] in "
        "('fastapi','starlette','uvicorn','provider_gateway','opentelemetry.sdk')]\n"
        "assert not bad, bad\n"
        "assert not [m for m in sys.modules if m.startswith('opentelemetry.sdk')]\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_sdk_core_does_not_import_gateway_modules() -> None:
    root = Path(__file__).resolve().parents[2] / "provider_sdk"
    for path in root.rglob("*.py"):
        text = path.read_text()
        for forbidden in ("import fastapi", "from fastapi", "provider_gateway", "import uvicorn"):
            assert forbidden not in text, f"{path} imports {forbidden}"


async def test_sdk_works_without_any_otel_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Global no-op tracer (trace export disabled) must not affect calls."""
    from tests.conftest import API_KEYS, MockProvider

    mock = MockProvider()
    mock.json(completion("ok"))
    cfg = SDKConfig.from_env(API_KEYS)
    sdk = ProviderSDK(cfg, transports={"deepseek": mock.transport()})
    resp = await sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
    assert resp.text == "ok" and resp.trace.trace_id is None
    await sdk.aclose()


async def test_unreachable_collector_does_not_fail_calls(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from provider_sdk.observability.setup import configure_telemetry

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")  # nothing listens
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1")
    handle = configure_telemetry(
        "test", set_global=False, schedule_delay_ms=50, export_timeout_ms=500
    )
    sdk = ProviderSDK(
        harness.sdk.config,
        tracer=handle.tracer,
        meter=handle.meter,
        transports={"deepseek": harness.mock.transport()},
    )
    for _ in range(3):
        harness.mock.json(completion("ok"))
    t0 = time.monotonic()
    for _ in range(3):
        assert (
            await sdk.chat(provider="deepseek", model="deepseek-flash", prompt="x")
        ).text == "ok"
    assert time.monotonic() - t0 < 1.0
    await sdk.aclose()
    t1 = time.monotonic()
    handle.shutdown(timeout_s=2)
    assert time.monotonic() - t1 < 2.5  # bounded flush + shutdown


def test_proxy_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    from provider_sdk.transport import resolve_proxy

    for var in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
        monkeypatch.delenv(var.upper(), raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://egress:3128")
    monkeypatch.setenv("NO_PROXY", "internal.example,localhost")
    url = "https://api.deepseek.com"
    assert resolve_proxy(url, proxy=None, trust_env=True) == "http://egress:3128"
    assert resolve_proxy("https://llm.internal.example/v1", proxy=None, trust_env=True) is None
    assert resolve_proxy(url, proxy=None, trust_env=False) is None
    assert resolve_proxy(url, proxy="http://explicit:1", trust_env=True) == "http://explicit:1"
    cfg = SDKConfig.from_env(
        {
            "DEEPSEEK_API_KEY": "k",
            "PROVIDER_SDK_DEEPSEEK_PROXY": "http://p:1",
            "PROVIDER_SDK_TRUST_ENV": "false",
        }
    )
    assert cfg.providers["deepseek"].proxy == "http://p:1"
    assert cfg.providers["deepseek"].trust_env is False
