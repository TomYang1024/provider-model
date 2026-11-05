"""由应用自己初始化 OpenTelemetry 和 JSON 日志，SDK 自动继承应用的 Trace 上下文。

运行：OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 DEEPSEEK_API_KEY=... \
    python examples/with_telemetry.py
"""

import asyncio

import structlog

from provider_sdk import ProviderSDK, RequestContext
from provider_sdk.observability import configure_logging
from provider_sdk.observability.setup import configure_telemetry


async def main() -> None:
    logs = configure_logging(service="my-agent")  # JSON 输出到 stdout，有界队列
    telemetry = configure_telemetry("my-agent")  # BatchSpanProcessor + OTLP 导出
    tracer = telemetry.tracer
    try:
        async with ProviderSDK.from_env(
            tracer=tracer, meter=telemetry.meter, service_name="my-agent"
        ) as sdk:
            with tracer.start_as_current_span("agent.plan"):  # SDK Span 自动成为它的子 Span
                resp = await sdk.chat(
                    provider="deepseek",
                    model="deepseek-flash",
                    prompt="ping",
                    context=RequestContext(user_id="u-42", session_id="s-1"),
                )
                structlog.get_logger("app").info("agent.answer", chars=len(resp.text))
    finally:
        telemetry.shutdown(timeout_s=5)  # 有时间上限的 Flush
        logs.shutdown()


asyncio.run(main())
